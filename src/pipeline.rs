//! アクター結合。各パイプライン段を独立 Tokio タスクとして spawn し、`mpsc` で接続する。
//! ウォーターマークは [`Msg`] としてインバンド伝播する。

use crate::aggregator::WatermarkAggregator;
use crate::config::Config;
use crate::decode::DecodeReject;
use crate::dedup::Deduplicator;
use crate::domain::{Msg, PositionRecord, RawSensorEvent};
use crate::downsampler::Downsampler;
use crate::metrics::Metrics;
use crate::state::AircraftStateManager;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const CHAN_CAP: usize = 4096;

/// 現在時刻を Unix エポック起点 100ns で返す（リアルタイムのドレイン用）。
fn now_100ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0) / 100
}

/// 構築したパイプラインのハンドル。
pub struct Pipeline {
    /// 外部からイベントを投入する入口。
    pub input: mpsc::Sender<RawSensorEvent>,
    /// Downsampler が確定フラッシュした DB 行（ウォーターマーク制御付き）。
    pub output: mpsc::Receiver<Msg<PositionRecord>>,
    pub handles: Vec<JoinHandle<()>>,
}

/// 全タスクの終了を待つ（入口を drop した後に呼ぶ）。
pub async fn join(handles: Vec<JoinHandle<()>>) {
    for h in handles {
        let _ = h.await;
    }
}

/// パイプラインを spawn する。
///
/// * `restore`: State Manager に事前シードする (icao, call_sign, squawk)（再計算フェーズ1）。
/// * `drain_interval`: Some の場合、その間隔で壁時計ウォーターマークを進める（リアルタイム）。
///   None の場合は完全データ駆動（再計算の決定性）。
pub fn spawn(
    cfg: &Config,
    metrics: Arc<Metrics>,
    restore: Vec<(u32, Option<String>, Option<String>)>,
    drain_interval: Option<Duration>,
) -> Pipeline {
    let (tx_raw, rx_raw) = mpsc::channel::<RawSensorEvent>(CHAN_CAP);
    let (tx1, rx1) = mpsc::channel::<Msg<RawSensorEvent>>(CHAN_CAP);
    let (tx2, rx2) = mpsc::channel::<Msg<RawSensorEvent>>(CHAN_CAP);
    let (tx3, rx3) = mpsc::channel::<Msg<PositionRecord>>(CHAN_CAP);
    let (tx_out, rx_out) = mpsc::channel::<Msg<PositionRecord>>(CHAN_CAP);

    let mut handles = Vec::new();
    handles.push(tokio::spawn(aggregator_task(
        WatermarkAggregator::new(cfg.watermark_timeout_ms),
        rx_raw,
        tx1,
        metrics.clone(),
        drain_interval,
    )));
    handles.push(tokio::spawn(dedup_task(
        Deduplicator::new(cfg.dedup_ttl_ms),
        rx1,
        tx2,
        metrics.clone(),
    )));

    let mut state = AircraftStateManager::new(cfg.debounce_n, cfg.surface_ref);
    for (icao, cs, sq) in restore {
        state.restore(icao, cs, sq);
    }
    handles.push(tokio::spawn(state_task(state, rx2, tx3, metrics.clone())));
    handles.push(tokio::spawn(downsampler_task(
        Downsampler::new(cfg.block_size_ms),
        rx3,
        tx_out,
    )));

    Pipeline {
        input: tx_raw,
        output: rx_out,
        handles,
    }
}

async fn aggregator_task(
    mut agg: WatermarkAggregator,
    mut rx: mpsc::Receiver<RawSensorEvent>,
    tx: mpsc::Sender<Msg<RawSensorEvent>>,
    metrics: Arc<Metrics>,
    drain_interval: Option<Duration>,
) {
    // ドレイン用インターバル（リアルタイムのみ）。None の場合は永久に発火しない。
    let mut ticker = drain_interval.map(tokio::time::interval);

    loop {
        let maybe_ev = match &mut ticker {
            Some(t) => {
                tokio::select! {
                    ev = rx.recv() => ev,
                    _ = t.tick() => {
                        if let Some(wm) = agg.advance_to(now_100ns()) {
                            if tx.send(Msg::Watermark(wm)).await.is_err() { return; }
                        }
                        continue;
                    }
                }
            }
            None => rx.recv().await,
        };

        let Some(ev) = maybe_ev else { break };

        // 既にウォーターマークを越えた過去のイベントは破棄（遅延到着）。
        if ev.timestamp_100ns < agg.watermark() {
            Metrics::incr(&metrics.dropped_late);
            continue;
        }
        let wm = agg.observe(ev.sensor_id, ev.timestamp_100ns);
        if tx.send(Msg::Event(ev)).await.is_err() {
            return;
        }
        if let Some(wm) = wm {
            if tx.send(Msg::Watermark(wm)).await.is_err() {
                return;
            }
        }
    }
    // 入力終了: 残りを全フラッシュさせる最終ウォーターマーク。
    let _ = tx.send(Msg::Watermark(i64::MAX)).await;
}

async fn dedup_task(
    mut dedup: Deduplicator,
    mut rx: mpsc::Receiver<Msg<RawSensorEvent>>,
    tx: mpsc::Sender<Msg<RawSensorEvent>>,
    metrics: Arc<Metrics>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            Msg::Event(ev) => {
                if dedup.accept(ev.frame, ev.timestamp_100ns) {
                    if tx.send(Msg::Event(ev)).await.is_err() {
                        return;
                    }
                } else {
                    Metrics::incr(&metrics.deduped_dropped);
                }
            }
            Msg::Watermark(t) => {
                dedup.purge(t);
                if tx.send(Msg::Watermark(t)).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn state_task(
    mut state: AircraftStateManager,
    mut rx: mpsc::Receiver<Msg<RawSensorEvent>>,
    tx: mpsc::Sender<Msg<PositionRecord>>,
    metrics: Arc<Metrics>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            Msg::Event(ev) => match state.process(&ev) {
                Ok(Some(rec)) => {
                    Metrics::incr(&metrics.positions_emitted);
                    if tx.send(Msg::Event(rec)).await.is_err() {
                        return;
                    }
                }
                Ok(None) => {}
                Err(reject) => count_reject(&metrics, reject),
            },
            Msg::Watermark(t) => {
                if tx.send(Msg::Watermark(t)).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn downsampler_task(
    mut ds: Downsampler,
    mut rx: mpsc::Receiver<Msg<PositionRecord>>,
    tx: mpsc::Sender<Msg<PositionRecord>>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            Msg::Event(rec) => ds.ingest(rec),
            Msg::Watermark(t) => {
                for rec in ds.flush(t) {
                    if tx.send(Msg::Event(rec)).await.is_err() {
                        return;
                    }
                }
                if tx.send(Msg::Watermark(t)).await.is_err() {
                    return;
                }
            }
        }
    }
    // 念のため残存ブロックを全フラッシュ。
    for rec in ds.drain_all() {
        let _ = tx.send(Msg::Event(rec)).await;
    }
}

fn count_reject(metrics: &Metrics, reject: DecodeReject) {
    match reject {
        DecodeReject::CrcInvalid => Metrics::incr(&metrics.rejected_crc),
        DecodeReject::ParseError => Metrics::incr(&metrics.parse_error),
        DecodeReject::UnsupportedDf => Metrics::incr(&metrics.unsupported_df),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Mode;
    use crate::domain::ModeSFrame;
    use std::collections::HashMap;

    fn test_config() -> Config {
        Config {
            mode: Mode::Recompute,
            sensors: HashMap::new(),
            routing_to_sensor: HashMap::new(),
            amqp_url: None,
            db_url: String::new(),
            block_size_ms: 1000,
            dedup_ttl_ms: 50,
            watermark_timeout_ms: 1000,
            debounce_n: 3,
            surface_ref: None,
            recompute_from: None,
            recompute_to: None,
            restore_lookback_seconds: 0,
            data_dir: None,
            prefetch: 1000,
        }
    }

    fn ev(hex: &str, ts: i64) -> RawSensorEvent {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let mut arr = [0u8; 14];
        arr.copy_from_slice(&bytes);
        RawSensorEvent {
            sensor_id: 1,
            timestamp_100ns: ts,
            rssi_dbm: -50,
            frame: ModeSFrame::Long(arr),
        }
    }

    /// even/odd ペアを投入し、ドレイン後に丸め済みの位置レコードが 1 件出ることを検証する。
    /// アクター結合・インバンドウォーターマーク・終端ドレインを通しで確認する。
    #[tokio::test]
    async fn end_to_end_emits_rounded_position() {
        let cfg = test_config();
        let metrics = Metrics::new();
        let mut pl = spawn(&cfg, metrics.clone(), vec![], None);

        // 同一機体・同一ブロック内（block 0 = [0, 10_000_000)）の even -> odd。
        pl.input.send(ev("8D40058B58C901375147EFD09357", 1_000_000)).await.unwrap();
        pl.input.send(ev("8D40058B58C904A87F402D3B8C59", 2_000_000)).await.unwrap();

        // 入口を閉じてパイプラインをドレイン。
        drop(pl.input);

        let mut records = Vec::new();
        while let Some(msg) = pl.output.recv().await {
            if let Msg::Event(rec) = msg {
                records.push(rec);
            }
        }
        join(pl.handles).await;

        assert_eq!(records.len(), 1, "exactly one rounded position expected");
        let r = &records[0];
        assert_eq!(r.mode_s_code, 0x40058B);
        assert_eq!(r.timestamp_100ns, 0, "rounded to block boundary");
        assert!((r.lat - 49.81755).abs() < 1e-3, "lat={}", r.lat);
        assert!(!r.on_ground);
        assert_eq!(metrics.positions_emitted.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    /// TDOA 重複（同一ビット列を別センサーが受信）が 1 件に排除されることを検証する。
    #[tokio::test]
    async fn end_to_end_dedup_drops_spatial_duplicate() {
        let cfg = test_config();
        let metrics = Metrics::new();
        let mut pl = spawn(&cfg, metrics.clone(), vec![], None);

        let mut a = ev("8D40058B58C901375147EFD09357", 1_000_000);
        let mut b = a; // 同一ビット列を別センサーが少し遅れて受信
        b.sensor_id = 2;
        b.timestamp_100ns = 1_200_000;
        pl.input.send(a).await.unwrap();
        pl.input.send(b).await.unwrap();
        drop(pl.input);

        while pl.output.recv().await.is_some() {}
        join(pl.handles).await;

        // a の odd 相方が無いので位置は出ないが、重複排除は 1 件カウントされる。
        let _ = &mut a;
        assert_eq!(metrics.deduped_dropped.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
