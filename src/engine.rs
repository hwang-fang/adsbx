//! 同期処理コア（DESIGN §3）。
//!
//! aggregator → dedup → state → downsampler はいずれも I/O を持たない軽量処理の
//! ため、タスク分割せず単一の同期関数列に合成する。ウォーターマークはチャネル
//! メッセージではなく関数呼び出しで伝え、「イベント処理 → 確定処理」の順序は
//! コア内の逐次実行で保証する。並行処理は端（AMQP receiver / DB writer）のみ。

use crate::aggregator::WatermarkAggregator;
use crate::config::Config;
use crate::decode::DecodeReject;
use crate::dedup::Deduplicator;
use crate::domain::{PositionRecord, RawSensorEvent};
use crate::downsampler::Downsampler;
use crate::metrics::Metrics;
use crate::state::AircraftStateManager;
use crate::time::{Dur100ns, Ts100ns};
use std::sync::Arc;

pub struct Engine {
    agg: WatermarkAggregator,
    dedup: Deduplicator,
    state: AircraftStateManager,
    ds: Downsampler,
    metrics: Arc<Metrics>,
    /// 確定処理（purge/flush）を実行する最小のウォーターマーク前進幅（§3.2）。
    confirm_quantum: Dur100ns,
    /// 最後に確定処理を実行したウォーターマーク。この時刻以前のブロックは排出済み。
    confirmed: Ts100ns,
}

impl Engine {
    /// * `restore`: State Manager に事前シードする (icao, call_sign, squawk)（再計算フェーズ1）。
    pub fn new(
        cfg: &Config,
        metrics: Arc<Metrics>,
        restore: Vec<(u32, Option<String>, Option<String>)>,
    ) -> Self {
        let mut state = AircraftStateManager::new(cfg.debounce_n, cfg.surface_ref);
        for (icao, cs, sq) in restore {
            state.restore(icao, cs, sq);
        }
        let block = Dur100ns::from_ms(cfg.block_size_ms);
        let ttl = Dur100ns::from_ms(cfg.dedup_ttl_ms);
        Self {
            agg: WatermarkAggregator::new(Dur100ns::from_ms(cfg.watermark_timeout_ms)),
            dedup: Deduplicator::new(ttl),
            state,
            ds: Downsampler::new(block),
            metrics,
            confirm_quantum: Dur100ns(block.0.min(ttl.0).max(1)),
            confirmed: Ts100ns::MIN,
        }
    }

    /// 確定済みウォーターマーク。この時刻以前に終端を持つブロックはすべて排出済み
    /// （再計算のストリーミング書き込みが分終端との比較に使う）。
    pub fn confirmed(&self) -> Ts100ns {
        self.confirmed
    }

    /// 1 イベントを同期処理し、確定した行を返す。
    pub fn process(&mut self, ev: RawSensorEvent) -> Vec<PositionRecord> {
        // 既にウォーターマークを越えた過去のイベントは破棄（遅延到着）。
        if ev.ts < self.agg.watermark() {
            Metrics::incr(&self.metrics.dropped_late);
            return Vec::new();
        }
        let wm = self.agg.observe(ev.sensor_id, ev.ts);

        if self.dedup.accept(ev.frame, ev.ts) {
            match self.state.process(&ev) {
                Ok(Some(rec)) => {
                    Metrics::incr(&self.metrics.positions_emitted);
                    self.ds.ingest(rec);
                }
                Ok(None) => {}
                Err(reject) => self.count_reject(reject),
            }
        } else {
            Metrics::incr(&self.metrics.deduped_dropped);
        }

        // イベント処理の後に確定処理（旧パイプラインの「Event → Watermark 順」に対応）。
        match wm {
            Some(wm) => self.confirm(wm),
            None => Vec::new(),
        }
    }

    /// 壁時計フロンティアまでウォーターマークを進める（リアルタイムの定期ドレイン用）。
    pub fn advance_wallclock(&mut self, now: Ts100ns) -> Vec<PositionRecord> {
        match self.agg.advance_to(now) {
            Some(wm) => self.confirm(wm),
            None => Vec::new(),
        }
    }

    /// 入力終端。残存ブロックをすべて排出する。
    pub fn finish(&mut self) -> Vec<PositionRecord> {
        let mut out = self.confirm(Ts100ns::MAX);
        // flush(MAX) 後は空のはずだが、量子化やブロック境界の取りこぼしに対する念押し。
        out.extend(self.ds.drain_all());
        out
    }

    /// 確定処理。前回から `confirm_quantum` 以上前進したときのみ purge/flush を実行する
    /// （高レート時に保留全ブロック走査が毎イベント走るのを防ぐ。データ駆動なので決定的）。
    fn confirm(&mut self, wm: Ts100ns) -> Vec<PositionRecord> {
        if wm < self.confirmed.saturating_add(self.confirm_quantum) {
            return Vec::new();
        }
        self.confirmed = wm;
        self.dedup.purge(wm);
        self.ds.flush(wm)
    }

    fn count_reject(&self, reject: DecodeReject) {
        match reject {
            DecodeReject::CrcInvalid => Metrics::incr(&self.metrics.rejected_crc),
            DecodeReject::ParseError => Metrics::incr(&self.metrics.parse_error),
            DecodeReject::UnsupportedDf => Metrics::incr(&self.metrics.unsupported_df),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Mode;
    use crate::domain::{ModeSFrame, SensorId};
    use std::sync::atomic::Ordering;

    fn sid(s: &str) -> SensorId {
        SensorId::from_ascii(s.as_bytes()).unwrap()
    }

    fn test_config() -> Config {
        Config {
            mode: Mode::Recompute,
            sensors: Vec::new(),
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
            data_path_template: "%Y%m%d%H%M{sensor}.spkx".into(),
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
            sensor_id: sid("AB01"),
            ts: Ts100ns(ts),
            rssi_dbm: -50,
            frame: ModeSFrame::Long(arr),
        }
    }

    const EVEN: &str = "8D40058B58C901375147EFD09357";
    const ODD: &str = "8D40058B58C904A87F402D3B8C59";

    /// even/odd ペアを投入し、終端で丸め済みの位置レコードが 1 件出ることを検証する。
    /// コア合成・確定処理・終端ドレインを通しで確認する（旧パイプライン E2E の移植）。
    #[test]
    fn emits_rounded_position_after_finish() {
        let cfg = test_config();
        let metrics = Metrics::new();
        let mut e = Engine::new(&cfg, metrics.clone(), vec![]);

        // 同一機体・同一ブロック内（block 0 = [0, 10_000_000)）の even -> odd。
        // ウォーターマーク(2_000_000)がブロック終端に届かないため、この時点では出ない。
        assert!(e.process(ev(EVEN, 1_000_000)).is_empty());
        assert!(e.process(ev(ODD, 2_000_000)).is_empty());

        let records = e.finish();
        assert_eq!(records.len(), 1, "exactly one rounded position expected");
        let r = &records[0];
        assert_eq!(r.mode_s_code, 0x40058B);
        assert_eq!(r.ts, Ts100ns(0), "rounded to block boundary");
        assert!((r.lat - 49.81755).abs() < 1e-3, "lat={}", r.lat);
        assert!(!r.on_ground);
        assert_eq!(metrics.positions_emitted.load(Ordering::Relaxed), 1);
    }

    /// TDOA 重複（同一ビット列を別センサーが受信）が 1 件に排除されることを検証する。
    #[test]
    fn dedup_drops_spatial_duplicate() {
        let cfg = test_config();
        let metrics = Metrics::new();
        let mut e = Engine::new(&cfg, metrics.clone(), vec![]);

        let a = ev(EVEN, 1_000_000);
        let mut b = a; // 同一ビット列を別センサーが少し遅れて受信
        b.sensor_id = sid("AB02");
        b.ts = Ts100ns(1_200_000);
        e.process(a);
        e.process(b);
        e.finish();

        assert_eq!(metrics.deduped_dropped.load(Ordering::Relaxed), 1);
    }

    /// ウォーターマーク前進（量子化通過後）で、終端を待たずブロックが排出されること。
    #[test]
    fn watermark_advance_flushes_completed_block() {
        let cfg = test_config();
        let metrics = Metrics::new();
        let mut e = Engine::new(&cfg, metrics.clone(), vec![]);

        // block 0 内でペア確立（レコードは保留中）。
        e.process(ev(EVEN, 1_000_000));
        e.process(ev(ODD, 2_000_000));
        // 3 通目（ts=30_000_000）でウォーターマークが block 0 終端(10_000_000)を越え、
        // process の戻り値として block 0 のレコードが排出される。
        let out = e.process(ev(EVEN, 30_000_000));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ts, Ts100ns(0));

        // 3 通目自身は odd(2_000_000) と再ペアリングされ block 3 に入り、終端で出る。
        let rest = e.finish();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].ts, Ts100ns(30_000_000));
    }
}
