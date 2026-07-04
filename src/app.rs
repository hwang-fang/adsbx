//! モード別オーケストレーション（リアルタイム / 再計算）。
//!
//! 同期コア [`Engine`] を中心に、リアルタイムは「AMQP receiver タスク → Engine
//! 駆動ループ → DB writer タスク」の 2 チャネル構成、再計算はチャネルを使わない
//! 完全同期ループで駆動する（DESIGN §3）。

use crate::config::Config;
use crate::domain::{PositionRecord, RawSensorEvent};
use crate::engine::Engine;
use crate::metrics::Metrics;
use crate::time::{self, now_100ns, Ts100ns};
use crate::writer::{to_datetime, DbWriter};
use crate::receiver;
use anyhow::Result;
use chrono::{DateTime, Datelike, Duration as ChronoDuration, Timelike, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{error, info, warn};

const CHAN_CAP: usize = 4096;
const BATCH_SIZE: usize = 500;
const BATCH_INTERVAL: Duration = Duration::from_secs(1);
const METRICS_INTERVAL: Duration = Duration::from_secs(30);
/// DB 書き込みの有界リトライ（回数と間隔）。使い切ったらバッチを破棄する。
const FLUSH_ATTEMPTS: u32 = 3;
const FLUSH_RETRY_DELAY: Duration = Duration::from_millis(500);

/// リアルタイムモード。
pub async fn run_realtime(cfg: Config, metrics: Arc<Metrics>) -> Result<()> {
    let writer = DbWriter::connect(&cfg.db_url, metrics.clone()).await?;
    let mut engine = Engine::new(&cfg, metrics.clone(), vec![]);

    let (tx_in, mut rx_in) = mpsc::channel::<RawSensorEvent>(CHAN_CAP);
    let (tx_db, rx_db) = mpsc::channel::<PositionRecord>(CHAN_CAP);

    let receiver_task = {
        let cfg = cfg.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            if let Err(e) = receiver::run_amqp(&cfg, tx_in, metrics).await {
                error!("AMQP receiver stopped: {e:#}");
            }
        })
    };
    let writer_task = tokio::spawn(writer_consumer(writer, rx_db));
    let metrics_task = tokio::spawn(metrics_logger(metrics.clone()));

    // ドレイン用ウォーターマークはタイムアウト間隔で進める。
    let drain = Duration::from_millis(cfg.watermark_timeout_ms.max(100) as u64);
    let mut ticker = interval(drain);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    info!("realtime mode started");
    loop {
        tokio::select! {
            ev = rx_in.recv() => match ev {
                Some(ev) => {
                    for rec in engine.process(ev) {
                        let _ = tx_db.send(rec).await;
                    }
                }
                // receiver は再接続し続けるため通常ここには来ない（タスク異常終了時のみ）。
                None => break,
            },
            _ = ticker.tick() => {
                for rec in engine.advance_wallclock(now_100ns()) {
                    let _ = tx_db.send(rec).await;
                }
            }
            _ = &mut ctrl_c => {
                info!("ctrl-c received, shutting down");
                break;
            }
        }
    }

    // 入口を止め、残存ブロックを排出してから writer をドレインする。
    receiver_task.abort();
    for rec in engine.finish() {
        let _ = tx_db.send(rec).await;
    }
    drop(tx_db);
    let _ = writer_task.await;
    metrics_task.abort();
    info!("final metrics: {}", metrics.snapshot());
    Ok(())
}

/// Engine 出力をバッチ UPSERT する（500 件 or 1 秒）。
async fn writer_consumer(writer: DbWriter, mut rx: mpsc::Receiver<PositionRecord>) {
    let mut batch: Vec<PositionRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut ticker = interval(BATCH_INTERVAL);
    loop {
        tokio::select! {
            rec = rx.recv() => match rec {
                Some(rec) => {
                    batch.push(rec);
                    if batch.len() >= BATCH_SIZE {
                        flush(&writer, &mut batch).await;
                    }
                }
                None => {
                    flush(&writer, &mut batch).await;
                    break;
                }
            },
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    flush(&writer, &mut batch).await;
                }
            }
        }
    }
}

/// バッチを書き出す。一時的な DB 障害に備えて有界リトライし、使い切ったら破棄する
/// （UPSERT は冪等なので再送に安全。破棄は取りこぼし許容の設計に整合）。
async fn flush(writer: &DbWriter, batch: &mut Vec<PositionRecord>) {
    if batch.is_empty() {
        return;
    }
    for attempt in 1..=FLUSH_ATTEMPTS {
        match writer.upsert_batch(batch).await {
            Ok(()) => {
                batch.clear();
                return;
            }
            Err(e) if attempt < FLUSH_ATTEMPTS => {
                warn!(
                    "DB upsert failed (attempt {attempt}/{FLUSH_ATTEMPTS}, {} rows): {e:#}",
                    batch.len()
                );
                tokio::time::sleep(FLUSH_RETRY_DELAY).await;
            }
            Err(e) => {
                error!(
                    "DB upsert failed after {FLUSH_ATTEMPTS} attempts, dropping {} rows: {e:#}",
                    batch.len()
                );
            }
        }
    }
    batch.clear();
}

async fn metrics_logger(metrics: Arc<Metrics>) {
    let mut ticker = interval(METRICS_INTERVAL);
    ticker.tick().await; // 初回は即時発火するので捨てる
    loop {
        ticker.tick().await;
        info!("metrics: {}", metrics.snapshot());
    }
}

/// 再計算モード。チャネルを使わず Engine を同期駆動し、確定ウォーターマークが
/// 分終端を越えた分から順次 DELETE→INSERT する（ストリーミング書き込み、DESIGN §5）。
pub async fn run_recompute(cfg: Config, metrics: Arc<Metrics>) -> Result<()> {
    let writer = DbWriter::connect(&cfg.db_url, metrics.clone()).await?;
    let from = cfg.recompute_from.expect("validated");
    let to = cfg.recompute_to.expect("validated");
    let data_dir = PathBuf::from(cfg.data_dir.clone().expect("validated"));

    // フェーズ1: DB から状態復元。
    let window_start = from - ChronoDuration::seconds(cfg.restore_lookback_seconds as i64);
    let restore = writer.restore_states(window_start, from).await?;
    info!("phase1: restored {} aircraft states", restore.len());

    // 完全データ駆動（壁時計非依存）の同期コア。
    let mut engine = Engine::new(&cfg, metrics.clone(), restore);
    // 未書き込みの分バケット（ウォーターマーク遅延分のみ保持される）。
    let mut buckets: HashMap<i64, Vec<PositionRecord>> = HashMap::new();
    let mut next_write = from;

    // フェーズ2: 1 分前ファイルでメモリ状態を温める（出力は範囲外のため自然に MUTE）。
    let pre = from - ChronoDuration::minutes(1);
    let events = receiver::read_minute_file(&minute_path(&data_dir, pre), &metrics).await?;
    let n2 = events.len();
    for ev in events {
        collect(&mut buckets, engine.process(ev), from, to);
    }
    info!("phase2: pre-ran {} records from {}", n2, fmt_minute(pre));

    // フェーズ3: 対象レンジを 1 分ずつ投入し、確定した分から順次書き込む。
    let mut m = from;
    while m < to {
        let events = receiver::read_minute_file(&minute_path(&data_dir, m), &metrics).await?;
        let n = events.len();
        for ev in events {
            collect(&mut buckets, engine.process(ev), from, to);
        }
        info!("phase3: fed {} records from {}", n, fmt_minute(m));
        write_ready_minutes(&writer, &mut buckets, &mut next_write, to, engine.confirmed()).await?;
        m += ChronoDuration::minutes(1);
    }

    // 終端: 残存ブロックを排出し、未書き込みの全分を書く（欠損分の DELETE 含む）。
    collect(&mut buckets, engine.finish(), from, to);
    write_ready_minutes(&writer, &mut buckets, &mut next_write, to, Ts100ns::MAX).await?;

    info!("recompute done. metrics: {}", metrics.snapshot());
    Ok(())
}

/// Engine 出力のうち `[from, to)` の分だけをバケットへ蓄積する。
fn collect(
    buckets: &mut HashMap<i64, Vec<PositionRecord>>,
    records: Vec<PositionRecord>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) {
    for rec in records {
        let dt = to_datetime(rec.ts);
        if dt >= from && dt < to {
            buckets.entry(minute_key(dt)).or_default().push(rec);
        }
    }
}

/// 分終端が確定ウォーターマーク以下になった分を順次 DELETE→INSERT する。
/// ブロックは分境界を跨がないため、確定済みの分に後からレコードが増えることはない。
async fn write_ready_minutes(
    writer: &DbWriter,
    buckets: &mut HashMap<i64, Vec<PositionRecord>>,
    next_write: &mut DateTime<Utc>,
    to: DateTime<Utc>,
    confirmed: Ts100ns,
) -> Result<()> {
    while *next_write < to
        && time::from_datetime(*next_write + ChronoDuration::seconds(60)) <= confirmed
    {
        let rows = buckets.remove(&minute_key(*next_write)).unwrap_or_default();
        writer.recompute_minute(*next_write, &rows).await?;
        info!("wrote minute {} ({} rows)", fmt_minute(*next_write), rows.len());
        *next_write += ChronoDuration::minutes(1);
    }
    Ok(())
}

/// 分の一意キー（Unix 分）。
fn minute_key(dt: DateTime<Utc>) -> i64 {
    dt.timestamp().div_euclid(60)
}

fn minute_path(dir: &std::path::Path, dt: DateTime<Utc>) -> PathBuf {
    dir.join(format!("{}.bin", fmt_minute(dt)))
}

fn fmt_minute(dt: DateTime<Utc>) -> String {
    format!(
        "{:04}{:02}{:02}{:02}{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute()
    )
}
