//! モード別オーケストレーション（リアルタイム / 再計算）。

use crate::config::Config;
use crate::domain::{Msg, PositionRecord};
use crate::metrics::Metrics;
use crate::writer::{to_datetime, DbWriter};
use crate::{pipeline, receiver};
use anyhow::Result;
use chrono::{DateTime, Datelike, Duration as ChronoDuration, Timelike, Utc};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{error, info};

const BATCH_SIZE: usize = 500;
const BATCH_INTERVAL: Duration = Duration::from_secs(1);
const METRICS_INTERVAL: Duration = Duration::from_secs(30);

/// リアルタイムモード。
pub async fn run_realtime(cfg: Config, metrics: Arc<Metrics>) -> Result<()> {
    let writer = DbWriter::connect(&cfg.db_url, metrics.clone()).await?;

    // ドレイン用ウォーターマークはタイムアウト間隔で進める。
    let drain = Duration::from_millis(cfg.watermark_timeout_ms.max(100) as u64);
    let pl = pipeline::spawn(&cfg, metrics.clone(), vec![], Some(drain));
    let input = pl.input.clone();

    let writer_task = tokio::spawn(writer_consumer(writer, pl.output));
    let metrics_task = tokio::spawn(metrics_logger(metrics.clone()));

    info!("realtime mode started");
    tokio::select! {
        r = receiver::run_amqp(&cfg, input.clone(), metrics.clone()) => {
            if let Err(e) = r { error!("AMQP receiver stopped: {e:#}"); }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c received, shutting down");
        }
    }

    // 入口を閉じてパイプラインをドレインさせる。
    drop(input);
    drop(pl.input);
    pipeline::join(pl.handles).await;
    let _ = writer_task.await;
    metrics_task.abort();
    info!("final metrics: {}", metrics.snapshot());
    Ok(())
}

/// Downsampler 出力をバッチ UPSERT する（500 件 or 1 秒）。
async fn writer_consumer(writer: DbWriter, mut output: mpsc::Receiver<Msg<PositionRecord>>) {
    let mut batch: Vec<PositionRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut ticker = interval(BATCH_INTERVAL);
    loop {
        tokio::select! {
            msg = output.recv() => match msg {
                Some(Msg::Event(rec)) => {
                    batch.push(rec);
                    if batch.len() >= BATCH_SIZE {
                        flush(&writer, &mut batch).await;
                    }
                }
                Some(Msg::Watermark(_)) => {}
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

async fn flush(writer: &DbWriter, batch: &mut Vec<PositionRecord>) {
    if let Err(e) = writer.upsert_batch(batch).await {
        error!("DB upsert failed ({} rows): {e:#}", batch.len());
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

/// 再計算モード。
pub async fn run_recompute(cfg: Config, metrics: Arc<Metrics>) -> Result<()> {
    let writer = DbWriter::connect(&cfg.db_url, metrics.clone()).await?;
    let from = cfg.recompute_from.expect("validated");
    let to = cfg.recompute_to.expect("validated");
    let data_dir = PathBuf::from(cfg.data_dir.clone().expect("validated"));

    // フェーズ1: DB から状態復元。
    let window_start = from - ChronoDuration::seconds(cfg.restore_lookback_seconds as i64);
    let restore = writer.restore_states(window_start, from).await?;
    info!("phase1: restored {} aircraft states", restore.len());

    // 完全データ駆動（壁時計非依存）でパイプラインを構築。
    let pl = pipeline::spawn(&cfg, metrics.clone(), restore, None);
    let input = pl.input;

    // 出力収集タスク: [from, to) の分のレコードのみを分単位で蓄積（フェーズ2は分が範囲外
    // のため自然に MUTE される）。
    let collector = tokio::spawn(collect_minutes(pl.output, from, to));

    // フェーズ2: 1 分前ファイルでメモリ状態を温める。
    let pre = from - ChronoDuration::minutes(1);
    let n2 = receiver::replay_file(&minute_path(&data_dir, pre), &input, &metrics).await?;
    info!("phase2: pre-ran {} records from {}", n2, fmt_minute(pre));

    // フェーズ3: 対象レンジを 1 分ずつ投入。
    let mut m = from;
    while m < to {
        let n = receiver::replay_file(&minute_path(&data_dir, m), &input, &metrics).await?;
        info!("phase3: fed {} records from {}", n, fmt_minute(m));
        m += ChronoDuration::minutes(1);
    }

    drop(input);
    pipeline::join(pl.handles).await;
    let buckets = collector.await?;

    // 各分を独立トランザクションで DELETE -> INSERT（レコード無しの分も DELETE）。
    let mut m = from;
    while m < to {
        let key = minute_key(m);
        let rows = buckets.get(&key).cloned().unwrap_or_default();
        writer.recompute_minute(m, &rows).await?;
        info!("wrote minute {} ({} rows)", fmt_minute(m), rows.len());
        m += ChronoDuration::minutes(1);
    }

    info!("recompute done. metrics: {}", metrics.snapshot());
    Ok(())
}

/// パイプライン出力を分単位（`[from, to)` のみ）に蓄積する。
async fn collect_minutes(
    mut output: mpsc::Receiver<Msg<PositionRecord>>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> HashMap<i64, Vec<PositionRecord>> {
    let mut buckets: HashMap<i64, Vec<PositionRecord>> = HashMap::new();
    while let Some(msg) = output.recv().await {
        if let Msg::Event(rec) = msg {
            let dt = to_datetime(rec.timestamp_100ns);
            if dt >= from && dt < to {
                buckets.entry(minute_key(dt)).or_default().push(rec);
            }
        }
    }
    buckets
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
