//! AMQP Receiver（リアルタイム）と再計算用ファイルリーダ。腐敗防止層 (`wire`) を介して
//! 生バイト列を [`RawSensorEvent`] へ変換し、パイプラインへ投入する。

use crate::config::Config;
use crate::metrics::Metrics;
use crate::wire::{self, FrameReject, FILE_RECORD_LEN};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

/// 消費するキュー名（AMQP トポロジは暫定。実環境に合わせて差し替える）。
const QUEUE: &str = "adsb_raw";

/// AMQP からストリーム受信し、パイプライン入口へ投入する。`tx` がクローズするか
/// 接続が切れるまでブロックする。
pub async fn run_amqp(
    cfg: &Config,
    tx: mpsc::Sender<crate::domain::RawSensorEvent>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let url = cfg
        .amqp_url
        .as_deref()
        .context("amqp_url required for realtime")?;
    let conn = Connection::connect(url, ConnectionProperties::default())
        .await
        .context("AMQP connect")?;
    let channel = conn.create_channel().await.context("AMQP channel")?;
    channel
        .basic_qos(cfg.prefetch, BasicQosOptions::default())
        .await
        .context("basic_qos")?;

    let mut consumer = channel
        .basic_consume(
            QUEUE,
            "adsbx",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("basic_consume")?;

    while let Some(delivery) = consumer.next().await {
        let delivery = match delivery {
            Ok(d) => d,
            Err(e) => {
                warn!("AMQP delivery error: {e}");
                continue;
            }
        };

        let routing_key = delivery.routing_key.as_str();
        match cfg.routing_to_sensor.get(routing_key) {
            Some(&sensor_id) => match wire::parse_amqp_body(sensor_id, &delivery.data) {
                Ok(ev) => {
                    // 変換・投入できた時点で ack（取りこぼし許容・重複は UPSERT が吸収）。
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                    if tx.send(ev).await.is_err() {
                        break;
                    }
                }
                Err(reject) => {
                    count_frame_reject(&metrics, reject);
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                }
            },
            None => {
                Metrics::incr(&metrics.unknown_sensor);
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    }
    Ok(())
}

/// 1 分ファイル（生バイト列ログ）を読み、各レコードをパイプライン入口へ投入する。
/// ファイルが存在しない場合は警告して 0 を返す（欠損スキップ）。
pub async fn replay_file(
    path: &Path,
    tx: &mpsc::Sender<crate::domain::RawSensorEvent>,
    metrics: &Metrics,
) -> Result<usize> {
    let data = match tokio::fs::read(path).await {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!("recompute file missing, skipping: {}", path.display());
            return Ok(0);
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };

    if data.len() % FILE_RECORD_LEN != 0 {
        warn!(
            "file {} length {} is not a multiple of record size {}",
            path.display(),
            data.len(),
            FILE_RECORD_LEN
        );
    }

    let mut count = 0;
    for chunk in data.chunks_exact(FILE_RECORD_LEN) {
        match wire::parse_file_record(chunk) {
            Ok(ev) => {
                if tx.send(ev).await.is_err() {
                    break;
                }
                count += 1;
            }
            Err(reject) => count_frame_reject(metrics, reject),
        }
    }
    Ok(count)
}

fn count_frame_reject(metrics: &Metrics, reject: FrameReject) {
    match reject {
        FrameReject::MalformedShortFrame => Metrics::incr(&metrics.malformed_short_frame),
        FrameReject::BadLength => Metrics::incr(&metrics.parse_error),
    }
}
