//! AMQP Receiver（リアルタイム）と再計算用ファイルリーダ。腐敗防止層 (`wire`) を介して
//! 生バイト列を [`RawSensorEvent`] へ変換する。
//!
//! AMQP は接続断時に指数バックオフで自動再接続する（DESIGN §4.0）。

use crate::config::Config;
use crate::domain::RawSensorEvent;
use crate::metrics::Metrics;
use crate::wire::{self, FrameReject, FILE_RECORD_LEN};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicQosOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties, ExchangeKind};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// AMQP トポロジ（暫定。実環境に合わせて差し替える）。
/// receiver は起動時にこれらを冪等に宣言・束縛する。
const EXCHANGE: &str = "adsb";
const QUEUE: &str = "adsb_raw";

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// 1 回の接続セッションの終わり方。
enum SessionEnd {
    /// パイプライン入口が閉じた（シャットダウン）。
    InputClosed,
    /// 接続確立後にストリームが終了した（接続断）。
    ConnectionLost,
}

/// AMQP からストリーム受信し、パイプライン入口へ投入する。接続断・接続失敗は
/// 指数バックオフ（1s→2s→…→30s 上限、確立成功でリセット）で自動再接続する。
/// `tx` がクローズされたときのみ戻る。
pub async fn run_amqp(
    cfg: &Config,
    tx: mpsc::Sender<RawSensorEvent>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let url = cfg
        .amqp_url
        .as_deref()
        .context("amqp_url required for realtime")?;

    let mut backoff = INITIAL_BACKOFF;
    loop {
        match consume_session(url, cfg, &tx, &metrics).await {
            Ok(SessionEnd::InputClosed) => return Ok(()),
            Ok(SessionEnd::ConnectionLost) => {
                warn!("AMQP connection lost");
                backoff = INITIAL_BACKOFF; // 確立に成功していたのでリセット
            }
            Err(e) => warn!("AMQP session failed: {e:#}"),
        }
        if tx.is_closed() {
            return Ok(());
        }
        Metrics::incr(&metrics.amqp_reconnects);
        info!("reconnecting to AMQP in {backoff:?}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// 1 回の接続セッション: 接続 → トポロジ宣言 → 消費ループ。
async fn consume_session(
    url: &str,
    cfg: &Config,
    tx: &mpsc::Sender<RawSensorEvent>,
    metrics: &Metrics,
) -> Result<SessionEnd> {
    let conn = Connection::connect(url, ConnectionProperties::default())
        .await
        .context("AMQP connect")?;
    let channel = conn.create_channel().await.context("AMQP channel")?;
    channel
        .basic_qos(cfg.prefetch, BasicQosOptions::default())
        .await
        .context("basic_qos")?;

    declare_topology(&channel, cfg).await?;

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
                        return Ok(SessionEnd::InputClosed);
                    }
                }
                Err(reject) => {
                    count_frame_reject(metrics, reject);
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                }
            },
            None => {
                Metrics::incr(&metrics.unknown_sensor);
                let _ = delivery.ack(BasicAckOptions::default()).await;
            }
        }
    }
    Ok(SessionEnd::ConnectionLost)
}

/// 起動時に AMQP トポロジを冪等に宣言する。
///
/// * exchange `adsb` (direct, durable) — bind の前提として宣言する。
/// * queue `adsb_raw` (durable) — 本 consumer の占有キュー。
/// * config の各 sensor routing key で queue を exchange に束縛する。
///
/// 宣言は冪等。既存と同一引数なら無害、引数が食い違う場合のみエラー（早期検知）。
async fn declare_topology(channel: &lapin::Channel, cfg: &Config) -> Result<()> {
    channel
        .exchange_declare(
            EXCHANGE,
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("exchange_declare")?;

    channel
        .queue_declare(
            QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("queue_declare")?;

    for routing_key in cfg.routing_to_sensor.keys() {
        channel
            .queue_bind(
                QUEUE,
                EXCHANGE,
                routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .with_context(|| format!("queue_bind {routing_key}"))?;
    }

    let mut keys: Vec<&String> = cfg.routing_to_sensor.keys().collect();
    keys.sort();
    info!("declared AMQP topology: exchange={EXCHANGE} queue={QUEUE} bindings={keys:?}");
    Ok(())
}

/// 1 分ファイル（生バイト列ログ）を読み、パース済みイベント列を返す。
/// ファイルが存在しない場合は警告して空を返す（欠損スキップ）。
pub async fn read_minute_file(path: &Path, metrics: &Metrics) -> Result<Vec<RawSensorEvent>> {
    let data = match tokio::fs::read(path).await {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!("recompute file missing, skipping: {}", path.display());
            return Ok(Vec::new());
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

    let mut events = Vec::with_capacity(data.len() / FILE_RECORD_LEN);
    for chunk in data.chunks_exact(FILE_RECORD_LEN) {
        match wire::parse_file_record(chunk) {
            Ok(ev) => events.push(ev),
            Err(reject) => count_frame_reject(metrics, reject),
        }
    }
    Ok(events)
}

fn count_frame_reject(metrics: &Metrics, reject: FrameReject) {
    match reject {
        FrameReject::MalformedShortFrame => Metrics::incr(&metrics.malformed_short_frame),
        FrameReject::BadLength => Metrics::incr(&metrics.parse_error),
    }
}
