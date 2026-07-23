//! AMQP Receiver（リアルタイム）と再計算用ファイルリーダ。腐敗防止層 (`wire`) を介して
//! 生バイト列を [`RawSensorEvent`] へ変換する。
//!
//! AMQP は接続断時に指数バックオフで自動再接続する（DESIGN §4.0）。

use crate::config::Config;
use crate::domain::{RawSensorEvent, SensorId};
use crate::metrics::Metrics;
use crate::time::Ts100ns;
use crate::wire::{self, FrameReject, RECORD_LEN};
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

        // ヘッダを解析し sensor_id・分頭・レコード列を得る。
        let (sensor_id, minute_start, records) = match wire::parse_amqp_header(&delivery.data) {
            Ok(parts) => parts,
            Err(reject) => {
                warn!("malformed AMQP header: {reject:?}");
                Metrics::incr(&metrics.parse_error);
                let _ = delivery.ack(BasicAckOptions::default()).await;
                continue;
            }
        };
        if !cfg.sensors.contains(&sensor_id) {
            Metrics::incr(&metrics.unknown_sensor);
            let _ = delivery.ack(BasicAckOptions::default()).await;
            continue;
        }

        // ヘッダ受理時点で ack（取りこぼし許容・重複は UPSERT が吸収）。
        let _ = delivery.ack(BasicAckOptions::default()).await;

        let mut chunks = records.chunks_exact(RECORD_LEN);
        for chunk in &mut chunks {
            match wire::parse_record(sensor_id, minute_start, chunk) {
                Ok(ev) => {
                    if tx.send(ev).await.is_err() {
                        return Ok(SessionEnd::InputClosed);
                    }
                }
                Err(reject) => count_frame_reject(metrics, reject),
            }
        }
        if !chunks.remainder().is_empty() {
            warn!(
                "AMQP message from {sensor_id} has {} trailing bytes (not a multiple of {})",
                chunks.remainder().len(),
                RECORD_LEN
            );
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

    // routing key = センサーコード（sensor_id はメッセージヘッダから得るが、
    // 配送のため各センサーコードで束縛する）。
    for sensor in &cfg.sensors {
        let routing_key = sensor.as_str();
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

    let mut keys: Vec<String> = cfg.sensors.iter().map(|s| s.to_string()).collect();
    keys.sort();
    info!("declared AMQP topology: exchange={EXCHANGE} queue={QUEUE} bindings={keys:?}");
    Ok(())
}

/// センサー毎 1 分ファイル（`{分}{sensor_id}.spkx`）を読み、パース済みイベント列を返す。
/// sensor_id・分頭はファイル名由来なので呼び出し側が与える。
/// ファイルが存在しない場合は警告して空を返す（欠損スキップ）。
pub async fn read_sensor_minute_file(
    path: &Path,
    sensor_id: SensorId,
    minute_start: Ts100ns,
    metrics: &Metrics,
) -> Result<Vec<RawSensorEvent>> {
    let data = match tokio::fs::read(path).await {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!("recompute file missing, skipping: {}", path.display());
            return Ok(Vec::new());
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };

    if data.len() % RECORD_LEN != 0 {
        warn!(
            "file {} length {} is not a multiple of record size {}",
            path.display(),
            data.len(),
            RECORD_LEN
        );
    }

    let mut events = Vec::with_capacity(data.len() / RECORD_LEN);
    for chunk in data.chunks_exact(RECORD_LEN) {
        match wire::parse_record(sensor_id, minute_start, chunk) {
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
