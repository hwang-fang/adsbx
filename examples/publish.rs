//! 疎通確認用 AMQP テストパブリッシャ。
//!
//! トポロジ（exchange `adsb` / queue `adsb_raw` / routing key 束縛）を宣言し、
//! 既知の ADS-B even/odd ペア（＋TDOA 重複 1 件）を `wire` 暫定フォーマット
//! （`[ts i64 LE][rssi i16 LE][payload 14B]` = 24B）で publish する。
//!
//! 実行: `cargo run --example publish`

use lapin::options::{
    BasicPublishOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};
use std::time::{SystemTime, UNIX_EPOCH};

const EXCHANGE: &str = "adsb";
const QUEUE: &str = "adsb_raw";

fn now_100ns() -> i64 {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    (d.as_nanos() / 100) as i64
}

fn body(ts_100ns: i64, rssi: i16, payload_hex: &str) -> Vec<u8> {
    let payload: Vec<u8> = (0..payload_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&payload_hex[i..i + 2], 16).unwrap())
        .collect();
    let mut b = Vec::with_capacity(24);
    b.extend_from_slice(&ts_100ns.to_le_bytes());
    b.extend_from_slice(&rssi.to_le_bytes());
    b.extend_from_slice(&payload);
    b
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("AMQP_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into());
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;

    // DELETE_ONLY=1 のときはトポロジを削除して終了（アプリ単体起動の検証用）。
    if std::env::var("DELETE_ONLY").is_ok() {
        let _ = ch.queue_delete(QUEUE, QueueDeleteOptions::default()).await;
        let _ = ch
            .exchange_delete(EXCHANGE, ExchangeDeleteOptions::default())
            .await;
        println!("deleted topology");
        return Ok(());
    }

    // SKIP_TOPOLOGY=1 のときは publish のみ（稼働中の consumer を壊さないため）。
    if std::env::var("SKIP_TOPOLOGY").is_err() {
        // 既存トポロジ（過去の宣言と durable 設定が食い違う場合がある）を作り直す。
        let _ = ch.queue_delete(QUEUE, QueueDeleteOptions::default()).await;
        let _ = ch
            .exchange_delete(EXCHANGE, ExchangeDeleteOptions::default())
            .await;

        ch.exchange_declare(
            EXCHANGE,
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
        // 新しめの RabbitMQ は transient な非排他キューを拒否するため durable で宣言する。
        ch.queue_declare(
            QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
        for key in ["a", "b", "c"] {
            ch.queue_bind(
                QUEUE,
                EXCHANGE,
                key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
        }
    }

    let t = now_100ns();
    // (routing key, timestamp, rssi, payload)
    let msgs = [
        ("a", t, -50i16, "8D40058B58C901375147EFD09357"), // even (sensor 1)
        ("b", t + 2_000_000, -55, "8D40058B58C904A87F402D3B8C59"), // odd  (sensor 2)
        ("c", t + 300_000, -60, "8D40058B58C901375147EFD09357"), // even の TDOA 重複 (sensor 3)
    ];

    for (key, ts, rssi, hex) in msgs {
        ch.basic_publish(
            EXCHANGE,
            key,
            BasicPublishOptions::default(),
            &body(ts, rssi, hex),
            BasicProperties::default(),
        )
        .await?
        .await?;
        println!("published key={key} ts={ts} payload={hex}");
    }

    println!("done");
    Ok(())
}
