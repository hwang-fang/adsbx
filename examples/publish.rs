//! 疎通確認用 AMQP テストパブリッシャ。
//!
//! トポロジ（exchange `adsb` / queue `adsb_raw` / センサーコードでの routing key 束縛）を
//! 宣言し、既知の ADS-B even/odd ペア（＋TDOA 重複 1 件）を新フォーマットで publish する。
//!
//! メッセージ = `[sensor_id ASCII 4B][時刻 ASCII 14B(%Y%m%d%H%M%S)]` ヘッダ + 20B レコード列。
//! レコード = `[相対時刻 u32 LE(分内100ns)][payload 14B][波高値 u16 LE]`。
//!
//! 実行: `cargo run --example publish`

use chrono::{SecondsFormat, Utc};
use lapin::options::{
    BasicPublishOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};

const EXCHANGE: &str = "adsb";
const QUEUE: &str = "adsb_raw";
const SENSORS: [&str; 3] = ["AB01", "AB02", "AB03"];

/// dBm を波高値 u16 へエンコード（小数部 0）。
fn encode_signal(dbm: i32) -> u16 {
    (65535 - dbm.abs() * 256) as u16
}

/// 20B レコードを組み立てる。
fn record(rel_100ns: u32, payload_hex: &str, dbm: i32) -> Vec<u8> {
    let payload: Vec<u8> = (0..payload_hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&payload_hex[i..i + 2], 16).unwrap())
        .collect();
    let mut b = Vec::with_capacity(20);
    b.extend_from_slice(&rel_100ns.to_le_bytes());
    b.extend_from_slice(&payload);
    b.extend_from_slice(&encode_signal(dbm).to_le_bytes());
    b
}

/// メッセージ body = ヘッダ（sensor_id + 時刻）+ レコード列。
fn message(sensor_id: &str, header_ts: &str, records: &[Vec<u8>]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(sensor_id.as_bytes());
    b.extend_from_slice(header_ts.as_bytes());
    for r in records {
        b.extend_from_slice(r);
    }
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
        ch.queue_declare(
            QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
        for key in SENSORS {
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

    // 現在時刻の分頭からの相対オフセットでレコードを作る（絶対時刻 ≈ 現在＝壁時計）。
    let now = Utc::now();
    let header_ts = now.format("%Y%m%d%H%M%S").to_string();
    let rel_base = now.timestamp_subsec_nanos() / 100; // 秒内 100ns

    // 分頭からの相対時刻 = (秒 % 60)*1e7 + 秒内100ns。
    let sec_in_min = (now.timestamp().rem_euclid(60)) as u32;
    let rel = sec_in_min * 10_000_000 + rel_base;

    // (sensor, 相対時刻, rssi, payload)
    let msgs = [
        ("AB01", rel, -50, "8D40058B58C901375147EFD09357"), // even
        ("AB02", rel + 2_000_000, -55, "8D40058B58C904A87F402D3B8C59"), // odd
        ("AB03", rel + 300_000, -60, "8D40058B58C901375147EFD09357"), // even の TDOA 重複
    ];

    for (sensor, rel_ts, rssi, hex) in msgs {
        let body = message(sensor, &header_ts, &[record(rel_ts, hex, rssi)]);
        ch.basic_publish(
            EXCHANGE,
            sensor,
            BasicPublishOptions::default(),
            &body,
            BasicProperties::default(),
        )
        .await?
        .await?;
        println!("published sensor={sensor} rel={rel_ts} payload={hex}");
    }

    println!(
        "done (header_ts={}, wall={})",
        header_ts,
        now.to_rfc3339_opts(SecondsFormat::Millis, true)
    );
    Ok(())
}
