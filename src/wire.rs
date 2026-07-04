//! 腐敗防止層: AMQP / ファイルの生バイト列を内部ドメインモデルへ変換する。
//!
//! バイナリフォーマットへの依存はこのモジュールに完全に閉じ込める。後続パイプラインは
//! [`RawSensorEvent`] / [`ModeSFrame`] のみを扱う。
//!
//! NOTE: AMQP の実バイナリ仕様は暫定。ここで定義する枠組みは差し替え前提であり、
//! 変更時の影響範囲をこのファイルに限定する。
//!
//! 暫定フレーミング（リトルエンディアン）:
//!
//! * AMQP body: `[timestamp_100ns: i64][rssi_dbm: i16][payload: 14B]` = 24 バイト。
//!   sensor_id は routing key から取得する。
//! * ファイル record: `[sensor_id: u16][timestamp_100ns: i64][rssi_dbm: i16][payload: 14B]` = 26 バイト。
//!   再計算時はセンサー識別子も保存する。
//!
//! payload は常に 14 バイト。56bit フレームは末尾 56bit がゼロ埋めされている。

use crate::domain::{ModeSFrame, RawSensorEvent};
use crate::time::Ts100ns;

pub const PAYLOAD_LEN: usize = 14;
pub const AMQP_BODY_LEN: usize = 8 + 2 + PAYLOAD_LEN; // 24
pub const FILE_RECORD_LEN: usize = 2 + 8 + 2 + PAYLOAD_LEN; // 26

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameReject {
    /// レコード長が不正。
    BadLength,
    /// 短フレーム（DF<16）なのに末尾 56bit がゼロでない。
    MalformedShortFrame,
}

/// 14 バイトのペイロードを DF 値で長さ判定し [`ModeSFrame`] へ変換する。
///
/// DF<16 を 56bit（短）、DF>=16 を 112bit（長）とする。短フレーム判定時は末尾 56bit
/// がゼロであることを整合性チェックする。
pub fn parse_frame(payload: &[u8; PAYLOAD_LEN]) -> Result<ModeSFrame, FrameReject> {
    let df = payload[0] >> 3;
    if df < 16 {
        // 末尾 7 バイト(56bit)はゼロ埋めのはず。
        if payload[7..].iter().any(|&b| b != 0) {
            return Err(FrameReject::MalformedShortFrame);
        }
        let mut short = [0u8; 7];
        short.copy_from_slice(&payload[..7]);
        Ok(ModeSFrame::Short(short))
    } else {
        Ok(ModeSFrame::Long(*payload))
    }
}

/// AMQP body をパースする。sensor_id は呼び出し側が routing key から与える。
pub fn parse_amqp_body(
    sensor_id: u16,
    body: &[u8],
) -> Result<RawSensorEvent, FrameReject> {
    if body.len() != AMQP_BODY_LEN {
        return Err(FrameReject::BadLength);
    }
    let ts = Ts100ns(i64::from_le_bytes(body[0..8].try_into().unwrap()));
    let rssi_dbm = i16::from_le_bytes(body[8..10].try_into().unwrap());
    let payload: [u8; PAYLOAD_LEN] = body[10..24].try_into().unwrap();
    Ok(RawSensorEvent {
        sensor_id,
        ts,
        rssi_dbm,
        frame: parse_frame(&payload)?,
    })
}

/// 再計算用ファイルの 1 レコードをパースする。
pub fn parse_file_record(buf: &[u8]) -> Result<RawSensorEvent, FrameReject> {
    if buf.len() != FILE_RECORD_LEN {
        return Err(FrameReject::BadLength);
    }
    let sensor_id = u16::from_le_bytes(buf[0..2].try_into().unwrap());
    let ts = Ts100ns(i64::from_le_bytes(buf[2..10].try_into().unwrap()));
    let rssi_dbm = i16::from_le_bytes(buf[10..12].try_into().unwrap());
    let payload: [u8; PAYLOAD_LEN] = buf[12..26].try_into().unwrap();
    Ok(RawSensorEvent {
        sensor_id,
        ts,
        rssi_dbm,
        frame: parse_frame(&payload)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_long_frame() {
        let mut p = [0u8; 14];
        p[0] = 0x8D; // DF=17
        assert!(matches!(parse_frame(&p), Ok(ModeSFrame::Long(_))));
    }

    #[test]
    fn parses_short_frame_with_zero_padding() {
        let mut p = [0u8; 14];
        p[0] = 0x28; // DF=5 (0x28>>3 = 5)
        p[1] = 0xAB;
        // bytes[7..] はゼロのまま
        match parse_frame(&p) {
            Ok(ModeSFrame::Short(s)) => assert_eq!(s[1], 0xAB),
            other => panic!("expected short, got {other:?}"),
        }
    }

    #[test]
    fn rejects_short_frame_with_nonzero_padding() {
        let mut p = [0u8; 14];
        p[0] = 0x20; // DF=4
        p[10] = 0x01; // 末尾 56bit に非ゼロ -> 不正
        assert_eq!(parse_frame(&p), Err(FrameReject::MalformedShortFrame));
    }

    #[test]
    fn amqp_and_file_roundtrip_lengths() {
        let mut body = Vec::new();
        body.extend_from_slice(&123_456_789i64.to_le_bytes());
        body.extend_from_slice(&(-60i16).to_le_bytes());
        let mut payload = [0u8; 14];
        payload[0] = 0x8D;
        body.extend_from_slice(&payload);
        assert_eq!(body.len(), AMQP_BODY_LEN);
        let ev = parse_amqp_body(7, &body).unwrap();
        assert_eq!(ev.sensor_id, 7);
        assert_eq!(ev.ts, Ts100ns(123_456_789));
        assert_eq!(ev.rssi_dbm, -60);

        let mut rec = Vec::new();
        rec.extend_from_slice(&9u16.to_le_bytes());
        rec.extend_from_slice(&body);
        assert_eq!(rec.len(), FILE_RECORD_LEN);
        let ev2 = parse_file_record(&rec).unwrap();
        assert_eq!(ev2.sensor_id, 9);
        assert_eq!(ev2.ts, Ts100ns(123_456_789));
    }

    #[test]
    fn rejects_bad_length() {
        assert_eq!(parse_amqp_body(1, &[0u8; 10]), Err(FrameReject::BadLength));
        assert_eq!(parse_file_record(&[0u8; 10]), Err(FrameReject::BadLength));
    }
}
