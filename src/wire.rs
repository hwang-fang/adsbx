//! 腐敗防止層: MQ / ファイルの生バイト列を内部ドメインモデルへ変換する。
//!
//! バイナリフォーマット（レコード構造・ヘッダ・波高値エンコード・ファイル名規則）への
//! 依存はこのモジュールに完全に閉じ込める。後続パイプラインは [`RawSensorEvent`] /
//! [`ModeSFrame`] のみを扱う。
//!
//! フレーミング（リトルエンディアン）:
//!
//! * **固定長レコード（20B）** — MQ・ファイル共通:
//!   `[相対時刻: u32(分内 0~599_999_999, ×100ns)][payload: 14B][波高値: u16]`
//! * **MQ メッセージ**: `[sensor_id: ASCII 4B][時刻: ASCII 14B(%Y%m%d%H%M%S)]` = 18B の
//!   ヘッダに、20B レコードが N 個続く（1 秒分をまとめて配送）。絶対時刻 = ヘッダを分に
//!   切り捨てた分頭 + レコード相対時刻。
//! * **保存ファイル**: `{時刻:%Y%m%d%H%M}{sensor_id}.spkx`。中身はヘッダ無しの 20B レコード列。
//!   sensor_id・分頭はファイル名から得る。
//!
//! payload は常に 14 バイト。56bit フレームは末尾 56bit がゼロ埋めされている。

use crate::domain::{ModeSFrame, RawSensorEvent, SensorId};
use crate::time::{from_datetime, Ts100ns};
use chrono::{TimeZone, Utc};

pub const PAYLOAD_LEN: usize = 14;
/// 固定長レコード長（相対時刻 u32 + payload 14B + 波高値 u16）。
pub const RECORD_LEN: usize = 4 + PAYLOAD_LEN + 2; // 20
pub const SENSOR_ID_LEN: usize = 4;
pub const HEADER_TS_LEN: usize = 14;
/// MQ ヘッダ長（sensor_id 4B + 時刻 14B）。
pub const AMQP_HEADER_LEN: usize = SENSOR_ID_LEN + HEADER_TS_LEN; // 18

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameReject {
    /// レコード長が不正。
    BadLength,
    /// 短フレーム（DF<16）なのに末尾 56bit がゼロでない。
    MalformedShortFrame,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum HeaderReject {
    /// body がヘッダ長に満たない。
    BadLength,
    /// sensor_id が規約（英大文字2字+数字2字）外。
    BadSensorId,
    /// 時刻フィールドが %Y%m%d%H%M%S として解釈できない。
    BadTimestamp,
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

/// 波高値 u16 を dBm（-255 ~ 0 の整数）へデコードする。
///
/// 上位 8bit が絶対値整数部のビット反転、下位 8bit が絶対値小数部のビット反転。
/// `-1 * (65535 - value) / 256` で整数 dBm を得る（小数部は切り捨て）。
pub fn signal_dbm(value: u16) -> i16 {
    (-((65535 - value as i32) / 256)) as i16
}

/// 固定長レコード（20B）を絶対時刻付き [`RawSensorEvent`] へ変換する。
/// sensor_id と分頭は MQ ヘッダ／ファイル名から与えられる。
pub fn parse_record(
    sensor_id: SensorId,
    minute_start: Ts100ns,
    buf: &[u8],
) -> Result<RawSensorEvent, FrameReject> {
    if buf.len() != RECORD_LEN {
        return Err(FrameReject::BadLength);
    }
    let rel = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let payload: [u8; PAYLOAD_LEN] = buf[4..18].try_into().unwrap();
    let signal = u16::from_le_bytes(buf[18..20].try_into().unwrap());
    Ok(RawSensorEvent {
        sensor_id,
        ts: Ts100ns(minute_start.0 + rel as i64),
        rssi_dbm: signal_dbm(signal),
        frame: parse_frame(&payload)?,
    })
}

/// MQ メッセージのヘッダを解析し、`(sensor_id, 分頭, レコード列スライス)` を返す。
/// レコード列は呼び出し側が [`RECORD_LEN`] 単位で [`parse_record`] にかける。
pub fn parse_amqp_header(body: &[u8]) -> Result<(SensorId, Ts100ns, &[u8]), HeaderReject> {
    if body.len() < AMQP_HEADER_LEN {
        return Err(HeaderReject::BadLength);
    }
    let sensor_id =
        SensorId::from_ascii(&body[0..SENSOR_ID_LEN]).ok_or(HeaderReject::BadSensorId)?;
    let ts_ascii = std::str::from_utf8(&body[SENSOR_ID_LEN..AMQP_HEADER_LEN])
        .map_err(|_| HeaderReject::BadTimestamp)?;
    let minute_start = parse_minute(ts_ascii).ok_or(HeaderReject::BadTimestamp)?;
    Ok((sensor_id, minute_start, &body[AMQP_HEADER_LEN..]))
}

/// `%Y%m%d%H%M%S`（14 桁 ASCII）を UTC の**分頭**（秒切り捨て）へ変換する。
///
/// レコードの相対時刻が分内なので、ここでは分頭のみ必要。桁を固定幅で切り出して
/// 解析する（`%Y` の貪欲一致による連結文字列の誤読を避けるため）。
fn parse_minute(s: &str) -> Option<Ts100ns> {
    if s.len() != HEADER_TS_LEN || !s.is_ascii() {
        return None;
    }
    let year: i32 = s[0..4].parse().ok()?;
    let month: u32 = s[4..6].parse().ok()?;
    let day: u32 = s[6..8].parse().ok()?;
    let hour: u32 = s[8..10].parse().ok()?;
    let min: u32 = s[10..12].parse().ok()?;
    let sec: u32 = s[12..14].parse().ok()?;
    if sec >= 60 {
        return None;
    }
    // 秒は 0 に落として分頭にする。
    let dt = Utc.with_ymd_and_hms(year, month, day, hour, min, 0).single()?;
    Some(from_datetime(dt))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(s: &str) -> SensorId {
        SensorId::from_ascii(s.as_bytes()).unwrap()
    }

    /// 20B レコードを組み立てる（相対時刻・payload・波高値）。
    fn record(rel: u32, payload: &[u8; 14], signal: u16) -> Vec<u8> {
        let mut b = Vec::with_capacity(RECORD_LEN);
        b.extend_from_slice(&rel.to_le_bytes());
        b.extend_from_slice(payload);
        b.extend_from_slice(&signal.to_le_bytes());
        b
    }

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
    fn decodes_signal_dbm() {
        // -50 dBm: value = 65535 - 50*256 = 52735
        assert_eq!(signal_dbm(52735), -50);
        // 端点
        assert_eq!(signal_dbm(65535), 0);
        assert_eq!(signal_dbm(0), -255);
    }

    #[test]
    fn parses_record_with_absolute_time() {
        let minute = from_datetime(Utc.with_ymd_and_hms(2026, 6, 27, 12, 0, 0).unwrap());
        let mut payload = [0u8; 14];
        payload[0] = 0x8D;
        let rec = record(1_000_000, &payload, 52735);
        let ev = parse_record(sid("AB01"), minute, &rec).unwrap();
        assert_eq!(ev.sensor_id, sid("AB01"));
        assert_eq!(ev.ts, Ts100ns(minute.0 + 1_000_000));
        assert_eq!(ev.rssi_dbm, -50);
        assert!(matches!(ev.frame, ModeSFrame::Long(_)));
    }

    #[test]
    fn parses_amqp_header_and_records() {
        let mut payload = [0u8; 14];
        payload[0] = 0x8D;
        let mut body = Vec::new();
        body.extend_from_slice(b"AB01"); // sensor_id
        body.extend_from_slice(b"20260627120005"); // %Y%m%d%H%M%S（秒は分頭で捨てる）
        body.extend_from_slice(&record(2_000_000, &payload, 52735));
        body.extend_from_slice(&record(3_000_000, &payload, 52735));

        let (s, minute, records) = parse_amqp_header(&body).unwrap();
        assert_eq!(s, sid("AB01"));
        // 12:00:05 -> 分頭 12:00:00
        assert_eq!(
            minute,
            from_datetime(Utc.with_ymd_and_hms(2026, 6, 27, 12, 0, 0).unwrap())
        );
        assert_eq!(records.len(), 2 * RECORD_LEN);

        let evs: Vec<_> = records
            .chunks_exact(RECORD_LEN)
            .map(|c| parse_record(s, minute, c).unwrap())
            .collect();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].ts, Ts100ns(minute.0 + 2_000_000));
        assert_eq!(evs[1].ts, Ts100ns(minute.0 + 3_000_000));
    }

    #[test]
    fn rejects_bad_header() {
        assert_eq!(parse_amqp_header(&[0u8; 10]), Err(HeaderReject::BadLength));
        // 小文字センサー
        let mut body = Vec::from(&b"ab01"[..]);
        body.extend_from_slice(b"20260627120000");
        assert_eq!(parse_amqp_header(&body), Err(HeaderReject::BadSensorId));
        // 不正時刻
        let mut body = Vec::from(&b"AB01"[..]);
        body.extend_from_slice(b"20261327120000"); // 月=13
        assert_eq!(parse_amqp_header(&body), Err(HeaderReject::BadTimestamp));
    }

    #[test]
    fn rejects_bad_record_length() {
        let minute = Ts100ns(0);
        assert_eq!(
            parse_record(sid("AB01"), minute, &[0u8; 10]),
            Err(FrameReject::BadLength)
        );
    }
}
