//! Mode-S / ADS-B フレームのデコード（I/O 非依存の純粋関数群）。
//!
//! rs1090 を「ステートレスな数学エンジン」として利用する。CPR の Odd/Even
//! ペアリング状態は持たず、ここでは 1 フレームを正規化された [`Decoded`] に変換
//! するだけ。状態保持・ペアリングは `state` モジュールの責務。

use crate::domain::ModeSFrame;
use rs1090::decode::adsb::{ADSB, ME};
use rs1090::decode::bds::bds05::AirbornePosition;
use rs1090::decode::bds::bds06::SurfacePosition;
use rs1090::decode::{Capability, Message, DF};

/// デコード不能・対象外として破棄する理由（可観測性カウンタに対応）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeReject {
    /// CRC（パリティ）不一致。
    CrcInvalid,
    /// ビット列のパース失敗。
    ParseError,
    /// 対象外の Downlink Format。
    UnsupportedDf,
}

/// 1 フレームから抽出した正規化結果。
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    /// ICAO 24bit アドレス。
    pub icao: u32,
    pub kind: DecodedKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecodedKind {
    /// 空中位置（CPR Odd/Even のいずれか片側）。`on_ground` は DF17 CA==4 由来。
    Airborne {
        pos: AirbornePosition,
        on_ground: bool,
    },
    /// 地表面位置（参照座標からローカル復号する）。
    Surface(SurfacePosition),
    /// 便名。
    Callsign(String),
    /// スコーク（4 桁 HEX 文字列）と緊急判定。
    Squawk { code: String, emergency: bool },
    /// 気圧高度（ft）。
    BaroAltitude(i32),
    /// 対象 DF だが本システムでは利用しないメッセージ（速度等）。
    Ignored,
}

/// 緊急スコーク。
const EMERGENCY_SQUAWKS: [&str; 3] = ["7500", "7600", "7700"];

/// スコーク（`IdentityCode` の内部値）を 4 桁 HEX 文字列へ整形する。
fn format_squawk(code: u16) -> String {
    format!("{code:04x}")
}

/// 便名の末尾パディング（空白・`_`）を除去する。
fn clean_callsign(raw: &str) -> String {
    raw.trim_end_matches([' ', '_']).to_string()
}

/// ADS-B (DF17/18) の ME フィールドを正規化する。
fn decode_me(icao: u32, me: &ME, on_ground_ca: bool) -> Decoded {
    let kind = match me {
        ME::BDS05 { inner, .. } => DecodedKind::Airborne {
            pos: *inner,
            on_ground: on_ground_ca,
        },
        ME::BDS06 { inner, .. } => DecodedKind::Surface(*inner),
        ME::BDS08 { inner, .. } => DecodedKind::Callsign(clean_callsign(&inner.callsign)),
        _ => DecodedKind::Ignored,
    };
    Decoded { icao, kind }
}

/// 1 フレームをデコードする。状態は持たない。
pub fn decode_frame(frame: &ModeSFrame) -> Result<Decoded, DecodeReject> {
    let bytes = frame.as_bytes();
    let message = Message::try_from(bytes).map_err(|e| classify_error(&e.to_string()))?;

    // DF4/5/20/21 では crc フィールドが AP パリティ overlay から復元した ICAO24。
    let icao_from_parity = message.crc;

    let decoded = match &message.df {
        DF::ExtendedSquitterADSB(ADSB {
            capability,
            icao24,
            message: me,
            ..
        }) => {
            let on_ground = *capability == Capability::AG_GROUND;
            decode_me(icao24.0, me, on_ground)
        }
        DF::ExtendedSquitterTisB { cf, .. } => {
            // DF18 は CA を持たない（CF）。on_ground は surface 由来のみ。
            decode_me(cf.aa.0, &cf.me, false)
        }
        DF::SurveillanceAltitudeReply { ac, .. } | DF::CommBAltitudeReply { ac, .. } => Decoded {
            icao: icao_from_parity,
            kind: DecodedKind::BaroAltitude(ac.0 as i32),
        },
        DF::SurveillanceIdentityReply { id, .. } | DF::CommBIdentityReply { id, .. } => {
            let code = format_squawk(id.0);
            let emergency = EMERGENCY_SQUAWKS.contains(&code.as_str());
            Decoded {
                icao: icao_from_parity,
                kind: DecodedKind::Squawk { code, emergency },
            }
        }
        _ => return Err(DecodeReject::UnsupportedDf),
    };
    Ok(decoded)
}

/// rs1090 のエラーメッセージから破棄理由を推定する。
fn classify_error(msg: &str) -> DecodeReject {
    if msg.contains("CRC") {
        DecodeReject::CrcInvalid
    } else {
        DecodeReject::ParseError
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn long(hex: &str) -> ModeSFrame {
        let bytes = hex_to_bytes(hex);
        let mut arr = [0u8; 14];
        arr.copy_from_slice(&bytes);
        ModeSFrame::Long(arr)
    }

    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn decodes_airborne_position() {
        // 既知の DF17 BDS05 (even) サンプル。
        let d = decode_frame(&long("8D40621D58C382D690C8AC2863A7")).unwrap();
        assert_eq!(d.icao, 0x40621D);
        match d.kind {
            DecodedKind::Airborne { on_ground, .. } => assert!(!on_ground),
            other => panic!("expected airborne, got {other:?}"),
        }
    }

    #[test]
    fn decodes_callsign() {
        // 既知の DF17 BDS08 identification サンプル -> "KLM1023".
        let d = decode_frame(&long("8D4840D6202CC371C32CE0576098")).unwrap();
        assert_eq!(d.icao, 0x4840D6);
        match d.kind {
            DecodedKind::Callsign(cs) => assert_eq!(cs, "KLM1023"),
            other => panic!("expected callsign, got {other:?}"),
        }
    }

    #[test]
    fn rejects_corrupted_adsb_with_crc() {
        // 末尾を壊して CRC 不一致にする。
        let d = decode_frame(&long("8D40621D58C382D690C8AC2863A0"));
        assert!(matches!(d, Err(DecodeReject::CrcInvalid)));
    }

    #[test]
    fn formats_emergency_squawk() {
        assert_eq!(format_squawk(0x7700), "7700");
        assert!(EMERGENCY_SQUAWKS.contains(&"7700"));
    }
}
