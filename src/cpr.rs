//! CPR（Compact Position Reporting）座標算出の薄いラッパ。
//!
//! 数学は rs1090 のステートレス関数に委譲する。Odd/Even のキャッシュ自体は
//! `state` モジュールが保持し、ここでは「揃った 2 メッセージ」「surface + 参照
//! 座標」から絶対座標を求める純粋関数のみを提供する。

use rs1090::decode::bds::bds05::AirbornePosition;
use rs1090::decode::bds::bds06::SurfacePosition;
use rs1090::decode::cpr::{airborne_position, surface_position_with_reference};

pub use rs1090::decode::cpr::Position;

/// 空中位置を even/odd ペアからグローバル復号する。
///
/// 2 引数の Odd/Even 順序は rs1090 側で `parity` を見て判定されるため、
/// 呼び出し側はキャッシュ済みと新着をそのまま渡してよい。両方が同じ parity
/// の場合は `None`。
pub fn global_airborne(a: &AirbornePosition, b: &AirbornePosition) -> Option<Position> {
    airborne_position(a, b)
}

/// 地表面位置を参照座標（45NM 以内）からローカル復号する。
pub fn local_surface(msg: &SurfacePosition, ref_lat: f64, ref_lon: f64) -> Option<Position> {
    surface_position_with_reference(msg, ref_lat, ref_lon)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::{decode_frame, DecodedKind};
    use crate::domain::ModeSFrame;

    fn long(hex: &str) -> ModeSFrame {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let mut arr = [0u8; 14];
        arr.copy_from_slice(&bytes);
        ModeSFrame::Long(arr)
    }

    fn airborne(hex: &str) -> AirbornePosition {
        match decode_frame(&long(hex)).unwrap().kind {
            DecodedKind::Airborne { pos, .. } => pos,
            other => panic!("expected airborne, got {other:?}"),
        }
    }

    #[test]
    fn global_decode_matches_known_pair() {
        // rs1090 公式テストと同一の同一機体ペア。latest(=2 番目)の位置が返る。
        let m1 = airborne("8D40058B58C901375147EFD09357");
        let m2 = airborne("8D40058B58C904A87F402D3B8C59");
        let pos = global_airborne(&m1, &m2).expect("global decode");
        assert!((pos.latitude - 49.81755).abs() < 1e-3, "lat={}", pos.latitude);
        assert!((pos.longitude - 6.08442).abs() < 1e-3, "lon={}", pos.longitude);
    }

    #[test]
    fn same_parity_returns_none() {
        let m1 = airborne("8D40058B58C901375147EFD09357");
        assert!(global_airborne(&m1, &m1).is_none());
    }
}
