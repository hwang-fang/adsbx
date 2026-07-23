//! Aircraft State Manager。
//!
//! CPR Odd/Even のペアリング（自作キャッシュ＋ rs1090 ステートレス関数）と、
//! メタデータ（便名・スコーク・高度）の状態マージを行う。位置が確定した瞬間にのみ
//! [`PositionRecord`] を生成し、最新メタデータをスナップショットとして焼き込む。

use crate::cpr::{global_airborne, local_surface};
use crate::decode::{decode_frame, DecodeReject, DecodedKind};
use crate::domain::{PositionRecord, RawSensorEvent};
use crate::time::{Dur100ns, Ts100ns};
use rs1090::decode::bds::bds05::AirbornePosition;
use rs1090::decode::cpr::CPRFormat;
use std::collections::HashMap;

/// Odd/Even のペアリングを有効とみなす最大時間差（10 秒）。
const PAIR_WINDOW: Dur100ns = Dur100ns(10 * 10_000_000);

/// N-Strike デバウンス対象フィールドの状態。
#[derive(Default, Clone)]
struct Debounced {
    confirmed: Option<String>,
    candidate: Option<(String, u32)>,
}

impl Debounced {
    /// 新しい観測値を適用する。確定したら `confirmed` を更新する。
    fn observe(&mut self, value: String, n: u32) {
        if self.confirmed.as_deref() == Some(value.as_str()) {
            // 確定値と同じ再受信はカウンタに無関係（無視）。
            self.candidate = None;
            return;
        }
        let count = match &self.candidate {
            Some((c, k)) if *c == value => k + 1,
            _ => 1,
        };
        if count >= n {
            self.confirmed = Some(value);
            self.candidate = None;
        } else {
            self.candidate = Some((value, count));
        }
    }

    /// 閾値を無視して即時確定する（緊急スコーク用）。
    fn force(&mut self, value: String) {
        self.confirmed = Some(value);
        self.candidate = None;
    }
}

#[derive(Default)]
struct AircraftState {
    even: Option<(Ts100ns, AirbornePosition)>,
    odd: Option<(Ts100ns, AirbornePosition)>,
    call_sign: Debounced,
    squawk: Debounced,
    alt: Option<i32>,
}

pub struct AircraftStateManager {
    aircraft: HashMap<u32, AircraftState>,
    debounce_n: u32,
    surface_ref: Option<(f64, f64)>,
}

impl AircraftStateManager {
    pub fn new(debounce_n: u32, surface_ref: Option<(f64, f64)>) -> Self {
        Self {
            aircraft: HashMap::new(),
            debounce_n: debounce_n.max(1),
            surface_ref,
        }
    }

    /// 再計算フェーズ1: DB から復元した確定メタデータでシードする。
    pub fn restore(&mut self, icao: u32, call_sign: Option<String>, squawk: Option<String>) {
        let st = self.aircraft.entry(icao).or_default();
        st.call_sign.confirmed = call_sign;
        st.squawk.confirmed = squawk;
    }

    /// 1 イベントを処理する。位置が確定した場合のみ [`PositionRecord`] を返す。
    /// デコード不能なら破棄理由を `Err` で返す（呼び出し側がカウンタ計上）。
    pub fn process(&mut self, ev: &RawSensorEvent) -> Result<Option<PositionRecord>, DecodeReject> {
        let decoded = decode_frame(&ev.frame)?;
        let icao = decoded.icao;
        let n = self.debounce_n;
        let st = self.aircraft.entry(icao).or_default();

        let record = match decoded.kind {
            DecodedKind::Callsign(cs) => {
                st.call_sign.observe(cs, n);
                None
            }
            DecodedKind::Squawk { code, emergency } => {
                if emergency {
                    st.squawk.force(code);
                } else {
                    st.squawk.observe(code, n);
                }
                None
            }
            DecodedKind::BaroAltitude(alt) => {
                // 連続変化量のためデバウンスせず即時上書き（Last-Write-Wins）。
                st.alt = Some(alt);
                None
            }
            DecodedKind::Airborne { pos, on_ground } => {
                Self::handle_airborne(st, icao, ev.ts, pos, on_ground)
            }
            DecodedKind::Surface(sp) => match self.surface_ref {
                Some((ref_lat, ref_lon)) => {
                    local_surface(&sp, ref_lat, ref_lon).map(|p| PositionRecord {
                        mode_s_code: icao,
                        ts: ev.ts,
                        lat: p.latitude,
                        lon: p.longitude,
                        alt: None, // 地上に気圧高度は無い
                        call_sign: st.call_sign.confirmed.clone(),
                        squawk: st.squawk.confirmed.clone(),
                        on_ground: true,
                    })
                }
                None => None, // 参照座標未設定では地上位置を復号できない
            },
            DecodedKind::Ignored => None,
        };
        Ok(record)
    }

    fn handle_airborne(
        st: &mut AircraftState,
        icao: u32,
        ts: Ts100ns,
        pos: AirbornePosition,
        on_ground: bool,
    ) -> Option<PositionRecord> {
        // 受信した parity をキャッシュへ。
        let other = match pos.parity {
            CPRFormat::Even => {
                st.even = Some((ts, pos));
                st.odd
            }
            CPRFormat::Odd => {
                st.odd = Some((ts, pos));
                st.even
            }
        };

        // 反対 parity が新鮮なペアとして揃っていればグローバル復号。
        let (other_ts, other_pos) = other?;
        if ts.abs_delta(other_ts) > PAIR_WINDOW {
            return None;
        }
        // rs1090 は第 2 引数を「最新」とみなしその位置を返す。現イベント `pos` を
        // 最新として渡すことで、ev.ts 時点の座標を得る。
        let position = global_airborne(&other_pos, &pos)?;

        Some(PositionRecord {
            mode_s_code: icao,
            ts,
            lat: position.latitude,
            lon: position.longitude,
            alt: pos.alt.map(|a| a as i32).or(st.alt),
            call_sign: st.call_sign.confirmed.clone(),
            squawk: st.squawk.confirmed.clone(),
            on_ground,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ModeSFrame, SensorId};

    fn ev(hex: &str, ts: i64) -> RawSensorEvent {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        let mut arr = [0u8; 14];
        arr.copy_from_slice(&bytes);
        RawSensorEvent {
            sensor_id: SensorId::from_ascii(b"AB01").unwrap(),
            ts: Ts100ns(ts),
            rssi_dbm: -50,
            frame: ModeSFrame::Long(arr),
        }
    }

    #[test]
    fn nstrike_requires_consecutive_n() {
        let mut d = Debounced::default();
        d.observe("AAA".into(), 3);
        assert_eq!(d.confirmed, None);
        d.observe("AAA".into(), 3);
        assert_eq!(d.confirmed, None);
        d.observe("AAA".into(), 3);
        assert_eq!(d.confirmed.as_deref(), Some("AAA"));
    }

    #[test]
    fn nstrike_resets_on_interruption() {
        let mut d = Debounced::default();
        d.observe("AAA".into(), 3);
        d.observe("AAA".into(), 3);
        d.observe("BBB".into(), 3); // 割り込みでカウンタリセット
        d.observe("AAA".into(), 3); // AAA=1
        d.observe("AAA".into(), 3); // AAA=2
        assert_eq!(d.confirmed, None); // 割り込み後は改めて 3 回連続が必要
        d.observe("AAA".into(), 3); // AAA=3 -> 確定
        assert_eq!(d.confirmed.as_deref(), Some("AAA"));
    }

    #[test]
    fn emergency_squawk_is_immediate() {
        let mut mgr = AircraftStateManager::new(3, None);
        // DF5 緊急スコーク（7700）相当の合成は難しいので force 経路を直接検証。
        let st = mgr.aircraft.entry(0xABCDEF).or_default();
        st.squawk.observe("1200".into(), 3);
        st.squawk.observe("1200".into(), 3);
        assert_eq!(st.squawk.confirmed, None);
        st.squawk.force("7700".into());
        assert_eq!(st.squawk.confirmed.as_deref(), Some("7700"));
    }

    #[test]
    fn airborne_emits_record_only_after_global_pair() {
        let mut mgr = AircraftStateManager::new(3, None);
        // 1 通目（even）では確定しない。
        let r1 = mgr
            .process(&ev("8D40058B58C901375147EFD09357", 1000))
            .unwrap();
        assert!(r1.is_none());
        // 2 通目（odd, 新鮮）でグローバル復号 -> レコード生成。
        let r2 = mgr
            .process(&ev("8D40058B58C904A87F402D3B8C59", 2000))
            .unwrap()
            .expect("position record");
        assert_eq!(r2.mode_s_code, 0x40058B);
        assert!((r2.lat - 49.81755).abs() < 1e-3);
        assert!((r2.lon - 6.08442).abs() < 1e-3);
        assert!(!r2.on_ground);
    }

    #[test]
    fn stale_pair_does_not_decode() {
        let mut mgr = AircraftStateManager::new(3, None);
        mgr.process(&ev("8D40058B58C901375147EFD09357", 0)).unwrap();
        // 反対 parity が 11 秒後 -> ペア窓(10s)外なので復号しない。
        let r = mgr
            .process(&ev("8D40058B58C904A87F402D3B8C59", 110_000_000))
            .unwrap();
        assert!(r.is_none());
    }
}
