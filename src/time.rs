//! 時間の型付け（DESIGN §3.3）。
//!
//! 100ns 値の生 `i64` をモジュール間で受け渡すと ms との単位取り違えが起きやすいため、
//! 時刻 [`Ts100ns`] と幅 [`Dur100ns`] を newtype で区別する。ms → 100ns の換算と
//! 飽和演算はこのモジュールに閉じ込める。終端処理で `Ts100ns::MAX` 近傍の算術が
//! 発生するため、加減算は必ず飽和させる（呼び出し側の注意に依存しない）。

use chrono::{DateTime, Utc};

/// Unix エポック起点・100ns 単位・GPS 規律 UTC の時刻。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ts100ns(pub i64);

/// 100ns 単位の時間幅。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dur100ns(pub i64);

impl Ts100ns {
    pub const MIN: Ts100ns = Ts100ns(i64::MIN);
    pub const MAX: Ts100ns = Ts100ns(i64::MAX);

    pub fn saturating_add(self, d: Dur100ns) -> Ts100ns {
        Ts100ns(self.0.saturating_add(d.0))
    }

    pub fn saturating_sub(self, d: Dur100ns) -> Ts100ns {
        Ts100ns(self.0.saturating_sub(d.0))
    }

    /// 2 時刻の差の絶対値。`MIN`/`MAX` 近傍でも溢れない。
    pub fn abs_delta(self, other: Ts100ns) -> Dur100ns {
        Dur100ns(self.0.saturating_sub(other.0).saturating_abs())
    }
}

impl Dur100ns {
    pub fn from_ms(ms: u32) -> Dur100ns {
        Dur100ns(ms as i64 * 10_000)
    }
}

/// 現在時刻（壁時計）を [`Ts100ns`] で返す（リアルタイムのドレイン用）。
pub fn now_100ns() -> Ts100ns {
    Ts100ns(Utc::now().timestamp_nanos_opt().unwrap_or(0) / 100)
}

/// UTC 日時を [`Ts100ns`] へ変換する（分境界とウォーターマークの比較用）。
pub fn from_datetime(dt: DateTime<Utc>) -> Ts100ns {
    Ts100ns(dt.timestamp() * 10_000_000 + dt.timestamp_subsec_nanos() as i64 / 100)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturates_at_extremes() {
        // 終端の Watermark(MAX) に対する算術がパニックしないこと。
        assert_eq!(Ts100ns::MAX.saturating_add(Dur100ns(1)), Ts100ns::MAX);
        assert_eq!(Ts100ns::MIN.saturating_sub(Dur100ns(1)), Ts100ns::MIN);
        assert_eq!(Ts100ns::MAX.abs_delta(Ts100ns::MIN), Dur100ns(i64::MAX));
    }

    #[test]
    fn converts_ms() {
        assert_eq!(Dur100ns::from_ms(50), Dur100ns(500_000));
        assert_eq!(Dur100ns::from_ms(1000), Dur100ns(10_000_000));
    }

    #[test]
    fn converts_datetime_roundtrip() {
        let dt = DateTime::from_timestamp(1_000_000_000, 500).unwrap();
        assert_eq!(from_datetime(dt), Ts100ns(1_000_000_000 * 10_000_000 + 5));
    }
}
