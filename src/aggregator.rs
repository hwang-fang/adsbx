//! Watermark Aggregator。
//!
//! 各センサーの最新受信時刻を追跡し、「時刻 T まで完了」を表すウォーターマークを
//! 単調増加で算出する。フロンティア（全センサー最大時刻）から `timeout` 以上遅れた
//! センサーは除外して前進する。
//!
//! ウォーターマークは完全にデータ駆動であり、壁時計に依存しない（再計算の決定性の
//! ため）。リアルタイムの「全センサー停止時のドレイン」は Engine 側の定期
//! `advance_to` で補う。

use crate::domain::SensorId;
use crate::time::{Dur100ns, Ts100ns};
use std::collections::HashMap;

pub struct WatermarkAggregator {
    timeout: Dur100ns,
    /// sensor_id -> 観測した最大時刻。
    latest: HashMap<SensorId, Ts100ns>,
    watermark: Ts100ns,
}

impl WatermarkAggregator {
    pub fn new(timeout: Dur100ns) -> Self {
        Self {
            timeout,
            latest: HashMap::new(),
            watermark: Ts100ns::MIN,
        }
    }

    pub fn watermark(&self) -> Ts100ns {
        self.watermark
    }

    /// イベントを観測し、ウォーターマークを再計算する。前進した場合は新しい値を返す。
    pub fn observe(&mut self, sensor_id: SensorId, ts: Ts100ns) -> Option<Ts100ns> {
        let e = self.latest.entry(sensor_id).or_insert(Ts100ns::MIN);
        if ts > *e {
            *e = ts;
        }
        self.recompute()
    }

    /// 壁時計起点などの外部フロンティアまで、タイムアウト境界でウォーターマークを進める
    /// （リアルタイムのドレイン用）。前進した場合は新しい値を返す。
    pub fn advance_to(&mut self, frontier: Ts100ns) -> Option<Ts100ns> {
        let candidate = frontier.saturating_sub(self.timeout);
        if candidate > self.watermark {
            self.watermark = candidate;
            Some(self.watermark)
        } else {
            None
        }
    }

    fn recompute(&mut self) -> Option<Ts100ns> {
        let frontier = self.latest.values().copied().max().unwrap_or(Ts100ns::MIN);
        // フロンティアから timeout 以内のセンサーのみを「アクティブ」とみなし、その最小を取る。
        let cutoff = frontier.saturating_sub(self.timeout);
        let candidate = self
            .latest
            .values()
            .copied()
            .filter(|&t| t >= cutoff)
            .min()
            .unwrap_or(Ts100ns::MIN);

        if candidate > self.watermark {
            self.watermark = candidate;
            Some(self.watermark)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agg_1s() -> WatermarkAggregator {
        WatermarkAggregator::new(Dur100ns::from_ms(1000)) // timeout 1s = 10_000_000
    }

    fn sid(s: &str) -> SensorId {
        SensorId::from_ascii(s.as_bytes()).unwrap()
    }

    #[test]
    fn advances_when_all_sensors_progress() {
        let mut a = agg_1s();
        let (s1, s2) = (sid("AB01"), sid("AB02"));
        // 単一センサーでは観測時刻 = フロンティアなので watermark も追従。
        assert_eq!(a.observe(s1, Ts100ns(10_000_000)), Some(Ts100ns(10_000_000)));
        // 2 センサー目が登場。min(10s, 20s) = 10s だが既に 10s なので前進なし。
        assert_eq!(a.observe(s2, Ts100ns(20_000_000)), None);
        // sensor1 が 25s まで進むと min = 20s（sensor2）へ前進。
        assert_eq!(a.observe(s1, Ts100ns(25_000_000)), Some(Ts100ns(20_000_000)));
    }

    #[test]
    fn excludes_lagging_sensor_beyond_timeout() {
        let mut a = agg_1s();
        let (s1, s2) = (sid("AB01"), sid("AB02"));
        a.observe(s1, Ts100ns(10_000_000));
        a.observe(s2, Ts100ns(10_000_000));
        // sensor1 が 100s まで前進、sensor2 は 10s で停止（90s 遅延 > 1s）。
        // sensor2 は除外され、watermark は sensor1 基準で 100s へ。
        assert_eq!(
            a.observe(s1, Ts100ns(100_000_000)),
            Some(Ts100ns(100_000_000))
        );
    }

    #[test]
    fn watermark_is_monotonic() {
        let mut a = agg_1s();
        let s1 = sid("AB01");
        a.observe(s1, Ts100ns(50_000_000));
        // 過去のイベントが来ても後退しない。
        assert_eq!(a.observe(s1, Ts100ns(1_000_000)), None);
        assert_eq!(a.watermark(), Ts100ns(50_000_000));
    }
}
