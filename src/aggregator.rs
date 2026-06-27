//! Watermark Aggregator。
//!
//! 各センサーの最新受信時刻を追跡し、「時刻 T まで完了」を表すウォーターマークを
//! 単調増加で算出する。フロンティア（全センサー最大時刻）から `timeout` 以上遅れた
//! センサーは除外して前進する。
//!
//! ウォーターマークは完全にデータ駆動であり、壁時計に依存しない（再計算の決定性の
//! ため）。リアルタイムの「全センサー停止時のドレイン」はドライバ側の定期 tick で
//! 補う。

use std::collections::HashMap;

pub struct WatermarkAggregator {
    timeout_100ns: i64,
    /// sensor_id -> 観測した最大 timestamp_100ns。
    latest: HashMap<u16, i64>,
    watermark: i64,
}

impl WatermarkAggregator {
    pub fn new(timeout_ms: u32) -> Self {
        Self {
            timeout_100ns: timeout_ms as i64 * 10_000,
            latest: HashMap::new(),
            watermark: i64::MIN,
        }
    }

    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    /// イベントを観測し、ウォーターマークを再計算する。前進した場合は新しい値を返す。
    pub fn observe(&mut self, sensor_id: u16, timestamp_100ns: i64) -> Option<i64> {
        let e = self.latest.entry(sensor_id).or_insert(i64::MIN);
        if timestamp_100ns > *e {
            *e = timestamp_100ns;
        }
        self.recompute()
    }

    /// 壁時計起点などの外部フロンティアまで、タイムアウト境界でウォーターマークを進める
    /// （リアルタイムのドレイン用）。前進した場合は新しい値を返す。
    pub fn advance_to(&mut self, frontier_100ns: i64) -> Option<i64> {
        let candidate = frontier_100ns - self.timeout_100ns;
        if candidate > self.watermark {
            self.watermark = candidate;
            Some(self.watermark)
        } else {
            None
        }
    }

    fn recompute(&mut self) -> Option<i64> {
        let frontier = self.latest.values().copied().max().unwrap_or(i64::MIN);
        // フロンティアから timeout 以内のセンサーのみを「アクティブ」とみなし、その最小を取る。
        let cutoff = frontier - self.timeout_100ns;
        let candidate = self
            .latest
            .values()
            .copied()
            .filter(|&t| t >= cutoff)
            .min()
            .unwrap_or(i64::MIN);

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

    #[test]
    fn advances_when_all_sensors_progress() {
        let mut a = WatermarkAggregator::new(1000); // timeout 1s = 10_000_000
        // 単一センサーでは観測時刻 = フロンティアなので watermark も追従。
        assert_eq!(a.observe(1, 10_000_000), Some(10_000_000));
        // 2 センサー目が登場。min(10s, 20s) = 10s だが既に 10s なので前進なし。
        assert_eq!(a.observe(2, 20_000_000), None);
        // sensor1 が 25s まで進むと min = 20s（sensor2）へ前進。
        assert_eq!(a.observe(1, 25_000_000), Some(20_000_000));
    }

    #[test]
    fn excludes_lagging_sensor_beyond_timeout() {
        let mut a = WatermarkAggregator::new(1000); // 1s
        a.observe(1, 10_000_000);
        a.observe(2, 10_000_000);
        // sensor1 が 100s まで前進、sensor2 は 10s で停止（90s 遅延 > 1s）。
        // sensor2 は除外され、watermark は sensor1 基準で 100s へ。
        assert_eq!(a.observe(1, 100_000_000), Some(100_000_000));
    }

    #[test]
    fn watermark_is_monotonic() {
        let mut a = WatermarkAggregator::new(1000);
        a.observe(1, 50_000_000);
        // 過去のイベントが来ても後退しない。
        assert_eq!(a.observe(1, 1_000_000), None);
        assert_eq!(a.watermark(), 50_000_000);
    }
}
