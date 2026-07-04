//! Downsampler（時間ブロック集約）。
//!
//! 高頻度な座標を指定ブロック（ミリ秒）に丸め、`(mode_s, block_id)` 単位で
//! Last-Write-Wins 集約する。ウォーターマーク確定で、終端が過ぎたブロックを
//! フラッシュする。

use crate::domain::PositionRecord;
use crate::time::{Dur100ns, Ts100ns};
use std::collections::HashMap;

pub struct Downsampler {
    /// ブロック幅。
    block: Dur100ns,
    blocks: HashMap<(u32, i64), PositionRecord>,
}

impl Downsampler {
    pub fn new(block: Dur100ns) -> Self {
        Self {
            block,
            blocks: HashMap::new(),
        }
    }

    fn block_id(&self, ts: Ts100ns) -> i64 {
        ts.0.div_euclid(self.block.0)
    }

    /// 位置レコードを取り込む。同一 `(mode_s, block)` は最終値で上書きし、
    /// `ts` はブロック境界に丸める。
    pub fn ingest(&mut self, mut rec: PositionRecord) {
        let bid = self.block_id(rec.ts);
        rec.ts = Ts100ns(bid * self.block.0);
        self.blocks.insert((rec.mode_s_code, bid), rec);
    }

    /// 終端（block 開始 + 幅）が `watermark` 以下になったブロックを確定フラッシュする。
    pub fn flush(&mut self, watermark: Ts100ns) -> Vec<PositionRecord> {
        let block = self.block.0;
        let mut out = Vec::new();
        self.blocks.retain(|&(_, bid), rec| {
            let block_end = Ts100ns(bid.saturating_add(1).saturating_mul(block));
            if block_end <= watermark {
                out.push(rec.clone());
                false
            } else {
                true
            }
        });
        out
    }

    /// 残存する全ブロックを無条件にフラッシュする（処理終了時の念押し排出）。
    pub fn drain_all(&mut self) -> Vec<PositionRecord> {
        self.blocks.drain().map(|(_, rec)| rec).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(code: u32, ts: i64, lat: f64) -> PositionRecord {
        PositionRecord {
            mode_s_code: code,
            ts: Ts100ns(ts),
            lat,
            lon: 0.0,
            alt: Some(10000),
            call_sign: None,
            squawk: None,
            on_ground: false,
        }
    }

    fn ds_1s() -> Downsampler {
        Downsampler::new(Dur100ns::from_ms(1000)) // 1000ms = 10_000_000 (100ns)
    }

    #[test]
    fn rounds_timestamp_to_block_boundary() {
        let mut d = ds_1s();
        d.ingest(rec(0xAAA, 10_000_000 + 3_456_789, 1.0));
        let out = d.flush(Ts100ns::MAX);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ts, Ts100ns(10_000_000));
    }

    #[test]
    fn last_write_wins_within_block() {
        let mut d = ds_1s();
        d.ingest(rec(0xAAA, 10_100_000, 1.0));
        d.ingest(rec(0xAAA, 10_900_000, 2.0)); // 同一ブロック後着 -> 上書き
        let out = d.flush(Ts100ns::MAX);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].lat, 2.0);
    }

    #[test]
    fn flush_respects_block_end_vs_watermark() {
        let mut d = ds_1s(); // block [10_000_000, 20_000_000)
        d.ingest(rec(0xAAA, 15_000_000, 1.0));
        // ブロック終端 20_000_000 にウォーターマークが届くまでフラッシュしない。
        assert!(d.flush(Ts100ns(19_999_999)).is_empty());
        let out = d.flush(Ts100ns(20_000_000));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn separate_blocks_and_aircraft_are_independent() {
        let mut d = ds_1s();
        d.ingest(rec(0xAAA, 10_500_000, 1.0));
        d.ingest(rec(0xBBB, 10_500_000, 1.0));
        d.ingest(rec(0xAAA, 25_500_000, 1.0)); // 別ブロック
        let out = d.flush(Ts100ns(20_000_000));
        assert_eq!(out.len(), 2); // 最初のブロックの 2 機体のみ
    }
}
