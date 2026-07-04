//! Exact TTL Deduplicator。
//!
//! 空間伝播遅延（TDOA）による同一メッセージの重複を、生ビット列キーで先着排除する。
//! ウォーターマーク進行に応じて TTL 切れのビット列をパージする（Tick 跨ぎ対応）。

use crate::domain::ModeSFrame;
use crate::time::{Dur100ns, Ts100ns};
use std::collections::{BTreeMap, HashSet};

pub struct Deduplicator {
    ttl: Dur100ns,
    seen: HashSet<ModeSFrame>,
    /// 失効時刻 -> その時刻に失効するフレーム群。
    expiry: BTreeMap<Ts100ns, Vec<ModeSFrame>>,
}

impl Deduplicator {
    pub fn new(ttl: Dur100ns) -> Self {
        Self {
            ttl,
            seen: HashSet::new(),
            expiry: BTreeMap::new(),
        }
    }

    /// フレームを判定する。先着なら `true`（次段へ流す）、重複なら `false`（破棄）。
    pub fn accept(&mut self, frame: ModeSFrame, ts: Ts100ns) -> bool {
        if !self.seen.insert(frame) {
            return false;
        }
        let expire_at = ts.saturating_add(self.ttl);
        self.expiry.entry(expire_at).or_default().push(frame);
        true
    }

    /// ウォーターマーク `T` までに失効したビット列を HashSet からパージする。
    pub fn purge(&mut self, watermark: Ts100ns) {
        // split_off(&k) は k 以降を残すので、k = watermark+1 で「<= watermark」を取り出す。
        // 終端の Ts100ns::MAX でも飽和して溢れない。
        let mut expired = self.expiry.split_off(&watermark.saturating_add(Dur100ns(1)));
        std::mem::swap(&mut expired, &mut self.expiry);
        for (_, frames) in expired {
            for f in frames {
                self.seen.remove(&f);
            }
        }
    }

    #[cfg(test)]
    pub fn live_len(&self) -> usize {
        self.seen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(byte: u8) -> ModeSFrame {
        let mut a = [0u8; 14];
        a[0] = byte;
        ModeSFrame::Long(a)
    }

    fn dedup_50ms() -> Deduplicator {
        Deduplicator::new(Dur100ns::from_ms(50)) // 50ms = 500_000 (100ns)
    }

    #[test]
    fn first_come_wins_duplicate_dropped() {
        let mut d = dedup_50ms();
        assert!(d.accept(f(1), Ts100ns(1000)));
        // 同一ビット列が TTL 窓内に到着 -> 破棄。
        assert!(!d.accept(f(1), Ts100ns(1200)));
        // 別ビット列は通過。
        assert!(d.accept(f(2), Ts100ns(1300)));
    }

    #[test]
    fn purge_releases_after_ttl() {
        let mut d = dedup_50ms();
        d.accept(f(1), Ts100ns(1_000_000));
        assert_eq!(d.live_len(), 1);
        // 失効時刻 = 1_000_000 + 500_000 = 1_500_000。手前ではパージされない。
        d.purge(Ts100ns(1_499_999));
        assert_eq!(d.live_len(), 1);
        // 失効時刻に達したらパージされ、同一ビット列が再度先着扱いになる。
        d.purge(Ts100ns(1_500_000));
        assert_eq!(d.live_len(), 0);
        assert!(d.accept(f(1), Ts100ns(2_000_000)));
    }

    #[test]
    fn purge_with_max_watermark_does_not_overflow() {
        // 終端ドレインで使われる Ts100ns::MAX でパニックしないこと。
        let mut d = dedup_50ms();
        d.accept(f(1), Ts100ns(1_000_000));
        d.purge(Ts100ns::MAX);
        assert_eq!(d.live_len(), 0);
    }

    #[test]
    fn no_memory_leak_after_full_purge() {
        let mut d = dedup_50ms();
        for i in 0..1000 {
            d.accept(f((i % 250) as u8), Ts100ns(1_000_000 + i as i64));
        }
        d.purge(Ts100ns(i64::MAX / 2));
        assert_eq!(d.live_len(), 0);
        assert!(d.expiry.is_empty());
    }
}
