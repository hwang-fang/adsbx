//! 可観測性カウンタ。`tracing` で定期出力する。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct Metrics {
    pub rejected_crc: AtomicU64,
    pub parse_error: AtomicU64,
    pub unsupported_df: AtomicU64,
    pub malformed_short_frame: AtomicU64,
    pub dropped_late: AtomicU64,
    pub unknown_sensor: AtomicU64,
    pub deduped_dropped: AtomicU64,
    pub positions_emitted: AtomicU64,
    pub db_upserts: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn incr(field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(field: &AtomicU64, n: u64) {
        field.fetch_add(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> String {
        format!(
            "rejected_crc={} parse_error={} unsupported_df={} malformed_short_frame={} \
             dropped_late={} unknown_sensor={} deduped_dropped={} positions_emitted={} db_upserts={}",
            self.rejected_crc.load(Ordering::Relaxed),
            self.parse_error.load(Ordering::Relaxed),
            self.unsupported_df.load(Ordering::Relaxed),
            self.malformed_short_frame.load(Ordering::Relaxed),
            self.dropped_late.load(Ordering::Relaxed),
            self.unknown_sensor.load(Ordering::Relaxed),
            self.deduped_dropped.load(Ordering::Relaxed),
            self.positions_emitted.load(Ordering::Relaxed),
            self.db_upserts.load(Ordering::Relaxed),
        )
    }
}
