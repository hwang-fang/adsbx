//! DB Writer。`sqlx` によるバルク UPSERT と、再計算の 1 分単位 DELETE→INSERT。

use crate::domain::{mode_s_hex, PositionRecord};
use crate::metrics::Metrics;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, QueryBuilder, Postgres};
use std::sync::Arc;

/// Unix エポック起点 100ns 値を UTC 日時へ変換する。
pub fn to_datetime(timestamp_100ns: i64) -> DateTime<Utc> {
    let secs = timestamp_100ns.div_euclid(10_000_000);
    let nanos = (timestamp_100ns.rem_euclid(10_000_000) * 100) as u32;
    DateTime::from_timestamp(secs, nanos).unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap())
}

#[derive(Clone)]
pub struct DbWriter {
    pool: PgPool,
    metrics: Arc<Metrics>,
}

impl DbWriter {
    pub async fn connect(db_url: &str, metrics: Arc<Metrics>) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(db_url)
            .await
            .context("connecting to PostgreSQL")?;
        Ok(Self { pool, metrics })
    }

    /// 複数行 UPSERT（リアルタイム・再計算共通の INSERT 部分）。
    fn push_insert<'a>(qb: &mut QueryBuilder<'a, Postgres>, rows: &'a [PositionRecord]) {
        qb.push(
            "INSERT INTO raw_adsb_records \
             (timestamp, mode_s_code, lat, lon, alt, call_sign, squawk, on_ground) ",
        );
        qb.push_values(rows, |mut b, r| {
            b.push_bind(to_datetime(r.timestamp_100ns))
                .push_bind(mode_s_hex(r.mode_s_code))
                .push_bind(r.lat)
                .push_bind(r.lon)
                .push_bind(r.alt)
                .push_bind(r.call_sign.clone())
                .push_bind(r.squawk.clone())
                .push_bind(r.on_ground);
        });
        qb.push(
            " ON CONFLICT (mode_s_code, timestamp) DO UPDATE SET \
             lat = EXCLUDED.lat, lon = EXCLUDED.lon, alt = EXCLUDED.alt, \
             call_sign = EXCLUDED.call_sign, squawk = EXCLUDED.squawk, \
             on_ground = EXCLUDED.on_ground",
        );
    }

    /// リアルタイム: バッチを UPSERT する。
    pub async fn upsert_batch(&self, rows: &[PositionRecord]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut qb = QueryBuilder::new("");
        Self::push_insert(&mut qb, rows);
        qb.build().execute(&self.pool).await.context("upsert batch")?;
        Metrics::add(&self.metrics.db_upserts, rows.len() as u64);
        Ok(())
    }

    /// 再計算: 対象 1 分 `[minute_start, minute_start + 60s)` を全機体まとめて DELETE し、
    /// その後 INSERT するトランザクション。
    pub async fn recompute_minute(
        &self,
        minute_start: DateTime<Utc>,
        rows: &[PositionRecord],
    ) -> Result<()> {
        let minute_end = minute_start + chrono::Duration::seconds(60);
        let mut tx = self.pool.begin().await.context("begin recompute tx")?;

        sqlx::query!(
            "DELETE FROM raw_adsb_records WHERE timestamp >= $1 AND timestamp < $2",
            minute_start,
            minute_end
        )
        .execute(&mut *tx)
        .await
        .context("delete minute range")?;

        if !rows.is_empty() {
            let mut qb = QueryBuilder::new("");
            Self::push_insert(&mut qb, rows);
            qb.build().execute(&mut *tx).await.context("insert minute rows")?;
        }

        tx.commit().await.context("commit recompute tx")?;
        Metrics::add(&self.metrics.db_upserts, rows.len() as u64);
        Ok(())
    }

    /// 再計算フェーズ1: 復元窓内の機体ごと最新 (call_sign, squawk) を取得する。
    /// 戻り値は (icao, call_sign, squawk)。
    pub async fn restore_states(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<Vec<(u32, Option<String>, Option<String>)>> {
        let recs = sqlx::query!(
            r#"SELECT DISTINCT ON (mode_s_code)
                   mode_s_code AS "mode_s_code!", call_sign, squawk
               FROM raw_adsb_records
               WHERE timestamp >= $1 AND timestamp < $2
               ORDER BY mode_s_code, timestamp DESC"#,
            window_start,
            window_end
        )
        .fetch_all(&self.pool)
        .await
        .context("restore states")?;

        Ok(recs
            .into_iter()
            .filter_map(|r| {
                u32::from_str_radix(&r.mode_s_code, 16)
                    .ok()
                    .map(|icao| (icao, r.call_sign, r.squawk))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_epoch_100ns_to_utc() {
        // 1_000_000_000 * 10_000_000 (100ns) = 1e9 秒 = 2001-09-09T01:46:40Z
        let dt = to_datetime(1_000_000_000 * 10_000_000);
        assert_eq!(dt.timestamp(), 1_000_000_000);
        // 端数 100ns。
        let dt2 = to_datetime(10_000_000 + 5); // 1.0000005 秒
        assert_eq!(dt2.timestamp(), 1);
        assert_eq!(dt2.timestamp_subsec_nanos(), 500);
    }
}
