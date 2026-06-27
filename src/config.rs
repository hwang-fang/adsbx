//! 起動引数のパースとバリデーション。

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    Realtime,
    Recompute,
}

/// ADS-B 統合解析・DB登録システム。
#[derive(Debug, Parser)]
#[command(name = "adsbx", version, about)]
pub struct Cli {
    #[arg(long, value_enum)]
    pub mode: Mode,

    /// 静的センサー集合。`id:routing_key` をカンマ区切りで指定（例 `1:sensorA,2:sensorB`）。
    #[arg(long, value_delimiter = ',')]
    pub sensors: Vec<String>,

    #[arg(long)]
    pub amqp_url: Option<String>,

    #[arg(long)]
    pub db_url: String,

    /// ダウンサンプリングのブロックサイズ（ミリ秒）。
    #[arg(long)]
    pub block_size_ms: u32,

    #[arg(long, default_value_t = 50)]
    pub dedup_ttl_ms: u32,

    /// 遅延センサーを除外して前進するためのタイムアウト閾値（ミリ秒）。
    #[arg(long)]
    pub watermark_timeout_ms: u32,

    #[arg(long, default_value_t = 3)]
    pub debounce_n: u32,

    #[arg(long)]
    pub surface_ref_lat: Option<f64>,
    #[arg(long)]
    pub surface_ref_lon: Option<f64>,

    /// 再計算レンジ開始（UTC, 分精度。例 `2026-06-27T12:00:00Z`）。
    #[arg(long)]
    pub recompute_from: Option<DateTime<Utc>>,
    #[arg(long)]
    pub recompute_to: Option<DateTime<Utc>>,

    /// フェーズ1（DB状態復元）の遡及秒数。
    #[arg(long, default_value_t = 0)]
    pub restore_lookback_seconds: u64,

    /// 生バイトログの格納ディレクトリ（再計算時）。
    #[arg(long)]
    pub data_dir: Option<String>,

    /// AMQP prefetch (QoS)。
    #[arg(long, default_value_t = 1000)]
    pub prefetch: u16,
}

/// バリデーション済みの実行設定。
#[derive(Debug, Clone)]
pub struct Config {
    pub mode: Mode,
    /// sensor_id -> routing_key（リアルタイムのバインド/検証に使用）。
    pub sensors: HashMap<u16, String>,
    /// routing_key -> sensor_id（受信時の逆引き）。
    pub routing_to_sensor: HashMap<String, u16>,
    pub amqp_url: Option<String>,
    pub db_url: String,
    pub block_size_ms: u32,
    pub dedup_ttl_ms: u32,
    pub watermark_timeout_ms: u32,
    pub debounce_n: u32,
    pub surface_ref: Option<(f64, f64)>,
    pub recompute_from: Option<DateTime<Utc>>,
    pub recompute_to: Option<DateTime<Utc>>,
    pub restore_lookback_seconds: u64,
    pub data_dir: Option<String>,
    pub prefetch: u16,
}

impl Config {
    pub fn from_cli(cli: Cli) -> Result<Self> {
        validate_block_size(cli.block_size_ms)?;

        let mut sensors = HashMap::new();
        let mut routing_to_sensor = HashMap::new();
        for spec in &cli.sensors {
            let (id_str, key) = spec
                .split_once(':')
                .with_context(|| format!("invalid --sensors entry (expected id:key): {spec}"))?;
            let id: u16 = id_str
                .parse()
                .with_context(|| format!("invalid sensor id: {id_str}"))?;
            sensors.insert(id, key.to_string());
            routing_to_sensor.insert(key.to_string(), id);
        }
        if sensors.is_empty() {
            bail!("--sensors must declare at least one sensor");
        }

        let surface_ref = match (cli.surface_ref_lat, cli.surface_ref_lon) {
            (Some(lat), Some(lon)) => Some((lat, lon)),
            (None, None) => None,
            _ => bail!("--surface-ref-lat and --surface-ref-lon must be provided together"),
        };

        match cli.mode {
            Mode::Realtime => {
                if cli.amqp_url.is_none() {
                    bail!("--amqp-url is required in realtime mode");
                }
            }
            Mode::Recompute => {
                let from = cli
                    .recompute_from
                    .context("--recompute-from is required in recompute mode")?;
                let to = cli
                    .recompute_to
                    .context("--recompute-to is required in recompute mode")?;
                if to <= from {
                    bail!("--recompute-to must be after --recompute-from");
                }
                if cli.data_dir.is_none() {
                    bail!("--data-dir is required in recompute mode");
                }
            }
        }

        Ok(Config {
            mode: cli.mode,
            sensors,
            routing_to_sensor,
            amqp_url: cli.amqp_url,
            db_url: cli.db_url,
            block_size_ms: cli.block_size_ms,
            dedup_ttl_ms: cli.dedup_ttl_ms,
            watermark_timeout_ms: cli.watermark_timeout_ms,
            debounce_n: cli.debounce_n,
            surface_ref,
            recompute_from: cli.recompute_from,
            recompute_to: cli.recompute_to,
            restore_lookback_seconds: cli.restore_lookback_seconds,
            data_dir: cli.data_dir,
            prefetch: cli.prefetch,
        })
    }
}

/// ブロックサイズ `S`(ms) の制約を検証する。
///
/// * `S <= 1000` のとき `1000 % S == 0`
/// * `S > 1000` のとき `S % 1000 == 0`
/// * 常に `60000 % S == 0`（ブロックが 1 分ファイル境界を跨がない）
pub fn validate_block_size(s: u32) -> Result<()> {
    if s == 0 {
        bail!("--block-size-ms must be > 0");
    }
    if s <= 1000 {
        if 1000 % s != 0 {
            bail!("--block-size-ms <= 1000 must divide 1000 (got {s})");
        }
    } else if !s.is_multiple_of(1000) {
        bail!("--block-size-ms > 1000 must be a multiple of 1000 (got {s})");
    }
    if !60_000u32.is_multiple_of(s) {
        bail!("--block-size-ms must divide 60000 to avoid crossing 1-minute file boundaries (got {s})");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_block_sizes() {
        for s in [1, 2, 4, 5, 10, 20, 50, 100, 200, 250, 500, 1000, 2000, 5000, 30_000, 60_000] {
            assert!(validate_block_size(s).is_ok(), "S={s} should be valid");
        }
    }

    #[test]
    fn rejects_non_divisors_of_1000() {
        // 3, 7 do not divide 1000
        assert!(validate_block_size(3).is_err());
        assert!(validate_block_size(7).is_err());
    }

    #[test]
    fn rejects_blocks_crossing_minute_boundary() {
        // 7000 % 1000 == 0 だが 60000 % 7000 != 0
        assert!(validate_block_size(7000).is_err());
        // 1500: 1000 の倍数でないので前段で弾かれる
        assert!(validate_block_size(1500).is_err());
    }

    #[test]
    fn rejects_zero() {
        assert!(validate_block_size(0).is_err());
    }
}
