//! 起動引数のパースとバリデーション。

use crate::domain::SensorId;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Timelike, Utc};
use clap::{Parser, ValueEnum};
use std::collections::HashSet;

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

    /// 静的センサー集合。4 文字コード（英大文字2字+数字2字）をカンマ区切りで指定
    /// （例 `AB01,AB02`）。
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
    /// 宣言済みセンサー集合（AMQP バインド・未宣言検出・再計算のファイル列挙に使用）。
    pub sensors: Vec<SensorId>,
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

        let mut sensors = Vec::new();
        let mut seen = HashSet::new();
        for spec in &cli.sensors {
            let id = SensorId::from_ascii(spec.as_bytes()).with_context(|| {
                format!("invalid --sensors entry (expected 2 uppercase letters + 2 digits): {spec}")
            })?;
            if !seen.insert(id) {
                bail!("duplicate sensor in --sensors: {spec}");
            }
            sensors.push(id);
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
                // 1分ファイル・分単位 DELETE 範囲・ブロック境界との整合のため分精度必須。
                ensure_minute_aligned("--recompute-from", from)?;
                ensure_minute_aligned("--recompute-to", to)?;
                if cli.data_dir.is_none() {
                    bail!("--data-dir is required in recompute mode");
                }
            }
        }

        Ok(Config {
            mode: cli.mode,
            sensors,
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

/// 再計算レンジが分境界（秒・サブ秒 = 0）であることを検証する。
fn ensure_minute_aligned(name: &str, dt: DateTime<Utc>) -> Result<()> {
    if dt.second() != 0 || dt.nanosecond() != 0 {
        bail!("{name} must be minute-aligned (seconds and sub-seconds must be zero): {dt}");
    }
    Ok(())
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
        for s in [
            1, 2, 4, 5, 10, 20, 50, 100, 200, 250, 500, 1000, 2000, 5000, 30_000, 60_000,
        ] {
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

    fn base_cli() -> Cli {
        Cli {
            mode: Mode::Realtime,
            sensors: vec!["AB01".into(), "AB02".into()],
            amqp_url: Some("amqp://localhost".into()),
            db_url: "postgres://localhost/adsbx".into(),
            block_size_ms: 1000,
            dedup_ttl_ms: 50,
            watermark_timeout_ms: 1000,
            debounce_n: 3,
            surface_ref_lat: None,
            surface_ref_lon: None,
            recompute_from: None,
            recompute_to: None,
            restore_lookback_seconds: 0,
            data_dir: None,
            prefetch: 1000,
        }
    }

    #[test]
    fn rejects_duplicate_sensor() {
        let mut cli = base_cli();
        cli.sensors = vec!["AB01".into(), "AB01".into()];
        assert!(Config::from_cli(cli).is_err());
    }

    #[test]
    fn rejects_malformed_sensor_code() {
        let mut cli = base_cli();
        cli.sensors = vec!["ab01".into()]; // 小文字は規約外
        assert!(Config::from_cli(cli).is_err());
        let mut cli = base_cli();
        cli.sensors = vec!["AB1".into()]; // 3 文字
        assert!(Config::from_cli(cli).is_err());
    }

    fn recompute_cli(from: &str, to: &str) -> Cli {
        let mut cli = base_cli();
        cli.mode = Mode::Recompute;
        cli.amqp_url = None;
        cli.data_dir = Some("/data".into());
        cli.recompute_from = Some(from.parse().unwrap());
        cli.recompute_to = Some(to.parse().unwrap());
        cli
    }

    #[test]
    fn rejects_non_minute_aligned_recompute_range() {
        let cli = recompute_cli("2026-06-27T12:00:30Z", "2026-06-27T12:05:00Z");
        assert!(Config::from_cli(cli).is_err());
        let cli = recompute_cli("2026-06-27T12:00:00Z", "2026-06-27T12:05:59Z");
        assert!(Config::from_cli(cli).is_err());
    }

    #[test]
    fn accepts_minute_aligned_recompute_range() {
        let cli = recompute_cli("2026-06-27T12:00:00Z", "2026-06-27T12:05:00Z");
        assert!(Config::from_cli(cli).is_ok());
    }
}
