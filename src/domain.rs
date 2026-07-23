//! 内部ドメインモデル。
//!
//! AMQP のバイナリ仕様（暫定）に依存するのは腐敗防止層 (`wire`) のみで、
//! 後続パイプラインはここで定義する純粋な Rust 構造体だけを扱う。

use crate::time::Ts100ns;

/// センサー識別子（英大文字2字 + 数字2字の 4 文字コード）。
///
/// MQ メッセージのヘッダ、または保存ファイル名に埋め込まれた 4 文字コードから得る。
/// 固定長・`Copy` なので aggregator の `HashMap` キーにそのまま使える。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SensorId([u8; 4]);

impl SensorId {
    /// 4 バイトの ASCII（英大文字2字 + 数字2字）から構築する。規約外なら `None`。
    pub fn from_ascii(bytes: &[u8]) -> Option<SensorId> {
        let b: [u8; 4] = bytes.try_into().ok()?;
        if b[0].is_ascii_uppercase()
            && b[1].is_ascii_uppercase()
            && b[2].is_ascii_digit()
            && b[3].is_ascii_digit()
        {
            Some(SensorId(b))
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &str {
        // from_ascii を通っていれば必ず ASCII。
        std::str::from_utf8(&self.0).unwrap_or("????")
    }
}

impl std::fmt::Display for SensorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::fmt::Debug for SensorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SensorId({:?})", self.as_str())
    }
}

/// Mode-S フレーム。AMQP 上は 56bit メッセージも末尾 56bit ゼロ埋めの 14 バイトで
/// 届くが、内部では DF 値から判定した本来の長さで保持する。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ModeSFrame {
    /// 56bit (DF 0/4/5/11 など)
    Short([u8; 7]),
    /// 112bit (DF 16/17/18/19/20/21 など)
    Long([u8; 14]),
}

impl ModeSFrame {
    /// デコードに渡す生バイト列。
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            ModeSFrame::Short(b) => b,
            ModeSFrame::Long(b) => b,
        }
    }
}

/// 腐敗防止層を通過した後の共通内部イベント。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawSensorEvent {
    /// MQ ヘッダ／ファイル名由来のセンサー識別子。
    pub sensor_id: SensorId,
    /// 受信時刻（分頭 + レコード相対時刻。GPS 規律 UTC）。
    pub ts: Ts100ns,
    /// 波高値をデコードした dBm（-255 ~ 0）。
    pub rssi_dbm: i16,
    pub frame: ModeSFrame,
}

/// State Manager が「位置確定」時に生成し Downsampler へ流すレコード。
/// 最新メタデータがスナップショットとして焼き込まれている。
#[derive(Debug, Clone, PartialEq)]
pub struct PositionRecord {
    pub mode_s_code: u32,
    /// Downsampler 通過後はブロック境界に丸められている。
    pub ts: Ts100ns,
    pub lat: f64,
    pub lon: f64,
    pub alt: Option<i32>,
    pub call_sign: Option<String>,
    pub squawk: Option<String>,
    /// 行単位で算出（surface 由来、または DF17 CA==4）。
    pub on_ground: bool,
}

/// ICAO 24bit アドレスを DB 格納用の 6 桁大文字 HEX に整形する。
pub fn mode_s_hex(code: u32) -> String {
    format!("{code:06X}")
}
