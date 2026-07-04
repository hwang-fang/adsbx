//! 内部ドメインモデル。
//!
//! AMQP のバイナリ仕様（暫定）に依存するのは腐敗防止層 (`wire`) のみで、
//! 後続パイプラインはここで定義する純粋な Rust 構造体だけを扱う。

use crate::time::Ts100ns;

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
    /// AMQP routing key 由来のセンサー識別子。
    pub sensor_id: u16,
    /// 受信時刻（GPS 規律 UTC）。
    pub ts: Ts100ns,
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
