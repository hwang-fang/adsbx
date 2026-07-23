#!/usr/bin/env python3
"""再計算モードの実DB検証用に、既知の ADS-B even/odd ペアを含む
センサー毎 1 分ファイル（`{time:%Y%m%d%H%M}{sensor_id}.spkx`）を生成する。

固定長レコード (20 バイト, little-endian):
  [相対時刻 u32(分内 0~599999999, /100ns)][ビット列 u8*14][波高値 u16]

波高値エンコード: 上位8bit=絶対値整数部のビット反転 / 下位8bit=小数部のビット反転。
  value = 65535 - abs(dbm)*256  （デコードは -1*(65535-value)/256）

既知ペア (icao=40058B) はグローバル復号で (49.81755, 6.08442) になる。
even を AB01、odd を AB02、TDOA 重複(even 再受信)を AB03 が受信した想定。
"""
import struct
import sys
from datetime import datetime, timezone

# 既知の DF17 BDS05 even / odd（同一機体）
EVEN = bytes.fromhex("8D40058B58C901375147EFD09357")
ODD = bytes.fromhex("8D40058B58C904A87F402D3B8C59")


def encode_signal(dbm: int) -> int:
    """dBm(-255~0) を波高値 u16 へ（小数部 0）。"""
    return 65535 - abs(dbm) * 256


def record(rel_100ns: int, payload: bytes, dbm: int) -> bytes:
    assert len(payload) == 14
    return struct.pack("<I", rel_100ns) + payload + struct.pack("<H", encode_signal(dbm))


def main():
    # 対象分: 2026-06-27T12:00:00Z
    minute = datetime(2026, 6, 27, 12, 0, 0, tzinfo=timezone.utc)
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    prefix = f"{out_dir}/{minute:%Y%m%d%H%M}"

    # センサー毎に 1 ファイル。相対時刻は分内 100ns 単位（同一ブロック=1000ms 内）。
    files = {
        "AB01": [record(1_000_000, EVEN, -50)],  # +0.1s even
        "AB02": [record(2_000_000, ODD, -55)],   # +0.2s odd（別センサー）
        "AB03": [record(1_300_000, EVEN, -60)],  # +0.13s even の TDOA 重複
    }

    for sensor, records in files.items():
        fname = f"{prefix}{sensor}.spkx"
        with open(fname, "wb") as f:
            f.write(b"".join(records))
        print(f"wrote {fname} ({len(records)} records)")


if __name__ == "__main__":
    main()
