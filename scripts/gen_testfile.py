#!/usr/bin/env python3
"""再計算モードの実DB検証用に、既知の ADS-B even/odd ペアを含む 1 分ファイルを生成する。

レコード形式 (26 バイト, little-endian):
  [sensor_id u16][timestamp_100ns i64][rssi_dbm i16][payload 14B]

既知ペア (icao=40058B) はグローバル復号で (49.81755, 6.08442) になる。
"""
import struct
import sys
from datetime import datetime, timezone

# 既知の DF17 BDS05 even / odd（同一機体）
EVEN = bytes.fromhex("8D40058B58C901375147EFD09357")
ODD = bytes.fromhex("8D40058B58C904A87F402D3B8C59")

def rec(sensor_id: int, ts_100ns: int, rssi: int, payload: bytes) -> bytes:
    assert len(payload) == 14
    return struct.pack("<hqh", sensor_id, ts_100ns, rssi) + payload

def main():
    # 対象分: 2026-06-27T12:00:00Z
    minute = datetime(2026, 6, 27, 12, 0, 0, tzinfo=timezone.utc)
    base_100ns = int(minute.timestamp()) * 10_000_000

    out_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    fname = f"{out_dir}/{minute:%Y%m%d%H%M}.bin"

    records = b""
    # 同一ブロック内 (block=1000ms) に even -> odd を配置。
    records += rec(1, base_100ns + 1_000_000, -50, EVEN)  # +0.1s
    records += rec(2, base_100ns + 2_000_000, -55, ODD)   # +0.2s, 別センサー
    # TDOA 重複: sensor3 が even を僅差で再受信（先着排除で落ちるはず）
    records += rec(3, base_100ns + 1_300_000, -60, EVEN)

    with open(fname, "wb") as f:
        f.write(records)
    print(f"wrote {fname} ({len(records)} bytes, {len(records)//26} records)")

if __name__ == "__main__":
    main()
