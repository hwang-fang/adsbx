# adsbx — ADS-B 統合解析・DB 登録システム

複数センサーから AMQP 経由で受信する ADS-B / Mode-S 生データ（100ns 精度タイムスタンプ付きビット列）を統合・重複排除・デコードし、指定した時間分解能にダウンサンプリングして PostgreSQL へ登録する Rust 製バックエンドアプリケーションです。

## 主な機能

- **TDOA 重複排除**: 複数センサーが受信した同一ビット列を先着のみ通過（TTL 窓、既定 50ms）
- **Mode-S / ADS-B デコード**: DF17/18（位置・便名）、DF4/20（気圧高度）、DF5/21（スコーク・緊急）を [rs1090](https://crates.io/crates/rs1090) で復号
- **CPR 座標計算**: 空中は even/odd ペアのグローバル復号、地上は参照座標からのローカル復号。ペアリング状態は自前管理
- **メタデータの状態管理**: 便名・スコークは N-Strike デバウンス（緊急スコークは即時確定）、高度は Last-Write-Wins
- **ダウンサンプリング**: 時間ブロック単位の Last-Write-Wins 集約でDB書き込み量を削減
- **2 つの稼働モード**:
  - **リアルタイム**: AMQP からストリーム受信（接続断は指数バックオフで自動再接続）
  - **再計算**: 過去の 1 分毎ファイルから冪等に DB を再構築（分単位 DELETE→INSERT）

## アーキテクチャ

```
リアルタイム:
[AMQP] ──► receiver task ──► Engine 駆動ループ ──► writer task ──► [PostgreSQL]
                              └ 同期合成: watermark → dedup → state(CPR) → downsampler

再計算（チャネルなし・完全データ駆動で決定的）:
[1分ファイル] ──► Engine ──► 確定した分から順次 DELETE→INSERT
```

処理の中核は I/O を持たない同期コア `Engine` で、Tokio タスクは I/O を持つ両端（AMQP receiver / DB writer）のみです。設計の詳細は [docs/DESIGN.md](docs/DESIGN.md)、実装の解説は [docs/CODE_GUIDE.md](docs/CODE_GUIDE.md) を参照してください。

## 動作環境

| 要件 | 内容 |
|---|---|
| Rust | edition 2021（rustc 1.90 で動作確認） |
| PostgreSQL | 動作確認は 18.x。`TIMESTAMPTZ` が使えるバージョンであれば可 |
| AMQP ブローカー | RabbitMQ 等（AMQP 0-9-1）。リアルタイムモードのみ必要 |
| OS | Linux / Windows（Pure Rust・C/C++ 依存なし） |

## セットアップ

### 1. データベース

```sh
createdb adsb_pipeline_test   # 任意の DB 名
psql "$DATABASE_URL" -f migrations/20260627183814_create_raw_adsb_records.sql
```

sqlx-cli を使う場合は `cargo sqlx migrate run` でも適用できます。スキーマは `raw_adsb_records` 単一テーブル＋ `UNIQUE(mode_s_code, timestamp)`（冪等 UPSERT の要）です。

### 2. 環境変数

`.env`（gitignore 済み）に接続先を書きます。これは `sqlx::query!` マクロのコンパイル時検証にも使われます:

```
DATABASE_URL=postgres://user:pass@localhost:5432/adsb_pipeline_test
```

DB に接続せずビルドする場合は、同梱の `.sqlx/` オフラインキャッシュにより `SQLX_OFFLINE=true` で可能です。静的 SQL を変更した際は `cargo sqlx prepare` でキャッシュを再生成してください。

## ビルド・テスト

```sh
cargo build --release           # DATABASE_URL 到達可、または
SQLX_OFFLINE=true cargo build --release

SQLX_OFFLINE=true cargo test    # 単体 + Engine 結合テスト（DB/ブローカー不要）
```

## 使用法

### リアルタイムモード

```sh
cargo run --release -- \
  --mode realtime \
  --sensors 1:sensorA,2:sensorB \
  --amqp-url "amqp://guest:guest@localhost:5672/%2f" \
  --db-url "postgres://user:pass@localhost:5432/adsb_pipeline_test" \
  --block-size-ms 1000 \
  --watermark-timeout-ms 1000
```

起動時に AMQP トポロジ（exchange `adsb` / queue `adsb_raw` / routing key 束縛）を冪等に宣言するため、まっさらなブローカーでも単体で起動できます。Ctrl-C で残存データを排出してからグレースフルに終了します。

### 再計算モード

`--data-dir` 配下の 1 分毎ファイル `YYYYMMDDHHMM.bin`（UTC）を読み、対象レンジを分単位の DELETE→INSERT で冪等に再構築します:

```sh
cargo run --release -- \
  --mode recompute \
  --sensors 1:sensorA,2:sensorB \
  --db-url "postgres://user:pass@localhost:5432/adsb_pipeline_test" \
  --block-size-ms 1000 \
  --watermark-timeout-ms 1000 \
  --recompute-from 2026-06-27T12:00:00Z \
  --recompute-to   2026-06-27T12:10:00Z \
  --restore-lookback-seconds 300 \
  --data-dir /path/to/minute-files
```

開始前に DB から機体メタデータを復元し、開始 1 分前のファイルで CPR / 重複排除のメモリ状態を温めてから本処理に入ります（ウォームアップの詳細は DESIGN §5）。欠損ファイルは警告してスキップされ、その分は DELETE のみ実行されます。

### 起動引数

| 引数 | 用途 | 既定 |
|---|---|---|
| `--mode realtime\|recompute` | 稼働モード | — |
| `--sensors <id:key,...>` | 静的センサー集合（id と routing key の対応、重複不可） | — |
| `--amqp-url` | AMQP 接続先（realtime 必須） | — |
| `--db-url` | PostgreSQL 接続先 | — |
| `--block-size-ms <S>` | ダウンサンプリングブロック（`60000 % S == 0` 等の制約あり） | — |
| `--dedup-ttl-ms` | 重複排除 TTL | 50 |
| `--watermark-timeout-ms` | 遅延センサー除外閾値 | — |
| `--debounce-n` | N-Strike 回数 | 3 |
| `--surface-ref-lat` / `--surface-ref-lon` | 地上 CPR 参照座標（両方同時指定） | — |
| `--recompute-from` / `--recompute-to` | 再計算レンジ（UTC・分境界必須） | — |
| `--restore-lookback-seconds` | 状態復元の遡及秒数 | 0 |
| `--data-dir` | 1 分ファイル格納ディレクトリ（recompute 必須） | — |
| `--prefetch` | AMQP prefetch (QoS) | 1000 |

### データフォーマット（暫定）

バイナリ仕様は暫定で、依存コードは `src/wire.rs`（腐敗防止層）に隔離されています。いずれもリトルエンディアン、ペイロードは 14 バイト固定（56bit フレームは末尾ゼロ埋め）:

- AMQP body（24B）: `[timestamp_100ns: i64][rssi_dbm: i16][payload: 14B]` — sensor_id は routing key 由来
- ファイルレコード（26B）: `[sensor_id: u16][timestamp_100ns: i64][rssi_dbm: i16][payload: 14B]`

タイムスタンプは Unix エポック起点・100ns 単位・GPS 規律 UTC です。

## 動作確認ツール

```sh
cargo run --example publish        # 既知の even/odd ペア + TDOA 重複を AMQP へ publish
                                   # SKIP_TOPOLOGY=1: publish のみ / DELETE_ONLY=1: トポロジ削除
python3 scripts/gen_testfile.py .  # 再計算検証用の 1 分ファイルを生成
```

## ログ・可観測性

`RUST_LOG` でログレベルを制御します（既定 `info`）。破棄・処理カウンタ（CRC 不一致、対象外 DF、遅延破棄、重複排除、UPSERT 件数、AMQP 再接続回数など）を 30 秒毎と終了時に出力します。

## ドキュメント

- [docs/DESIGN.md](docs/DESIGN.md) — 実装仕様書（意思決定の背景・厳密な仕様）
- [docs/CODE_GUIDE.md](docs/CODE_GUIDE.md) — コード解説（モジュール毎の要点・落とし穴）
