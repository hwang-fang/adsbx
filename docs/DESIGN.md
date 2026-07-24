# ADS-B 統合解析・DB登録システム 実装仕様書

## 1. システム概要

本システムは、複数センサーからAMQP経由で受信するADS-B/Mode-S生データ（100ns精度のタイムスタンプ付きビット列）を統合・重複排除・デコードし、指定された時間分解能（ダウンサンプリング）でPostgreSQLへ登録するRust製バックエンドアプリケーションである。

* **言語・制約**: Pure Rust (C/C++依存禁止)。Windows/Linux両対応。
* **主要クレート**: `tokio` (非同期ランタイム), `lapin` (AMQP), `sqlx` (PostgreSQL), `rs1090` (Mode-S / ADS-B / FLARM デコードおよびCPR座標計算のコアライブラリ。**ステートレスな数学エンジンとして利用**し、状態管理は本アプリ側で保持する), `tracing` (ログ・可観測性)。
* **稼働モード**:
  1. **リアルタイムモード**: AMQPからストリーム受信。
  2. **再計算モード**: 過去の1分毎ファイル（生バイト列ログ）から読み込み（冪等性を担保したDB再構築）。

### 1.1. 前提条件

* **クロック同期**: 全センサーは GPS 規律の UTC 時刻に同期している（相互誤差 < 数ms）。これにより TDOA 重複排除（ビット列＋時刻窓）が成立する。
* **タイムスタンプ**: `timestamp_100ns` は **Unixエポック起点・100ナノ秒単位の i64**。`TIMESTAMPTZ` へは `timestamp_100ns × 100` ナノ秒 = UTC として変換する。

### 1.2. 対象メッセージ種別 (Downlink Format)

| DF | 内容 | フレーム長 | 抽出フィールド |
|---|---|---|---|
| DF17 / DF18 | ADS-B 拡張スキッタ | 112bit (`[u8;14]`) | 位置(BDS0,5/0,6)・便名(BDS0,8)・地上判定 |
| DF5 / DF21 | Identity (Mode A code) | 56/112bit | スコーク（緊急含む） |
| DF4 / DF20 | Altitude | 56/112bit | 気圧高度 |

* 上記以外のDF、CRC（パリティ）不一致、パース不能なフレームは**破棄し、カウンタで計数**する。
* DF4/5/20/21 の `mode_s_code`（ICAO 24bitアドレス）は AP（パリティ overlay）から rs1090 を用いて復元する。

## 2. データベーススキーマ（PostgreSQL）

UIアプリからの高速な `SELECT *` を実現するため、単一の `raw_adsb_records` テーブルに完全な状態スナップショットとして格納する。

```sql
CREATE TABLE raw_adsb_records (
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    mode_s_code VARCHAR(6) NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    alt INT,
    call_sign VARCHAR(8),
    squawk VARCHAR(4),
    on_ground BOOLEAN NOT NULL,
    CONSTRAINT uq_raw_records_modes_time UNIQUE (mode_s_code, timestamp)
);
CREATE INDEX idx_raw_records_time ON raw_adsb_records (timestamp);
CREATE INDEX idx_raw_records_mode_s_time ON raw_adsb_records (mode_s_code, timestamp DESC);
```

* **行生成は「位置確定時のみ」**。`lat`/`lon` が得られた瞬間のみ行を生成し、その時点の最新メタデータ（`alt`/`call_sign`/`squawk`）をスナップショットとして焼き込む。
* **`on_ground`**: 次のいずれかで `TRUE`。
  * Surface position として取得した座標であるとき。
  * DF17 メッセージの CA（Capability）== 4（on-ground）であるとき。
  * DF18 は CA を持たない（CF）ため、surface 由来か否かのみで判定する。
* **冪等性 (UPSERT)**: `(mode_s_code, timestamp)` の一意制約により、リアルタイム・再計算とも `INSERT ... ON CONFLICT (mode_s_code, timestamp) DO UPDATE` で冪等化する。`timestamp` はダウンサンプリングでブロック境界に丸められているため、機体×ブロックで一意になる。
* **保持・パーティション**: 本アプリは過去データの削除を行わない（別アプリが読み出し・削除を管轄）。データ規模（最大 ~10万行/日・保持2週間で ~140万行）も小さいためパーティションは設けない。

## 3. 処理アーキテクチャ（同期コア + 非同期エッジ）

処理の中核（Watermark Aggregator → Deduplicator → State Manager → Downsampler）はいずれも I/O を持たないメモリ内処理であるため、**単一の同期コア `Engine` に合成**する。並行処理（Tokioタスク）は I/O を持つ両端（AMQP Receiver / DB Writer）のみに置く。

```
[AMQP Receiver task] ──mpsc──► [Engine 駆動ループ] ──mpsc──► [DB Writer task]
                                └ 同期合成: aggregator → dedup → state → downsampler
```

* `Engine::process(RawSensorEvent) -> Vec<PositionRecord>`: 1 イベントを同期処理し、確定した行を返す。
* `Engine::finish() -> Vec<PositionRecord>`: 入力終端時に残存ブロックを全排出する。
* 再計算時はチャネルを使わず、File Reader が読んだレコードを **Engine に直接同期供給**する（決定的で、仮想時間も不要）。
* バックプレッシャは入口の有界チャネルと「コアが 1 件ずつ処理する」ことで成立する。
* 中間段をタスク分割しない理由: 各段はマイクロ秒オーダーの軽量処理であり、パイプライン並列の利益よりチャネル越しの順序・終端の推論コストが上回るため。段が CPU バウンド化した場合に初めてタスク分割を再検討する。

### 3.1. ウォーターマーク

「時刻 T (100ns) まで完了した」を表す単調増加値。**チャネルメッセージではなく関数呼び出しで伝える**: aggregator が前進を返したとき、Engine が dedup のパージと downsampler のフラッシュを直接呼ぶ。イベント処理 → 確定処理の順序はコア内の逐次実行で自明に保証される。

* **時間駆動の出所をモード別に分離する**:
  * **リアルタイム**: 各センサー受信 `timestamp_100ns` の最小ウォーターマーク＋タイムアウトで前進（壁時計は定期ドレイン `advance_wallclock` のみに使用）。
  * **再計算**: ファイル内データの `timestamp_100ns` のみが時間を駆動（壁時計非依存）。これにより決定的テストと冪等性を両立する。

### 3.2. 確定処理の量子化

ウォーターマークはイベント毎に前進し得るが、確定処理（dedup パージ・保留全ブロックの走査を伴う downsampler フラッシュ）を毎イベント実行すると高レート時に無駄が大きい。Engine は**前回確定時刻から `min(block_size, dedup_ttl)` 以上前進したときのみ**確定処理を実行する。量子化はデータ駆動なので再計算の決定性は保たれる。終端（`finish`）は無条件に確定する。

### 3.3. 時間の型付け

100ns 値の生 `i64` をモジュール間で受け渡さない。時刻 `Ts100ns` と幅 `Dur100ns` の newtype を `time` モジュールに定義し、ms → 100ns 換算と飽和演算をそこに閉じ込める。終端処理で `Ts100ns::MAX` 近傍の算術が発生するため、ウォーターマークに対する加減算は**型の実装として必ず飽和**させる（呼び出し側の注意に依存しない）。

## 4. 各コアモジュールの厳密な実装仕様

### 4.0. AMQP Receiver & Payload Adapter (腐敗防止層)

* **役割**: 受信したバイナリデータをパースし、共通の内部構造体 `RawSensorEvent` に変換してから次段へ渡す。
* **入力データ形式**（リトルエンディアン）:
  * **固定長レコード（20 バイト）** — MQ・ファイルで共通:

    | フィールド | 型 | 内容 |
    |---|---|---|
    | 相対時刻 | `u32` | **当該分内**の相対時刻 `0 ~ 599_999_999`（×100ns） |
    | ビット列 | `u8 × 14` | Mode-S ペイロード（56bit は末尾ゼロ埋め） |
    | 波高値 | `u16` | 信号強度（後述のエンコード） |

  * **MQ メッセージ** = ヘッダ + レコード列（1 秒分をまとめて配送、毎秒 1 通）:
    * ヘッダ = `[sensor_id: ASCII 4B（英大文字2字+数字2字）][時刻: ASCII 14B（%Y%m%d%H%M%S, UTC）]` = 18 バイト。
    * ヘッダ以降に 20 バイトレコードが N 個続く（body 長 = `18 + 20 × N`）。
    * **絶対時刻** = ヘッダ時刻を**分に切り捨てた分頭** + レコードの相対時刻。ヘッダの秒はレコードが分内相対のため分頭算出にのみ用いる。
  * **保存ファイル**: **センサー毎 × 1 分毎**に `{時刻:%Y%m%d%H%M}{sensor_id}.spkx`（UTC）で保存。中身は MQ と同一の 20 バイトレコード列（ヘッダ無し）。sensor_id・分頭は**ファイル名から**得る。
* **センサー識別**: `sensor_id` は **MQ ヘッダ／ファイル名**の 4 文字コードから取得する（`SensorId([u8;4])`）。
* **波高値のデコード**: メッセージパルス中の最大電界強度を `-255 ~ 0 dBm` で表す。16bit の上位 8bit が絶対値整数部のビット反転、下位 8bit が絶対値小数部のビット反転。`-1 * (65535 - value) / 256` で dBm 整数値を得る。
* **フレーム長判定**:
  * 長さ判定は **先頭5bitの DF 値**で行う（DF < 16 → 56bit、DF ≧ 16 → 112bit）。
  * 短フレーム判定時は「末尾56bitがゼロであること」を**整合性チェック**として併用し、ゼロでなければ不正として計数破棄する。
* **内部ドメインモデル**:

```rust
pub struct SensorId([u8; 4]); // 英大文字2字 + 数字2字

pub enum ModeSFrame {
    Short([u8; 7]),  // 56bit
    Long([u8; 14]),  // 112bit
}

pub struct RawSensorEvent {
    pub sensor_id: SensorId,   // MQ ヘッダ／ファイル名由来
    pub ts: Ts100ns,           // 分頭 + レコード相対時刻（絶対時刻）
    pub rssi_dbm: i16,         // 波高値をデコードした dBm
    pub frame: ModeSFrame,
}
```

* **隔離設計**: バイナリフォーマットの構造（エンディアン、ヘッダ、フィールド順序、波高値エンコード、ゼロパディング規約、ファイル名規則）に依存するコードは、このモジュールに完全に閉じ込めること。後続パイプラインは必ず `RawSensorEvent` を受け取って処理する。
* **AMQP 消費セマンティクス**:
  * ヘッダ検証（sensor_id が宣言済み）後に **ack**、その後レコード列を順次投入する（取りこぼしは許容、二重時は UPSERT が吸収）。ヘッダ不正・未宣言センサーは計数して ack 破棄。
  * prefetch (QoS) を設定しバックプレッシャを掛ける（既定 1000）。
  * 接続断は指数バックオフで自動再接続する。

### 4.1. Watermark Aggregator (Tick制御)

* **役割**: 各センサーからの1秒毎のデータをバッファリングし、「時刻T」の完了を判定して `Watermark(T)` を次段へ送る。
* **センサー集合**: 起動時引数 `--sensors`（4 文字コードのカンマ区切り、例 `AB01,AB02`）で**静的に宣言**する。未宣言の `sensor_id`（ヘッダ／ファイル名由来）が来た場合は警告ログ＋計数。
* **完了条件**: 宣言された全センサーの最新受信時刻（ウォーターマーク）が時刻Tを超えた場合。または一部センサーの遅延がタイムアウト閾値（`--watermark-timeout-ms`）を超過した場合は、当該センサーを除外して前進する。

### 4.2. Exact TTL Deduplicator (重複排除)

* **役割**: 空間伝播遅延（TDOA）による同一メッセージの重複を排除する（Tick跨ぎ対応）。**先着データのみ**を次段へ流す（信号強度比較は行わない）。
* **アルゴリズム**:
  * メッセージの生ビット列（`ModeSFrame`）をキーとする。ModeSCode は使用しない。
  * `HashSet<ModeSFrame>` を用いて $O(1)$ で先着判定。先着データのみ次段へ流す。
  * `BTreeMap<Ts100ns, Vec<ModeSFrame>>` を用いて有効期限（既定 `--dedup-ttl-ms` = 50ms）を管理する。ウォーターマーク確定時に失効済みのビット列を HashSet からパージする。

### 4.3. Aircraft State Manager (状態保持とCPR計算)

* **役割**: CPR（Odd/Even）のペアリングと、メタデータの状態マージを行う。
* **CPR 状態所有（流儀A）**: 機体ごとに直近の even/odd メッセージを**自作キャッシュで保持**し、rs1090 の**ステートレス関数**を呼び出す。
  * 空中: `airborne_position(even, odd)`（グローバル復号）。
  * 地上: `surface_position_with_reference(msg, ref)`（参照座標は `--surface-ref-lat/lon` で設定注入、45NM以内）。
  * **空中はグローバル復号（even/odd ペア確立）が取れるまで座標を出力しない**。
  * rs1090 は I/O を持たないステートレスな数学エンジンとして扱い、状態管理はすべて本マネージャが持つ。
* **スナップショット保持フィールドと更新規則**:

  | フィールド | 保持 | 更新規則 |
  |---|---|---|
  | `call_sign` | スナップショット | **N-Strikeデバウンス** |
  | `squawk` | スナップショット | **N-Strikeデバウンス＋緊急即時** |
  | `alt`（気圧高度） | スナップショット | **Last-Write-Wins（即時・デバウンス無し）** |
  | `lat`/`lon` | Odd/Evenキャッシュ | グローバル復号で確定 |
  | `on_ground` | 非保持 | 位置確定行ごとに算出 |

* **メタデータのデバウンス（N-Strikeルール）** — `call_sign` と `squawk` のみに適用:
  * 現在の確定値と異なる新候補値が**途切れず連続N回**（既定 `--debounce-n` = 3）受信された場合のみ上書き更新する。途中に別値が割り込んだらカウンタはその値用にリセットする。
  * `call_sign` と `squawk` は**独立した候補カウンタ**を持つ。
  * 確定済みの値と同じ値の再受信はカウンタに無関係（無視）。
  * **緊急Squawk (`7700`, `7600`, `7500`)** は1回受信で即時確定（閾値無視・カウンタリセット）。緊急状態の**解除**（通常スコークへの復帰）は通常のN-Strike規則に従う。
* **`alt`**: 連続変化する量のためデバウンスせず、最新値で即時上書き（Last-Write-Wins）。

### 4.4. Downsampler (時間ブロック集約)

* **役割**: 高頻度な座標データを、指定された時間ブロック（ミリ秒）に丸め、DB書き込み量を削減する。
* **初期化バリデーション**: ブロックサイズ `S`（`--block-size-ms`）は起動時引数で指定。次をすべて満たさなければ起動時パニック。
  * `S <= 1000` の場合、`1000 % S == 0`。
  * `S > 1000` の場合、`S % 1000 == 0`。
  * **加えて常に `60000 % S == 0`**（ブロックが1分ファイル境界を跨がず、再計算の1分単位 DELETE→INSERT と整合させるため）。
* **集約アルゴリズム**:
  * 絶対時間のブロックID `block_id = timestamp_100ns / (S * 10_000)` を算出。行の `timestamp` はブロック境界に丸める。
  * `HashMap<(ModeSCode, BlockID), Record>` を使用し「最終値上書き（Last-Write-Wins）」で更新する。ウォーターマーク確定時（§3.2 の量子化に従う）に、終端が確定時刻以前のブロックIDをフラッシュする。

### 4.5. DB Writer (マイクロバッチ登録)

* **役割**: `sqlx` を用いたバルク UPSERT。
* **バッチ**: 「**500件 または 1秒**のどちらか先」でフラッシュ。`INSERT ... VALUES (...),(...) ON CONFLICT (mode_s_code, timestamp) DO UPDATE`（COPY は ON CONFLICT 非対応のため複数行 INSERT を使用）。プールは小さく設定（max 5）。
* **INSERT のチャンク化**: PostgreSQL の bind パラメータ上限（65,535 個 / 文）に達しないよう、複数行 INSERT は **1 文あたり最大 5,000 行**（8 パラメータ × 5,000 = 40,000）にチャンクする。再計算の 1 分 INSERT も同一トランザクション内でチャンクする。
* **書き込み失敗時**: リアルタイムの UPSERT 失敗は短い間隔で**有界リトライ（3 回）**し、それでも失敗した場合はバッチを破棄してログ・計数する（取りこぼし許容の設計に整合。冪等キーにより再送しても安全）。
* **再計算時の冪等性**: 1分ファイル単位で、対象1分（`[T, T+60s)`）のレコードを全機体まとめて `DELETE` し、その後 `INSERT` するトランザクションを実行する（各1分を独立トランザクションでコミット）。

## 5. 再計算モード（Batch）の特殊起動シーケンス

再計算は **レンジ指定**（`--recompute-from <UTC> --recompute-to <UTC>`、分精度）。CPRの欠落やTDOAの重複漏れを防ぐため、以下のウォームアップを厳密に実装する。ウォームアップはレンジ先頭で1回のみ行う。

1. **フェーズ1（DBからの状態復元）**: 対象開始時刻から `--restore-lookback-seconds` 秒遡って観測された全機体の最新 `call_sign`, `squawk` を DB から取得し、`AircraftStateManager` を初期化する。
   * 注: 進行中の N-Strike カウンタは復元されないが、フェーズ2のプレランニングで収束するため実用上問題ない。
2. **フェーズ2（1分前ファイルのプレランニング）**: 対象開始時刻の「1分前」のファイルをデコーダーパイプラインに流す。このフェーズ中は DB Writer への送信を **MUTE（破棄）** し、メモリ状態（CPR の Odd/Even キャッシュや Deduplicator の HashSet）を温めることのみを行う。
3. **フェーズ3（本処理）**: 対象開始時刻に入った瞬間に DB Writer を **UNMUTE** し、各1分を独立トランザクションで DELETE→INSERT する。
* **ストリーミング書き込み**: 出力を全量メモリに蓄積せず、**確定ウォーターマークが分終端を越えた分から順次** DELETE→INSERT する。ブロックは 1 分境界を跨がない（§4.4）ため、分終端 ≦ 確定ウォーターマークならその分の全レコードが排出済みであることが保証される。メモリ使用量はウォーターマーク遅延分に有界となり、途中失敗時も書き込み済みの分は完結している（再実行は冪等）。パイプライン終端で残りの分をまとめて書く（レコードの無い分も DELETE のみ実行してクリーンアップする）。
* **ファイル特定（パステンプレート）**: 実データはセンサー・日付で入れ子ディレクトリに配置される（例 `{YYYYMM}/{sensor}/{YYYYMMDD}/spkx/{YYYYMMDDHHmm}{sensor}.spkx`）。この配置を柔軟に表現するため、`--data-dir` からの相対パスを**テンプレート** `--data-path-template` で与える:
  * `{sensor}` はセンサーコードに展開。
  * chrono の **strftime 指定子**（`%Y %m %d %H %M` 等）は対象分の UTC 日時に展開。
  * 既定は後方互換のフラット配置 `%Y%m%d%H%M{sensor}.spkx`。上記入れ子は `%Y%m/{sensor}/%Y%m%d/spkx/%Y%m%d%H%M{sensor}.spkx`。
  * テンプレートは起動時に strftime 妥当性を検証する（不正指定子はエラー）。
* **各分について宣言済み全センサーのファイル**を読み、`RawSensorEvent` の絶対時刻でマージソートしてから投入する（複数センサーを跨いでもウォーターマークの単調性を保つため）。欠損ファイルは警告計数してスキップする（その分は DELETE のみ実行され DB 上は空になる）。

## 6. 起動引数一覧

| 引数 | 用途 | 既定 |
|---|---|---|
| `--mode realtime\|recompute` | 稼働モード | — |
| `--sensors <code,...>` | 静的センサー集合（4 文字コード列、例 `AB01,AB02`） | — |
| `--amqp-url` | AMQP 接続先 | — |
| `--db-url` | PostgreSQL 接続先 | — |
| `--block-size-ms <S>` | ダウンサンプリングブロック（ms） | — |
| `--dedup-ttl-ms` | Dedup TTL | 50 |
| `--watermark-timeout-ms` | 遅延センサー除外閾値 | — |
| `--debounce-n` | N-Strike 回数 | 3 |
| `--surface-ref-lat` / `--surface-ref-lon` | 地上CPR参照座標（設定注入） | — |
| `--recompute-from` / `--recompute-to <UTC>` | 再計算レンジ（分精度） | — |
| `--restore-lookback-seconds` | フェーズ1遡及秒 | — |
| `--data-dir` | 生バイトログ格納ディレクトリ（ルート） | — |
| `--data-path-template` | `--data-dir` からの相対パステンプレート（`{sensor}` + strftime） | `%Y%m%d%H%M{sensor}.spkx` |

**バリデーション規則**（起動時にエラーで停止）:

* `--sensors` の各要素は 4 文字コード（英大文字2字+数字2字）。**規約外・重複はエラー**。
* `--recompute-from` / `--recompute-to` は**分境界（秒・サブ秒 = 0）でなければエラー**（1分ファイル・分単位 DELETE 範囲・ブロック境界との整合のため）。
* `--block-size-ms` は §4.4 の3条件。`--surface-ref-lat/lon` は両方同時指定のみ可。

## 7. 実装の優先順位とテスト要件

1. **Decoder & CPRロジック**: I/Oを持たない純粋関数として `decode`/`cpr` モジュールに隔離し、単体テストを徹底する。rs1090 はステートレスに利用する。
2. **Deduplicator & Downsampler**: メモリリークがないこと、Tick進行に伴うパージ・フラッシュ処理が正しく動くことを仮想時間（`tokio::time::pause`）を用いてテストする。
3. **Actor結合**: 最後にチャネルで各タスクを接続し、統合テストを行う。

## 8. 可観測性

`tracing` でログ出力する。最低限、以下の破棄・処理カウンタを記録する（Prometheus 等のエクスポータは初期スコープ外）。

* `rejected_crc`（CRC不一致）
* `unsupported_df`（対象外DF）
* `malformed_short_frame`（末尾56bit非ゼロ等の不整合）
* `dropped_late`（タイムアウト確定後に到着した遅延データ）
* `unknown_sensor`（未宣言 sensor_id）
* 処理レート・DB書込（UPSERT）件数

## 9. クレート構成

単一バイナリ＋モジュール分割。

* `domain` — `RawSensorEvent` / `ModeSFrame` / `PositionRecord` 等の内部ドメインモデル。
* `time` — `Ts100ns` / `Dur100ns` の時間 newtype（ms 換算・飽和演算を集約）。
* `config` — 起動引数パース・バリデーション。
* `decode` / `cpr` — I/O 非依存の純粋関数群（rs1090 ラッパ・CPR）。単体テスト対象。
* `receiver` — AMQP Receiver（再接続込み）＋腐敗防止層／再計算時の File Reader。
* `aggregator` — Watermark Aggregator。
* `dedup` — Exact TTL Deduplicator。
* `state` — Aircraft State Manager（CPRキャッシュ・N-Strike）。
* `downsampler` — 時間ブロック集約。
* `engine` — 同期処理コア（aggregator→dedup→state→downsampler の合成、確定処理の量子化）。
* `writer` — DB Writer（UPSERT・再計算トランザクション）。
