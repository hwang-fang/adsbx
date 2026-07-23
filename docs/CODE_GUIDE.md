# コード解説ガイド

本ドキュメントは、レビュー用に各ファイル・主要な構造体・関数の役割と、
**一見して分かりにくい重要ポイント・注意点・工夫**をまとめたものです。
仕様の意思決定背景は [`DESIGN.md`](./DESIGN.md) を参照してください。

---

## 0. 全体像

### データフロー

```
リアルタイム:
[AMQP] ──► receiver task ──mpsc──► Engine 駆動ループ ──mpsc──► writer task ──► [PostgreSQL]
（腐敗防止層 wire で ヘッダ+20Bレコード列 → RawSensorEvent）│
                                              └ 同期合成: aggregator → dedup → state → downsampler

再計算（チャネルなし・完全同期）:
[センサー毎1分ファイル.spkx] ──► read_sensor_minute_file ──► ts マージソート
    ──► Engine.process ──► 分バケット ──► recompute_minute
```

処理の中核（aggregator/dedup/state/downsampler）は **I/O を持たない同期コア `Engine`** に
合成されており、Tokio タスクは I/O を持つ端（AMQP receiver / DB writer）のみです。
ウォーターマークはチャネルメッセージではなく**関数呼び出し**で伝わります。

### モジュール一覧

| ファイル | レイヤ | 役割 |
|---|---|---|
| `time.rs` | 純粋 | 時間 newtype（`Ts100ns`/`Dur100ns`、ms換算・飽和演算を集約） |
| `domain.rs` | 純粋 | 内部ドメインモデル（型のみ） |
| `config.rs` | 純粋 | 起動引数パース・バリデーション |
| `wire.rs` | 純粋(I/O隣接) | 腐敗防止層：生バイト列 ↔ ドメイン |
| `decode.rs` | 純粋 | rs1090 ラッパ：1 フレーム → 正規化結果 |
| `cpr.rs` | 純粋 | CPR 座標計算（rs1090 ステートレス関数の薄いラッパ） |
| `dedup.rs` | 純粋 | TTL 重複排除 |
| `aggregator.rs` | 純粋 | ウォーターマーク算出 |
| `state.rs` | 純粋 | 機体状態（CPRペアリング・N-Strike・スナップショット） |
| `downsampler.rs` | 純粋 | 時間ブロック集約 |
| `engine.rs` | 純粋（結合） | 同期処理コア：上記4段の合成＋確定処理の量子化 |
| `metrics.rs` | 横断 | カウンタ（可観測性） |
| `receiver.rs` | I/O | AMQP 消費（再接続込み）・トポロジ宣言・ファイル読み込み |
| `writer.rs` | I/O | PostgreSQL への UPSERT / 再計算トランザクション |
| `app.rs` | 結合 | モード別オーケストレーション |
| `main.rs` | 結合 | エントリポイント・モード分岐 |
| `examples/publish.rs` | 検証 | AMQP 疎通確認用パブリッシャ |

> **設計の芯**: I/O を持たない純粋ロジック（time/decode/cpr/dedup/aggregator/state/
> downsampler/engine/wire/config）と、I/O（receiver/writer/app）を明確に分離しています。
> 前者はすべて単体テスト済みで、**Tokio ランタイム無しでテストできます**（Engine の
> 結合テストも同期関数のテスト）。

---

## 1. `time.rs` — 時間の型付け

| 要素 | 役割 |
|---|---|
| `struct Ts100ns(i64)` | Unix エポック起点・100ns 単位・GPS 規律 UTC の時刻 |
| `struct Dur100ns(i64)` | 100ns 単位の時間幅。`from_ms(u32)` で ms から換算 |
| `Ts100ns::{MIN, MAX}` | 番兵値（`MAX` は終端ドレインに使用） |
| `saturating_add / saturating_sub / abs_delta` | 飽和算術 |
| `fn now_100ns()` | 壁時計 → `Ts100ns`（リアルタイムドレイン用） |
| `fn from_datetime(dt)` | `DateTime<Utc>` → `Ts100ns`（分境界比較用） |

**注意点**
- 生 `i64` の 100ns 値・ms 値をモジュール間で受け渡さないための newtype。**ms → 100ns の
  換算はこのファイルの `from_ms` に一本化**されています。
- 終端処理で `Ts100ns::MAX` が全段に渡るため、**加減算は型の実装として必ず飽和**します。
  旧実装では各所の `saturating_add` 忘れが panic 源でしたが（実DB検証で実際に発生）、
  現在は呼び出し側の注意に依存しません。回帰テスト `saturates_at_extremes` あり。

---

## 2. `domain.rs` — 内部ドメインモデル

| 要素 | 役割 |
|---|---|
| `struct SensorId([u8;4])` | センサー識別子（英大文字2字+数字2字）。`from_ascii` で検証構築 |
| `enum ModeSFrame { Short([u8;7]), Long([u8;14]) }` | Mode-S フレーム。56bit と 112bit を型で区別 |
| `ModeSFrame::as_bytes()` | デコードに渡す生スライス |
| `struct RawSensorEvent` | 腐敗防止層通過後の共通イベント（sensor_id / ts / rssi_dbm / frame） |
| `struct PositionRecord` | 位置確定時に生成される DB 行の元データ |
| `fn mode_s_hex(u32) -> String` | ICAO 24bit → `"%06X"` 文字列（DB の `mode_s_code`） |

**注意点**
- `SensorId` は MQ ヘッダ／ファイル名由来の 4 文字コード。固定長 `Copy` なので aggregator の `HashMap` キーにそのまま使えます。旧実装の routing key 由来 `u16` から置き換わりました。
- `ModeSFrame` が `Hash + Eq` を導出しているのは、**dedup の `HashSet` キーにそのまま使う**ためです。固定長配列なので `Copy` でき、所有権を気にせず使い回せます。
- 旧実装にあった `Msg<T>`（インバンドウォーターマーク）は**廃止**。ウォーターマークは Engine 内の関数呼び出しになったため、チャネルを流れるのは実データのみです。

---

## 3. `config.rs` — 起動引数

| 要素 | 役割 |
|---|---|
| `enum Mode { Realtime, Recompute }` | 稼働モード |
| `struct Cli` / `struct Config` | clap の生引数 / バリデーション済み設定 |
| `Config::from_cli()` | モード別必須引数・センサー集合・surface_ref・レンジ整合をチェック |
| `fn validate_block_size(s)` | ブロックサイズ制約の検証 |
| `fn ensure_minute_aligned(name, dt)` | 再計算レンジの分精度検証 |

**重要ポイント**
- `validate_block_size` の制約は3段:
  1. `S <= 1000` なら `1000 % S == 0`
  2. `S > 1000` なら `S % 1000 == 0`
  3. **常に `60000 % S == 0`** ← これが肝。**ブロックが 1 分ファイル境界を跨がない**ことを保証し、再計算の「1 分単位 DELETE→INSERT」と整合させます。
- `--sensors` は **4 文字コードのカンマ区切り**（例 `AB01,AB02`）。規約外（英大文字2字+数字2字でない）・重複はエラー。宣言済み集合は AMQP バインド・未宣言センサー検出・再計算のファイル列挙に使います。
- `--recompute-from/to` は**分境界（秒・サブ秒 = 0）必須**。1 分ファイル名・分単位 DELETE 範囲・ブロック境界の三者と整合させるため。
- `surface_ref` は lat/lon **両方揃って初めて** `Some`。片方だけはエラー。

---

## 4. `wire.rs` — 腐敗防止層（最重要の隔離点）

| 要素 | 役割 |
|---|---|
| 定数 `PAYLOAD_LEN=14` / `RECORD_LEN=20` / `AMQP_HEADER_LEN=18` | フレーミングのサイズ |
| `enum FrameReject { BadLength, MalformedShortFrame }` / `enum HeaderReject { BadLength, BadSensorId, BadTimestamp }` | 破棄理由 |
| `fn parse_frame(&[u8;14])` | DF 値でフレーム長を判定し `ModeSFrame` 化 |
| `fn signal_dbm(u16) -> i16` | 波高値 u16 → dBm デコード |
| `fn parse_record(sensor_id, minute_start, &[u8])` | 20B レコード → 絶対時刻付き `RawSensorEvent` |
| `fn parse_amqp_header(&[u8])` | MQ ヘッダ → `(sensor_id, 分頭, レコード列スライス)` |
| `fn parse_minute(&str)`（内部） | `%Y%m%d%H%M%S` → UTC 分頭 |

**注意点・工夫**
- **バイナリ仕様依存はこのファイルに完全隔離**。レコード構造・ヘッダ・波高値エンコード・ゼロ埋め・ファイル名規則の変更はここだけ差し替えれば後続は無変更。
- フレーム長は **先頭 5bit の DF 値**で判定（`df < 16` → 56bit短 / `>= 16` → 112bit長）。ゼロ埋めは**整合性チェック**（短フレームなのに末尾非ゼロ → `MalformedShortFrame`）として併用。
- フレーミング（リトルエンディアン）:
  - **固定長レコード（20B）** = `[相対時刻 u32(分内100ns)][payload 14B][波高値 u16]`（MQ・ファイル共通）
  - **MQ メッセージ** = `[sensor_id ASCII 4B][時刻 ASCII 14B]`（18B ヘッダ）+ 20B レコード × N。**絶対時刻 = ヘッダを分に切り捨てた分頭 + レコード相対時刻**。
  - **保存ファイル** `{分}{sensor_id}.spkx` = 20B レコード列のみ（sensor_id・分頭はファイル名由来）。
- **波高値のデコード**（`signal_dbm`）: 上位/下位 8bit がそれぞれ絶対値整数部/小数部のビット反転。`-(65535 - value)/256` で dBm 整数値（小数切り捨て）。範囲 -255〜0。
- **分頭の切り出し**は `%Y%m%d%H%M%S` を**固定幅で手動スライス**（`chrono` の `%Y` 貪欲一致で連結文字列を誤読しないため）。秒は分頭算出で捨てる（レコードが分内相対のため）。

---

## 5. `decode.rs` — フレームデコード（rs1090 ラッパ）

| 要素 | 役割 |
|---|---|
| `enum DecodeReject { CrcInvalid, ParseError, UnsupportedDf }` | 破棄理由（カウンタ対応） |
| `struct Decoded { icao, kind }` / `enum DecodedKind` | 正規化結果 |
| `fn decode_frame(&ModeSFrame)` | 1 フレーム → `Decoded`。**状態を持たない** |

**重要ポイント（一見分かりにくい点）**
- **ICAO アドレスの取得元が DF で異なる**:
  - DF17(`ExtendedSquitterADSB`) → `adsb.icao24`
  - DF18(`ExtendedSquitterTisB`) → `cf.aa`（ControlField）
  - **DF4/5/20/21 → `message.crc`**。rs1090 は短フレームの AP（パリティ overlay）から ICAO を逆算し、それを `crc` フィールドに格納する仕様。`crc` という名前だが実体は ICAO アドレス。
- **`on_ground` の算出**: DF17 のみ `capability == AG_GROUND (0x04)` で地上判定。DF18 は CA を持たないため常に `false`（地上判定は surface 由来のみ）。
- **スコークの整形**: `IdentityCode(u16)` を `format!("{:04x}")`。各桁が 0–7 なので HEX 表記＝オクタル表記になり、`7700`/`7600`/`7500` の緊急判定が文字列比較で成立。
- **`Ignored` と `UnsupportedDf` の違い**: 対象 DF だが使わないメッセージ（速度 BDS09 等）は `Ok(Ignored)` でカウントせず、対象外 DF のみ `Err(UnsupportedDf)` で計上。
- `classify_error` はエラー文字列に `"CRC"` を含むかのヒューリスティック。**rs1090 更新時に文言が変わると壊れる**ため、テスト `rejects_corrupted_adsb_with_crc` で固定してあります。
- `AC13Field(u16)` は rs1090 側で既に**フィート**に変換済み。`as i32` で DB の `alt INT` へ。

---

## 6. `cpr.rs` — CPR 座標計算

| 要素 | 役割 |
|---|---|
| `fn global_airborne(a, b)` | even/odd ペアからグローバル復号 |
| `fn local_surface(sp, ref_lat, ref_lon)` | 参照座標から地上位置をローカル復号 |

**最重要の落とし穴**
- rs1090 の `airborne_position(oldest, latest)` は **第 2 引数（latest）の位置を返す**。
  そのため `state.rs` では `global_airborne(&other_pos, &pos)`（キャッシュ済みを oldest、**現イベントを latest**）の順で呼び、「今受信したメッセージの時刻における座標」を得ています。引数順を逆にすると古い側の座標が返ります（テスト `global_decode_matches_known_pair` で固定）。
- 同一 parity 2 つを渡すと `None`。

---

## 7. `dedup.rs` — TTL 重複排除

| 要素 | 役割 |
|---|---|
| `struct Deduplicator { ttl, seen: HashSet, expiry: BTreeMap }` | 状態 |
| `fn accept(frame, ts) -> bool` | 先着なら `true`（通過）、重複なら `false`（破棄） |
| `fn purge(watermark)` | TTL 切れのビット列を `seen` から除去 |

**工夫・注意点**
- `HashSet<ModeSFrame>` で **O(1) 先着判定**、`BTreeMap<失効時刻, Vec<frame>>` で失効管理という二本立て。TDOA で別センサーから届く同一ビット列を先着のみ通過させます。
- `purge` の `split_off(&(watermark+1))` は「`<= watermark` を取り出す」ための +1。`Ts100ns::MAX` でも `saturating_add` で溢れません（`purge_with_max_watermark_does_not_overflow`）。
- メモリリーク防止は `no_memory_leak_after_full_purge` で担保。

---

## 8. `aggregator.rs` — ウォーターマーク算出

| 要素 | 役割 |
|---|---|
| `struct WatermarkAggregator { timeout, latest: HashMap, watermark }` | 状態 |
| `fn observe(sensor_id, ts) -> Option<Ts100ns>` | イベント観測。前進時は新値を返す |
| `fn advance_to(frontier) -> Option<Ts100ns>` | 外部フロンティア（壁時計）でドレイン |

**重要ポイント**
- ウォーターマーク = **「フロンティア（全センサー最大時刻）から timeout 以内のアクティブなセンサーの最小時刻」**。遅延しすぎたセンサーは分母から除外して前進します。
- **単調増加**（後退しない）。過去イベントが来ても据え置き。
- **モード別の時間駆動**: リアルタイムは `observe`＋`advance_to(now)`（Engine 経由の壁時計ドレイン）、再計算は `observe` のみ＝完全データ駆動で決定的。
- **運用上の注意**: `advance_to` は `now - timeout` まで進めるため、**データの ts が壁時計より大きく遅れていると `dropped_late` で捨てられます**。GPS 同期の本番（ts ≈ 壁時計）では無害ですが、テスト送信では ts をほぼ現在時刻にする必要があります。

---

## 9. `state.rs` — 機体状態管理（ロジックの中核）

| 要素 | 役割 |
|---|---|
| `struct Debounced` / `observe(value, n)` / `force(value)` | N-Strike デバウンス（便名・スコーク）と緊急即時確定 |
| `struct AircraftState` | 機体ごとの even/odd キャッシュ＋メタデータ |
| `struct AircraftStateManager` | 全機体の状態。デコードと状態マージの統括 |
| `fn restore(icao, cs, sq)` | 再計算フェーズ1: DB からの状態シード |
| `fn process(&ev)` | 1 イベント処理。位置確定時のみ `PositionRecord` を返す |

**重要ポイント・工夫**
- **CPR の状態はすべて自前で保持**（流儀A）。rs1090 はステートレスな数学関数としてのみ使用。
- **N-Strike セマンティクス**: 確定値と同じ再受信は無視（進行中の候補カウンタはリセット＝「連続」が途切れた扱い）。異なる候補が**連続 N 回**で確定。緊急スコーク `7500/7600/7700` は `force` で即時確定。`call_sign` と `squawk` は独立カウンタ。
- **フィールドごとに更新規則が違う**: `call_sign`/`squawk` → N-Strike、`alt` → 即時 Last-Write-Wins（連続変化量のため）、`on_ground` → 保持せず行ごとに算出。
- **空中はグローバル復号が成立するまで座標を出さない**。反対 parity がキャッシュにあり、かつ**ペア窓 `PAIR_WINDOW = 10秒` 以内**のときだけ復号（`stale_pair_does_not_decode`）。
- **地上（surface）は参照座標が必須**。未設定なら復号せず行を作らない。
- **`alt` の微妙な実装上の挙動（要レビュー）**: `handle_airborne` の `alt: pos.alt.map(...).or(st.alt)` は BDS05 自身の気圧高度を優先し、無ければ `st.alt`（DF4/20 由来）にフォールバック。BDS05 の alt は `st.alt` に**取り込まない**。「その位置フィックス時点の高度を使う」実用重視の挙動です。

---

## 10. `downsampler.rs` — 時間ブロック集約

| 要素 | 役割 |
|---|---|
| `struct Downsampler { block, blocks: HashMap<(u32,i64), PositionRecord> }` | 状態 |
| `fn ingest(rec)` | `(mode_s, block_id)` 単位で Last-Write-Wins。ts をブロック境界に丸める |
| `fn flush(watermark)` | 終端が watermark 以下のブロックを確定排出 |
| `fn drain_all()` | 残存全ブロックを排出（終端の念押し） |

**重要ポイント**
- `block_id = ts.div_euclid(block)`。**`div_euclid`** により負の timestamp でも正しく丸まります。
- `flush` は**ブロック終端 `(block_id+1)*block <= watermark`** のときのみ排出（`flush_respects_block_end_vs_watermark`）。
- `60000 % S == 0`（config 側で保証）により、各ブロックは必ず 1 分内に収まります。
- `flush` の呼び出し頻度は Engine 側の量子化（§11）で制御されます。ここ自体は毎回全ブロックを走査します。

---

## 11. `engine.rs` — 同期処理コア（旧 pipeline.rs の後継）

| 要素 | 役割 |
|---|---|
| `struct Engine` | aggregator/dedup/state/downsampler ＋確定状態を所有 |
| `fn process(ev) -> Vec<PositionRecord>` | 1 イベントの同期処理。確定行を返す |
| `fn advance_wallclock(now)` | 壁時計ドレイン（リアルタイムのみ） |
| `fn finish()` | 終端: `confirm(MAX)` ＋ `drain_all` |
| `fn confirmed() -> Ts100ns` | 確定済みウォーターマーク（再計算のストリーミング書き込みが参照） |
| `fn confirm(wm)`（内部） | 量子化チェック → `dedup.purge` → `ds.flush` |

**重要ポイント・工夫**
- **処理順序**: dropped_late 判定 → `agg.observe` → `dedup.accept` → `state.process` → `ds.ingest` → **最後に** `confirm(wm)`。イベント処理が確定処理より先に完了するため、旧パイプラインの「Event → Watermark 送信順の厳守」に相当する保証が逐次実行で自明に成立します。
- **確定処理の量子化**（DESIGN §3.2）: ウォーターマークはイベント毎に前進し得るが、`confirm` は前回から `min(block_size, dedup_ttl)` 以上進んだときのみ実行。高レート時に「保留全ブロック走査」が毎イベント走るのを防ぎます。**データ駆動の量子化なので再計算は決定的なまま**です。`finish` の `MAX` は必ず通過します。
- **重複イベントもウォーターマークは前進させる**（`accept` が false でも `observe` 済み）。旧パイプラインと同じ意味論です。
- テストは同期関数のテストで、**Tokio 不要**: E2E（ペア→丸め済み1行）、TDOA 重複排除、ウォーターマーク前進によるブロック排出の 3 本。

---

## 12. `metrics.rs` — 可観測性

- 各種 `AtomicU64` カウンタ（`Ordering::Relaxed`）を `Arc` 共有。`amqp_reconnects` で再接続回数も計上。
- `snapshot` を `app.rs` が定期（30秒）と終了時にログ出力。

---

## 13. `receiver.rs` — AMQP / ファイル入力

| 要素 | 役割 |
|---|---|
| `fn run_amqp(cfg, tx, metrics)` | 再接続ループ（指数バックオフ） |
| `fn consume_session(...)`（内部） | 1 接続セッション: 接続→トポロジ宣言→消費 |
| `fn declare_topology(channel, cfg)` | exchange/queue/binding（センサーコード）の冪等宣言 |
| `fn read_sensor_minute_file(path, sensor_id, minute_start, metrics)` | センサー毎1分ファイル → `Vec<RawSensorEvent>` |

**重要ポイント・工夫**
- **自動再接続**（DESIGN §4.0）: 接続失敗・接続断とも 1s→2s→…→30s 上限の指数バックオフで再試行。**接続確立に成功していたらバックオフをリセット**。`run_amqp` が戻るのはパイプライン入口（`tx`）が閉じたときだけです。
- **メッセージ処理**: 1 メッセージ = ヘッダ + N レコード。ヘッダを `parse_amqp_header` で解析し、`sensor_id` が**宣言済み集合に含まれるか**を検証（未宣言 → `unknown_sensor`）。以降 `parse_record` で 20B ずつ複数イベントを投入。routing key は配送用（sensor_id はヘッダから取得）。
- **トポロジ自前宣言**: 冪等・引数不一致時のみエラーで早期検知。binding はセンサーコードを routing key として張ります。
- **ack タイミング**: ヘッダ受理（宣言済みセンサー）時点で ack し、その後レコード列を投入（取りこぼし許容・二重時は UPSERT が吸収）。ヘッダ不正・未宣言センサーは計数して ack 破棄（再配送ループ防止）。
- `prefetch`(QoS) でバックプレッシャ。
- `read_sensor_minute_file` は**欠損ファイルを警告して空を返しスキップ**。`chunks_exact(20)` で端数を無視（端数は警告）。sensor_id・分頭は呼び出し側（ファイル名）から与える。

---

## 14. `writer.rs` — PostgreSQL 書き込み

| 要素 | 役割 |
|---|---|
| `fn to_datetime(Ts100ns) -> DateTime<Utc>` | 100ns 値 → UTC 日時 |
| `const MAX_ROWS_PER_INSERT = 5000` | 1 文あたりの行数上限 |
| `fn upsert_batch(rows)` | リアルタイムのバルク UPSERT（チャンク実行） |
| `fn recompute_minute(minute_start, rows)` | 再計算: 1 分 DELETE→INSERT トランザクション |
| `fn restore_states(start, end)` | 機体ごと最新 (call_sign, squawk) を取得 |

**重要ポイント**
- `to_datetime` は `div_euclid`/`rem_euclid` で**負値でも端数が正規化**されます。
- **INSERT のチャンク化**: PostgreSQL の bind パラメータ上限（65,535/文）に対し、8 パラメータ × 5,000 行 = 40,000 で頭打ち。`recompute_minute` も**同一トランザクション内で**チャンクします。`upsert_batch` のチャンク間はトランザクションで括りませんが、UPSERT は冪等なので途中失敗・再送に安全です。
- **バルク UPSERT は `QueryBuilder`**（行数可変・実行時クエリ）。**静的クエリ（DELETE / restore SELECT）は `sqlx::query!` マクロ**でコンパイル時照合し、`.sqlx/` によりオフライン／CI でもビルド可能。
  - `restore_states` の `mode_s_code AS "mode_s_code!"` は「NOT NULL 確定」を sqlx に明示する記法。
- `recompute_minute` は **1 分を DELETE → INSERT の 1 トランザクション**。0 件でも DELETE は実行 → データが消えた分のクリーンアップになる。

---

## 15. `app.rs` — モード別オーケストレーション

| 要素 | 役割 |
|---|---|
| 定数 `BATCH_SIZE=500` / `BATCH_INTERVAL=1s` / `FLUSH_ATTEMPTS=3` | 調整パラメータ |
| `fn run_realtime(cfg, metrics)` | リアルタイム稼働 |
| `fn writer_consumer` / `fn flush` | 500 件 or 1 秒のバッチ UPSERT（有界リトライ付き） |
| `fn run_recompute(cfg, metrics)` | 再計算（3 フェーズ・ストリーミング書き込み） |
| `fn read_minute_all_sensors` | 指定分の全センサーファイルを読み ts でマージソート |
| `fn collect` / `fn write_ready_minutes` | 分バケット蓄積と確定分の順次書き出し |
| `fn minute_path(dir, dt, sensor)` | `{分}{sensor}.spkx` パス生成 |

**重要ポイント**
- **リアルタイム**: receiver タスク（AMQP→`tx_in`）と writer タスク（`rx_db`→UPSERT）を spawn し、メインループが `select!` で「イベント受信 → `engine.process`」「ドレイン tick → `advance_wallclock`」「Ctrl-C」を捌く。終了時は receiver を止め、`engine.finish()` の残存行を writer に流してからドレイン。
- **flush の有界リトライ**: 一時的な DB 障害に 3 回まで再試行（間 500ms）、使い切ったらバッチを破棄してログ（UPSERT 冪等なので再送安全・取りこぼし許容の設計に整合）。
- **再計算はチャネルなしの完全同期ループ**。3 フェーズ:
  1. **フェーズ1**: `restore_states` で DB から確定メタデータをシード。
  2. **フェーズ2**: 1 分前の全センサーファイルを `engine.process` に流して CPR/dedup を温める。出力は `collect` の `[from, to)` フィルタで自然に捨てられる（明示的な MUTE フラグ不要）。
  3. **フェーズ3**: 対象レンジを 1 分ずつ投入し、**各分の投入後に `write_ready_minutes`** — 分終端 ≦ `engine.confirmed()` になった分から順次 `recompute_minute`（DELETE→INSERT）してバケットから除去。
- **センサー毎ファイルのマージ**: 入力がセンサー毎 `.spkx` に分割されたため、`read_minute_all_sensors` が 1 分ぶんの全センサーファイルを読み、**絶対時刻でソート**してから投入します。センサー間で時刻が前後してもウォーターマークの単調性（遅延イベントの `dropped_late` 誤破棄）を防ぐためです。
- **ストリーミング書き込みの保証**: ブロックは分境界を跨がない（config 保証）＋確定ウォーターマーク以前のブロックは排出済み（Engine 保証）なので、「分終端 ≦ confirmed」ならその分の全行が揃っています。メモリはウォーターマーク遅延分に有界で、途中失敗しても書き込み済みの分は完結（再実行は冪等）。
- 終端は `engine.finish()` を回収後、`Ts100ns::MAX` で残り全分を書く（**欠損分の DELETE 含む**）。

---

## 16. `main.rs` / `examples/publish.rs`

- `main.rs`: `tracing_subscriber` 初期化（`RUST_LOG`、既定 `info`）→ `Config` 検証 → モード分岐。
- `examples/publish.rs`: トポロジ宣言＋既知の even/odd ペア（＋TDOA 重複）を新フォーマット（ヘッダ+20Bレコード）で publish する検証補助。`SKIP_TOPOLOGY=1` / `DELETE_ONLY=1` で動作切替。

---

## 17. 横断的な設計上の要点（レビュー時に押さえる点）

1. **決定性（再計算）**: 時間は完全にデータ駆動（壁時計非依存）。確定処理の量子化もデータ駆動なので決定的。コアが同期関数のため、テストに仮想時間も Tokio も不要。
2. **冪等性**: `UNIQUE(mode_s_code, timestamp)` ＋ UPSERT（リアルタイム）／1 分 DELETE→INSERT（再計算）。何度実行しても収束。
3. **状態所有の明確化**: CPR・メタデータ・dedup・ウォーターマークの状態はすべて自前。rs1090 はステートレス利用。
4. **腐敗防止層**: バイナリ仕様変更の影響は `wire.rs` に限定。
5. **オーバーフロー安全**: 終端で `Ts100ns::MAX` が流れるが、時刻算術は `time.rs` の newtype が**型として飽和**するため、新規コードが生 `i64` に戻さない限り安全。
6. **障害復旧**: AMQP は指数バックオフで自動再接続（`amqp_reconnects` で計数）。DB 書き込みは有界リトライ後に破棄（冪等キーで再送安全）。
7. **既知の運用上の注意**: 壁時計ドレインと `dropped_late` の相互作用（§8）。GPS 同期前提では無害。
8. **テスト**: 42 件（純粋ロジックの単体＋ Engine 結合の E2E、すべて同期）。実 DB（PostgreSQL）・実ブローカー（RabbitMQ）での疎通も確認済み。
