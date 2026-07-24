# adsbx 座標ビューア (coord_viewer)

`raw_adsb_records` テーブルに登録済みの ADS-B 座標データを、Jupyter ノートブック上で
手軽に地図可視化するためのツールです。geopandas + contextily を使い、機体ごとの
点・軌跡を地図タイル上に描画します。

- ノートブック本体: [`coord_viewer.ipynb`](coord_viewer.ipynb)
- DB 接続情報はリポジトリルートの `.env` の `DATABASE_URL` から読み込みます
  （**パスワードをノートブックに直書きしません**）。

---

## セットアップ

Python 3.12 と `venv` を前提とします（グローバルに jupyter を入れず、このディレクトリ配下に閉じます）。

```bash
# リポジトリルートから実行
cd tools/coord_viewer

# 1. venv 作成
python3 -m venv .venv

# 2. 依存インストール
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

# 3. (任意) VSCode のカーネル一覧に出したい場合はカーネル登録
.venv/bin/python -m ipykernel install --user \
    --name adsbx-coord-viewer --display-name "Python 3 (coord_viewer venv)"
```

## VSCode での使い方

1. VSCode で `tools/coord_viewer/coord_viewer.ipynb` を開く。
2. 右上の **カーネル選択** で `tools/coord_viewer/.venv` のインタプリタ
   （または登録した "Python 3 (coord_viewer venv)"）を選ぶ。
3. 先頭の **「1. 調整用パラメータ」** セルを必要に応じて書き換える。
4. 上から順に全セルを実行する。最後のセルで地図が表示され、`map_output.png` が保存される。

### よく変えるパラメータ（先頭セル）

| 変数 | 意味 | 既定値 |
| --- | --- | --- |
| `TIME_FROM` / `TIME_TO` | 取得する時間範囲（UTC・ISO8601）。`None` で無制限 | 2026-06-10 00:00〜00:04Z |
| `FILTER_MODE_S` | mode_s_code（6桁16進）での絞り込み。`[]` で全機体 | `[]` |
| `FILTER_CALL_SIGN` | call_sign（便名）での完全一致絞り込み。`[]` で全便 | `[]` |
| `ON_GROUND` | `None`=両方 / `True`=地上のみ / `False`=上空のみ | `None` |
| `ROW_LIMIT` | 取得件数の上限（安全弁） | `100000` |
| `USE_BASEMAP` | 地図タイル背景を使うか（`False` で常にプレーン描画） | `True` |
| `OUTPUT_PNG` | 図の保存先ファイル名（`None` で保存しない） | `map_output.png` |

既定値は同梱の実データ（2026-06-10 の日本上空・関西〜四国付近、約 1000 行 / 13 機）が写るレンジです。

---

## 機能

- SQL で該当行を取得し、`lon`/`lat` から Point ジオメトリを作成（CRS=EPSG:4326）。
  `lat`/`lon` が NULL の行は除外。
- 点を **機体（mode_s_code）ごとに色分け**し、時刻順に結んだ **軌跡（線）** を描画。
- 各機体の末尾に `mode_s_code / 便名 / 高度(alt)` を注記。凡例に機体色と便名を表示。
- 機体数・行数・時間範囲・便名一覧・機体別内訳などの **要約表** を出力。

## 地図背景の挙動（既知の注意点）

- geopandas 1.0 以降で削除された `gpd.datasets.get_path('naturalearth_lowres')` には
  **依存していません**。
- 背景タイルは `contextily`（OpenStreetMap）を使うため **ネット接続が必要** です。
  取得に失敗した場合は `try/except` で捕捉し、**背景なしのプレーン描画へ自動フォールバック**
  します（オフラインでも図は必ず出ます）。常に背景を無効化したい場合は `USE_BASEMAP=False`。
- 図中のタイトル・凡例テキストは、日本語グリフを持つフォント（Noto Sans CJK JP など）が
  システムに無い環境では **文字化け(tofu)を避けるため自動的に英語ラベルへ切り替わります**。
  データ由来のラベル（mode_s_code / 便名 / alt）は元々 ASCII なので常に読めます。

## ヘッドレス実行（動作確認・CI 用）

```bash
cd tools/coord_viewer
.venv/bin/jupyter nbconvert --to notebook --execute \
    --output /tmp/executed.ipynb coord_viewer.ipynb
```

エラー無く完了し、`map_output.png` が生成されれば OK です。

## ファイル構成

| ファイル | 役割 |
| --- | --- |
| `coord_viewer.ipynb` | 本体ノートブック |
| `requirements.txt` | 検証済みの依存構成（固定バージョン） |
| `README.md` | 本ドキュメント |
| `build_notebook.py` | `coord_viewer.ipynb` を生成する補助スクリプト（保守用。編集はこちらで行うと差分が追いやすい） |
| `map_output.png` | 最後に実行したときの地図（生成物のサンプル） |
