"""coord_viewer.ipynb を生成するためのビルドスクリプト（開発補助用・成果物ではない）。

nbformat を使ってセルを組み立て、tools/coord_viewer/coord_viewer.ipynb を書き出す。
セル内容を Python 文字列として管理することで、レビュー・差分確認をしやすくする。
"""
import nbformat as nbf
from pathlib import Path

nb = nbf.v4.new_notebook()
cells = []


def md(src: str):
    cells.append(nbf.v4.new_markdown_cell(src.strip("\n")))


def code(src: str):
    cells.append(nbf.v4.new_code_cell(src.strip("\n")))


# ---------------------------------------------------------------------------
md(r"""
# adsbx 座標ビューア

`raw_adsb_records` テーブルに登録済みの ADS-B 座標データを、地図上に手軽に可視化するためのノートブックです。

- **DB 接続**はリポジトリルートの `.env` の `DATABASE_URL` から読み込みます（パスワードはここに直書きしません）。
- 先頭の **調整用パラメータ** セルを書き換えるだけで、時間範囲・機体・地上/上空などを絞り込めます。
- 地図背景（タイル）はネット接続時のみ描画し、**オフライン時は自動的に背景なしのプレーン図へフォールバック**します。

VSCode で開き、右上のカーネル選択で `tools/coord_viewer/.venv` を選んでから、上から順に実行してください。
""")

# ---------------------------------------------------------------------------
md(r"""
## 1. 調整用パラメータ

ここだけ書き換えれば描画対象を変えられます。既定値は同梱の実データ（2026-06-10 の日本上空）が写るレンジです。
""")

code(r'''
# === 調整用パラメータ（ここを書き換える） ===================================

# 時間範囲（UTC・ISO8601 文字列）。None にするとその側の制約を外す。
TIME_FROM = "2026-06-10T00:00:00Z"
TIME_TO   = "2026-06-10T00:04:00Z"

# mode_s_code での絞り込み（6桁16進の大文字リスト）。空リスト [] なら全機体。
FILTER_MODE_S = []          # 例: ["899121", "ABAF47"]

# call_sign（便名）での絞り込み。空リスト [] なら全便。前方一致ではなく完全一致。
FILTER_CALL_SIGN = []       # 例: ["EVA667"]

# on_ground の扱い: None=両方 / True=地上のみ / False=上空のみ
ON_GROUND = None

# 取得件数の上限（安全弁）。大きすぎる描画を防ぐ。
ROW_LIMIT = 100000

# 地図背景タイルを取得するか（False にすると常に背景なしのプレーン描画）。
USE_BASEMAP = True

# 図の保存先（None なら保存しない）。ヘッドレス検証ではこのパスに PNG を出す。
OUTPUT_PNG = "map_output.png"

# =============================================================================
print("パラメータ設定 OK")
''')

# ---------------------------------------------------------------------------
md(r"""
## 2. ライブラリ読み込みと DB 接続

`.env` は「このノートブックの親ディレクトリを上へ辿って最初に見つかった `.env`」を使います（＝リポジトリルートの `.env`）。
""")

code(r'''
import os
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as mcolors

# .env を探索して DATABASE_URL を読む（見つからなければ環境変数を利用）。
dotenv_path = find_dotenv(usecwd=True)
if dotenv_path:
    load_dotenv(dotenv_path)
    print(f".env を読み込みました: {dotenv_path}")
else:
    print(".env が見つかりませんでした。環境変数の DATABASE_URL を使用します。")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL が未設定です。リポジトリルートの .env を確認してください。")

# SQLAlchemy(psycopg3) 用に postgres:// を postgresql+psycopg:// へ正規化。
sa_url = DATABASE_URL
if sa_url.startswith("postgres://"):
    sa_url = "postgresql+psycopg://" + sa_url[len("postgres://"):]
elif sa_url.startswith("postgresql://"):
    sa_url = "postgresql+psycopg://" + sa_url[len("postgresql://"):]

engine = create_engine(sa_url)
# パスワードを表示しないよう、接続先の host/db だけ確認。
with engine.connect() as conn:
    who = conn.execute(text("SELECT current_database(), inet_server_addr()::text")).one()
print(f"接続 OK: database={who[0]} host={who[1]}")
''')

# ---------------------------------------------------------------------------
md(r"""
## 3. データ取得

パラメータをバインド変数として安全に渡し、`lat`/`lon` が NULL の行は除外します。
""")

code(r'''
conditions = ["lat IS NOT NULL", "lon IS NOT NULL"]
params = {}

if TIME_FROM is not None:
    conditions.append("timestamp >= :time_from")
    params["time_from"] = TIME_FROM
if TIME_TO is not None:
    conditions.append("timestamp < :time_to")
    params["time_to"] = TIME_TO
if FILTER_MODE_S:
    conditions.append("mode_s_code = ANY(:mode_s)")
    params["mode_s"] = list(FILTER_MODE_S)
if FILTER_CALL_SIGN:
    conditions.append("call_sign = ANY(:call_sign)")
    params["call_sign"] = list(FILTER_CALL_SIGN)
if ON_GROUND is not None:
    conditions.append("on_ground = :on_ground")
    params["on_ground"] = ON_GROUND

where_clause = " AND ".join(conditions)
sql = text(f"""
    SELECT timestamp, mode_s_code, lat, lon, alt, call_sign, squawk, on_ground
    FROM raw_adsb_records
    WHERE {where_clause}
    ORDER BY mode_s_code, timestamp
    LIMIT :row_limit
""")
params["row_limit"] = ROW_LIMIT

df = pd.read_sql(sql, engine, params=params, parse_dates=["timestamp"])
print(f"取得行数: {len(df)}")
df.head()
''')

# ---------------------------------------------------------------------------
md(r"""
## 4. GeoDataFrame へ変換

`lon`/`lat` から Point ジオメトリを作り、CRS=EPSG:4326（WGS84 緯度経度）を設定します。
""")

code(r'''
if df.empty:
    raise RuntimeError("該当データが 0 件でした。調整用パラメータ（特に TIME_FROM/TIME_TO）を見直してください。")

gdf = gpd.GeoDataFrame(
    df.copy(),
    geometry=[Point(xy) for xy in zip(df["lon"], df["lat"])],
    crs="EPSG:4326",
)
print(f"GeoDataFrame: {len(gdf)} 行 / CRS={gdf.crs}")
gdf.head()
''')

# ---------------------------------------------------------------------------
md(r"""
## 5. 要約表

機体数・行数・時間範囲・便名一覧などをまとめて確認します。
""")

code(r'''
n_aircraft = gdf["mode_s_code"].nunique()
t_min = gdf["timestamp"].min()
t_max = gdf["timestamp"].max()
call_signs = sorted({c for c in gdf["call_sign"].dropna().unique() if str(c).strip()})

print("=== 要約 ===")
print(f"総行数        : {len(gdf)}")
print(f"機体数        : {n_aircraft}")
print(f"時間範囲(UTC) : {t_min}  〜  {t_max}")
print(f"高度 alt      : min={gdf['alt'].min()}  max={gdf['alt'].max()}")
print(f"便名一覧      : {', '.join(call_signs) if call_signs else '(なし)'}")

# 機体ごとの内訳表。
summary = (
    gdf.groupby("mode_s_code")
    .agg(
        rows=("timestamp", "size"),
        call_signs=("call_sign", lambda s: ", ".join(sorted({str(x) for x in s.dropna() if str(x).strip()})) or "-"),
        t_start=("timestamp", "min"),
        t_end=("timestamp", "max"),
        alt_min=("alt", "min"),
        alt_max=("alt", "max"),
    )
    .sort_values("rows", ascending=False)
)
summary
''')

# ---------------------------------------------------------------------------
md(r"""
## 6. 地図可視化

- 点を機体（`mode_s_code`）ごとに色分け。
- 機体ごとに時刻順で結んだ**軌跡**（線）を描画。
- 各機体の軌跡末尾に `mode_s_code` と代表便名・高度を注記。
- 背景は Web メルカトル（EPSG:3857）に投影して contextily タイルを重畳。**取得失敗時は背景なしにフォールバック**。
""")

code(r'''
import warnings
from shapely.geometry import LineString
from matplotlib import font_manager

# 図中テキストの言語判定: 日本語グリフを持つフォントがあれば日本語、無ければ
# 文字化け(tofu)を避けるため英語ラベルにフォールバックする（環境非依存で堅牢に）。
_JP_CANDIDATES = ["Noto Sans CJK JP", "IPAexGothic", "IPAGothic", "TakaoGothic",
                  "Hiragino Sans", "Yu Gothic", "MS Gothic", "VL Gothic"]
_installed = {f.name for f in font_manager.fontManager.ttflist}
_jp_font = next((name for name in _JP_CANDIDATES if name in _installed), None)
if _jp_font:
    plt.rcParams["font.family"] = _jp_font
    JP = True
else:
    JP = False
    # 日本語フォントが無い環境ではグリフ欠落警告が大量に出るため抑制。
    warnings.filterwarnings("ignore", message="Glyph .* missing from font")

def L(ja: str, en: str) -> str:
    """日本語フォントがあれば日本語、無ければ英語ラベルを返す。"""
    return ja if JP else en

print("図中フォント:", _jp_font if JP else "日本語フォント無し → 英語ラベルで描画")

# 描画は Web メルカトル(3857)へ投影（タイル背景と整合させるため）。
gdf_3857 = gdf.to_crs(epsg=3857)

aircraft_ids = list(summary.index)  # 行数が多い順
cmap = plt.get_cmap("tab20", max(len(aircraft_ids), 1))
color_map = {ac: mcolors.to_hex(cmap(i)) for i, ac in enumerate(aircraft_ids)}

fig, ax = plt.subplots(figsize=(12, 10))

for ac in aircraft_ids:
    sub = gdf_3857[gdf_3857["mode_s_code"] == ac].sort_values("timestamp")
    color = color_map[ac]

    # 軌跡（2点以上あるときのみ線を描く）。
    if len(sub) >= 2:
        line = LineString([(p.x, p.y) for p in sub.geometry])
        gpd.GeoSeries([line], crs=3857).plot(ax=ax, color=color, linewidth=1.4, alpha=0.7, zorder=2)

    # 点。
    sub.plot(ax=ax, color=color, markersize=14, alpha=0.9, zorder=3)

    # 末尾に注記（便名・高度が分かるように）。
    last = sub.iloc[-1]
    cs = str(last["call_sign"]).strip() if pd.notna(last["call_sign"]) else ""
    label = f"{ac}" + (f" / {cs}" if cs else "") + (f"\nalt={last['alt']}" if pd.notna(last["alt"]) else "")
    ax.annotate(
        label,
        xy=(last.geometry.x, last.geometry.y),
        xytext=(4, 4), textcoords="offset points",
        fontsize=7, color=color,
        bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=color, alpha=0.7),
        zorder=4,
    )

# 凡例（機体色）。
from matplotlib.lines import Line2D
legend_elems = [
    Line2D([0], [0], marker="o", color="w", markerfacecolor=color_map[ac], markersize=8,
           label=f"{ac} ({summary.loc[ac, 'call_signs']})")
    for ac in aircraft_ids
]
ax.legend(handles=legend_elems, title=L("機体 (mode_s_code / 便名)", "aircraft (mode_s / call_sign)"),
          fontsize=7, title_fontsize=8, loc="upper left", framealpha=0.85)

# 背景タイル（ネット依存）。失敗しても図全体は止めない。
basemap_status = L("背景なし（USE_BASEMAP=False）", "no basemap (USE_BASEMAP=False)")
if USE_BASEMAP:
    try:
        import contextily as cx
        cx.add_basemap(ax, source=cx.providers.OpenStreetMap.Mapnik, crs="EPSG:3857")
        basemap_status = L("OpenStreetMap タイル", "OpenStreetMap tiles")
    except Exception as e:
        basemap_status = L(f"背景なしにフォールバック（{type(e).__name__}）", f"no-basemap fallback ({type(e).__name__})")
        print(f"[警告] 背景タイル取得に失敗したためプレーン描画にフォールバックします: {e}")

ax.set_title(
    L(f"adsbx 座標ビューア | 機体数={n_aircraft} 行数={len(gdf)} | 背景: {basemap_status}",
      f"adsbx coord viewer | aircraft={n_aircraft} rows={len(gdf)} | basemap: {basemap_status}"),
    fontsize=11,
)
ax.set_axis_off()
plt.tight_layout()

if OUTPUT_PNG:
    out_path = Path(OUTPUT_PNG)
    fig.savefig(out_path, dpi=130, bbox_inches="tight")
    print(f"図を保存しました: {out_path.resolve()}")

plt.show()
print(f"背景ステータス: {basemap_status}")
''')

# ---------------------------------------------------------------------------
md(r"""
## 7. 参考: 生データの確認

必要に応じて生の `GeoDataFrame` を確認できます（緯度経度は EPSG:4326 のまま）。
""")

code(r'''
gdf[["timestamp", "mode_s_code", "call_sign", "lat", "lon", "alt", "squawk", "on_ground"]].head(20)
''')

nb["cells"] = cells
nb["metadata"] = {
    "kernelspec": {"display_name": "Python 3 (coord_viewer venv)", "language": "python", "name": "python3"},
    "language_info": {"name": "python", "version": "3.12"},
}

out = Path(__file__).parent / "coord_viewer.ipynb"
nbf.write(nb, out)
print(f"書き出し完了: {out}")
