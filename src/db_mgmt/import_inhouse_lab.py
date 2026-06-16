"""
import_inhouse_lab.py
院内検査_統合.csv → SQLite lab_raw テーブル インポートスクリプト
差分インポート対応（Patient_ID + sample_date + item_code + value_raw の重複スキップ）
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

# ============================================================
# 設定
# ============================================================
CSV_PATH  = Path("/Users/muna/Hana_research/data/raw/AllLab/院内検査_統合.csv")
DB_PATH   = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")  # ← 必要に応じて変更

# 重複判定キー（この組み合わせが既存DBにあればスキップ）
DEDUP_KEYS = ["Patient_ID", "sample_date", "item_code", "value_raw"]

# ============================================================
# 1. CSV 読み込み
# ============================================================
print(f"[1] CSV 読み込み: {CSV_PATH}")

df = pd.read_csv(
    CSV_PATH,
    encoding="utf-8-sig",      # UTF-8 BOM付き（Excelが出力するCSVに多い）
    dtype=str,                 # 全列を文字列で読み込む（数値変換はしない）
    keep_default_na=False,     # 空欄を NaN にしない → 空文字で保持
)

print(f"    読み込み行数: {len(df):,} 行")
print(f"    列名: {list(df.columns)}")

# ============================================================
# 2. カラム rename
# ============================================================
df = df.rename(columns={
    "患者ID":   "Patient_ID",
    "検査日":   "sample_date",
    "検査項目": "item_name",
    "検査コード": "item_code",
    "検査値":   "value_raw",
    "単位":     "unit",
    "H/L":      "hl_flag",
    "低値":     "ref_low",
    "高値":     "ref_high",
    "コメント": "comment",
    "検査会社": "lab_company",
    "施設":     "facility",
    # source_file はCSV側にすでに存在すればそのまま使用
})

# ============================================================
# 3. 不要列を除去し、lab_raw に合わせた列順に整形
# ============================================================
# lab_raw の列（raw_id と imported_at は後で付与）
TARGET_COLS = [
    "Patient_ID", "sample_date", "item_name", "item_code",
    "value_raw", "unit", "hl_flag", "ref_low", "ref_high",
    "comment", "lab_company", "facility", "source_file"
]

# source_file 列が CSV にない場合は CSV ファイル名を入れる
if "source_file" not in df.columns:
    df["source_file"] = CSV_PATH.name

# 余分な列を落とす（患者名 など）
df = df[[c for c in TARGET_COLS if c in df.columns]]

# 想定列が全部揃っているか確認
missing = [c for c in TARGET_COLS if c not in df.columns]
if missing:
    print(f"[警告] 以下の列が見つかりません: {missing}")

# imported_at を付与
df["imported_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"[2] rename・整形完了: {len(df):,} 行 × {len(df.columns)} 列")

# ============================================================
# 4. SQLite 接続 & 差分インポート
# ============================================================
print(f"[3] DB 接続: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

# lab_raw テーブルが存在しない場合は作成
cur.execute("""
CREATE TABLE IF NOT EXISTS lab_raw (
    raw_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    Patient_ID  TEXT,
    sample_date TEXT,
    item_name   TEXT,
    item_code   TEXT,
    value_raw   TEXT,
    unit        TEXT,
    hl_flag     TEXT,
    ref_low     TEXT,
    ref_high    TEXT,
    comment     TEXT,
    lab_company TEXT,
    facility    TEXT,
    source_file TEXT,
    imported_at TEXT
)
""")
conn.commit()

# 既存レコードの重複キーセットを取得
print("[4] 既存データの重複キー取得中...")
existing_keys = set()
key_cols_str = ", ".join(DEDUP_KEYS)
for row in cur.execute(f"SELECT {key_cols_str} FROM lab_raw"):
    existing_keys.add(row)

print(f"    既存レコード数（キーセット）: {len(existing_keys):,}")

# 差分フィルタリング
def make_key(row):
    return tuple(str(row.get(k, "")) for k in DEDUP_KEYS)

mask_new = df.apply(lambda r: make_key(r) not in existing_keys, axis=1)
df_new   = df[mask_new].copy()

print(f"[5] 新規インポート対象: {len(df_new):,} 行 / スキップ（重複）: {(~mask_new).sum():,} 行")

# ============================================================
# 5. INSERT
# ============================================================
if len(df_new) == 0:
    print("[完了] 新規データなし。インポートをスキップしました。")
else:
    insert_cols = [
        "Patient_ID", "sample_date", "item_name", "item_code",
        "value_raw", "unit", "hl_flag", "ref_low", "ref_high",
        "comment", "lab_company", "facility", "source_file", "imported_at"
    ]
    placeholders = ", ".join(["?"] * len(insert_cols))
    col_str      = ", ".join(insert_cols)
    sql = f"INSERT INTO lab_raw ({col_str}) VALUES ({placeholders})"

    # 存在しない列は空文字で補完
    for c in insert_cols:
        if c not in df_new.columns:
            df_new[c] = ""

    records = df_new[insert_cols].values.tolist()
    cur.executemany(sql, records)
    conn.commit()
    print(f"[完了] {len(df_new):,} 件をインポートしました。")

conn.close()
print("DB 接続クローズ。処理終了。")