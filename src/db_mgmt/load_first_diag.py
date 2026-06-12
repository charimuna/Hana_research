import pandas as pd
import sqlite3
from pathlib import Path

# === パス設定（安定版）===
BASE_DIR = Path(__file__).resolve().parent

csv_path = BASE_DIR.parent / "data/processed/first_dis_with_category.csv"
db_path = BASE_DIR.parent / "data/db/Hana_Research.db"

# === 1. 読み込み ===
df = pd.read_csv(csv_path, encoding="utf-8-sig")

# === 2. 列名整理 ===
df.columns = df.columns.str.strip().str.replace("　", "")

# === 3. 必要列だけ抽出＆リネーム ===
df = df[["患者ID", "作成日", "病名", "カテゴリ"]].rename(columns={
    "患者ID": "ID",
    "作成日": "date",
    "病名": "diagnosis",
    "カテゴリ": "category"
})

# === 4. SQLite接続 ===
conn = sqlite3.connect(db_path)

# === 5. テーブル作成（上書き）===
df.to_sql(
    "first_diag",
    conn,
    if_exists="replace",
    index=False
)

conn.close()

print("Done: first_diag updated with date and category.")