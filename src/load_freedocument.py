import pandas as pd
import sqlite3
from pathlib import Path

# === パス設定 ===
BASE_DIR = Path(__file__).resolve().parent

csv_path = BASE_DIR.parent / "data/raw/FreeDocument_all_raw.csv"
db_path = BASE_DIR.parent / "data/db/Hana_Research.db"

# === 1. 読み込み ===
df = pd.read_csv(csv_path, encoding="utf-8-sig")

# === 2. 列名の完全クリーニング（これが核心）===
df.columns = (
    df.columns
    .str.strip()
    .str.replace("　", "", regex=False)
    .str.replace("\ufeff", "", regex=False)
)

# デバッグ（確認）
print(df.columns.tolist())

# === 3. 必要列抽出 ===
df = df[["患者ID", "作成日", "書類名", "内容"]]

# === 4. 日付変換（Excel対策込み）===
if df["作成日"].astype(str).str.match(r"^\d+$").all():
    df["作成日"] = pd.to_datetime(
        df["作成日"],
        origin="1899-12-30",
        unit="D",
        errors="coerce"
    )
else:
    df["作成日"] = pd.to_datetime(df["作成日"], errors="coerce")

df["作成日"] = df["作成日"].dt.strftime("%Y-%m-%d")

# === 5. リネーム ===
df = df.rename(columns={
    "患者ID": "Patients_ID",
    "作成日": "Date",
    "書類名": "document_type",
    "内容": "text_data"
})

# === 6. SQLite投入 ===
conn = sqlite3.connect(db_path)

df.to_sql(
    "Freedocument",
    conn,
    if_exists="replace",
    index=False
)

conn.close()

print("Done: Freedocument created.")