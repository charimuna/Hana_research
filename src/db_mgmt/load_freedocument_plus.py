import pandas as pd
import sqlite3
from pathlib import Path

# === パス設定 ===
NEW_CSV = Path("/Users/muna/Hana_research/data/raw/FirstSamari/pdf_summary_150331_260610.csv")
DB_PATH = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")

# === 1. 読み込み ===
df = pd.read_csv(NEW_CSV, encoding="utf-8-sig")

# === 2. 列名クリーニング ===
df.columns = (
    df.columns
    .str.strip()
    .str.replace("　", "", regex=False)
    .str.replace("\ufeff", "", regex=False)
)

print("列名:", df.columns.tolist())

# === 3. 必要列抽出 ===
df = df[["患者ID", "作成日", "書類名", "内容"]]

# === 4. 日付変換 ===
date_str = df["作成日"].astype(str).str.strip()

if date_str.str.match(r"^\d{8}$").all():
    df["作成日"] = pd.to_datetime(date_str, format="%Y%m%d", errors="coerce")
elif date_str.str.match(r"^\d{5}$").all():
    df["作成日"] = pd.to_datetime(
        date_str.astype(int), origin="1899-12-30", unit="D", errors="coerce"
    )
else:
    df["作成日"] = pd.to_datetime(date_str, errors="coerce")

df["作成日"] = df["作成日"].dt.strftime("%Y-%m-%d")

# === 5. リネーム ===
df = df.rename(columns={
    "患者ID": "Patients_ID",
    "作成日": "Date",
    "書類名": "document_type",
    "内容": "text_data"
})

# Study_ID列を追加（空欄）
df["Study_ID"] = None

# === document_typeに「外来」を含む行を除外 ===
before = len(df)
df = df[~df["document_type"].str.contains("外来", na=False)].reset_index(drop=True)
print(f"新CSV件数: {before} → 外来除外後: {len(df)}")

# === 6. 重複チェック & 追記 ===
conn = sqlite3.connect(DB_PATH)

# 既存テーブルのキーを取得
existing_keys = pd.read_sql(
    "SELECT Patients_ID, Date, document_type FROM Freedocument",
    conn
)

# マージして新規レコードのみ抽出
merged = df.merge(
    existing_keys,
    on=["Patients_ID", "Date", "document_type"],
    how="left",
    indicator=True
)
new_rows = df[merged["_merge"] == "left_only"].copy()

print(f"既存件数: {len(existing_keys)}")
print(f"重複除外後 追加件数: {len(new_rows)}")

if len(new_rows) == 0:
    print("追加するレコードはありませんでした。")
else:
    new_rows.to_sql(
        "Freedocument",
        conn,
        if_exists="append",
        index=False
    )
    print("Done: Freedocument に追記完了。")

conn.close()