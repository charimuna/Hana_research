import sqlite3
import pandas as pd

# パス
db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
csv_path = "/Users/muna/Hana_research/data/derived/tag_tate_with_HOT.csv"

# CSV読み込み
df = pd.read_csv(csv_path, dtype={"Patient_ID": str})

# カラム変換
df = df.rename(columns={
    "タグ": "category",
    "開始日": "start_date",
    "終了日": "end_date"
})

# 必要カラム
df = df[["Patient_ID", "category", "start_date", "end_date", "ongoing"]]

# --- 日付整形（YYYY-MM-DD） ---
df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

df["start_date"] = df["start_date"].dt.strftime("%Y-%m-%d")
df["end_date"] = df["end_date"].dt.strftime("%Y-%m-%d")

# NaT → None（空欄）
df = df.where(pd.notnull(df), None)

# --- ongoing のルール ---
# 終了日があれば 0、それ以外は空欄
df["ongoing"] = df["end_date"].apply(lambda x: 0 if x is not None else None)

# --- 重複処理 ---
# 同一 (Patient_ID, category, start_date, end_date) 内で
# ongoing=1 があれば優先、なければ最初の1件
df["ongoing_flag"] = df["ongoing"].fillna(0)
df = df.sort_values(by=["Patient_ID", "ongoing_flag"], ascending=[True, False])

df = df.drop_duplicates(
    subset=["Patient_ID", "category", "start_date", "end_date"],
    keep="first"
)

df = df.drop(columns=["ongoing_flag"])

# --- DB書き込み ---
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS intervention_history")

cursor.execute("""
CREATE TABLE intervention_history (
    Patient_ID TEXT,
    category TEXT,
    start_date TEXT,
    end_date TEXT,
    ongoing INTEGER
)
""")

df.to_sql("intervention_history", conn, if_exists="append", index=False)

# 確認
cursor.execute("SELECT COUNT(*) FROM intervention_history")
print("row count:", cursor.fetchone()[0])

conn.commit()
conn.close()