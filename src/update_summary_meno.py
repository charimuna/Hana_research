import sqlite3
import pandas as pd

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
csv_path = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260419.csv"

# CSVのmemo読み込み
df_csv = pd.read_csv(csv_path, encoding="cp932", low_memory=False)
df_csv = df_csv[["患者ID", "重要メモ"]].rename(columns={"患者ID": "Patient_ID", "重要メモ": "memo"})

# SQLite接続
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

updated = 0
skipped = 0

for _, row in df_csv.iterrows():
    pid = row["Patient_ID"]
    memo = row["memo"]

    if pd.isna(pid):
        skipped += 1
        continue

    cursor.execute("""
        UPDATE Background_summary
        SET memo = ?
        WHERE Patient_ID = ?
          AND Patient_ID IS NOT NULL
    """, (None if pd.isna(memo) else memo, pid))

    updated += cursor.rowcount

conn.commit()
conn.close()

print(f"完了：{updated} 行更新、{skipped} 行スキップ")