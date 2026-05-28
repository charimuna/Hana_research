import sqlite3
import pandas as pd
import re

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(db_path)

# Background_summaryから取得
df = pd.read_sql("""
    SELECT Patient_ID, Study_ID, memo
    FROM Background_summary
    WHERE memo IS NOT NULL
""", conn)

# サムスカ導入日抽出
def extract_tolvaptan_date(memo):

    # YYYY/MM/DD
    pattern_day = r'サムスカ導入\(?(\d{4})/(\d{1,2})/(\d{1,2})\)?'
    match_day = re.search(pattern_day, memo)

    if match_day:
        y, m, d = match_day.groups()

        return {
            "ind_date": f"{y}-{m.zfill(2)}-{d.zfill(2)}",
            "date_precision": "day",
            "raw_match": match_day.group(0)
        }

    # YYYY/MM
    pattern_month = r'サムスカ導入\(?(\d{4})/(\d{1,2})\)?'
    match_month = re.search(pattern_month, memo)

    if match_month:
        y, m = match_month.groups()

        return {
            "ind_date": f"{y}-{m.zfill(2)}-01",
            "date_precision": "month",
            "raw_match": match_month.group(0)
        }

    return {
        "ind_date": None,
        "date_precision": None,
        "raw_match": None
    }

# 展開
result = df["memo"].apply(extract_tolvaptan_date)

df["ind_date"] = result.apply(lambda x: x["ind_date"])
df["date_precision"] = result.apply(lambda x: x["date_precision"])
df["raw_match"] = result.apply(lambda x: x["raw_match"])

# 抽出できた患者のみ
df_tolvaptan = df[df["ind_date"].notna()][[
    "Patient_ID",
    "Study_ID",
    "ind_date",
    "date_precision",
    "raw_match"
]].reset_index(drop=True)

print(f"該当患者数: {len(df_tolvaptan)}")
print(df_tolvaptan.head(20))

# テーブル保存
df_tolvaptan.to_sql(
    "tolvaptan_study",
    conn,
    if_exists="replace",
    index=False
)

conn.commit()
conn.close()

print("完了：tolvaptan_study テーブル作成")