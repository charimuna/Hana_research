import sqlite3
import pandas as pd

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(db_path)

# --- 1. tolvaptan_study 取得 ---
df_tolv = pd.read_sql("""
    SELECT Patient_ID, Study_ID, ind_date, date_precision, raw_match
    FROM tolvaptan_study
""", conn)

# --- 2. ef_long から最古 visit_date の行を取得 ---
df_ef = pd.read_sql("""
    SELECT Study_ID, visit_date, value, matched_text, final_hf_class
    FROM ef_long
""", conn)

# Study_ID ごとに最古 visit_date の1行（任意）に絞る
df_ef_min = (
    df_ef.sort_values("visit_date")
         .drop_duplicates(subset=["Study_ID"], keep="first")
         [["Study_ID", "visit_date", "value", "matched_text", "final_hf_class"]]
         .reset_index(drop=True)
)

# カラム名を衝突回避のためリネーム
df_ef_min = df_ef_min.rename(columns={
    "visit_date":     "ef_visit_date",
    "value":          "ef_value",
    "matched_text":   "ef_matched_text",
    "final_hf_class": "ef_final_hf_class",
})

# --- 3. Study_ID で LEFT JOIN ---
df_result = df_tolv.merge(
    df_ef_min,
    on="Study_ID",
    how="left"
)

# --- 4. 確認 ---
print(f"tolvaptan_study 行数: {len(df_tolv)}")
print(f"結合後 行数        : {len(df_result)}")
print(f"ef_value が NULL  : {df_result['ef_value'].isna().sum()} 件")
print(df_result[["Patient_ID", "Study_ID", "ind_date",
                 "ef_visit_date", "ef_value", "ef_matched_text", "ef_final_hf_class"]].head(20))

# --- 5. 上書き保存 ---
df_result.to_sql(
    "tolvaptan_study",
    conn,
    if_exists="replace",
    index=False
)

conn.commit()
conn.close()

print("\n完了：tolvaptan_study テーブル更新")