import sqlite3
import pandas as pd

# パス
db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
output_path = "/Users/muna/Hana_research/output/脳血.csv"

# 接続
conn = sqlite3.connect(db_path)

# SQL（修正済み）
query = """
SELECT 
    u.Patient_ID,
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM first_diag b
            WHERE b.Patient_ID = u.Patient_ID
              AND b.category LIKE '%脳血管疾患%'
        ) THEN 1
        ELSE 0
    END AS 脳血
FROM unex_study u
ORDER BY u.Patient_ID
"""

# 実行
df = pd.read_sql_query(query, conn)

# CSV出力
df.to_csv(output_path, index=False, encoding="utf-8-sig")

conn.close()