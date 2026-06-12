import sqlite3

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# ① カラム追加（すでにあればエラーになるのでtryで回避）
try:
    cursor.execute("ALTER TABLE first_diag ADD COLUMN heart_failure INTEGER;")
except sqlite3.OperationalError:
    pass

# ② 初期化（全例0）
cursor.execute("UPDATE first_diag SET heart_failure = 0;")

# ③ 「心不全」を含む場合のみ1
cursor.execute("""
UPDATE first_diag
SET heart_failure = 1
WHERE diagnosis LIKE '%心不全%'
   OR diagnosis LIKE '%心 不全%';
""")

conn.commit()
conn.close()