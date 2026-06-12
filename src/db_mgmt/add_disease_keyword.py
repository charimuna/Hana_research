import sqlite3
import pandas as pd
from pathlib import Path

CSV_PATH = Path("/Users/muna/Hana_research/data/derived/disease_keyword_tate.csv")
DB_PATH  = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")

df = pd.read_csv(CSV_PATH, encoding="utf-8")
df = df.rename(columns={"カテゴリ": "category", "キーワード": "keyword"})
df = df[["category", "keyword"]].dropna(how="all")

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS disease_keyword_long (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT,
        keyword  TEXT
    )
""")

cur.execute("DELETE FROM disease_keyword_long")
df.to_sql("disease_keyword_long", con, if_exists="append", index=False)

con.commit()
print(f"Inserted {len(df)} rows into disease_keyword_long.")

cur.execute("SELECT category, keyword FROM disease_keyword_long LIMIT 10")
for row in cur.fetchall():
    print(row)

con.close()