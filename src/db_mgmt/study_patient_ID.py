import sqlite3
import pandas as pd

# =========================
# path settings
# =========================

RESEARCH_DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"

LINKAGE_DB = "/Volumes/linkage_secure/hana_linkage.db"

# =========================
# read research DB
# =========================

conn = sqlite3.connect(RESEARCH_DB)

df = pd.read_sql_query("""
SELECT DISTINCT
    Patient_ID,
    Study_ID
FROM Patient_Master
WHERE Study_ID IS NOT NULL
ORDER BY Patient_ID
""", conn)

conn.close()

# =========================
# validation
# =========================

if df["Patient_ID"].isnull().any():
    raise ValueError("NULL Patient_ID exists")

if df["Study_ID"].isnull().any():
    raise ValueError("NULL Study_ID exists")

if df["Patient_ID"].duplicated().any():
    raise ValueError("Duplicate Patient_ID exists")

if df["Study_ID"].duplicated().any():
    raise ValueError("Duplicate Study_ID exists")

# =========================
# create linkage DB
# =========================

link_conn = sqlite3.connect(LINKAGE_DB)

link_conn.execute("""
DROP TABLE IF EXISTS linkage
""")

link_conn.execute("""
CREATE TABLE linkage (
    Patient_ID INTEGER PRIMARY KEY,
    Study_ID TEXT UNIQUE NOT NULL
)
""")

df.to_sql(
    "linkage",
    link_conn,
    if_exists="append",
    index=False
)

# index
link_conn.execute("""
CREATE INDEX IF NOT EXISTS idx_study_id
ON linkage(Study_ID)
""")

link_conn.commit()

# =========================
# verification
# =========================

check = pd.read_sql_query("""
SELECT *
FROM linkage
LIMIT 10
""", link_conn)

print("\n=== linkage preview ===")
print(check)

count = pd.read_sql_query("""
SELECT COUNT(*) AS n
FROM linkage
""", link_conn)

print("\n=== total rows ===")
print(count)

link_conn.close()

print("\nDone.")