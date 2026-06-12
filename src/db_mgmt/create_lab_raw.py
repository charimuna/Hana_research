import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

# =====================
# paths
# =====================

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

csv_path = "/Users/muna/Hana_research/data/raw/AllLab/DS180208.csv"

# =====================
# connect sqlite
# =====================

conn = sqlite3.connect(db_path)

print("Connected to SQLite")

# =====================
# create table
# =====================

create_table_sql = """
CREATE TABLE IF NOT EXISTS lab_raw (

    raw_id INTEGER PRIMARY KEY AUTOINCREMENT,

    Patient_ID TEXT,

    sample_date TEXT,

    item_name TEXT,
    item_code TEXT,

    value_raw TEXT,

    unit TEXT,

    hl_flag TEXT,

    ref_low TEXT,
    ref_high TEXT,

    comment TEXT,

    lab_company TEXT,
    facility TEXT,

    source_file TEXT,
    imported_at TEXT
);
"""

conn.execute(create_table_sql)

conn.commit()

print("lab_raw table checked")

# =====================
# read csv
# =====================

df = pd.read_csv(csv_path, dtype=str)

print("CSV loaded")
print(df.shape)

# =====================
# drop unnecessary column
# =====================

if "Unnamed: 0" in df.columns:
    df = df.drop(columns=["Unnamed: 0"])

# =====================
# rename columns
# =====================

df = df.rename(columns={

    "患者ID": "Patient_ID",
    "検査日": "sample_date",

    "検査項目": "item_name",
    "検査コード": "item_code",

    "検査値": "value_raw",

    "単位": "unit",

    "H/L": "hl_flag",

    "低値": "ref_low",
    "高値": "ref_high",

    "コメント": "comment",

    "検査会社": "lab_company",
    "施設": "facility"
})

# =====================
# add metadata
# =====================

df["source_file"] = Path(csv_path).name

df["imported_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# =====================
# keep only required columns
# =====================

df = df[[
    "Patient_ID",
    "sample_date",
    "item_name",
    "item_code",
    "value_raw",
    "unit",
    "hl_flag",
    "ref_low",
    "ref_high",
    "comment",
    "lab_company",
    "facility",
    "source_file",
    "imported_at"
]]

# =====================
# insert into sqlite
# =====================

df.to_sql(
    "lab_raw",
    conn,
    if_exists="append",
    index=False
)

print("Inserted into lab_raw")

# =====================
# row count check
# =====================

count = conn.execute(
    "SELECT COUNT(*) FROM lab_raw"
).fetchone()[0]

print(f"Total rows in lab_raw: {count}")

# =====================
# close
# =====================

conn.close()

print("Done")