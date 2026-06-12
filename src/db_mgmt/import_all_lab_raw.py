import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

# =====================
# paths
# =====================

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

lab_dir = Path("/Users/muna/Hana_research/data/raw/AllLab")

log_path = "/Users/muna/Hana_research/src/import_errors.log"

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

# =====================
# get imported files
# =====================

existing_files_query = """
SELECT DISTINCT source_file
FROM lab_raw
"""

existing_files = pd.read_sql_query(
    existing_files_query,
    conn
)

imported_files = set(existing_files["source_file"].dropna())

print(f"Already imported files: {len(imported_files)}")

# =====================
# csv files
# =====================

csv_files = sorted(lab_dir.glob("*.csv"))

print(f"Found CSV files: {len(csv_files)}")

# =====================
# counters
# =====================

import_count = 0
skip_count = 0
error_count = 0

# =====================
# import loop
# =====================

for csv_file in csv_files:

    source_file = csv_file.name

    # -----------------
    # skip imported
    # -----------------

    if source_file in imported_files:

        print(f"SKIP: {source_file}")

        skip_count += 1

        continue

    try:

        print(f"IMPORT: {source_file}")

        # -----------------
        # read csv
        # -----------------

        df = pd.read_csv(csv_file, dtype=str)

        # -----------------
        # drop unnamed
        # -----------------

        if "Unnamed: 0" in df.columns:

            df = df.drop(columns=["Unnamed: 0"])

        # -----------------
        # rename columns
        # -----------------

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

        # -----------------
        # metadata
        # -----------------

        df["source_file"] = source_file

        df["imported_at"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # -----------------
        # keep columns
        # -----------------

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

        # -----------------
        # insert
        # -----------------

        df.to_sql(
            "lab_raw",
            conn,
            if_exists="append",
            index=False
        )

        conn.commit()

        import_count += 1

    except Exception as e:

        error_count += 1

        error_message = (
            f"{datetime.now()} | "
            f"{source_file} | "
            f"{str(e)}\n"
        )

        print(f"ERROR: {source_file}")

        with open(log_path, "a", encoding="utf-8") as f:

            f.write(error_message)

# =====================
# create indexes
# =====================

index_sql_list = [

    """
    CREATE INDEX IF NOT EXISTS idx_lab_patient
    ON lab_raw(Patient_ID)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_lab_date
    ON lab_raw(sample_date)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_lab_item
    ON lab_raw(item_code)
    """
]

for sql in index_sql_list:

    conn.execute(sql)

conn.commit()

# =====================
# final summary
# =====================

total_rows = conn.execute(
    "SELECT COUNT(*) FROM lab_raw"
).fetchone()[0]

print("\n========== SUMMARY ==========")

print(f"Imported files : {import_count}")

print(f"Skipped files  : {skip_count}")

print(f"Error files    : {error_count}")

print(f"Total rows     : {total_rows}")

# =====================
# close
# =====================

conn.close()

print("Done")