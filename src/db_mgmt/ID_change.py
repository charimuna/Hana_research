import sqlite3
import pandas as pd

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(DB_PATH)

# -----------------------------
# 1. Patient_Master 読み込み
# -----------------------------
pm = pd.read_sql_query("""
SELECT Patient_ID
FROM Patient_Master
ORDER BY Patient_ID
""", conn)

# -----------------------------
# 2. Study_ID 発番
# -----------------------------
pm["Study_ID"] = [
    f"P{i:06d}" for i in range(1, len(pm) + 1)
]

# -----------------------------
# 3. linkage テーブル作成
# -----------------------------
pm.to_sql(
    "study_id_linkage",
    conn,
    if_exists="replace",
    index=False
)

# -----------------------------
# 4. 各テーブルへ Study_ID 列追加
# -----------------------------
tables = {
    "Patient_Master": "Patient_ID",
    "Background_summary": "Patient_ID",
    "Freedocument": "Patients_ID",
    "event": "Patient_ID",
    "first_diag": "Patient_ID",
    "intervention_history": "Patient_ID",
    "unex_study": "Patient_ID",
    "unexpected_death": "Patient_ID"
}

for table, pid_col in tables.items():

    print(f"\nProcessing: {table}")

    # 既存列確認
    cols = pd.read_sql_query(
        f"PRAGMA table_info({table})",
        conn
    )

    if "Study_ID" not in cols["name"].values:

        conn.execute(
            f"ALTER TABLE {table} "
            f"ADD COLUMN Study_ID TEXT"
        )

    # 更新
    update_sql = f"""
    UPDATE {table}
    SET Study_ID = (
        SELECT s.Study_ID
        FROM study_id_linkage s
        WHERE s.Patient_ID = {table}.{pid_col}
    )
    """

    conn.execute(update_sql)
    conn.commit()

print("\nDone.")

# -----------------------------
# 5. 確認表示
# -----------------------------
check = pd.read_sql_query("""
SELECT Patient_ID, Study_ID
FROM Patient_Master
LIMIT 10
""", conn)

print(check)

conn.close()