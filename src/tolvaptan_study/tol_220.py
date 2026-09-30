import sqlite3
import pandas as pd
from pathlib import Path
import shutil

# =========================
# ファイルパス
# =========================
DB_PATH = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
CSV_PATH = Path("/Users/muna/Hana_research/src/tolvaptan_study/tol.sama.2026.csv")

# DBバックアップ
BACKUP_PATH = DB_PATH.with_name("Hana_Research_before_tol_update.db")


# =========================
# 1. DBバックアップ
# =========================
if not BACKUP_PATH.exists():
    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"Backup created: {BACKUP_PATH}")
else:
    print(f"Backup already exists: {BACKUP_PATH}")


# =========================
# 2. CSV読み込み
# =========================
df_csv = pd.read_csv(CSV_PATH, encoding="cp932")
df_csv = df_csv.rename(columns={"患者ID": "Patient_ID"})

print("CSV columns:")
print(df_csv.columns.tolist())

print(f"CSV rows: {len(df_csv)}")


# Patient_IDを整数として扱う
df_csv["Patient_ID"] = pd.to_numeric(
    df_csv["Patient_ID"],
    errors="coerce"
).astype("Int64")

# Patient_IDがNULLの行を除外
df_csv = df_csv.dropna(subset=["Patient_ID"]).copy()

# 重複Patient_IDを除去
df_csv = df_csv.drop_duplicates(subset=["Patient_ID"])

print(f"Unique Patient_IDs: {len(df_csv)}")


# =========================
# 3. SQLite接続
# =========================
conn = sqlite3.connect(DB_PATH)

try:
    # =========================
    # 4. Patient_MasterからStudy_ID取得
    # =========================
    patient_ids = df_csv["Patient_ID"].tolist()

    placeholders = ",".join(["?"] * len(patient_ids))

    query = f"""
        SELECT
            Patient_ID,
            Study_ID
        FROM Patient_Master
        WHERE Patient_ID IN ({placeholders})
    """

    df_master = pd.read_sql_query(
        query,
        conn,
        params=patient_ids
    )

    print(f"Patients found in Patient_Master: {len(df_master)}")


    # =========================
    # 5. CSVとPatient_Masterを結合
    # =========================
    df_new = df_csv[["Patient_ID"]].merge(
        df_master,
        on="Patient_ID",
        how="left"
    )

    # Patient_Masterに存在しなかった患者を確認
    missing_master = df_new[df_new["Study_ID"].isna()]

    if len(missing_master) > 0:
        print("\nWARNING: Patient_ID not found in Patient_Master:")
        print(missing_master["Patient_ID"].tolist())


    # =========================
    # 6. 既存 tolvaptan_study を読み込み
    # =========================
    df_old = pd.read_sql_query(
        """
        SELECT
            Patient_ID,
            Study_ID,
            ind_date,
            date_precision,
            raw_match,
            ef_visit_date,
            ef_value,
            ef_matched_text,
            ef_final_hf_class
        FROM tolvaptan_study
        """,
        conn
    )

    print(f"Existing rows: {len(df_old)}")


    # =========================
    # 7. 新規患者だけ抽出
    # =========================
    existing_patient_ids = set(
        df_old["Patient_ID"].dropna().astype(int)
    )

    df_add = df_new[
        ~df_new["Patient_ID"].astype(int).isin(existing_patient_ids)
    ].copy()


    # =========================
    # 8. 新規行を作成
    # =========================
    df_add["ind_date"] = None
    df_add["date_precision"] = None
    df_add["raw_match"] = None
    df_add["ef_visit_date"] = None
    df_add["ef_value"] = None
    df_add["ef_matched_text"] = None
    df_add["ef_final_hf_class"] = None

    df_add = df_add[
        [
            "Patient_ID",
            "Study_ID",
            "ind_date",
            "date_precision",
            "raw_match",
            "ef_visit_date",
            "ef_value",
            "ef_matched_text",
            "ef_final_hf_class"
        ]
    ]

    print(f"New patients to add: {len(df_add)}")


    # =========================
    # 9. 既存＋新規
    # =========================
    df_all = pd.concat(
        [df_old, df_add],
        ignore_index=True
    )


    # =========================
    # 10. Patient_ID PRIMARY KEY用の確認
    # =========================

    # Patient_ID NULLを確認
    null_patient_id = df_all["Patient_ID"].isna().sum()

    if null_patient_id > 0:
        raise ValueError(
            f"Patient_ID is NULL in {null_patient_id} existing rows. "
            "Cannot create Patient_ID PRIMARY KEY."
        )

    # Patient_ID重複を確認
    duplicates = df_all[
        df_all["Patient_ID"].duplicated(keep=False)
    ]

    if len(duplicates) > 0:
        print("Duplicate Patient_ID detected:")
        print(duplicates.sort_values("Patient_ID"))

        raise ValueError(
            "Duplicate Patient_ID exists. "
            "Table was NOT modified."
        )


    # =========================
    # 11. 新テーブル作成
    # =========================
    conn.execute("""
        CREATE TABLE tolvaptan_study_new (
            Patient_ID INTEGER PRIMARY KEY,
            Study_ID TEXT,
            ind_date TEXT,
            date_precision TEXT,
            raw_match TEXT,
            ef_visit_date TEXT,
            ef_value TEXT,
            ef_matched_text TEXT,
            ef_final_hf_class TEXT
        )
    """)


    # =========================
    # 12. データを書き込み
    # =========================
    df_all.to_sql(
        "tolvaptan_study_new",
        conn,
        if_exists="append",
        index=False
    )


    # =========================
    # 13. 旧テーブルを削除
    # =========================
    conn.execute("DROP TABLE tolvaptan_study")


    # =========================
    # 14. 新テーブルをリネーム
    # =========================
    conn.execute("""
        ALTER TABLE tolvaptan_study_new
        RENAME TO tolvaptan_study
    """)


    # =========================
    # 15. 確定
    # =========================
    conn.commit()

    print("\n===================================")
    print("Update completed successfully.")
    print("===================================")
    print(f"Old rows : {len(df_old)}")
    print(f"Added    : {len(df_add)}")
    print(f"New rows : {len(df_all)}")


finally:
    conn.close()