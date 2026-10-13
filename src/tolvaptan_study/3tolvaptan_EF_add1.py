import sqlite3
import pandas as pd

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"
DRY_RUN = False # 内容確認後に False にして再実行

conn = sqlite3.connect(DB_PATH)

try:
    # 1. tolvaptan_study
    df_tol = pd.read_sql(
        "SELECT Patient_ID, Study_ID, ind_date FROM tolvaptan_study", conn
    )
    df_tol["ind_dt"] = pd.to_datetime(df_tol["ind_date"], errors="coerce")
    print(f"全患者数: {len(df_tol)}")
    print(f"ind_date 解釈不能/NULL: {df_tol['ind_dt'].isna().sum()}")
    df_tol = df_tol.dropna(subset=["ind_dt"])

    # 2. ef_long（EF_valueのみ、数値化できる行のみ）
    df_ef = pd.read_sql("""
        SELECT Study_ID, visit_date, value, matched_text, final_hf_class
        FROM ef_long
        WHERE item = 'EF_value'
    """, conn)
    df_ef["value_num"] = pd.to_numeric(df_ef["value"], errors="coerce")
    df_ef["visit_dt"] = pd.to_datetime(df_ef["visit_date"], errors="coerce")
    df_ef = df_ef.dropna(subset=["value_num", "visit_dt"])

    # 3. 同一日付は平均。テキスト列は重複を除いて連結
    def join_unique(s):
        vals = [str(v) for v in s.dropna().unique() if str(v).strip()]
        return " / ".join(vals) if vals else None

    df_day = (
        df_ef.groupby(["Study_ID", "visit_date", "visit_dt"], as_index=False)
        .agg(
            ef_value=("value_num", "mean"),
            ef_matched_text=("matched_text", join_unique),
            ef_final_hf_class=("final_hf_class", join_unique),
        )
        .rename(columns={"visit_date": "ef_visit_date"})
    )

    # 4. ind_date に最も近い日を選択（同距離なら早い日付）
    df_m = df_tol.merge(df_day, on="Study_ID", how="inner")
    df_m["diff_days"] = (df_m["visit_dt"] - df_m["ind_dt"]).abs().dt.days
    df_update = (
        df_m.sort_values(["Patient_ID", "diff_days", "visit_dt"])
        .drop_duplicates("Patient_ID", keep="first")
        .reset_index(drop=True)
    )
    print(f"EFが見つかった患者数: {len(df_update)}")

    # 5. 確認用
    print("\n--- 更新対象 ---")
    print(df_update[[
        "Patient_ID", "Study_ID", "ind_date", "ef_visit_date",
        "diff_days", "ef_value", "ef_matched_text", "ef_final_hf_class"
    ]].to_string(index=False))
    print("\nind_dateとの差（日）の分布:")
    print(df_update["diff_days"].describe())

    # 6. UPDATE
    if DRY_RUN:
        print("\nDRY_RUN=True のため書き込みなし")
    else:
        before = conn.total_changes
        conn.executemany(
            """
            UPDATE tolvaptan_study
            SET ef_visit_date = ?,
                ef_value = ?,
                ef_matched_text = ?,
                ef_final_hf_class = ?
            WHERE Patient_ID = ?
            """,
            [
                (r.ef_visit_date, r.ef_value, r.ef_matched_text,
                 r.ef_final_hf_class, int(r.Patient_ID))
                for r in df_update.itertuples(index=False)
            ],
        )
        conn.commit()
        print(f"\nUPDATE件数: {conn.total_changes - before}")

finally:
    conn.close()