import sqlite3
import pandas as pd

# ==========================================
# ファイルパス
# ==========================================
DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"


# ==========================================
# SQLite接続
# ==========================================
conn = sqlite3.connect(DB_PATH)

try:

    # ==========================================
    # 1. tolvaptan_studyから
    #    EFが未入力の患者だけ取得
    # ==========================================
    df_tol = pd.read_sql("""
        SELECT
            Patient_ID,
            Study_ID,
            ef_visit_date,
            ef_value,
            ef_matched_text,
            ef_final_hf_class
        FROM tolvaptan_study
    """, conn)

    print(f"tolvaptan_study 全患者数: {len(df_tol)}")


    # ==========================================
    # 2. ef_longからデータ取得
    # ==========================================
    df_ef = pd.read_sql("""
        SELECT
            Study_ID,
            visit_date,
            value,
            matched_text,
            final_hf_class
        FROM ef_long
    """, conn)


    # ==========================================
    # 3. Study_IDごとに最古のvisit_dateを1件取得
    # ==========================================
    df_ef_min = (
        df_ef
        .sort_values("visit_date")
        .drop_duplicates(
            subset=["Study_ID"],
            keep="first"
        )
        .reset_index(drop=True)
    )


    # ==========================================
    # 4. EF関連カラム名を変更
    # ==========================================
    df_ef_min = df_ef_min.rename(columns={
        "visit_date": "ef_visit_date",
        "value": "ef_value",
        "matched_text": "ef_matched_text",
        "final_hf_class": "ef_final_hf_class"
    })


    # ==========================================
    # 5. ef_visit_dateがNULLの患者だけ対象
    # ==========================================
    df_target = df_tol[
        df_tol["ef_visit_date"].isna()
    ].copy()

    print(
        f"ef_visit_date がNULLの患者数: "
        f"{len(df_target)}"
    )


    # ==========================================
    # 6. Study_IDでEFデータと照合
    # ==========================================
    df_update = df_target[
        ["Patient_ID", "Study_ID"]
    ].merge(
        df_ef_min[
            [
                "Study_ID",
                "ef_visit_date",
                "ef_value",
                "ef_matched_text",
                "ef_final_hf_class"
            ]
        ],
        on="Study_ID",
        how="inner"
    )


    # ==========================================
    # 7. EFデータが存在する患者のみ
    # ==========================================
    print(
        f"EFデータが見つかった患者数: "
        f"{len(df_update)}"
    )


    # ==========================================
    # 8. 更新前確認
    # ==========================================
    print("\n--- 更新対象 ---")

    print(
        df_update[
            [
                "Patient_ID",
                "Study_ID",
                "ef_visit_date",
                "ef_value",
                "ef_matched_text",
                "ef_final_hf_class"
            ]
        ].to_string(index=False)
    )


    # ==========================================
    # 9. DBをUPDATE
    #
    # ef_visit_dateがNULLの患者だけ更新
    # ==========================================
    update_sql = """
        UPDATE tolvaptan_study
        SET
            ef_visit_date = ?,
            ef_value = ?,
            ef_matched_text = ?,
            ef_final_hf_class = ?
        WHERE Patient_ID = ?
          AND ef_visit_date IS NULL
    """

    update_data = [
        (
            row["ef_visit_date"],
            row["ef_value"],
            row["ef_matched_text"],
            row["ef_final_hf_class"],
            int(row["Patient_ID"])
        )
        for _, row in df_update.iterrows()
    ]

    conn.executemany(
        update_sql,
        update_data
    )

    conn.commit()


    # ==========================================
    # 10. 更新件数確認
    # ==========================================
    print(
        f"\n今回UPDATEした患者数: "
        f"{conn.total_changes}"
    )

    print("\n完了：EF情報を追加しました。")
    print(
        "既存のEF情報およびその他のカラムは変更していません。"
    )


finally:
    conn.close()