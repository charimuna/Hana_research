import sqlite3
import pandas as pd
import re
from pathlib import Path

# ==========================================
# ファイルパス
# ==========================================
DB_PATH = Path(
    "/Users/muna/Hana_research/data/db/Hana_Research.db"
)

CSV_PATH = Path(
    "/Users/muna/Hana_research/src/tolvaptan_study/tol.sama.2026.csv"
)


# ==========================================
# 1. CSV読み込み
# ==========================================
df_csv = pd.read_csv(
    CSV_PATH,
    encoding="cp932"
)

print(f"CSV rows: {len(df_csv)}")
print(f"CSV columns: {len(df_csv.columns)}")


# 患者IDをPatient_IDに統一
df_csv = df_csv.rename(
    columns={"患者ID": "Patient_ID"}
)

# 重要メモを明示
# O列 = 15列目 → Pythonでは14
memo_col = df_csv.columns[14]

print(f"重要メモ列: {memo_col}")

if memo_col != "重要メモ":
    raise ValueError(
        f"O列が「重要メモ」ではありません。実際の列名: {memo_col}"
    )


# Patient_IDを数値化
df_csv["Patient_ID"] = pd.to_numeric(
    df_csv["Patient_ID"],
    errors="coerce"
).astype("Int64")


# ==========================================
# 2. SQLite接続
# ==========================================
conn = sqlite3.connect(DB_PATH)

try:

    # ==========================================
    # 3. tolvaptan_studyから
    #    raw_match IS NULL の患者を取得
    # ==========================================
    df_target = pd.read_sql_query(
        """
        SELECT
            Patient_ID,
            Study_ID,
            ind_date,
            date_precision,
            raw_match
        FROM tolvaptan_study
        WHERE raw_match IS NULL
        """,
        conn
    )

    print(
        f"raw_match IS NULL の患者数: {len(df_target)}"
    )


    # ==========================================
    # 4. CSVのPatient_IDと照合
    # ==========================================
    df_target["Patient_ID"] = pd.to_numeric(
        df_target["Patient_ID"],
        errors="coerce"
    ).astype("Int64")

    df_match = df_target.merge(
        df_csv[
            [
                "Patient_ID",
                "重要メモ"
            ]
        ],
        on="Patient_ID",
        how="left"
    )

    print(
        f"CSVとPatient_IDが一致した患者数: "
        f"{df_match['重要メモ'].notna().sum()}"
    )


    # ==========================================
    # 5. サムスカ導入日を抽出
    #
    # 対応：
    # サムスカ導入（2020-04-01）
    # サムスカ導入（2020/04/01）
    # サムスカ導入(2020-04-01)
    # サムスカ導入(2020/04/01)
    # サムスカ導入 2020-04-01
    # サムスカ導入 2020/04/01
    #
    # YYYY-MM-DD
    # YYYY/MM/DD
    # YYYY-MM
    # YYYY/MM
    # ==========================================

    def extract_tol_date(memo):

        if pd.isna(memo):
            return {
                "ind_date": None,
                "date_precision": None,
                "raw_match": None
            }

        memo = str(memo)

        # --------------------------------------
        # 日まである場合
        # --------------------------------------
        pattern_day = (
            r"サムスカ導入"
            r"\s*[\(（]?"
            r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})"
            r"[\)）]?"
        )

        match_day = re.search(
            pattern_day,
            memo
        )

        if match_day:

            y, m, d = match_day.groups()

            # 0000-00-00等は無効
            if y == "0000" or m == "00" or d == "00":
                return {
                    "ind_date": None,
                    "date_precision": None,
                    "raw_match": match_day.group(0)
                }

            return {
                "ind_date": (
                    f"{y}-{m.zfill(2)}-{d.zfill(2)}"
                ),
                "date_precision": "day",
                "raw_match": match_day.group(0)
            }


        # --------------------------------------
        # 年月だけの場合
        # --------------------------------------
        pattern_month = (
            r"サムスカ導入"
            r"\s*[\(（]?"
            r"(\d{4})[-/](\d{1,2})"
            r"[\)）]?"
        )

        match_month = re.search(
            pattern_month,
            memo
        )

        if match_month:

            y, m = match_month.groups()

            # 0000-00等は無効
            if y == "0000" or m == "00":
                return {
                    "ind_date": None,
                    "date_precision": None,
                    "raw_match": match_month.group(0)
                }

            return {
                "ind_date": (
                    f"{y}-{m.zfill(2)}-01"
                ),
                "date_precision": "month",
                "raw_match": match_month.group(0)
            }


        # --------------------------------------
        # 見つからない場合
        # --------------------------------------
        return {
            "ind_date": None,
            "date_precision": None,
            "raw_match": None
        }


    # ==========================================
    # 6. 全対象患者について抽出
    # ==========================================
    result = df_match["重要メモ"].apply(
        extract_tol_date
    )

    df_match["new_ind_date"] = result.apply(
        lambda x: x["ind_date"]
    )

    df_match["new_date_precision"] = result.apply(
        lambda x: x["date_precision"]
    )

    df_match["new_raw_match"] = result.apply(
        lambda x: x["raw_match"]
    )


    # ==========================================
    # 7. 抽出結果を確認
    # ==========================================
    df_update = df_match[
        df_match["new_raw_match"].notna()
    ].copy()

    print(
        f"\nサムスカ導入日の抽出成功: "
        f"{len(df_update)}人"
    )

    print("\n--- 抽出結果 ---")

    print(
        df_update[
            [
                "Patient_ID",
                "Study_ID",
                "new_ind_date",
                "new_date_precision",
                "new_raw_match"
            ]
        ].to_string(index=False)
    )


    # ==========================================
    # 8. DB更新
    # ==========================================
    update_sql = """
        UPDATE tolvaptan_study
        SET
            ind_date = ?,
            date_precision = ?,
            raw_match = ?
        WHERE Patient_ID = ?
          AND raw_match IS NULL
    """

    update_data = [
        (
            row["new_ind_date"],
            row["new_date_precision"],
            row["new_raw_match"],
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
    # 9. 更新後確認
    # ==========================================
    updated_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM tolvaptan_study
        WHERE raw_match IS NOT NULL
        """
    ).fetchone()[0]

    remaining_null = conn.execute(
        """
        SELECT COUNT(*)
        FROM tolvaptan_study
        WHERE raw_match IS NULL
        """
    ).fetchone()[0]

    print("\n========================================")
    print("更新完了")
    print("========================================")
    print(
        f"今回更新した患者数: {len(df_update)}"
    )
    print(
        f"raw_matchあり（全体）: {updated_count}"
    )
    print(
        f"raw_match NULL（全体）: {remaining_null}"
    )


finally:
    conn.close()