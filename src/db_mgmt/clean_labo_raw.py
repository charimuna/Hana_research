import sqlite3
import pandas as pd

# =========================================
# 設定
# =========================================

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"
RAW_TABLE = "lab_raw"
CLEAN_TABLE = "lab_clean"


# =========================================
# DB接続
# =========================================

conn = sqlite3.connect(DB_PATH)
print("Loading table...")

df_all = pd.read_sql(
    f"SELECT * FROM {RAW_TABLE}",
    conn
)
print(f"Loaded rows: {len(df_all):,}")


# =========================================
# 採血日の datetime 化
# =========================================

print("Parsing sample_date...")

df_all["sample_date_parsed"] = pd.to_datetime(
    df_all["sample_date"],
    errors="coerce"
)

date_error_rate = df_all["sample_date_parsed"].isna().mean()
print(f"Date parse error rate: {date_error_rate:.4f}")


# =========================================
# 検査値の数値化
# =========================================

print("Converting value_raw to numeric...")

df_all["value_num"] = pd.to_numeric(
    df_all["value_raw"],
    errors="coerce"
)

numeric_rate = df_all["value_num"].notna().mean()
print(f"Numeric conversion rate: {numeric_rate:.4f}")


# =========================================
# 重複削除
# =========================================

print("Removing duplicates...")

n_before = len(df_all)

df_clean = df_all.drop_duplicates(
    subset=[
        "Patient_ID",
        "sample_date",
        "item_code",
        "value_num",
        "unit"
    ]
).copy()

n_after = len(df_clean)
removed = n_before - n_after

print(f"Before : {n_before:,}")
print(f"After  : {n_after:,}")
print(f"Removed: {removed:,} ({removed/n_before:.4%})")


# =========================================
# item_code 統一
# =========================================

def unify_code(df, item_name, old_code, new_code):
    """item_codeを統一する関数"""
    before = df[
        (df["item_name"] == item_name) &
        (df["item_code"] == old_code)
    ].shape[0]

    df.loc[
        (df["item_name"] == item_name) &
        (df["item_code"] == old_code),
        "item_code"
    ] = new_code

    after = df[
        (df["item_name"] == item_name) &
        (df["item_code"] == new_code)
    ].shape[0]

    print(f"{item_name}: {old_code}→{new_code}  変更件数:{before:,}  統一後合計:{after:,}")


print("\n--- item_code 統一開始 ---")

# CRE: 0069 → 0074
unify_code(df_clean, "ＣＲＥ",   "0069", "0074")

# Mg: 0111 → 0112
unify_code(df_clean, "Ｍｇ",     "0111", "0112")

# アルブミン: 0018 → 0179
unify_code(df_clean, "アルブミン", "0018", "0179")

print("--- item_code 統一完了 ---\n")


# =========================================
# 重複が残っていないか最終確認
# =========================================

dup_check = (
    df_clean.groupby([
        "Patient_ID",
        "sample_date",
        "item_code",
        "value_num",
        "unit"
    ])
    .size()
    .reset_index(name="n")
)

remaining_dup = (dup_check["n"] > 1).sum()
print(f"Remaining duplicate groups: {remaining_dup}")


# =========================================
# SQLite に保存
# =========================================

print("\nSaving cleaned table...")

df_clean.to_sql(
    CLEAN_TABLE,
    conn,
    if_exists="replace",
    index=False
)

print("Done.")
conn.close()