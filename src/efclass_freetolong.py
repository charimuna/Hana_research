"""
EF_wide.csv → Hana_Research.db の ef_long テーブルへのインポート

CSV カラム → ef_long カラム:
  Study_ID        → Study_ID
  Date            → visit_date
  item            → item
  value           → value
  matched_text    → matched_text
  hf_term_raw     → hf_term_raw
  hf_term_std     → hf_term_std
  ef_source       → ef_source
  final_hf_class  → final_hf_class
  conflict_flag   → conflict_flag
  operator        → 除外
  source_priority → 除外

ef_long 側のみのカラム (id, visit_datetime, record_no) は NULL で挿入される。
重複チェック: Study_ID + visit_date + item + ef_source が既存と一致する行はスキップ。
"""

import sqlite3
import pandas as pd
from pathlib import Path

CSV_PATH     = Path("/Users/muna/Hana_research/data/processed/EF_wide.csv")
DB_PATH      = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
TABLE        = "ef_long"
UNIQUE_KEYS  = ["Study_ID", "visit_date", "item", "ef_source"]
RENAME_MAP   = {"Date": "visit_date"}
EXCLUDE_COLS = {"operator", "source_priority"}
DB_COLS      = {
    "Study_ID", "visit_date", "item", "value",
    "matched_text", "hf_term_raw", "hf_term_std",
    "ef_source", "final_hf_class", "conflict_flag",
}


def main():
    # --- CSV 読み込み ---
    df = pd.read_csv(CSV_PATH)
    print(f"[INFO] CSV loaded: {len(df)} rows")
    print(f"[INFO] CSV columns: {df.columns.tolist()}")

    # --- 除外 ---
    drop_cols = [c for c in df.columns if c in EXCLUDE_COLS]
    df = df.drop(columns=drop_cols)
    print(f"[INFO] Dropped: {drop_cols}")

    # --- リネーム ---
    df = df.rename(columns=RENAME_MAP)

    # --- DB カラムと一致するものだけ残す（余剰カラム安全ガード）---
    keep = [c for c in df.columns if c in DB_COLS]
    unexpected = [c for c in df.columns if c not in DB_COLS]
    if unexpected:
        print(f"[WARN] Unexpected columns (skipped): {unexpected}")
    df = df[keep]

    # --- visit_date を YYYY-MM-DD に正規化 ---
    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # --- conflict_flag を整数に ---
    if "conflict_flag" in df.columns:
        df["conflict_flag"] = pd.to_numeric(df["conflict_flag"], errors="coerce").astype("Int64")

    print(f"[INFO] Final columns to insert: {df.columns.tolist()}")

    # --- DB 書き込み（重複スキップ）---
    with sqlite3.connect(DB_PATH) as con:
        existing = pd.read_sql(
            f"SELECT {', '.join(UNIQUE_KEYS)} FROM {TABLE}", con
        )
        before = len(df)
        df = df.merge(existing, on=UNIQUE_KEYS, how="left", indicator=True)
        df = df[df["_merge"] == "left_only"].drop(columns="_merge")
        skipped = before - len(df)
        if skipped:
            print(f"[INFO] Skipped {skipped} duplicate rows")

        if df.empty:
            print("[INFO] No new rows to insert.")
            return

        df.to_sql(TABLE, con, if_exists="append", index=False)
        count = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]

    print(f"[INFO] Inserted {len(df)} rows → {TABLE} (total rows now: {count})")


if __name__ == "__main__":
    main()