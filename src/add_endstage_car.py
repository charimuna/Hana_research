"""
add_endstage_car.py

eventテーブルに endstage_car カラムを追加し、
CSVの「タグ」カラムに「癌末期」が含まれる患者は「癌末期」、
含まれない患者は「0」を入れるスクリプト。
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ── パス設定 ──────────────────────────────────────────────────────────────────
DB_PATH  = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
CSV_PATH = Path("/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260419_filtered.csv")

# ── CSVカラム名 ───────────────────────────────────────────────────────────────
COL_PATIENT_ID = "患者ID"
COL_TAG        = "タグ"

KEYWORD        = "癌末期"


def has_endstage(tag_value: str | None) -> bool:
    """タグ文字列に「癌末期」が含まれるか判定"""
    if not tag_value or pd.isna(tag_value):
        return False
    # スペース区切りで分割して完全一致チェック
    tags = str(tag_value).split(" ")
    return KEYWORD in tags


def main():
    # ── CSV読み込み ──────────────────────────────────────────────────────────
    print(f"[1/4] CSV読み込み中: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, dtype=str)

    required = [COL_PATIENT_ID, COL_TAG]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSVに必要なカラムが見つかりません: {missing}")

    # ── endstage_car の値を計算 ───────────────────────────────────────────────
    print("[2/4] 「癌末期」タグの判定中")
    df["endstage_car"] = df[COL_TAG].apply(
        lambda t: KEYWORD if has_endstage(t) else "0"
    )

    matched = (df["endstage_car"] == KEYWORD).sum()
    print(f"      癌末期タグあり: {matched}件 / なし: {len(df) - matched}件")

    # ── DB接続・カラム追加・UPDATE ────────────────────────────────────────────
    print(f"[3/4] DB接続: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    try:
        # カラムが存在しない場合のみ ALTER TABLE
        cur.execute("PRAGMA table_info(event)")
        existing_cols = [row[1] for row in cur.fetchall()]
        if "endstage_car" not in existing_cols:
            print("      endstage_car カラムを追加中 (ALTER TABLE)...")
            cur.execute("ALTER TABLE event ADD COLUMN endstage_car TEXT")
        else:
            print("      endstage_car カラムは既に存在します。値を上書きします。")

        # patient_ID ごとに UPDATE
        print("      endstage_car を更新中...")
        update_count = 0
        skip_count   = 0

        for _, row in df.iterrows():
            patient_id   = str(row[COL_PATIENT_ID]).strip()
            endstage_val = row["endstage_car"]

            cur.execute("SELECT 1 FROM event WHERE patient_ID = ?", (patient_id,))
            if cur.fetchone():
                cur.execute(
                    "UPDATE event SET endstage_car = ? WHERE patient_ID = ?",
                    (endstage_val, patient_id)
                )
                update_count += 1
            else:
                skip_count += 1

        conn.commit()
        print(f"      UPDATE: {update_count}件 / DBに存在しないためスキップ: {skip_count}件")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] ロールバックしました: {e}")
        raise
    finally:
        conn.close()

    print("[4/4] 完了!")


if __name__ == "__main__":
    main()