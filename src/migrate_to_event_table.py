"""
migrate_to_event_table.py

CSVからSQLiteのeventテーブルへデータを移行するスクリプト。

処理内容:
- eventテーブルの対象カラム(patient_ID, status, death_date, last_confirmed_date)を一度全削除してINSERT
- last_confirmed_date は 死亡日・終了日・管理料設定（最終算定日） の中で最も新しい日付を使用
- CSVにあってDBに存在しないpatient_IDは新規INSERT
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ── パス設定 ──────────────────────────────────────────────────────────────────
DB_PATH  = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
CSV_PATH = Path("/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260419_filtered.csv")

# ── CSVカラム名 → DBカラム名 のマッピング ────────────────────────────────────
COL_PATIENT_ID  = "患者ID"
COL_STATUS      = "診療ステータス"
COL_DEATH_DATE  = "死亡日"
COL_END_DATE    = "終了日"
COL_MGMT_DATE   = "管理料設定（最終算定日）"


def parse_date(series: pd.Series) -> pd.Series:
    """文字列の日付列をdatetime型に変換（パース失敗はNaT）"""
    return pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")


def calc_last_confirmed(row) -> str | None:
    """死亡日・終了日・管理料設定（最終算定日）の中で最も新しい日付を返す"""
    dates = [row["_death"], row["_end"], row["_mgmt"]]
    valid = [d for d in dates if pd.notna(d)]
    if not valid:
        return None
    return max(valid).strftime("%Y-%m-%d")


def main():
    # ── CSV読み込み ──────────────────────────────────────────────────────────
    print(f"[1/4] CSV読み込み中: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, dtype=str)  # 全列を文字列で読む（日付の欠損対策）

    # 必要カラムの存在チェック
    required = [COL_PATIENT_ID, COL_STATUS, COL_DEATH_DATE, COL_END_DATE, COL_MGMT_DATE]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSVに必要なカラムが見つかりません: {missing}")

    # ── 日付変換 ─────────────────────────────────────────────────────────────
    print("[2/4] 日付変換・last_confirmed_date計算中")
    df["_death"] = parse_date(df[COL_DEATH_DATE])
    df["_end"]   = parse_date(df[COL_END_DATE])
    df["_mgmt"]  = parse_date(df[COL_MGMT_DATE])

    # death_date: NaT → None（DB用）
    df["death_date_str"] = df["_death"].apply(
        lambda d: d.strftime("%Y-%m-%d") if pd.notna(d) else None
    )

    # last_confirmed_date: 3列の最大値
    df["last_confirmed_date"] = df.apply(calc_last_confirmed, axis=1)

    # ── DBへ書き込み ─────────────────────────────────────────────────────────
    print(f"[3/4] DB接続: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    try:
        # 既存レコードのうち対象カラムをNULLリセット（全行）
        # → テーブル構造は維持し、4カラムのみ書き換える方針
        print("      既存レコードの対象カラムをリセット中...")
        cur.execute("""
            UPDATE event
            SET status = NULL,
                death_date = NULL,
                last_confirmed_date = NULL
        """)

        # UPSERT: patient_IDが既存ならUPDATE、なければINSERT
        print("      CSV → event テーブルへUPSERT中...")
        upsert_count = 0
        insert_count = 0

        for _, row in df.iterrows():
            patient_id          = str(row[COL_PATIENT_ID]).strip()
            status              = row[COL_STATUS] if pd.notna(row[COL_STATUS]) else None
            death_date          = row["death_date_str"]
            last_confirmed_date = row["last_confirmed_date"]

            # patient_IDの存在確認
            cur.execute("SELECT 1 FROM event WHERE patient_ID = ?", (patient_id,))
            exists = cur.fetchone()

            if exists:
                cur.execute("""
                    UPDATE event
                    SET status = ?,
                        death_date = ?,
                        last_confirmed_date = ?
                    WHERE patient_ID = ?
                """, (status, death_date, last_confirmed_date, patient_id))
                upsert_count += 1
            else:
                cur.execute("""
                    INSERT INTO event (patient_ID, status, death_date, last_confirmed_date)
                    VALUES (?, ?, ?, ?)
                """, (patient_id, status, death_date, last_confirmed_date))
                insert_count += 1

        conn.commit()
        print(f"      UPDATE: {upsert_count}件 / INSERT: {insert_count}件")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] ロールバックしました: {e}")
        raise
    finally:
        conn.close()

    print("[4/4] 完了!")


if __name__ == "__main__":
    main()