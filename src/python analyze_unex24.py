"""
analyze_unex24.py

対象患者の絞り込みとunex24の度数集計スクリプト。

【絞り込み条件】
1. Patient_Master.First_Visit_Date が 2015-03-01 〜 2025-03-31
2. Patient_Master.Age_at_Visit が 20 以上
3. event.death_date が NOT NULL かつ 2025-10-31 以前
4. event.endstage_car が '癌末期' の患者を除外

【集計】
上記条件を満たす患者の unexpected_death.unex24 の 0/1 の度数を表示・CSV出力
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ── パス設定 ──────────────────────────────────────────────────────────────────
DB_PATH     = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
OUTPUT_PATH = Path("/Users/muna/Hana_research/data/raw/NowSamari/unex24_frequency.csv")

# ── クエリ ────────────────────────────────────────────────────────────────────
QUERY = """
SELECT
    ud.unex24
FROM Patient_Master pm
JOIN event e
    ON pm.patient_ID = e.patient_ID
JOIN unexpected_death ud
    ON pm.patient_ID = ud.patient_ID
WHERE
    -- 条件1: 初診日の範囲
    pm.First_Visit_Date BETWEEN '2015-03-01' AND '2025-03-31'
    -- 条件2: 初診時年齢20歳以上
    AND pm.Age_at_Visit >= 20
    -- 条件3: 死亡日が埋まっていて2025-10-31以前
    AND e.death_date IS NOT NULL
    AND e.death_date <= '2025-10-31'
    -- 条件4: 癌末期を除外
    AND (e.endstage_car IS NULL OR e.endstage_car != '癌末期')
"""


def main():
    print(f"[1/3] DB接続: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        print("[2/3] 対象患者の抽出・集計中...")
        df = pd.read_sql_query(QUERY, conn)

        total = len(df)
        print(f"\n      対象患者数（unexpected_deathレコード）: {total} 件")

        if total == 0:
            print("      該当患者がいませんでした。条件を確認してください。")
            return

        # unex24 の度数集計
        freq = df["unex24"].value_counts().sort_index().reset_index()
        freq.columns = ["unex24", "度数"]
        freq["割合(%)"] = (freq["度数"] / total * 100).round(1)

        # ── ターミナル出力 ────────────────────────────────────────────────────
        print("\n【unex24 度数集計】")
        print(f"{'unex24':<10} {'度数':>8} {'割合(%)':>10}")
        print("-" * 30)
        for _, row in freq.iterrows():
            print(f"{str(row['unex24']):<10} {int(row['度数']):>8} {row['割合(%)']:>9}%")
        print("-" * 30)
        print(f"{'合計':<10} {total:>8} {'100.0':>9}%")

        # ── CSV出力 ───────────────────────────────────────────────────────────
        print(f"\n[3/3] CSV出力: {OUTPUT_PATH}")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        freq.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print("      完了!")

    except Exception as e:
        print(f"[ERROR] {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()