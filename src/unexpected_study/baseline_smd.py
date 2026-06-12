"""
baseline_smd.py

対象患者を絞り込み、基礎疾患カテゴリと heart_failure について
unex24 の 0/1 群間で罹患率を比較し SMD を算出するスクリプト。

【対象患者の絞り込み条件】
1. Patient_Master.First_Visit_Date が 2015-03-01 〜 2025-03-31
2. Patient_Master.Age_at_Visit >= 20
3. event.death_date が NOT NULL かつ <= 2025-10-31
4. event.endstage_car != '癌末期'（NULL は通す）

【2群】
unexpected_death.unex24 = 0 vs 1

【比較変数】
- first_diag.category の各カテゴリ（罹患あり=1, なし=0）
- first_diag.heart_failure（0/1）

【出力】
- ターミナル表示
- CSV: baseline_smd.csv
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

# ── パス設定 ──────────────────────────────────────────────────────────────────
DB_PATH     = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
OUTPUT_PATH = Path("/Users/muna/Hana_research/data/raw/NowSamari/baseline_smd.csv")


def get_pid_col(conn: sqlite3.Connection, table: str) -> str:
    """テーブルのPRAGMAからpatient_ID系カラム名を大文字小文字問わず取得"""
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    for c in cols:
        if c.lower() == "patient_id":
            return c
    raise ValueError(f"[{table}] に patient_ID 系カラムが見つかりません。カラム一覧: {cols}")


def smd_binary(p1: float, p2: float) -> float:
    """2群の割合からSMDを算出（二値変数用）"""
    denom = np.sqrt((p1 * (1 - p1) + p2 * (1 - p2)) / 2)
    if denom == 0:
        return np.nan
    return abs(p1 - p2) / denom


def main():
    print(f"[1/5] DB接続: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # ── 各テーブルのPatient_IDカラム名を動的取得 ─────────────────────────
        pid_pm = get_pid_col(conn, "Patient_Master")
        pid_ev = get_pid_col(conn, "event")
        pid_ud = get_pid_col(conn, "unexpected_death")
        pid_fd = get_pid_col(conn, "first_diag")
        print(f"      Patient_IDカラム確認: PM={pid_pm}, event={pid_ev}, ud={pid_ud}, fd={pid_fd}")

        # ── 対象患者の抽出 ────────────────────────────────────────────────────
        print("[2/5] 対象患者の抽出中...")
        patient_query = f"""
        SELECT DISTINCT
            pm.{pid_pm} AS pid,
            ud.unex24
        FROM Patient_Master pm
        JOIN event e ON pm.{pid_pm} = e.{pid_ev}
        JOIN unexpected_death ud ON pm.{pid_pm} = ud.{pid_ud}
        WHERE
            pm.First_Visit_Date BETWEEN '2015-03-01' AND '2025-03-31'
            AND pm.Age_at_Visit >= 20
            AND e.death_date IS NOT NULL
            AND e.death_date <= '2025-10-31'
            AND (e.endstage_car IS NULL OR e.endstage_car != '癌末期')
            AND ud.unex24 IN (0, 1)
        """
        patients = pd.read_sql_query(patient_query, conn)
        patients["unex24"] = patients["unex24"].astype(int)

        n_total = len(patients)
        n0 = (patients["unex24"] == 0).sum()
        n1 = (patients["unex24"] == 1).sum()
        print(f"      対象患者数: {n_total}件  (unex24=0: {n0}件 / unex24=1: {n1}件)")

        if n_total == 0:
            print("      該当患者がいません。条件を確認してください。")
            return

        # ── first_diag の取得・対象患者に絞る ────────────────────────────────
        print("[3/5] 基礎疾患データの取得・整形中...")
        diag_query = f"""
        SELECT {pid_fd} AS pid, category, heart_failure
        FROM first_diag
        """
        diag_df = pd.read_sql_query(diag_query, conn)

        # 対象患者のみに絞る
        diag_df = diag_df[diag_df["pid"].isin(patients["pid"])]

        # カテゴリの正規化（前後の空白除去）
        diag_df["category"] = diag_df["category"].str.strip()

        # ── カテゴリの横持ち変換（患者×カテゴリ の有無フラグ） ───────────────
        cat_pivot = (
            diag_df.groupby(["pid", "category"])
            .size()
            .unstack(fill_value=0)
            .clip(upper=1)
            .reset_index()
        )

        # heart_failure: 患者ごとに最大値（1が1件でもあれば1）
        hf = (
            diag_df.groupby("pid")["heart_failure"]
            .max()
            .reset_index()
        )

        # 対象患者全員をベースにマージ（疾患なし患者は0埋め）
        base = patients[["pid", "unex24"]].copy()
        base = base.merge(cat_pivot, on="pid", how="left")
        base = base.merge(hf, on="pid", how="left")

        # NaN → 0（該当疾患なし）
        cat_cols = [c for c in cat_pivot.columns if c != "pid"]
        base[cat_cols] = base[cat_cols].fillna(0).astype(int)
        base["heart_failure"] = base["heart_failure"].fillna(0).astype(int)

        # ── SMD 計算 ──────────────────────────────────────────────────────────
        print("[4/5] SMD計算中...")
        group0 = base[base["unex24"] == 0]
        group1 = base[base["unex24"] == 1]

        variables = cat_cols + ["heart_failure"]
        results = []

        for var in variables:
            p0 = group0[var].mean()
            p1 = group1[var].mean()
            smd = smd_binary(p0, p1)
            results.append({
                "変数": var,
                f"unex24=0 罹患率 (n={n0})": f"{p0:.3f} ({int(round(p0*n0))}/{n0})",
                f"unex24=1 罹患率 (n={n1})": f"{p1:.3f} ({int(round(p1*n1))}/{n1})",
                "SMD": round(smd, 4) if not np.isnan(smd) else "N/A",
                "SMD≥0.1": "★" if (not np.isnan(smd) and smd >= 0.1) else "",
            })

        result_df = pd.DataFrame(results).sort_values("変数")

        # ── ターミナル表示 ─────────────────────────────────────────────────────
        print("\n【Baseline Characteristics: SMD比較】")
        print(f"{'変数':<25} {'unex24=0':>22} {'unex24=1':>22} {'SMD':>8} {'':>5}")
        print("-" * 85)
        for _, row in result_df.iterrows():
            smd_val = str(row["SMD"])
            flag    = row["SMD≥0.1"]
            print(
                f"{str(row['変数']):<25} "
                f"{str(row[f'unex24=0 罹患率 (n={n0})']):<22} "
                f"{str(row[f'unex24=1 罹患率 (n={n1})']):<22} "
                f"{smd_val:>8} {flag:>5}"
            )
        print("-" * 85)
        print("★ SMD ≥ 0.1 は群間差が大きい変数")

        # ── CSV出力 ───────────────────────────────────────────────────────────
        print(f"\n[5/5] CSV出力: {OUTPUT_PATH}")
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print("      完了!")

    except Exception as e:
        print(f"[ERROR] {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()