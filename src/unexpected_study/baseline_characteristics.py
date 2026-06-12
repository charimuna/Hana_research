"""
baseline_characteristics.py

対象患者（前回と同じ絞り込み条件）について、
unex24 の 0/1 群間でベースライン特性を比較するスクリプト。

【比較変数】
■ 連続変数（平均±SD、群間差）
  - Age_at_Visit（Patient_Master）
  - Follow-up期間・日数（event.death_date - Patient_Master.First_Visit_Date）

■ 割合変数（割合、SMD）
  - 84歳未満の割合（Age_at_Visit < 84）
  - 男性の割合（Gender == '男性'）
  - Non-bedridden割合（JABC が J1/J2/A1/A2）
  - 自宅の割合（facility == '自宅'）

【出力】
  - ターミナル表示
  - /Users/muna/Hana_research/output/baseline_characteristics.csv
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from scipy import stats

# ── パス設定 ──────────────────────────────────────────────────────────────────
DB_PATH     = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
OUTPUT_DIR  = Path("/Users/muna/Hana_research/output")
OUTPUT_PATH = OUTPUT_DIR / "baseline_characteristics.csv"

# Non-bedriddenとみなすJABCの値
NON_BEDRIDDEN = {"J1", "J2", "A1", "A2"}


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
    """二値変数のSMD（pooled SD法）"""
    denom = np.sqrt((p1 * (1 - p1) + p2 * (1 - p2)) / 2)
    if denom == 0:
        return np.nan
    return abs(p1 - p2) / denom


def fmt_cont(mean: float, sd: float) -> str:
    return f"{mean:.1f} ± {sd:.1f}"


def fmt_prop(p: float, n_group: int) -> str:
    count = int(round(p * n_group))
    return f"{p*100:.1f}% ({count}/{n_group})"


def main():
    print(f"[1/5] DB接続: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # ── Patient_ID カラム名を動的取得 ─────────────────────────────────────
        pid_pm = get_pid_col(conn, "Patient_Master")
        pid_ev = get_pid_col(conn, "event")
        pid_ud = get_pid_col(conn, "unexpected_death")
        pid_bs = get_pid_col(conn, "Background_summury")

        # ── 対象患者の抽出 ────────────────────────────────────────────────────
        print("[2/5] 対象患者の抽出中...")
        patient_query = f"""
        SELECT DISTINCT
            pm.{pid_pm}         AS pid,
            pm.Age_at_Visit,
            pm.Gender,
            pm.First_Visit_Date,
            e.death_date,
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
        df = pd.read_sql_query(patient_query, conn)
        df["unex24"] = df["unex24"].astype(int)

        # Follow-up期間（日数）を計算
        df["First_Visit_Date"] = pd.to_datetime(df["First_Visit_Date"], errors="coerce")
        df["death_date"]       = pd.to_datetime(df["death_date"], errors="coerce")
        df["followup_days"]    = (df["death_date"] - df["First_Visit_Date"]).dt.days

        n_total = len(df)
        n0 = (df["unex24"] == 0).sum()
        n1 = (df["unex24"] == 1).sum()
        print(f"      対象患者数: {n_total}件  (unex24=0: {n0}件 / unex24=1: {n1}件)")

        # ── Background_summury の取得 ─────────────────────────────────────────
        print("[3/5] Background_summuryデータの取得中...")
        bs_query = f"""
        SELECT {pid_bs} AS pid, JABC, facility
        FROM Background_summury
        """
        bs_df = pd.read_sql_query(bs_query, conn)
        bs_df = bs_df[bs_df["pid"].isin(df["pid"])]

        # 患者ごとに最新1行のみ使う（重複がある場合に備え最初の1行）
        bs_df = bs_df.drop_duplicates(subset="pid", keep="first")

        # Non-bedridden フラグ
        bs_df["non_bedridden"] = bs_df["JABC"].str.strip().isin(NON_BEDRIDDEN).astype(int)

        # 自宅フラグ
        bs_df["is_home"] = (bs_df["facility"].str.strip() == "自宅").astype(int)

        # メインDFにマージ
        df = df.merge(bs_df[["pid", "non_bedridden", "is_home"]], on="pid", how="left")
        df["non_bedridden"] = df["non_bedridden"].fillna(0).astype(int)
        df["is_home"]       = df["is_home"].fillna(0).astype(int)

        # ── 2群に分割 ─────────────────────────────────────────────────────────
        g0 = df[df["unex24"] == 0].copy()
        g1 = df[df["unex24"] == 1].copy()

        # ── 集計 ──────────────────────────────────────────────────────────────
        print("[4/5] 集計・SMD計算中...")
        results = []

        # ---- 連続変数 --------------------------------------------------------
        def add_continuous(label: str, col: str):
            v0 = g0[col].dropna()
            v1 = g1[col].dropna()
            m0, s0 = v0.mean(), v0.std()
            m1, s1 = v1.mean(), v1.std()
            # Cohen's d (pooled SD)
            pooled_sd = np.sqrt(((len(v0)-1)*s0**2 + (len(v1)-1)*s1**2) / (len(v0)+len(v1)-2))
            cohens_d  = abs(m1 - m0) / pooled_sd if pooled_sd > 0 else np.nan
            # t検定
            _, pval = stats.ttest_ind(v0, v1, equal_var=False)
            results.append({
                "変数":              label,
                "種別":              "連続",
                f"unex24=0 (n={n0})": fmt_cont(m0, s0),
                f"unex24=1 (n={n1})": fmt_cont(m1, s1),
                "SMD / Cohen's d":   round(cohens_d, 4) if not np.isnan(cohens_d) else "N/A",
                "p値(t検定)":        f"{pval:.4f}",
                "SMD≥0.1":          "★" if (not np.isnan(cohens_d) and cohens_d >= 0.1) else "",
            })

        add_continuous("Age_at_Visit（歳）",      "Age_at_Visit")
        add_continuous("Follow-up期間（日数）",    "followup_days")

        # ---- 割合変数 --------------------------------------------------------
        def add_binary(label: str, col: str):
            p0 = g0[col].mean()
            p1 = g1[col].mean()
            smd = smd_binary(p0, p1)
            results.append({
                "変数":              label,
                "種別":              "割合",
                f"unex24=0 (n={n0})": fmt_prop(p0, n0),
                f"unex24=1 (n={n1})": fmt_prop(p1, n1),
                "SMD / Cohen's d":   round(smd, 4) if not np.isnan(smd) else "N/A",
                "p値(t検定)":        "",
                "SMD≥0.1":          "★" if (not np.isnan(smd) and smd >= 0.1) else "",
            })

        # 84歳未満フラグ
        df["under84"]  = (df["Age_at_Visit"] < 84).astype(int)
        g0["under84"]  = (g0["Age_at_Visit"] < 84).astype(int)
        g1["under84"]  = (g1["Age_at_Visit"] < 84).astype(int)

        # 男性フラグ
        df["is_male"]  = (df["Gender"].str.strip() == "男性").astype(int)
        g0["is_male"]  = (g0["Gender"].str.strip() == "男性").astype(int)
        g1["is_male"]  = (g1["Gender"].str.strip() == "男性").astype(int)

        add_binary("84歳未満の割合",           "under84")
        add_binary("男性の割合",               "is_male")
        add_binary("Non-bedridden割合（J/A）", "non_bedridden")
        add_binary("自宅の割合",               "is_home")

        result_df = pd.DataFrame(results)

        # ── ターミナル表示 ─────────────────────────────────────────────────────
        col0 = f"unex24=0 (n={n0})"
        col1 = f"unex24=1 (n={n1})"
        col_smd = "SMD / Cohen's d"
        print("\n【Baseline Characteristics】")
        print(f"{'変数':<28} {'種別':<5} {col0:>22} {col1:>22} {'SMD/d':>8} {'p値':>8} {''}")
        print("-" * 100)
        for _, row in result_df.iterrows():
            print(
                f"{str(row['変数']):<28} "
                f"{str(row['種別']):<5} "
                f"{str(row[col0]):>22} "
                f"{str(row[col1]):>22} "
                f"{str(row[col_smd]):>8} "
                f"{str(row['p値(t検定)']):>8} "
                f"{str(row['SMD≥0.1'])}"
            )
        print("-" * 100)
        print("★ SMD/Cohen's d ≥ 0.1 は群間差が大きい変数")
        print("連続変数: 平均 ± SD、Cohen's d（pooled SD）、Welch's t検定")
        print("割合変数: % (件数/総数)、SMD（pooled SD法）")

        # ── CSV出力 ───────────────────────────────────────────────────────────
        print(f"\n[5/5] CSV出力: {OUTPUT_PATH}")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print("      完了!")

    except Exception as e:
        print(f"[ERROR] {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()