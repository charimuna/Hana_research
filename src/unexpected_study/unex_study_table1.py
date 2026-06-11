"""
unex_study  Table 1  ―  unex_death 0/1 による2群比較
  ・連続変数 : mean±SD, t検定
  ・カテゴリ変数 : n(%), χ²検定
  ・SMD : 0.1以上に * フラグ
出力 : /Users/muna/Hana_research/output/unex_study_table1.csv
"""

import sqlite3
import os
import numpy as np
import pandas as pd
from scipy import stats

# ── パス設定 ──────────────────────────────────────────
DB_PATH  = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUT_DIR  = "/Users/muna/Hana_research/output"
OUT_FILE = os.path.join(OUT_DIR, "unex_study_table1.csv")

# ── DB読み込み ─────────────────────────────────────────
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM unex_study", conn)
conn.close()

# ── ADL 二値化 ─────────────────────────────────────────
#   J1,J2,A1,A2 → Non-bedridden = 1
#   B1,B2,C1,C2 → Bedridden     = 0
def adl_to_binary(val):
    if pd.isna(val):
        return np.nan
    v = str(val).strip().upper()
    if v.startswith("J") or v.startswith("A"):
        return 1
    elif v.startswith("B") or v.startswith("C"):
        return 0
    return np.nan

df["ADL_bin"] = df["ADL"].apply(adl_to_binary)

# ── 解析対象変数の定義 ──────────────────────────────────
CONTINUOUS = [
    ("age",    "Age (years)"),
    ("フォロー", "Follow-up (days)"),
]

CATEGORICAL = [
    ("gender_male1",   "Male gender"),
    ("VISITplace_Home1", "Visit place: Home"),
    ("ADL_bin",        "Non-bedridden ADL (J/A)"),
    ("HF01",           "Heart failure"),
    ("ins",            "Insulin use"),
    ("認知症",          "Dementia"),
    ("認知機能障害",    "Cognitive impairment"),
    ("脳血管疾患",      "Cerebrovascular disease"),
    ("呼吸器疾患",      "Respiratory disease"),
    ("心臓疾患",        "Cardiac disease"),
    ("腎疾患",          "Renal disease"),
    ("中心静脈",        "Central venous catheter"),
    ("EN",             "Enteral nutrition"),
    ("尿カテ",          "Urinary catheter"),
    ("透析",            "Dialysis"),
    ("lastHOT",        "Home oxygen therapy"),
]

GROUP_COL = "unex_death"
SMD_FLAG  = 0.1   # このSMD以上に * を付ける

# ── SMD 計算関数 ────────────────────────────────────────
def smd_continuous(g0, g1):
    """連続変数のSMD (pooled SD)"""
    n0, n1 = len(g0.dropna()), len(g1.dropna())
    m0, m1 = g0.mean(), g1.mean()
    s0, s1 = g0.std(ddof=1), g1.std(ddof=1)
    pooled = np.sqrt(((n0 - 1) * s0**2 + (n1 - 1) * s1**2) / (n0 + n1 - 2))
    return abs(m1 - m0) / pooled if pooled > 0 else np.nan

def smd_binary(g0, g1):
    """2値変数のSMD"""
    p0 = g0.mean()
    p1 = g1.mean()
    denom = np.sqrt((p0 * (1 - p0) + p1 * (1 - p1)) / 2)
    return abs(p1 - p0) / denom if denom > 0 else np.nan

def fmt_smd(smd):
    if pd.isna(smd):
        return ""
    flag = "*" if smd >= SMD_FLAG else ""
    return f"{smd:.3f}{flag}"

# ── 2群に分割 ──────────────────────────────────────────
df0 = df[df[GROUP_COL] == 0]
df1 = df[df[GROUP_COL] == 1]
n_total = len(df)
n0, n1  = len(df0), len(df1)

rows = []

# ── ヘッダー行 ─────────────────────────────────────────
rows.append({
    "Variable": "n",
    "Total":    str(n_total),
    "Unex_death=0": str(n0),
    "Unex_death=1": str(n1),
    "p-value": "",
    "SMD":     "",
})

# ── 連続変数 ───────────────────────────────────────────
for col, label in CONTINUOUS:
    all_v = df[col].dropna()
    v0    = df0[col].dropna()
    v1    = df1[col].dropna()

    total_str = f"{all_v.mean():.1f} ± {all_v.std(ddof=1):.1f}"
    v0_str    = f"{v0.mean():.1f} ± {v0.std(ddof=1):.1f}"
    v1_str    = f"{v1.mean():.1f} ± {v1.std(ddof=1):.1f}"

    _, pval = stats.ttest_ind(v0.values, v1.values, equal_var=False)
    smd = smd_continuous(v0, v1)

    rows.append({
        "Variable":     label,
        "Total":        total_str,
        "Unex_death=0": v0_str,
        "Unex_death=1": v1_str,
        "p-value":      f"{pval:.3f}",
        "SMD":          fmt_smd(smd),
    })

# ── カテゴリ変数 ───────────────────────────────────────
for col, label in CATEGORICAL:
    # 中心静脈はREAL型なので0/1に丸める
    series = df[col]
    if series.dtype == float:
        series = series.round().astype("Int64")

    all_n = series.notna().sum()
    all_1 = (series == 1).sum()

    s0 = df0[col]
    if s0.dtype == float:
        s0 = s0.round().astype("Int64")
    s1 = df1[col]
    if s1.dtype == float:
        s1 = s1.round().astype("Int64")

    n0_valid = s0.notna().sum()
    n1_valid = s1.notna().sum()
    c0 = (s0 == 1).sum()
    c1 = (s1 == 1).sum()

    total_str = f"{int(all_1)} ({100*all_1/all_n:.1f}%)" if all_n > 0 else ""
    v0_str    = f"{int(c0)} ({100*c0/n0_valid:.1f}%)" if n0_valid > 0 else ""
    v1_str    = f"{int(c1)} ({100*c1/n1_valid:.1f}%)" if n1_valid > 0 else ""

    # χ²検定 (2×2 contingency table)
    ct = np.array([
        [int(c0),         int(n0_valid - c0)],
        [int(c1),         int(n1_valid - c1)],
    ])
    try:
        _, pval, _, _ = stats.chi2_contingency(ct, correction=False)
    except Exception:
        pval = np.nan

    smd = smd_binary(
        s0.dropna().astype(int),
        s1.dropna().astype(int)
    )

    rows.append({
        "Variable":     label,
        "Total":        total_str,
        "Unex_death=0": v0_str,
        "Unex_death=1": v1_str,
        "p-value":      f"{pval:.3f}" if not np.isnan(pval) else "",
        "SMD":          fmt_smd(smd),
    })

# ── 出力 ───────────────────────────────────────────────
os.makedirs(OUT_DIR, exist_ok=True)
result_df = pd.DataFrame(rows, columns=[
    "Variable", "Total", "Unex_death=0", "Unex_death=1", "p-value", "SMD"
])
result_df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

print(f"✅ 完了！ → {OUT_FILE}")
print(result_df.to_string(index=False))