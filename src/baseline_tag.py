import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

# --- パス設定 ---
DB_PATH     = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUTPUT_DIR  = Path("/Users/muna/Hana_research/data/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV  = OUTPUT_DIR / "unex24_comparison.csv"

# --- DB接続 ---
conn = sqlite3.connect(DB_PATH)

# --- 対象患者の抽出 ---
query = """
SELECT
    pm.Patient_ID,
    bs.tag,
    ud.unex24
FROM Patient_Master pm
JOIN event e          ON pm.Patient_ID = e.Patient_ID
JOIN Background_summary bs ON pm.Patient_ID = bs.Patient_ID
JOIN unexpected_death ud   ON pm.Patient_ID = ud.Patient_ID
WHERE
    pm.First_Visit_Date BETWEEN '2015-03-01' AND '2025-03-31'
    AND pm.Age_at_Visit >= 20
    AND e.death_date IS NOT NULL
    AND e.death_date <= '2025-10-31'
    AND (e.endstage_car IS NULL OR e.endstage_car != '癌末期')
    AND ud.unex24 IN (0, 1)
"""
df = pd.read_sql_query(query, conn)
conn.close()

print(f"抽出患者数: {len(df)}人  （unex24=0: {(df.unex24==0).sum()}人 / unex24=1: {(df.unex24==1).sum()}人）\n")

# --- タグのダミー変数化 ---
# tagカラムに複数タグがカンマ区切り等で入っている場合に対応
TAG_KEYWORDS = {
    "胃管 or PEG":                       ["胃管", "PEG", "peg"],
    "膀胱瘻カテーテル or 尿道カテーテル": ["膀胱瘻", "尿道カテーテル"],
    "インスリン":         ["インスリン"],
    "透析":               ["透析"],
}

for label, keywords in TAG_KEYWORDS.items():
    pattern = "|".join(keywords)
    df[label] = df["tag"].fillna("").str.contains(pattern, case=False).astype(int)

# --- intervention_historyテーブルのat_end = 1 の割合 ---
conn = sqlite3.connect(DB_PATH)

# 対象Patient_IDのみ絞り込む
patient_ids = df["Patient_ID"].tolist()
placeholders = ",".join(["?"] * len(patient_ids))

ih_query = f"""
SELECT Patient_ID,
       MAX(CASE WHEN at_end = 1 THEN 1 ELSE 0 END) AS at_end_1
FROM intervention_history
WHERE Patient_ID IN ({placeholders})
GROUP BY Patient_ID
"""
ih_df = pd.read_sql_query(ih_query, conn, params=patient_ids)
conn.close()

# Patient_IDの型を統一（int64 vs str の不一致を防ぐ）
df["Patient_ID"]    = df["Patient_ID"].astype(str)
ih_df["Patient_ID"] = ih_df["Patient_ID"].astype(str)

df = df.merge(ih_df, on="Patient_ID", how="left")
df["at_end_1"] = df["at_end_1"].fillna(0).astype(int)

# --- 比較する変数リスト ---
compare_vars = list(TAG_KEYWORDS.keys()) + ["at_end_1"]

# --- SMD計算関数（二値変数） ---
def calc_smd_binary(p1, p2, n1, n2):
    """Cohen's h に基づくSMD（二値変数）"""
    if n1 == 0 or n2 == 0:
        return np.nan
    pooled_sd = np.sqrt((p1 * (1 - p1) + p2 * (1 - p2)) / 2)
    if pooled_sd == 0:
        return np.nan
    return abs(p1 - p2) / pooled_sd

# --- 2群間比較 ---
g0 = df[df["unex24"] == 0]
g1 = df[df["unex24"] == 1]
n0, n1 = len(g0), len(g1)

results = []
for var in compare_vars:
    p0 = g0[var].mean()
    p1 = g1[var].mean()
    smd = calc_smd_binary(p0, p1, n0, n1)
    results.append({
        "変数":              var,
        f"unex24=0 割合 (n={n0})": f"{p0:.1%}  ({int(g0[var].sum())}/{n0})",
        f"unex24=1 割合 (n={n1})": f"{p1:.1%}  ({int(g1[var].sum())}/{n1})",
        "SMD":               f"{smd:.3f}" if not np.isnan(smd) else "N/A",
    })

result_df = pd.DataFrame(results)

# --- ターミナル出力 ---
print("=" * 75)
print(f"  unex24 2群間比較   対照群(0): n={n0}   介入群(1): n={n1}")
print("=" * 75)
print(result_df.to_string(index=False))
print("=" * 75)
print("\n※ SMD ≥ 0.1 で臨床的に意味のある差とみなすことが多い\n")

# --- CSV保存 ---
result_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"✅ CSVを保存しました: {OUTPUT_CSV}")