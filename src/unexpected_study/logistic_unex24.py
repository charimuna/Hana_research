import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from statsmodels.formula.api import logit

# --- パス設定 ---
DB_PATH    = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUTPUT_DIR = Path("/Users/muna/Hana_research/data/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = OUTPUT_DIR / "logistic_unex24.csv"

# --- DB接続・患者抽出 ---
conn = sqlite3.connect(DB_PATH)

base_query = """
SELECT DISTINCT
    pm.Patient_ID,
    pm.Age_at_Visit,
    pm.Gender,
    bs.JABC,
    bs.tag,
    ud.unex24
FROM Patient_Master pm
JOIN event e                ON pm.Patient_ID = e.Patient_ID
JOIN Background_summary bs  ON pm.Patient_ID = bs.Patient_ID
JOIN unexpected_death ud    ON pm.Patient_ID = ud.Patient_ID
WHERE
    pm.First_Visit_Date BETWEEN '2015-03-01' AND '2025-03-31'
    AND pm.Age_at_Visit >= 20
    AND e.death_date IS NOT NULL
    AND e.death_date <= '2025-10-31'
    AND (e.endstage_car IS NULL OR e.endstage_car != '癌末期')
    AND ud.unex24 IN (0, 1)
"""
df = pd.read_sql_query(base_query, conn)
df["Patient_ID"] = df["Patient_ID"].astype(str)

print(f"抽出患者数: {len(df)}人  （unex24=0: {(df.unex24==0).sum()}人 / unex24=1: {(df.unex24==1).sum()}人）\n")

# --- intervention_history: at_end（1件でもあれば1）---
patient_ids  = df["Patient_ID"].tolist()
placeholders = ",".join(["?"] * len(patient_ids))

ih_df = pd.read_sql_query(
    f"""
    SELECT Patient_ID,
           MAX(CASE WHEN at_end = 1 THEN 1 ELSE 0 END) AS at_end_1
    FROM intervention_history
    WHERE Patient_ID IN ({placeholders})
    GROUP BY Patient_ID
    """,
    conn, params=patient_ids
)
ih_df["Patient_ID"] = ih_df["Patient_ID"].astype(str)

# --- first_diag: 認知症・肺疾患・心疾患（1行1診断、複数行あり）---
diag_df = pd.read_sql_query(
    f"""
    SELECT Patient_ID, category
    FROM first_diag
    WHERE Patient_ID IN ({placeholders})
    """,
    conn, params=patient_ids
)
conn.close()

diag_df["Patient_ID"] = diag_df["Patient_ID"].astype(str)

for label, keyword in [("認知症", "【認知症】"), ("肺疾患", "【肺疾患】"), ("心疾患", "【心疾患】")]:
    matched = diag_df[diag_df["category"] == keyword]["Patient_ID"].drop_duplicates()
    df[label] = df["Patient_ID"].isin(matched).astype(int)

# --- マージ ---
df = df.merge(ih_df, on="Patient_ID", how="left")
df["at_end_1"] = df["at_end_1"].fillna(0).astype(int)

# --- 説明変数の作成 ---
df["age_le84"]        = (df["Age_at_Visit"].fillna(999) <= 80).astype(int)
df["male"]            = (df["Gender"].fillna("") == "男性").astype(int)
df["non_bedridden"]   = df["JABC"].fillna("").isin(["J1","J2","A1","A2"]).astype(int)
df["gastric_tube_peg"] = df["tag"].fillna("").str.contains("胃管|PEG|peg", case=False).astype(int)

features = [
    "age_le84", "male", "non_bedridden",
    "認知症", "肺疾患", "心疾患",
    "at_end_1", "gastric_tube_peg",
]

# --- 完全分離・ゼロ分散の診断と自動除外 ---
print("【変数ごとの分布確認】")
for f in features:
    vc  = df.groupby("unex24")[f].mean()
    var = df[f].var()
    print(f"  {f:30s}  unex24=0: {vc.get(0, float('nan')):.3f}  "
          f"unex24=1: {vc.get(1, float('nan')):.3f}  分散: {var:.4f}")

zero_var = [f for f in features if df[f].var() == 0]
if zero_var:
    print(f"\n⚠️  分散=0のため除外: {zero_var}")
    features = [f for f in features if f not in zero_var]

complete_sep = []
for f in features:
    vc = df.groupby("unex24")[f].mean()
    if vc.get(0, 0.5) in (0.0, 1.0) or vc.get(1, 0.5) in (0.0, 1.0):
        complete_sep.append(f)
if complete_sep:
    print(f"⚠️  完全分離のため除外: {complete_sep}")
    features = [f for f in features if f not in complete_sep]

print(f"\n最終モデルの変数: {features}\n")

# --- ロジスティック回帰（BFGS法で特異行列を回避）---
formula = "unex24 ~ " + " + ".join(features)
model   = logit(formula, data=df).fit(maxiter=500, method="bfgs", disp=False)

# --- 結果整形 ---
VAR_LABEL = {
    "Intercept":        "切片",
    "age_le84":         "80歳以下 (vs 81歳以上)",
    "male":             "男性 (vs 女性)",
    "non_bedridden":    "Non-bedridden (vs bedridden)",
    "認知症":           "認知症",
    "肺疾患":           "肺疾患",
    "心疾患":           "心疾患",
    "at_end_1":         "at_end=1 (intervention_history)",
    "gastric_tube_peg": "胃管 or PEG",
}

summary = pd.DataFrame({
    "変数":        model.params.index,
    "OR":          np.exp(model.params).round(3),
    "95%CI_lower": np.exp(model.conf_int()[0]).round(3),
    "95%CI_upper": np.exp(model.conf_int()[1]).round(3),
    "p値":         model.pvalues.round(4),
})
summary["変数"]  = summary["変数"].map(VAR_LABEL).fillna(summary["変数"])
summary["95%CI"] = summary.apply(lambda r: f"[{r['95%CI_lower']}, {r['95%CI_upper']}]", axis=1)
out = summary[["変数","OR","95%CI","p値"]].reset_index(drop=True)

# --- ターミナル出力 ---
print("=" * 65)
print("  多変量ロジスティック回帰  目的変数: unex24 (1=介入群)")
print(f"  n = {int(model.nobs)}   Pseudo R² (McFadden) = {model.prsquared:.4f}")
print("=" * 65)
print(out.to_string(index=False))
print("=" * 65)
print("\n※ OR: オッズ比  95%CI: 95%信頼区間\n")

# --- CSV保存 ---
out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"✅ CSVを保存しました: {OUTPUT_CSV}")