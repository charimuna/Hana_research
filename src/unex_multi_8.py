"""
unex_study 多変量ロジスティック回帰（HF01除外・論文用）
- 単変量 OR
- 多変量 OR（全変数モデル）
- VIF
- AUC
- Nagelkerke R²
"""

import sqlite3
import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.metrics import roc_auc_score

# ── パス ─────────────────────────────
DB_PATH  = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUT_DIR  = "/Users/muna/Hana_research/output"
OUT_FILE = os.path.join(OUT_DIR, "unex_study_multivariate_final.csv")

# ── DB読み込み ───────────────────────
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM unex_study", conn)
conn.close()

# ── ADL 二値化 ──────────────────────
def adl_to_binary(val):
    if pd.isna(val):
        return np.nan
    v = str(val).strip().upper()
    if v.startswith(("J", "A")):
        return 1
    elif v.startswith(("B", "C")):
        return 0
    return np.nan

df["ADL_bin"] = df["ADL"].apply(adl_to_binary)

# ── 変数（HF01削除） ─────────────────
OUTCOME = "unex_death"

VARS = [
    ("age85under1",  "Age ≤84"),
    ("ADL_bin",      "Non-bedridden ADL"),
    ("gender_male1", "Male"),
    # ("HF01",       "Heart failure"), ← 削除
    ("認知症",        "Dementia"),
    ("呼吸器疾患",    "Respiratory disease"),
    ("心臓疾患",      "Cardiac disease"),
    ("EN",           "Enteral nutrition"),
    ("lastHOT",      "Home oxygen"),
]

VAR_COLS   = [v[0] for v in VARS]
VAR_LABELS = {v[0]: v[1] for v in VARS}

# ── データ整形 ──────────────────────
cols_needed = [OUTCOME] + VAR_COLS
ana = df[cols_needed].copy()

for c in VAR_COLS:
    ana[c] = pd.to_numeric(ana[c], errors="coerce")

ana = ana.dropna()
ana_fl = ana.astype(float)

print(f"N (complete case) = {len(ana_fl)}")
print("\nEvent count:")
print(ana_fl[OUTCOME].value_counts())

# ── ロジスティック関数 ─────────────
def run_logit(X, y):
    X = sm.add_constant(X)
    return sm.Logit(y, X).fit(disp=0)

def extract_or(model, var):
    or_ = np.exp(model.params[var])
    ci  = np.exp(model.conf_int().loc[var])
    p   = model.pvalues[var]
    return or_, ci[0], ci[1], p

# ── 単変量 ──────────────────────────
uni_results = {}
for col in VAR_COLS:
    try:
        m = run_logit(ana_fl[[col]], ana_fl[OUTCOME])
        uni_results[col] = extract_or(m, col)
    except:
        uni_results[col] = (np.nan, np.nan, np.nan, np.nan)

# ── 多変量（全投入） ────────────────
full_model = run_logit(ana_fl[VAR_COLS], ana_fl[OUTCOME])

multi_results = {}
for col in VAR_COLS:
    multi_results[col] = extract_or(full_model, col)

# ── VIF ─────────────────────────────
X_vif = sm.add_constant(ana_fl[VAR_COLS])
vif_data = []
for i, col in enumerate(X_vif.columns):
    if col == "const":
        continue
    vif = variance_inflation_factor(X_vif.values, i)
    vif_data.append((col, vif))

print("\nVIF:")
for v in vif_data:
    print(f"{v[0]}: {v[1]:.2f}")

# ── AUC ─────────────────────────────
y_true = ana_fl[OUTCOME]
y_pred = full_model.predict(sm.add_constant(ana_fl[VAR_COLS]))
auc = roc_auc_score(y_true, y_pred)
print(f"\nAUC: {auc:.3f}")

# ── Nagelkerke R² ──────────────────
def nagelkerke_r2(model, n):
    llf = model.llf
    llnull = model.llnull
    r2 = 1 - np.exp((2/n)*(llnull-llf))
    r2max = 1 - np.exp((2/n)*llnull)
    return r2/r2max

nagel = nagelkerke_r2(full_model, len(ana_fl))
print(f"Nagelkerke R²: {nagel:.3f}")

# ── 出力整形 ───────────────────────
def fmt(or_, lo, hi, p):
    if np.isnan(or_):
        return "", ""
    return f"{or_:.2f} [{lo:.2f}-{hi:.2f}]", f"{p:.3f}"

rows = []
for col in VAR_COLS:
    u = uni_results[col]
    m = multi_results[col]

    u_str, u_p = fmt(*u)
    m_str, m_p = fmt(*m)

    rows.append({
        "Variable": VAR_LABELS[col],
        "Univariate OR [95%CI]": u_str,
        "Univariate p": u_p,
        "Adjusted OR [95%CI]": m_str,
        "Adjusted p": m_p,
    })

# モデル指標
rows.append({})
rows.append({"Variable": "AUC", "Univariate OR [95%CI]": f"{auc:.3f}"})
rows.append({"Variable": "Nagelkerke R²", "Univariate OR [95%CI]": f"{nagel:.3f}"})
rows.append({"Variable": "AIC", "Univariate OR [95%CI]": f"{full_model.aic:.3f}"})
rows.append({"Variable": "N", "Univariate OR [95%CI]": str(len(ana_fl))})

# ── 保存 ───────────────────────────
os.makedirs(OUT_DIR, exist_ok=True)
pd.DataFrame(rows).to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

print(f"\n保存完了: {OUT_FILE}")