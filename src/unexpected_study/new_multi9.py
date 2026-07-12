# ==========================================
# Multivariable logistic regression
# Outcome: unex_death
# Age: continuous variable
# Follow-up: log-transformed continuous variable
# ADL: non-bedridden vs bedridden
# ==========================================

import os
import sqlite3
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.metrics import roc_auc_score, roc_curve
from scipy.stats import chi2
import matplotlib.pyplot as plt


# ------------------------------------------
# SQLite
# ------------------------------------------
DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(DB)

df = pd.read_sql("""
SELECT
    age,
    gender_male1,
    ADL,
    unex_death,
    認知症,
    呼吸器疾患,
    心臓疾患,
    フォロー,
    EN,
    lastHOT
FROM unex_study
""", conn)

conn.close()

# ------------------------------------------
# Rename variables
# ------------------------------------------
df = df.rename(columns={
    "認知症": "dementia",
    "呼吸器疾患": "resp",
    "心臓疾患": "heart",
    "フォロー": "followup",
    "lastHOT": "HOT"
})

# ------------------------------------------
# ADL coding
# J1/J2/A1/A2 = non-bedridden
# B1/B2/C1/C2 = bedridden
# Other or missing ADL values are treated as missing
# ------------------------------------------
nonbed = ["J1", "J2", "A1", "A2"]
bed = ["B1", "B2", "C1", "C2"]

df["nonbedridden"] = np.where(
    df["ADL"].isin(nonbed), 1,
    np.where(df["ADL"].isin(bed), 0, np.nan)
)

# ------------------------------------------
# Follow-up log transformation
# ------------------------------------------
if (df["followup"] <= 0).any():
    raise ValueError(
        "followup includes values <= 0; log transformation is invalid."
    )

df["log_followup"] = np.log(df["followup"])

# ------------------------------------------
# Variables used in the model
# ------------------------------------------
vars_use = [
    "unex_death",
    "age",
    "gender_male1",
    "nonbedridden",
    "dementia",
    "resp",
    "heart",
    "log_followup",
    "EN",
    "HOT"
]

df = df[vars_use].dropna()

print("Complete-case sample size:", df.shape[0])
print("Events:", int((df["unex_death"] == 1).sum()))
print("Non-events:", int((df["unex_death"] == 0).sum()))

# ------------------------------------------
# Force numeric type
# ------------------------------------------
for col in vars_use:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.dropna()

# ------------------------------------------
# Basic checks
# ------------------------------------------
if not set(df["unex_death"].unique()).issubset({0, 1}):
    raise ValueError("unex_death must be coded as 0/1.")

print("\nOutcome distribution")
print(df["unex_death"].value_counts().sort_index())

print("\nPredictor distributions")
for col in [
    "gender_male1",
    "nonbedridden",
    "dementia",
    "resp",
    "heart",
    "EN",
    "HOT"
]:
    print(f"\n{col}")
    print(df[col].value_counts().sort_index())

# ------------------------------------------
# Logistic regression
# ------------------------------------------
X = df.drop(columns="unex_death")
X = sm.add_constant(X)
y = df["unex_death"]

model = sm.Logit(y, X).fit()
print(model.summary())

# ------------------------------------------
# Odds ratio table
# ------------------------------------------
coef = model.params
ci = model.conf_int()
pvalues = model.pvalues

or_table = pd.DataFrame({
    "Variable": coef.index,
    "Coef": coef.values,
    "OR": np.exp(coef.values),
    "Lower95": np.exp(ci[0].values),
    "Upper95": np.exp(ci[1].values),
    "P": pvalues.values
})

or_table = or_table[or_table["Variable"] != "const"]

print("\nOdds ratio table")
print(or_table)

# ------------------------------------------
# VIF
# ------------------------------------------
vif = pd.DataFrame({
    "Variable": X.columns,
    "VIF": [
        variance_inflation_factor(X.values, i)
        for i in range(X.shape[1])
    ]
})

print("\nVIF")
print(vif)

# ------------------------------------------
# Hosmer-Lemeshow test
# ------------------------------------------
pred = model.predict(X)

tmp = pd.DataFrame({
    "y": y,
    "pred": pred
})

tmp["group"] = pd.qcut(tmp["pred"], 10, duplicates="drop")

grouped = tmp.groupby("group", observed=True)

obs = grouped["y"].sum()
exp = grouped["pred"].sum()
n = grouped.size()

hl_components = ((obs - exp) ** 2) / (exp * (1 - exp / n))
HL = hl_components.sum()

df_hl = len(obs) - 2
p_hl = 1 - chi2.cdf(HL, df_hl)

print("\nHosmer-Lemeshow")
print("Chi2 =", HL)
print("df =", df_hl)
print("P =", p_hl)

# ------------------------------------------
# ROC / AUC
# ------------------------------------------
auc = roc_auc_score(y, pred)

print("\nAUC =", auc)

fpr, tpr, _ = roc_curve(y, pred)

# ------------------------------------------
# ROC plot
# ------------------------------------------
plt.figure(figsize=(5, 5))
plt.plot(fpr, tpr, label=f"AUC = {auc:.3f}")
plt.plot([0, 1], [0, 1], "--")
plt.xlabel("False positive rate")
plt.ylabel("True positive rate")
plt.legend()
plt.tight_layout()
plt.show()

# ------------------------------------------
# Age vs outcome
# ------------------------------------------
plt.figure(figsize=(7, 5))
plt.scatter(
    df["age"],
    df["unex_death"],
    alpha=0.2,
    s=18
)

lowess = sm.nonparametric.lowess(
    df["unex_death"],
    df["age"],
    frac=0.4
)

plt.plot(
    lowess[:, 0],
    lowess[:, 1],
    linewidth=3
)

plt.xlabel("Age")
plt.ylabel("Unexplained death")
plt.tight_layout()
plt.show()