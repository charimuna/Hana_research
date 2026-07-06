# ==========================================
# Multivariable logistic regression
# Age as continuous variable
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
# 出力先（Desktop / multi.csv）
# ------------------------------------------
OUTPUT_CSV = os.path.expanduser("~/Desktop/multi.csv")

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
    lastHOT,
    EN
FROM unex_study
""", conn)
conn.close()

# ------------------------------------------
# ADL → nonbedridden
# ------------------------------------------
nonbed = ["J1", "J2", "A1", "A2"]
df["nonbedridden"] = df["ADL"].isin(nonbed).astype(int)

# ------------------------------------------
# rename
# ------------------------------------------
df = df.rename(columns={
    "認知症": "dementia",
    "呼吸器疾患": "resp",
    "心臓疾患": "heart",
    "lastHOT": "HOT",
    "EN": "EN"
})

# ------------------------------------------
# 欠損除去
# ------------------------------------------
vars_use = [
    "unex_death",
    "age",
    "gender_male1",
    "nonbedridden",
    "dementia",
    "resp",
    "heart",
    "HOT",
    "EN"
]
df = df[vars_use].dropna()
print(df.shape)

# ------------------------------------------
# Logistic regression
# ------------------------------------------
X = df.drop(columns="unex_death")
X = sm.add_constant(X)
y = df["unex_death"]
model = sm.Logit(y, X).fit()
print(model.summary())

# ------------------------------------------
# OR
# ------------------------------------------
coef = model.params
CI = model.conf_int()
OR = np.exp(coef)
Lower = np.exp(CI[0])
Upper = np.exp(CI[1])
result = pd.DataFrame({
    "Variable": coef.index,
    "Coef": coef.values,
    "OR": OR.values,
    "Lower95": Lower.values,
    "Upper95": Upper.values,
    "P": model.pvalues.values
})
print("\n")
print(result)

# ------------------------------------------
# VIF
# ------------------------------------------
vif = pd.DataFrame()
vif["Variable"] = X.columns
vif["VIF"] = [
    variance_inflation_factor(X.values, i)
    for i in range(X.shape[1])
]
print("\nVIF")
print(vif)

# ------------------------------------------
# Hosmer-Lemeshow
# ------------------------------------------
pred = model.predict(X)
tmp = pd.DataFrame({
    "y": y,
    "pred": pred
})
tmp["group"] = pd.qcut(tmp["pred"], 10, duplicates="drop")
obs = tmp.groupby("group")["y"].sum()
exp = tmp.groupby("group")["pred"].sum()
n = tmp.groupby("group").size()
HL = np.sum(
    ((obs - exp) ** 2) / (exp * (1 - exp / n))
)
df_hl = len(obs) - 2
p_hl = 1 - chi2.cdf(HL, df_hl)
print("\nHosmer-Lemeshow")
print("Chi2 =", HL)
print("P =", p_hl)

# ------------------------------------------
# ROC / AUC
# ------------------------------------------
auc = roc_auc_score(y, pred)
print("\nAUC =", auc)
fpr, tpr, _ = roc_curve(y, pred)

# ------------------------------------------
# CSVへ出力（Desktop/multi.csv）
# ------------------------------------------
# 1) メインのOR表
result.to_csv(OUTPUT_CSV, index=False)

# 2) 続けてVIF・モデル適合度指標を同じファイルに追記
with open(OUTPUT_CSV, "a", encoding="utf-8-sig") as f:
    f.write("\n")

vif.to_csv(OUTPUT_CSV, mode="a", index=False, encoding="utf-8-sig")

with open(OUTPUT_CSV, "a", encoding="utf-8-sig") as f:
    f.write("\n")
    f.write("Metric,Value\n")
    f.write(f"N,{df.shape[0]}\n")
    f.write(f"Hosmer-Lemeshow Chi2,{HL}\n")
    f.write(f"Hosmer-Lemeshow df,{df_hl}\n")
    f.write(f"Hosmer-Lemeshow P,{p_hl}\n")
    f.write(f"AUC,{auc}\n")

print(f"\nCSV出力完了: {OUTPUT_CSV}")

# ------------------------------------------
# ROC plot
# ------------------------------------------
plt.figure(figsize=(5, 5))
plt.plot(fpr, tpr, label=f"AUC={auc:.3f}")
plt.plot([0, 1], [0, 1], "--")
plt.xlabel("False Positive Rate")
plt.ylabel("True Positive Rate")
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
plt.ylabel("Absence of documented recognition")
plt.tight_layout()
plt.show()