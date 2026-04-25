import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

# ---------- フォント埋め込み ----------
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42

# ---------- DB ----------
conn = sqlite3.connect("/Users/muna/Hana_research/data/db/Hana_Research.db")
df = pd.read_sql("SELECT score_Group, unex_death FROM unex_study", conn)
conn.close()

# ---------- 順序 ----------
order = ["low", "mid", "high"]
df["score_Group"] = pd.Categorical(df["score_Group"], categories=order, ordered=True)

# ---------- 集計 ----------
summary = df.groupby("score_Group").agg(
    events=("unex_death", "sum"),
    total=("unex_death", "count")
).reset_index()

summary["p"] = summary["events"] / summary["total"]

# ---------- Wilson CI ----------
def wilson_ci(k, n, z=1.96):
    p = k / n
    denom = 1 + z**2 / n
    center = (p + z**2/(2*n)) / denom
    margin = z * np.sqrt((p*(1-p) + z**2/(4*n)) / n) / denom
    return center - margin, center + margin

cis = summary.apply(lambda r: wilson_ci(r["events"], r["total"]), axis=1)
summary["low"] = [c[0] for c in cis]
summary["high"] = [c[1] for c in cis]

# ---------- %変換 ----------
summary["percent"] = summary["p"] * 100
summary["low"] = summary["low"] * 100
summary["high"] = summary["high"] * 100

# ---------- 表示ラベル ----------
labels = ["Low (0–1)", "Intermediate (2–3)", "High (4–6)"]

# ---------- プロット ----------
fig, ax = plt.subplots(figsize=(5.2, 4.2))

x = np.arange(len(summary))
y = summary["percent"]

yerr = [
    y - summary["low"],
    summary["high"] - y
]

# 棒グラフ
ax.bar(x, y, edgecolor='black', color='gray', alpha=0.5, width=0.6)

# エラーバー
ax.errorbar(x, y, yerr=yerr, fmt='none', capsize=4, color='black')

# ---------- % + CI ----------
for i, row in summary.iterrows():
    ax.text(
        i,
        row["high"] + 1.5,
        f"{row['percent']:.1f}%\n({row['low']:.1f}–{row['high']:.1f})",
        ha='center',
        va='bottom',
        fontsize=8
    )

# ---------- 軸 ----------
ax.set_xticks(x)
ax.set_xticklabels(labels, fontsize=8)
ax.set_ylabel("Unexpected death (%)", fontsize=8)
ax.set_ylim(0, 45)

# ---------- 下部テキスト ----------
y1 = -7   # events
y2 = -11  # total

for i, row in summary.iterrows():
    ax.text(i, y1, f"{int(row['events'])}", ha='center', va='top', fontsize=7)
    ax.text(i, y2, f"{int(row['total'])}", ha='center', va='top', fontsize=7)

# 行ラベル（clip_on=False を追加）
ax.text(-0.6, y1, "Unexpected deaths", ha='right', va='top', fontsize=7, clip_on=False)
ax.text(-0.6, y2, "Total patients",    ha='right', va='top', fontsize=7, clip_on=False)

# ---------- レイアウト（左余白拡張・下余白縮小） ----------
plt.subplots_adjust(left=0.27, right=0.98, bottom=0.26)  # ← ここを変更

# ---------- 保存 ----------
plt.savefig("figure2.pdf")
plt.close()