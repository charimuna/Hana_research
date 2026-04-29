import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

# ---------- フォント設定（論文用サンセリフ体） ----------
mpl.rcParams['pdf.fonttype'] = 42
mpl.rcParams['ps.fonttype'] = 42
plt.rcParams['font.family'] = 'Arial'  # フォントをArialに指定

# ---------- DB接続（既存のコードを維持） ----------
conn = sqlite3.connect("/Users/muna/Hana_research/data/db/Hana_Research.db")
df = pd.read_sql("SELECT score_Group, unex_death FROM unex_study", conn)
conn.close()

# ---------- データ整理 ----------
order = ["low", "mid", "high"]
df["score_Group"] = pd.Categorical(df["score_Group"], categories=order, ordered=True)

summary = df.groupby("score_Group").agg(
    events=("unex_death", "sum"),
    total=("unex_death", "count")
).reset_index()

summary["p"] = summary["events"] / summary["total"]

# ---------- Wilson CI 計算 ----------
def wilson_ci(k, n, z=1.96):
    p = k / n
    denom = 1 + z**2 / n
    center = (p + z**2/(2*n)) / denom
    margin = z * np.sqrt((p*(1-p) + z**2/(4*n)) / n) / denom
    return (center - margin) * 100, (center + margin) * 100

summary[["low_ci", "high_ci"]] = summary.apply(
    lambda r: pd.Series(wilson_ci(r["events"], r["total"])), axis=1
)
summary["percent"] = summary["p"] * 100

# ---------- プロット作成 ----------
fig, ax = plt.subplots(figsize=(5.5, 4.5))

x = np.arange(len(summary))
y = summary["percent"]
yerr = [y - summary["low_ci"], summary["high_ci"] - y]

# 棒グラフ（エッジを細く、色を落ち着いたグレーに）
ax.bar(x, y, edgecolor='black', color='lightgray', linewidth=0.8, width=0.6)

# エラーバー（キャップサイズを小さめに設定）
ax.errorbar(x, y, yerr=yerr, fmt='none', capsize=3, color='black', linewidth=1)

# ---------- 統計情報の追加 ----------
# %表示（CIは削除してスッキリさせる）
for i, val in enumerate(y):
    ax.text(i, summary["high_ci"][i] + 1.0, f"{val:.1f}%", 
            ha='center', va='bottom', fontsize=9, fontweight='bold')

# P for trend を右上に配置
ax.text(0.95, 0.95, 'P for trend < 0.001', transform=ax.transAxes, 
        ha='right', va='top', fontsize=9, fontstyle='italic')

# ---------- 軸の設定 ----------
labels = ["Low (0–1)", "Intermediate (2–3)", "High (4–6)"]
ax.set_xticks(x)
ax.set_xticklabels(labels, fontsize=9)
ax.set_ylabel("Unexpected death (%)", fontsize=10)
ax.set_ylim(0, 45)

# 目盛りを外側に向ける（医学論文の定番スタイル）
ax.tick_params(direction='out', length=4)

# ---------- 下部テキスト（n数のまとめ方） ----------
y_row1 = -7   # Unexpected deaths 用の高さ
y_row2 = -11  # Total patients 用の高さ

for i, row in summary.iterrows():
    ax.text(i, y_row1, f"{int(row['events'])}", ha='center', va='top', fontsize=8)
    ax.text(i, y_row2, f"{int(row['total'])}", ha='center', va='top', fontsize=8)

# 左側のラベル（太字で明示）
ax.text(-0.6, y_row1, "Unexpected deaths, n", ha='right', va='top', fontsize=8, fontweight='bold', clip_on=False)
ax.text(-0.6, y_row2, "Total patients, n",    ha='right', va='top', fontsize=8, fontweight='bold', clip_on=False)

# ---------- レイアウト調整 ----------
plt.subplots_adjust(left=0.3, right=0.95, bottom=0.25)

# ---------- 保存 ----------
plt.savefig("figure2_refined.pdf", bbox_inches='tight')
plt.show()