import sqlite3
import pandas as pd

# DBパス
db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

# 接続
conn = sqlite3.connect(db_path)

# データ取得
query = """
SELECT フォロー, unex_death
FROM unex_study
WHERE フォロー IS NOT NULL AND unex_death IS NOT NULL
"""
df = pd.read_sql_query(query, conn)

# 型確認（重要）
df["フォロー"] = pd.to_numeric(df["フォロー"], errors="coerce")
df = df.dropna()

# ---- total ----
total_mean = df["フォロー"].mean()
total_sd = df["フォロー"].std()

print(f"Total: {total_mean:.2f} ± {total_sd:.2f}")

# ---- 2群 ----
group_stats = df.groupby("unex_death")["フォロー"].agg(["mean", "std", "count"])

for idx, row in group_stats.iterrows():
    print(f"unex_death={idx}: {row['mean']:.2f} ± {row['std']:.2f} (n={int(row['count'])})")