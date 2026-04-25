import pandas as pd
import sqlite3

# --- 定型: 接続 ---
conn = sqlite3.connect('Hana_Research.db')

# --- ここを書き換えて解析！ ---
query = "......"  # ここにSQLを書く
df = pd.read_sql_query(query, conn)

# --- 定型: 確認 ---
print(f"合計人数: {len(df)}人")
print(df.describe())  # 統計の傾向（平均・最大・最小など）をパッと出す
print(df.shape)  # (1281, 4) のように「人数, 項目の数」が表示される
print(df.info())   # データの欠損（抜け）がないか一瞬でわかる
# 85歳以上の患者さんだけを抽出する命令に書き換え
query = "SELECT * FROM patients WHERE age >= 85"
df_over85 = pd.read_sql_query(query, conn)

print(len(df_over85))  # 85歳以上の該当人数が表示される