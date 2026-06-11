import pandas as pd
import sqlite3

# ===== パス =====
csv_path = "/Users/muna/Hana_research/data/derived/unexpect_01_n1500.csv"
db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

# ===== CSV読み込み（文字コード対応）=====
df = None
for enc in ["cp932", "utf-8", "shift_jis"]:
    try:
        df = pd.read_csv(csv_path, dtype=str, encoding=enc)
        print(f"読み込み成功: {enc}")
        break
    except:
        continue

if df is None:
    raise Exception("CSV読み込み失敗")

# ===== 列取得（A列, S列）=====
patient_col = df.columns[0]   # A列
unex_col = df.columns[18]     # S列（0-indexで18）

df_sub = df[[patient_col, unex_col]].copy()
df_sub.columns = ["patient_ID", "unex24"]

# ===== 不要行除外 =====
df_sub = df_sub[df_sub["patient_ID"].notna()]

# ===== DB接続 =====
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# ===== テーブル作成（上書き）=====
cur.execute("DROP TABLE IF EXISTS unexpected_death")

cur.execute("""
CREATE TABLE unexpected_death (
    patient_ID TEXT,
    unex24 INTEGER
)
""")

# ===== データ型調整 =====
df_sub["unex24"] = pd.to_numeric(df_sub["unex24"], errors="coerce")

# ===== 書き込み =====
df_sub.to_sql("unexpected_death", conn, if_exists="append", index=False)

conn.commit()
conn.close()

print("完了: unexpected_death テーブル作成")
print("件数:", len(df_sub))
print("ユニーク患者数:", df_sub["patient_ID"].nunique())