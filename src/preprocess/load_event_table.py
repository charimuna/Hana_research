import pandas as pd
import sqlite3

# パス
db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
csv_path = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data(20251116).csv"

# CSV読み込み
df = pd.read_csv(csv_path, encoding="cp932")

# ===== 日付処理関数 =====
def format_date(series):
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y/%m/%d")

# 対象カラム（CSV側の名前に合わせて修正すること）
df["death_date"] = format_date(df["死亡日"])
df["end_date"] = format_date(df["終了日"])
df["last_billing_date"] = format_date(df["管理料設定（最終算定日）"])

# ===== Last_confirmed_date 作成 =====
df["last_confirmed_date"] = pd.concat(
    [df["end_date"], df["last_billing_date"], df["death_date"]],
    axis=1
).max(axis=1)

# NaT対策（1970/01/01防止）
df["last_confirmed_date"] = df["last_confirmed_date"].fillna("")

# ===== 必要カラム抽出 =====
out_df = pd.DataFrame({
    "patient_ID": df["患者ID"],
    "status": df["診療ステータス"],
    "death_date": df["death_date"].fillna(""),
    "last_confirmed_date": df["last_confirmed_date"]
})

# ===== SQLiteに書き込み =====
conn = sqlite3.connect(db_path)
out_df.to_sql("event", conn, if_exists="append", index=False)
conn.close()

print("完了")