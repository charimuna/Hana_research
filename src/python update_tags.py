import sqlite3
import pandas as pd

# --- パス設定 ---
DB_PATH  = "/Users/muna/Hana_research/data/db/Hana_Research.db"
CSV_PATH = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260419_filtered.csv"

# --- CSVの読み込み ---
df = pd.read_csv(CSV_PATH)

# 必要な列の存在確認
required_cols = {"患者ID", "タグ"}
missing = required_cols - set(df.columns)
if missing:
    raise ValueError(f"CSVに必要な列が見つかりません: {missing}")

# 使う列だけ取り出す
tag_map = df[["患者ID", "タグ"]].dropna(subset=["患者ID"])

# --- DB操作 ---
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

try:
    # tagカラムが存在しない場合のみ追加
    existing_cols = [row[1] for row in cursor.execute("PRAGMA table_info(Background_summary)")]
    if "tag" not in existing_cols:
        cursor.execute("ALTER TABLE Background_summury ADD COLUMN tag TEXT")
        print("✅ tagカラムを追加しました")
    else:
        print("ℹ️  tagカラムは既に存在します（上書き更新します）")

    # Patient_IDで紐づけてtagを更新
    updated = 0
    skipped = 0
    for _, row in tag_map.iterrows():
        patient_id = row["患者ID"]
        tag_value  = row["タグ"]

        cursor.execute(
            "UPDATE Background_summury SET tag = ? WHERE Patient_ID = ?",
            (tag_value, patient_id)
        )
        if cursor.rowcount > 0:
            updated += 1
        else:
            skipped += 1

    conn.commit()
    print(f"✅ 更新完了: {updated}件 更新 / {skipped}件 DBに該当レコードなし（スキップ）")

except Exception as e:
    conn.rollback()
    print(f"❌ エラーが発生しました: {e}")
    raise

finally:
    conn.close()