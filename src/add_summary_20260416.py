import sqlite3
import pandas as pd

# ===== パス設定 =====
db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
csv_path = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260416.csv"

# ===== CSV読み込み =====
df = pd.read_csv(csv_path, encoding="cp932")

# ===== 必要カラム抽出 & リネーム =====
df_selected = df[[
    "患者ID",
    "PS",
    "訪問先区分",
    "施設名",
    "寝たきり度",
    "認知度",
    "介護認定",
    "重要メモ"
]].rename(columns={
    "患者ID": "patient_ID",
    "PS": "PS",
    "訪問先区分": "visit_place",
    "施設名": "facility",
    "寝たきり度": "JABC",
    "認知度": "ninchido",
    "介護認定": "Kaiyodo",
    "重要メモ": "memo"
})

# ===== NULLを「自宅」に置換 =====
df_selected["facility"] = df_selected["facility"].fillna("自宅")

# ===== 除外条件 =====
exclude_keywords = ["外来", "はなまるクリニック職員", "特別養護老人ホーム"]
mask = df_selected["facility"].astype(str).str.contains("|".join(exclude_keywords), na=False)
df_filtered = df_selected[~mask]

# ===== SQLite接続 =====
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# ===== テーブル作成（上書き）=====
df_filtered.to_sql(
    "Background_summary",
    conn,
    if_exists="replace",
    index=False
)

# ===== インデックス作成 =====
cursor.execute("""
CREATE INDEX idx_background_summary_patient_id
ON Background_summary(patient_ID);
""")

conn.commit()
conn.close()

print("完了：Background_summary テーブル作成 + index付与")