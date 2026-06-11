import pandas as pd
import sqlite3
import os

# ファイルパス
CSV_PATH = "/Users/muna/Hana_research/data/processed/予期せぬ死亡_n1281.csv"
DB_PATH  = "/Users/muna/Hana_research/data/db/Hana_Research.db"
TABLE_NAME = "unex_study"

# --- 1. CSVを読み込む ---
print(f"CSVを読み込み中: {CSV_PATH}")
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")   # BOM付きUTF-8にも対応
print(f"  行数: {len(df):,}  列数: {len(df.columns)}")

# --- 2. DBに接続（存在しない場合は自動作成） ---
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

# --- 3. テーブルを作成（既存の場合はスキップ） ---
create_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    "Patient_ID"       INTEGER,
    "age"              INTEGER,
    "gender_male1"     INTEGER,
    "VISITplace_Home1" INTEGER,
    "ADL"              TEXT,
    "Death"            TEXT,
    "unex_death"       INTEGER,
    "HF01"             INTEGER,
    "ins"              INTEGER,
    "認知症"           INTEGER,
    "認知機能障害"     INTEGER,
    "脳血管疾患"       INTEGER,
    "呼吸器疾患"       INTEGER,
    "心臓疾患"         INTEGER,
    "腎疾患"           INTEGER,
    "フォロー"         INTEGER,
    "中心静脈"         REAL,
    "EN"               INTEGER,
    "Unex_48"          REAL,
    "尿カテ"           INTEGER,
    "透析"             INTEGER,
    "lastHOT"          INTEGER,
    "age85under1"      INTEGER,
    "score"            INTEGER,
    "score_Group"      TEXT,
    "Dementia*ADL"     INTEGER,
    "HOT*Resp"         INTEGER,
    "Follow90up1"      INTEGER,
    "age4bun"          INTEGER
);
"""
cur.execute(create_sql)
conn.commit()
print(f"テーブル '{TABLE_NAME}' を確認／作成しました。")

# --- 4. 列名をテーブル定義に合わせて整合性チェック ---
expected_columns = [
    "Patient_ID", "age", "gender_male1", "VISITplace_Home1",
    "ADL", "Death", "unex_death", "HF01", "ins",
    "認知症", "認知機能障害", "脳血管疾患", "呼吸器疾患", "心臓疾患", "腎疾患",
    "フォロー", "中心静脈", "EN", "Unex_48", "尿カテ", "透析", "lastHOT",
    "age85under1", "score", "score_Group", "Dementia*ADL", "HOT*Resp",
    "Follow90up1", "age4bun"
]

missing = [c for c in expected_columns if c not in df.columns]
extra   = [c for c in df.columns if c not in expected_columns]

if missing:
    print(f"[警告] CSV に存在しない列: {missing}")
if extra:
    print(f"[情報] テーブル定義にない追加列（無視されます）: {extra}")

# テーブル定義の列のみ抽出し順序を揃える
available = [c for c in expected_columns if c in df.columns]
df_insert = df[available]

# --- 5. データを挿入 ---
print("データを挿入中...")
df_insert.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
conn.commit()

# --- 6. 確認 ---
count = cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
print(f"✅ 完了！ '{TABLE_NAME}' テーブルの総行数: {count:,}")

conn.close()