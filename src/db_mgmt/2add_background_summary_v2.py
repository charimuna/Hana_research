"""
Background_summary を NowSamari の患者CSVから更新する（v2, 2026-10-22）

v1 からの変更点
- 取り込み時の除外（施設名: 外来・職員・特養 / visit_place=外来 の DELETE）を廃止。
  ID範囲内の全患者を入れ、除外は解析段階で行う。
- 列を追加: death_place（死亡場所）, death_comment（死亡コメント）,
  end_comment（終了コメント）, end_date（終了日）
- 備考は個人情報を含むため取り込まない。
- 実行前に Background_summary をDB内の別テーブルへ退避し、成功したら退避表を削除する
  （失敗時は退避表を残す）。
- CSVは NowSamari フォルダ内の最新の patient_data_YYYYMMDD.csv を自動で使う
  （引数 --csv で指定も可）。

使い方
  python src/db_mgmt/2add_background_summary_v2.py
  python src/db_mgmt/2add_background_summary_v2.py --csv /path/to/patient_data_20270101.csv
- Study_ID は変更しない（UPSERT対象外。件数の不変を検査）。
- 新規に入る患者の Study_ID は NULL（付与は sync_study_id.py）。
"""
import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"
CSV_DIR = Path("/Users/muna/Hana_research/data/raw/NowSamari")
MIN_PATIENT_ID = 150001   # 以上を対象
MAX_PATIENT_ID = 500000   # 未満を対象
ap = argparse.ArgumentParser()
ap.add_argument("--csv", help="患者CSVのパス（省略時は最新の patient_data_YYYYMMDD.csv。_filtered等は除く）")
ap.add_argument("--db", default=DB_PATH, help="DBのパス（テスト用）")
args = ap.parse_args()
DB_PATH = args.db
if args.csv:
    CSV_PATH = Path(args.csv)
else:
    cands = sorted(CSV_DIR.glob("patient_data_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].csv"))
    if not cands:
        raise FileNotFoundError(f"{CSV_DIR} に patient_data_*.csv がありません")
    CSV_PATH = cands[-1]   # ファイル名の日付で最新

RENAME = {
    "患者ID": "Patient_ID",
    "PS": "PS",
    "訪問先区分": "visit_place",
    "施設名": "facility",
    "寝たきり度": "JABC",
    "認知度": "ninchido",
    "介護認定": "Kaiyodo",
    "重要メモ": "memo",
    "タグ": "tag",
    "死亡場所": "death_place",
    "死亡コメント": "death_comment",
    "終了コメント": "end_comment",
    "終了日": "end_date",
}
NEW_COLS = ["death_place", "death_comment", "end_comment", "end_date"]
cols = list(RENAME.values())
update_cols = [c for c in cols if c != "Patient_ID"]

# ===== CSV =====
df = pd.read_csv(CSV_PATH, encoding="cp932", dtype=str, low_memory=False)
print("CSV:", CSV_PATH, "shape:", df.shape)
missing_csv = set(RENAME) - set(df.columns)
if missing_csv:
    raise RuntimeError(f"CSVに存在しない列: {missing_csv}")
df = df[list(RENAME)].rename(columns=RENAME)

# ===== Patient_ID 範囲 =====
df["Patient_ID"] = pd.to_numeric(df["Patient_ID"], errors="coerce")
n0 = len(df)
df = df[(df["Patient_ID"] >= MIN_PATIENT_ID) & (df["Patient_ID"] < MAX_PATIENT_ID)].copy()
df["Patient_ID"] = df["Patient_ID"].astype(int)
print(f"ID範囲外・非数値で除外: {n0 - len(df)}行 / 対象: {len(df)}行")

# ===== 施設名の補完（v1と同じ） =====
df["facility"] = df["facility"].fillna("自宅")

# ===== 文字列の前後空白を除去、空文字は NULL =====
for c in update_cols:
    df[c] = df[c].str.strip().replace("", None)

# ===== 重複 =====
dup = df[df["Patient_ID"].duplicated(keep=False)]
if not dup.empty:
    raise RuntimeError(f"CSV内で Patient_ID が重複: {dup['Patient_ID'].unique()[:10].tolist()}")

rows = df.astype(object).where(df.notna(), None).values.tolist()
sql = (
    f"INSERT INTO Background_summary ({','.join(cols)}) "
    f"VALUES ({','.join('?' * len(cols))}) "
    "ON CONFLICT(Patient_ID) DO UPDATE SET "
    + ",".join(f"{c}=excluded.{c}" for c in update_cols)
)

conn = sqlite3.connect(DB_PATH)
try:
    # ----- 退避 -----
    bak = f"Background_summary_bak_{datetime.now():%Y%m%d_%H%M%S}"
    conn.execute(f"CREATE TABLE {bak} AS SELECT * FROM Background_summary")
    conn.commit()
    print("退避テーブル:", bak)

    # ----- 列追加 -----
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(Background_summary)")}
    for c in NEW_COLS:
        if c not in existing_cols:
            conn.execute(f"ALTER TABLE Background_summary ADD COLUMN {c} TEXT")
            print("列追加:", c)
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(Background_summary)")}
    missing = set(cols + ["Study_ID"]) - existing_cols
    if missing:
        raise RuntimeError(f"テーブルに存在しない列: {missing}")

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_background_summary_patient_id "
        "ON Background_summary(Patient_ID)"
    )

    existing_ids = {r[0] for r in conn.execute("SELECT Patient_ID FROM Background_summary")}
    csv_ids = set(df["Patient_ID"])
    sid_before = dict(conn.execute(
        "SELECT Patient_ID, Study_ID FROM Background_summary WHERE Study_ID IS NOT NULL"))

    conn.execute("BEGIN")
    conn.executemany(sql, rows)

    sid_after = dict(conn.execute(
        "SELECT Patient_ID, Study_ID FROM Background_summary WHERE Study_ID IS NOT NULL"))
    if sid_before != sid_after:
        raise RuntimeError("Study_ID が変化しました。ロールバックします")

    conn.commit()
    conn.execute(f"DROP TABLE {bak}")
    conn.commit()
    print("退避テーブルを削除:", bak)
    total = conn.execute("SELECT COUNT(*) FROM Background_summary").fetchone()[0]
    print(f"UPSERT完了: 新規 {len(csv_ids - existing_ids)} / 更新 {len(csv_ids & existing_ids)} "
          f"/ CSVになく未変更 {len(existing_ids - csv_ids)} / 合計 {total}")
    print(f"Study_ID 付与済み: {len(sid_after)}（不変）")
    for c in NEW_COLS:
        n = conn.execute(f"SELECT COUNT({c}) FROM Background_summary").fetchone()[0]
        print(f"  {c}: 値あり {n}")
except Exception:
    conn.rollback()
    print("失敗。退避テーブルは残しています（Background_summary_bak_*）")
    raise
finally:
    conn.close()

print("⚠️ 次に src/db_mgmt/sync_study_id.py を実行して Study_ID を付与してください（まずドライラン、次に --commit）")
