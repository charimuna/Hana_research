import os
import random
import re
import shutil
import sqlite3
from datetime import datetime

import pandas as pd

# =========================
# settings
# =========================
CSV_PATH = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260914.csv"
LINKAGE_DB = "/Volumes/linkage_secure/hana_linkage.db"

MIN_PATIENT_ID = 150001   # 以上を対象
MAX_PATIENT_ID = 500000   # 未満を対象(500000以上は除外)

MAX_STUDY_NO = 999999     # P000001 ~ P999999
ID_PATTERN = re.compile(r"^P(\d{6})$")

# =========================
# 事前チェック
# =========================
mount_dir = os.path.dirname(LINKAGE_DB)
if not os.path.isdir(mount_dir):
    raise RuntimeError(f"リンケージ用ボリュームがマウントされていません: {mount_dir}")

# =========================
# CSV読み込み → ユニークな Patient_ID
# =========================
df = pd.read_csv(CSV_PATH, encoding="cp932", usecols=["患者ID"], low_memory=False)
pid = pd.to_numeric(df["患者ID"], errors="coerce")
n_nonnum = int(pid.isna().sum())
pid = pid.dropna()
pid = pid[(pid >= MIN_PATIENT_ID) & (pid < MAX_PATIENT_ID)].astype(int)
csv_ids = set(pid.unique().tolist())

print("CSV:", CSV_PATH)
print(f"CSV rows: {len(df)} / 非数値・欠損: {n_nonnum}行")
print(f"対象ユニーク Patient_ID: {len(csv_ids)}人")

if not csv_ids:
    raise RuntimeError("対象の Patient_ID が0件です")

# =========================
# 既存リンケージの読み込み(なければ空)
# =========================
db_exists = os.path.exists(LINKAGE_DB)
conn = sqlite3.connect(LINKAGE_DB)
try:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS linkage (
            Patient_ID INTEGER PRIMARY KEY,
            Study_ID   TEXT UNIQUE NOT NULL
        )
    """)
    conn.commit()

    existing = dict(conn.execute("SELECT Patient_ID, Study_ID FROM linkage"))
    n_before = len(existing)

    # 既存 Study_ID の形式チェック & 使用済み番号
    used_nos = set()
    for p, s in existing.items():
        m = ID_PATTERN.match(s)
        if not m:
            raise RuntimeError(f"想定外の Study_ID 形式: Patient_ID={p}, Study_ID={s}")
        used_nos.add(int(m.group(1)))
    if len(used_nos) != n_before:
        raise RuntimeError("既存の Study_ID に重複があります")

    # =========================
    # 追加対象 = CSVにあってリンケージにない患者
    # =========================
    new_ids = sorted(csv_ids - set(existing))
    n_not_in_csv = len(set(existing) - csv_ids)
    print(f"リンケージ既存: {n_before}人 / 新規追加: {len(new_ids)}人 "
          f"/ 既存のうちCSVにいない(変更しない): {n_not_in_csv}人")

    if not new_ids:
        print("追加なし。リンケージは変更していません。")
    else:
        unused = [n for n in range(1, MAX_STUDY_NO + 1) if n not in used_nos]
        if len(new_ids) > len(unused):
            raise RuntimeError(f"未使用番号が足りません: 必要 {len(new_ids)} / 残り {len(unused)}")

        # 未使用番号からランダムに選ぶ(シードは記録しない)
        chosen = random.SystemRandom().sample(unused, len(new_ids))
        rows = [(p, f"P{n:06d}") for p, n in zip(new_ids, chosen)]

        # 変更前にバックアップ
        if db_exists:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = f"{LINKAGE_DB}.bak_{stamp}"
            conn.close()
            shutil.copy2(LINKAGE_DB, backup)
            print("バックアップ:", backup)
            conn = sqlite3.connect(LINKAGE_DB)

        # 追記のみ(既存行は更新・削除しない)
        conn.execute("BEGIN")
        conn.executemany("INSERT INTO linkage (Patient_ID, Study_ID) VALUES (?, ?)", rows)

        # 検証
        n_after = conn.execute("SELECT COUNT(*) FROM linkage").fetchone()[0]
        if n_after != n_before + len(rows):
            raise RuntimeError(f"件数不一致: {n_after} != {n_before}+{len(rows)}")

        linked = {r[0] for r in conn.execute("SELECT Patient_ID FROM linkage")}
        missing = csv_ids - linked
        if missing:
            raise RuntimeError(f"リンケージにない CSV患者: {len(missing)}人")

        changed = [p for p, s in existing.items()
                   if conn.execute("SELECT Study_ID FROM linkage WHERE Patient_ID=?", (p,)).fetchone()[0] != s]
        if changed:
            raise RuntimeError(f"既存の Study_ID が変化: {len(changed)}件")

        conn.commit()
        print(f"追加完了: {len(rows)}人 (合計 {n_after}人)")

    # =========================
    # 確認
    # =========================
    print("\n=== linkage preview ===")
    print(pd.read_sql_query("SELECT * FROM linkage ORDER BY Study_ID LIMIT 10", conn))
    print("\n=== total rows ===")
    print(pd.read_sql_query("SELECT COUNT(*) AS n FROM linkage", conn))
except Exception:
    conn.rollback()
    raise
finally:
    conn.close()

print("\nDone.")