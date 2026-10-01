import sqlite3
import pandas as pd

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
csv_path = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20261001.csv"
MIN_PATIENT_ID = 150001   # 以上を対象
MAX_PATIENT_ID = 500000   # 未満を対象(500000以上は除外)

# INSERT用の列順(Study_IDは含めない)
cols = ["Patient_ID", "PS", "visit_place", "facility",
        "JABC", "ninchido", "Kaiyodo", "memo", "tag"]
update_cols = [c for c in cols if c != "Patient_ID"]

# ===== CSV読み込み =====
df = pd.read_csv(csv_path, encoding="cp932", low_memory=False)
print("CSV:", csv_path)
print("shape:", df.shape, "/ 先頭列:", list(df.columns[:5]))

# ===== 必要カラム抽出 & リネーム =====
df = df[["患者ID", "PS", "訪問先区分", "施設名",
         "寝たきり度", "認知度", "介護認定", "重要メモ", "タグ"]].rename(columns={
    "患者ID": "Patient_ID",
    "訪問先区分": "visit_place",
    "施設名": "facility",
    "寝たきり度": "JABC",
    "認知度": "ninchido",
    "介護認定": "Kaiyodo",
    "重要メモ": "memo",
    "タグ": "tag",
})
df = df[cols]

# ===== Patient_ID を数値化し、150001以上 500000未満のみ残す =====
df["Patient_ID"] = pd.to_numeric(df["Patient_ID"], errors="coerce")
n_before = len(df)
df = df[(df["Patient_ID"] >= MIN_PATIENT_ID) & (df["Patient_ID"] < MAX_PATIENT_ID)].copy()
df["Patient_ID"] = df["Patient_ID"].astype(int)
print(f"Patient_ID が {MIN_PATIENT_ID}未満・{MAX_PATIENT_ID}以上・非数値で除外: {n_before - len(df)}行")

# ===== 施設名の補完・除外 =====
df["facility"] = df["facility"].fillna("自宅")
exclude = "|".join(["外来", "はなまるクリニック職員", "特別養護老人ホーム"])
df = df[~df["facility"].astype(str).str.contains(exclude, na=False)]

# ===== 重複チェック =====
dup = df[df["Patient_ID"].duplicated(keep=False)]
if not dup.empty:
    raise RuntimeError(
        f"CSV内で Patient_ID が重複: {dup['Patient_ID'].nunique()}種類 / {len(dup)}行 "
        f"(例: {dup['Patient_ID'].unique()[:5].tolist()})"
    )

# NaN -> None(SQLiteでNULLにする)
rows = df.astype(object).where(df.notna(), None).values.tolist()

sql = (
    f"INSERT INTO Background_summary ({','.join(cols)}) "
    f"VALUES ({','.join('?' * len(cols))}) "
    f"ON CONFLICT(Patient_ID) DO UPDATE SET "
    + ",".join(f"{c}=excluded.{c}" for c in update_cols)
)

conn = sqlite3.connect(db_path)
try:
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(Background_summary)")}
    missing = set(cols + ["Study_ID"]) - existing_cols
    if missing:
        raise RuntimeError(f"既存テーブルに存在しない列: {missing}")

    # Patient_ID の UNIQUE 制約(UPSERT に必要)。既にあれば何もしない
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_background_summary_patient_id "
        "ON Background_summary(Patient_ID)"
    )

    existing_ids = {r[0] for r in conn.execute("SELECT Patient_ID FROM Background_summary")}
    csv_ids = set(df["Patient_ID"])
    n_new = len(csv_ids - existing_ids)
    n_upd = len(csv_ids & existing_ids)
    n_untouched = len(existing_ids - csv_ids)

    before = conn.execute(
        "SELECT COUNT(*) FROM Background_summary WHERE Study_ID IS NOT NULL"
    ).fetchone()[0]

    conn.execute("BEGIN")

    # ----- UPSERT -----
    conn.executemany(sql, rows)

    after_upsert = conn.execute(
        "SELECT COUNT(*) FROM Background_summary WHERE Study_ID IS NOT NULL"
    ).fetchone()[0]
    if before != after_upsert:
        raise RuntimeError(f"UPSERTでStudy_IDの件数が変化しました: {before} -> {after_upsert}")

    # ----- visit_place が外来の患者を削除 -----
    n_out_total = conn.execute(
        "SELECT COUNT(*) FROM Background_summary WHERE visit_place = '外来'"
    ).fetchone()[0]
    n_out_with_sid = conn.execute(
        "SELECT COUNT(*) FROM Background_summary WHERE visit_place = '外来' AND Study_ID IS NOT NULL"
    ).fetchone()[0]
    conn.execute("DELETE FROM Background_summary WHERE visit_place = '外来'")

    after_delete = conn.execute(
        "SELECT COUNT(*) FROM Background_summary WHERE Study_ID IS NOT NULL"
    ).fetchone()[0]
    if after_delete != before - n_out_with_sid:
        raise RuntimeError("Study_ID の件数が想定と一致しません")

    conn.commit()
    print(f"UPSERT完了: 新規 {n_new}件 / 更新 {n_upd}件 / CSVになく未変更 {n_untouched}件")
    print(f"外来(visit_place)削除: {n_out_total}件(うち Study_ID 付与済み {n_out_with_sid}件)")
    print(f"Study_ID 付与済み: {before}件 -> {after_delete}件")
except Exception:
    conn.rollback()
    raise
finally:
    conn.close()