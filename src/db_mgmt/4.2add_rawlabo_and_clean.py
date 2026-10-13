"""
add_fix_and_build_clean.py
STEP 1: CSV を lab_raw に差分追加（Patient_ID + sample_date + item_code + value_raw の重複スキップ）
STEP 2: lab_raw から整形・重複除去・コード統一をして lab_clean を作成

使い方:
  python add_fix_and_build_clean.py                       # 下の CSV_PATH を使う
  python add_fix_and_build_clean.py /path/to/new_file.csv # CSV を引数で指定
"""

import sys
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime


# ============================================================
# 設定（元のスクリプトのまま）
# ============================================================
CSV_PATH = Path("/Users/muna/Hana_research/data/raw/AllLab/correctdata_20260616.csv")
DB_PATH = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")  # ← 必要に応じて変更

# 重複判定キー（この組み合わせが既存DBにあればスキップ）
DEDUP_KEYS = ["Patient_ID", "sample_date", "item_code", "value_raw"]

RAW_TABLE = "lab_raw"
CLEAN_TABLE = "lab_clean"


# ============================================================
# STEP 1: CSV を lab_raw に差分追加
# ============================================================
def step1_add_to_lab_raw(csv_path):
    # ------------------------------------------------------------
    # 1. CSV 読み込み
    # ------------------------------------------------------------
    print(f"[1] CSV 読み込み: {csv_path}")

    df = pd.read_csv(
        csv_path,
        encoding="utf-8-sig",      # UTF-8 BOM付き（Excelが出力するCSVに多い）
        dtype=str,                 # 全列を文字列で読み込む（数値変換はしない）
        keep_default_na=False,     # 空欄を NaN にしない → 空文字で保持
    )

    print(f"    読み込み行数: {len(df):,} 行")
    print(f"    列名: {list(df.columns)}")

    # ------------------------------------------------------------
    # 2. カラム rename
    # ------------------------------------------------------------
    df = df.rename(columns={
        "患者ID":   "Patient_ID",
        "検査日":   "sample_date",
        "検査項目": "item_name",
        "検査コード": "item_code",
        "検査値":   "value_raw",
        "単位":     "unit",
        "H/L":      "hl_flag",
        "低値":     "ref_low",
        "高値":     "ref_high",
        "コメント": "comment",
        "検査会社": "lab_company",
        "施設":     "facility",
        # source_file はCSV側にすでに存在すればそのまま使用
    })

    # ------------------------------------------------------------
    # 3. 不要列を除去し、lab_raw に合わせた列順に整形
    # ------------------------------------------------------------
    TARGET_COLS = [
        "Patient_ID", "sample_date", "item_name", "item_code",
        "value_raw", "unit", "hl_flag", "ref_low", "ref_high",
        "comment", "lab_company", "facility", "source_file"
    ]

    # source_file 列が CSV にない場合は CSV ファイル名を入れる
    if "source_file" not in df.columns:
        df["source_file"] = csv_path.name

    # 余分な列を落とす（患者名 など）
    df = df[[c for c in TARGET_COLS if c in df.columns]]

    # 想定列が全部揃っているか確認
    missing = [c for c in TARGET_COLS if c not in df.columns]
    if missing:
        print(f"[警告] 以下の列が見つかりません: {missing}")

    # imported_at を付与
    df["imported_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"[2] rename・整形完了: {len(df):,} 行 × {len(df.columns)} 列")

    # ------------------------------------------------------------
    # 4. SQLite 接続 & 差分インポート
    # ------------------------------------------------------------
    print(f"[3] DB 接続: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # lab_raw テーブルが存在しない場合は作成
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
        raw_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        Patient_ID  TEXT,
        sample_date TEXT,
        item_name   TEXT,
        item_code   TEXT,
        value_raw   TEXT,
        unit        TEXT,
        hl_flag     TEXT,
        ref_low     TEXT,
        ref_high    TEXT,
        comment     TEXT,
        lab_company TEXT,
        facility    TEXT,
        source_file TEXT,
        imported_at TEXT
    )
    """)
    conn.commit()

    # 既存レコードの重複キーセットを取得
    print("[4] 既存データの重複キー取得中...")
    existing_keys = set()
    key_cols_str = ", ".join(DEDUP_KEYS)
    for row in cur.execute(f"SELECT {key_cols_str} FROM {RAW_TABLE}"):
        existing_keys.add(row)

    print(f"    既存レコード数（キーセット）: {len(existing_keys):,}")

    # 差分フィルタリング
    def make_key(row):
        return tuple(str(row.get(k, "")) for k in DEDUP_KEYS)

    mask_new = df.apply(lambda r: make_key(r) not in existing_keys, axis=1)
    df_new = df[mask_new].copy()

    print(f"[5] 新規インポート対象: {len(df_new):,} 行 / スキップ（重複）: {(~mask_new).sum():,} 行")

    # ------------------------------------------------------------
    # 5. INSERT
    # ------------------------------------------------------------
    if len(df_new) == 0:
        print("[完了] 新規データなし。インポートをスキップしました。")
    else:
        insert_cols = [
            "Patient_ID", "sample_date", "item_name", "item_code",
            "value_raw", "unit", "hl_flag", "ref_low", "ref_high",
            "comment", "lab_company", "facility", "source_file", "imported_at"
        ]
        placeholders = ", ".join(["?"] * len(insert_cols))
        col_str = ", ".join(insert_cols)
        sql = f"INSERT INTO {RAW_TABLE} ({col_str}) VALUES ({placeholders})"

        # 存在しない列は空文字で補完
        for c in insert_cols:
            if c not in df_new.columns:
                df_new[c] = ""

        records = df_new[insert_cols].values.tolist()
        cur.executemany(sql, records)
        conn.commit()
        print(f"[完了] {len(df_new):,} 件をインポートしました。")

    conn.close()
    print("DB 接続クローズ。")


# ============================================================
# STEP 2: lab_raw → lab_clean（整形・重複除去・コード統一）
# ============================================================
def step2_build_lab_clean():
    # =========================================
    # DB接続
    # =========================================
    conn = sqlite3.connect(DB_PATH)
    print("Loading table...")

    df_all = pd.read_sql(
        f"SELECT * FROM {RAW_TABLE}",
        conn
    )
    print(f"Loaded rows: {len(df_all):,}")

    # =========================================
    # 採血日の datetime 化
    # =========================================
    print("Parsing sample_date...")

    df_all["sample_date_parsed"] = pd.to_datetime(
        df_all["sample_date"],
        errors="coerce"
    )

    date_error_rate = df_all["sample_date_parsed"].isna().mean()
    print(f"Date parse error rate: {date_error_rate:.4f}")

    # =========================================
    # 検査値の数値化
    # =========================================
    print("Converting value_raw to numeric...")

    df_all["value_num"] = pd.to_numeric(
        df_all["value_raw"],
        errors="coerce"
    )

    numeric_rate = df_all["value_num"].notna().mean()
    print(f"Numeric conversion rate: {numeric_rate:.4f}")

    # =========================================
    # item_code 統一（重複削除より先に行う）
    # =========================================
    def unify_code(df, item_name, old_code, new_code):
        """item_codeを統一する関数"""
        before = df[
            (df["item_name"] == item_name) &
            (df["item_code"] == old_code)
        ].shape[0]

        df.loc[
            (df["item_name"] == item_name) &
            (df["item_code"] == old_code),
            "item_code"
        ] = new_code

        after = df[
            (df["item_name"] == item_name) &
            (df["item_code"] == new_code)
        ].shape[0]

        print(f"{item_name}: {old_code}→{new_code}  変更件数:{before:,}  統一後合計:{after:,}")

    print("\n--- item_code 統一開始 ---")

    # CRE: 0069 → 0074
    unify_code(df_all, "ＣＲＥ", "0069", "0074")

    # Mg: 0111 → 0112
    unify_code(df_all, "Ｍｇ", "0111", "0112")

    # アルブミン: 0018 → 0179
    unify_code(df_all, "アルブミン", "0018", "0179")

    print("--- item_code 統一完了 ---\n")

    # =========================================
    # 重複削除（value_raw もキーに含める）
    # =========================================
    print("Removing duplicates...")

    DUP_SUBSET = [
        "Patient_ID",
        "sample_date",
        "item_code",
        "value_num",
        "value_raw",
        "unit"
    ]

    n_before = len(df_all)

    df_clean = df_all.drop_duplicates(subset=DUP_SUBSET).copy()

    n_after = len(df_clean)
    removed = n_before - n_after

    print(f"Before : {n_before:,}")
    print(f"After  : {n_after:,}")
    print(f"Removed: {removed:,} ({removed/n_before:.4%})")

    # =========================================
    # 重複が残っていないか最終確認
    # （dropna=False: NaN/NULL を含む行も確認の対象にする）
    # =========================================
    dup_check = (
        df_clean.groupby(DUP_SUBSET, dropna=False)
        .size()
        .reset_index(name="n")
    )

    remaining_dup = (dup_check["n"] > 1).sum()
    print(f"Remaining duplicate groups: {remaining_dup}")

    # =========================================
    # SQLite に保存
    # =========================================
    print("\nSaving cleaned table...")

    df_clean.to_sql(
        CLEAN_TABLE,
        conn,
        if_exists="replace",
        index=False
    )

    print("Done.")
    conn.close()


# ============================================================
# 実行（STEP 1 → STEP 2）
# ============================================================
if __name__ == "__main__":
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else CSV_PATH

    print("\n" + "=" * 60)
    print(f"STEP 1: {csv_path.name} を lab_raw に差分追加")
    print("=" * 60)
    step1_add_to_lab_raw(csv_path)

    print("\n" + "=" * 60)
    print("STEP 2: lab_clean を作成")
    print("=" * 60)
    step2_build_lab_clean()

    print("\n全ステップ完了。")