"""
ORCAの処方データと病名データを、1つのSQLiteデータベースに取り込む。

作成するテーブル（2つのみ）
  - orca_receipt_details : 処方・診療行為の日別実績
  - orca_diagnoses       : 病名

取り込み済みファイルの判定は、各テーブルの source_file 列（ファイル名）で行う。
同じファイル名がすでにテーブルにあれば、そのファイルはスキップする。
"""

from pathlib import Path
from datetime import datetime
import codecs
import io
import sqlite3
import unicodedata

import pandas as pd


# ============================================================
# 1. 設定
# ============================================================

# 処方ファイル（.txt）のフォルダ
PRESCRIPTION_DIR = Path(
    "/Users/muna/Hana_research/data/raw/ORCAから処方"
)

# 病名ファイル（.txt）のフォルダ
DIAGNOSIS_DIR = Path(
    "/Users/muna/Hana_research/data/raw/ORCAから病名"
)

# データベースの保存先
DB_PATH = Path(
    "/Users/muna/Hana_research/data/db/Hana_Research.db"
)

# テーブル名
RECEIPT_TABLE = "orca_receipt_details"
DIAGNOSIS_TABLE = "orca_diagnoses"


# ============================================================
# 2. 取り込む項目
# ============================================================

# --- 処方：元データから保持する14項目 ---
BASE_COLUMNS = [
    "診療年月",
    "患者番号",
    "剤番号",
    "剤内番号",
    "診療行為区分",
    "診療種別区分",
    "明細診療行為コード",
    "明細数量",
    "明細回数",
    "明細単位コード",
    "明細単位名称",
    "明細名称",
    "明細正式名称",
    "薬価基準コード",
]

# 処方：日別実績の列
DAY_COLUMNS = [f"{day}日" for day in range(1, 32)]

# 処方：14項目に対応するDBの列名（BASE_COLUMNSと同じ順）
RECEIPT_DB_COLUMNS = [
    "billing_month",
    "Patient_ID",
    "drug_group_no",
    "detail_seq",
    "service_category",
    "service_type",
    "detail_code",
    "detail_quantity",
    "detail_count",
    "unit_code",
    "unit_name",
    "detail_name",
    "official_detail_name",
    "drug_price_code",
]

# 処方：自由記載のため、文字化けを許容する列
RECEIPT_FREE_TEXT = {"明細名称", "明細正式名称", "明細単位名称"}


# --- 病名：（元の列名, DBの列名） ---
DIAGNOSIS_COLUMN_MAP = [
    ("医療機関ID",         "medical_institution_id"),
    ("診療年月",           "billing_month"),
    ("レコード識別",       "record_id"),
    ("レセプト種別コード", "receipt_type_code"),
    ("入外区分",           "inout_category"),
    ("患者番号",           "Patient_ID"),          # 処方と同じ名称に統一
    ("患者ID",             "orca_patient_id"),     # ORCA内部ID（患者番号とは別列）
    ("性別",               "sex"),
    ("生年月日",           "birth_date"),
    ("診療科コード",       "dept_code"),
    ("レセ電診療科コード", "receden_dept_code"),
    ("診療開始日",         "start_date"),
    ("連番",               "seq_no"),
    ("病名",               "diagnosis_name"),
    ("病名文字数",         "name_char_count"),
    ("疑いフラグ",         "suspected_flag"),
    ("主病名フラグ",       "main_diagnosis_flag"),
    ("慢性疾患区分",       "chronic_category"),
    ("転帰区分",           "outcome_category"),
    ("転帰日",             "outcome_date"),
    ("病名コード数",       "code_count"),
    ("病名コード1",        "diagnosis_code_1"),
    ("病名コード2",        "diagnosis_code_2"),
    ("病名コード3",        "diagnosis_code_3"),
]

DIAGNOSIS_SOURCE_COLUMNS = [src for src, _ in DIAGNOSIS_COLUMN_MAP]
DIAGNOSIS_DB_COLUMNS = [db for _, db in DIAGNOSIS_COLUMN_MAP]

# 病名：自由記載のため、文字化けを許容する列
DIAGNOSIS_FREE_TEXT = {"病名"}


# ============================================================
# 3. 文字列の正規化
# ============================================================

def normalize_text(value):
    """
    全角・半角の違いを統一し、前後の空白を除去する。
    """

    if value is None:
        return ""

    value = unicodedata.normalize("NFKC", str(value))

    return value.strip()


# ============================================================
# 4. 文字コード処理（処方・病名で共通）
# ============================================================

def _orca_decode_handler(err):
    """
    cp932 で不正な \\x81\\xb0 を「〜」として復元する。
    それ以外の不正バイトは「�」に置換する。
    """
    if isinstance(err, UnicodeDecodeError):
        if err.object[err.start:err.start + 2] == b"\x81\xb0":
            return ("〜", err.start + 2)
        return ("�", err.start + 1)
    raise err


codecs.register_error("orca_wave", _orca_decode_handler)


def decode_orca_file(file_path):
    """
    ORCAのtxtファイルを文字列に変換する。
    utf-8-sig → cp932 の順に試し、失敗した場合は cp932 で
    \\x81\\xb0 を「〜」に復元し、それ以外の不正バイトは「�」に置換する。
    """

    raw = Path(file_path).read_bytes()

    for encoding in ["utf-8-sig", "cp932"]:
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue

    n_wave = raw.count(b"\x81\xb0")
    text = raw.decode("cp932", errors="orca_wave")

    n_unknown = text.count("�")
    print(
        f"注意: {Path(file_path).name} で \\x81\\xb0 を「〜」として復元"
        f"（最大 {n_wave} 箇所）。未知の不正バイト: {n_unknown} 箇所"
    )

    if n_unknown:
        for line_no, line in enumerate(text.splitlines(), start=1):
            if "�" in line:
                print(f"  行{line_no}: {line[:120]}")

    return text, "cp932 (errors=orca_wave)"


def check_no_garbled(df, columns, free_text, file_path):
    """
    置換文字「�」が、自由記載以外の列に混入していたらエラーにする。
    """

    for col in columns:
        if col in free_text:
            continue

        bad = df[col].str.contains("�", regex=False)

        if bad.any():
            raise ValueError(
                f"文字化けが重要列に混入: {file_path} / 列={col} / "
                f"行(0始まり)={df.index[bad].tolist()[:10]}"
            )


# ============================================================
# 5. ファイルの読み込み
# ============================================================

def read_receipt_file(file_path):
    """
    処方ファイルを読み込む。必要な列がなければエラーにする。
    """

    text, used_encoding = decode_orca_file(file_path)

    df = pd.read_csv(
        io.StringIO(text),
        sep=",",
        dtype=str,
        keep_default_na=False,
    )

    df.columns = [normalize_text(col) for col in df.columns]

    required_columns = BASE_COLUMNS + DAY_COLUMNS

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"必要な列がありません: {file_path}\n"
            f"不足している列: {missing_columns}"
        )

    df = df[required_columns].copy()

    for col in required_columns:
        df[col] = df[col].map(normalize_text)

    check_no_garbled(df, required_columns, RECEIPT_FREE_TEXT, file_path)

    print(f"読み込み: {Path(file_path).name} ({used_encoding})")

    return df


def read_diagnosis_file(file_path):
    """
    病名ファイルを読み込む。必要な列がなければエラーにする。
    """

    text, used_encoding = decode_orca_file(file_path)

    df = pd.read_csv(
        io.StringIO(text),
        sep=",",
        dtype=str,
        keep_default_na=False,
    )

    df.columns = [normalize_text(col) for col in df.columns]

    missing_columns = [
        col for col in DIAGNOSIS_SOURCE_COLUMNS
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"必要な列がありません: {file_path}\n"
            f"不足している列: {missing_columns}\n"
            f"ファイルの列名（先頭40件）: {list(df.columns)[:40]}"
        )

    df = df[DIAGNOSIS_SOURCE_COLUMNS].copy()

    for col in DIAGNOSIS_SOURCE_COLUMNS:
        df[col] = df[col].map(normalize_text)

    check_no_garbled(
        df, DIAGNOSIS_SOURCE_COLUMNS, DIAGNOSIS_FREE_TEXT, file_path
    )

    df.columns = DIAGNOSIS_DB_COLUMNS

    print(f"読み込み: {Path(file_path).name} ({used_encoding})")

    return df


# ============================================================
# 6. 日別実績の判定・日付作成（処方）
# ============================================================

def is_nonzero(value):
    """
    空欄、0、0.0、<br>などは保存対象外。
    それ以外は保存対象とする。
    """

    value = normalize_text(value)

    if value == "":
        return False

    if value.lower() in {"<br>", "nan", "none", "null"}:
        return False

    try:
        if float(value) == 0:
            return False
    except ValueError:
        # 数値に変換できない値は、勝手に捨てない
        pass

    return True


def make_service_date(billing_month, day):
    """
    例: 診療年月 = 201507、日 = 3  →  2015-07-03
    """

    billing_month = normalize_text(billing_month)
    day = int(day)

    if len(billing_month) != 6 or not billing_month.isdigit():
        raise ValueError(f"診療年月が不正です: {billing_month}")

    year = int(billing_month[:4])
    month = int(billing_month[4:6])

    # 実在する日付か確認する
    return datetime(year, month, day).strftime("%Y-%m-%d")


# ============================================================
# 7. テーブル作成（2つのみ）
# ============================================================

def create_tables(conn):

    # ---------- 処方・診療行為の日別実績 ----------
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {RECEIPT_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            billing_month TEXT NOT NULL,
            service_date TEXT NOT NULL,
            Patient_ID TEXT NOT NULL,

            drug_group_no TEXT,
            detail_seq TEXT,
            service_category TEXT,
            service_type TEXT,
            detail_code TEXT,
            detail_quantity TEXT,
            detail_count TEXT,
            unit_code TEXT,
            unit_name TEXT,
            detail_name TEXT,
            official_detail_name TEXT,
            drug_price_code TEXT,

            daily_value TEXT NOT NULL,

            source_file TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,

            UNIQUE (
                source_file,
                source_row_number,
                service_date
            )
        )
    """)

    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_orca_patient_date
        ON {RECEIPT_TABLE} (Patient_ID, service_date)
    """)

    # ---------- 病名 ----------
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {DIAGNOSIS_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            medical_institution_id TEXT,
            billing_month TEXT NOT NULL,
            record_id TEXT,
            receipt_type_code TEXT,
            inout_category TEXT,
            Patient_ID TEXT NOT NULL,
            orca_patient_id TEXT,
            sex TEXT,
            birth_date TEXT,
            dept_code TEXT,
            receden_dept_code TEXT,
            start_date TEXT,
            seq_no TEXT,
            diagnosis_name TEXT,
            name_char_count TEXT,
            suspected_flag TEXT,
            main_diagnosis_flag TEXT,
            chronic_category TEXT,
            outcome_category TEXT,
            outcome_date TEXT,
            code_count TEXT,
            diagnosis_code_1 TEXT,
            diagnosis_code_2 TEXT,
            diagnosis_code_3 TEXT,

            source_file TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,

            UNIQUE (
                source_file,
                source_row_number
            )
        )
    """)

    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_orca_dx_patient_month
        ON {DIAGNOSIS_TABLE} (Patient_ID, billing_month)
    """)

    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_orca_dx_code
        ON {DIAGNOSIS_TABLE} (diagnosis_code_1)
    """)

    conn.commit()


# ============================================================
# 8. 取り込み済み判定
# ============================================================

def already_imported(conn, table, source_file):
    """
    同じファイル名が、テーブルの source_file にあれば True。
    """

    row = conn.execute(
        f"SELECT 1 FROM {table} WHERE source_file = ? LIMIT 1",
        (source_file,),
    ).fetchone()

    return row is not None


# ============================================================
# 9. 処方ファイルの取り込み
# ============================================================

def import_receipt_file(conn, file_path):

    source_file = file_path.name

    if already_imported(conn, RECEIPT_TABLE, source_file):
        print(f"取り込み済みのためスキップ: {source_file}")
        return

    df = read_receipt_file(file_path)

    source_rows = len(df)
    imported_rows = 0

    insert_cols = (
        RECEIPT_DB_COLUMNS[:1]            # billing_month
        + ["service_date"]
        + RECEIPT_DB_COLUMNS[1:]          # Patient_ID 以降
        + ["daily_value", "source_file", "source_row_number"]
    )

    insert_sql = (
        f"INSERT INTO {RECEIPT_TABLE} ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(['?'] * len(insert_cols))})"
    )

    conn.execute("BEGIN")

    try:

        batch = []
        batch_size = 10000

        for row_number, (_, row) in enumerate(df.iterrows(), start=2):

            billing_month = row["診療年月"]

            for day in range(1, 32):

                daily_value = row[f"{day}日"]

                # 0や空欄の日は保存しない
                if not is_nonzero(daily_value):
                    continue

                service_date = make_service_date(billing_month, day)

                base_values = [row[col] for col in BASE_COLUMNS]

                batch.append((
                    base_values[0],        # billing_month
                    service_date,
                    base_values[1],        # Patient_ID
                    *base_values[2:],
                    daily_value,
                    source_file,
                    row_number,
                ))

                if len(batch) >= batch_size:
                    conn.executemany(insert_sql, batch)
                    imported_rows += len(batch)
                    batch.clear()

        if batch:
            conn.executemany(insert_sql, batch)
            imported_rows += len(batch)

        conn.commit()

        print(f"取り込み完了: {source_file}")
        print(f"  元データの行数: {source_rows:,}")
        print(f"  保存した日別データ数: {imported_rows:,}")

    except Exception:
        conn.rollback()
        raise


# ============================================================
# 10. 病名ファイルの取り込み
# ============================================================

def import_diagnosis_file(conn, file_path):

    source_file = file_path.name

    if already_imported(conn, DIAGNOSIS_TABLE, source_file):
        print(f"取り込み済みのためスキップ: {source_file}")
        return

    df = read_diagnosis_file(file_path)

    source_rows = len(df)

    # 元ファイルの行番号（1行目はヘッダー）
    df["source_file"] = source_file
    df["source_row_number"] = range(2, source_rows + 2)

    insert_cols = DIAGNOSIS_DB_COLUMNS + ["source_file", "source_row_number"]

    insert_sql = (
        f"INSERT INTO {DIAGNOSIS_TABLE} ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(['?'] * len(insert_cols))})"
    )

    rows = df[insert_cols].values.tolist()

    conn.execute("BEGIN")

    try:

        conn.executemany(insert_sql, rows)
        conn.commit()

        print(f"取り込み完了: {source_file}")
        print(f"  元データの行数: {source_rows:,}")
        print(f"  保存した行数: {len(rows):,}")

    except Exception:
        conn.rollback()
        raise


# ============================================================
# 11. フォルダ単位の処理
# ============================================================

def list_txt_files(folder):

    folder = Path(folder)

    if not folder.is_dir():
        raise FileNotFoundError(f"入力フォルダが見つかりません:\n{folder}")

    return sorted(
        p for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() == ".txt"
    )


def import_folder(conn, folder, import_func, label):

    files = list_txt_files(folder)

    print(f"\n===== {label}：対象ファイル数 {len(files)} =====")

    for file_path in files:

        try:
            import_func(conn, file_path)

        except Exception as e:
            print(f"\nエラー: {file_path.name}")
            print(e)
            raise


# ============================================================
# 12. メイン処理
# ============================================================

def main(
    receipt_dir=PRESCRIPTION_DIR,
    diagnosis_dir=DIAGNOSIS_DIR,
    db_path=DB_PATH,
    run_receipt=True,
    run_diagnosis=True,
):

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:

        create_tables(conn)

        if run_receipt:
            import_folder(
                conn, receipt_dir, import_receipt_file, "処方"
            )

        if run_diagnosis:
            import_folder(
                conn, diagnosis_dir, import_diagnosis_file, "病名"
            )

    print("\nすべての処理が終了しました。")
    print(f"データベース: {db_path}")


# 実行
if __name__ == "__main__":
    main()