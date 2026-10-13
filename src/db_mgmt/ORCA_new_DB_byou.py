from pathlib import Path
from datetime import datetime
import codecs
import hashlib
import io
import sqlite3
import unicodedata
 
import pandas as pd
 
 
# ============================================================
# 1. 設定
# ============================================================
 
# ORCA病名ファイル（.txt）を置くフォルダ
INPUT_DIR = Path(
    "/Users/muna/Hana_research/data/raw/ORCAから病名"
)
 
# 処方と同じデータベースに病名テーブルを追加する
DB_PATH = Path(
    "/Users/muna/Hana_research/data/db/orca_receipt.db"
)
 
# 病名テーブル
TABLE_NAME = "orca_diagnoses"
 
# 病名ファイルの取り込み履歴テーブル（処方とは別管理）
IMPORT_TABLE = "orca_diagnosis_imported_files"
 
 
# ============================================================
# 2. 取り込む項目（元の列名, DBの列名）
# ============================================================
 
COLUMN_MAP = [
    ("医療機関ID",         "medical_institution_id"),
    ("診療年月",           "billing_month"),
    ("レコード識別",       "record_id"),
    ("レセプト種別コード", "receipt_type_code"),
    ("入外区分",           "inout_category"),
    ("患者番号",           "Patient_ID"),          # 処方DBと同じ名称に統一
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
 
SOURCE_COLUMNS = [src for src, _ in COLUMN_MAP]
DB_COLUMNS = [db for _, db in COLUMN_MAP]
 
# 自由記載のため、文字化けを許容する列
FREE_TEXT_COLUMNS = {"病名"}
 
 
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
# 4. 文字コード処理
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
 
 
def read_orca_diagnosis_file(file_path):
    """
    ORCA病名ファイルを読み込む。
    文字コードは utf-8-sig → cp932 の順に試し、
    失敗した場合は cp932 で \\x81\\xb0 を「〜」に復元し、
    それ以外の不正バイトは「�」に置換する。
    """
 
    raw = Path(file_path).read_bytes()
 
    text = None
    used_encoding = None
 
    for encoding in ["utf-8-sig", "cp932"]:
        try:
            text = raw.decode(encoding)
            used_encoding = encoding
            break
        except UnicodeDecodeError:
            continue
 
    if text is None:
        n_wave = raw.count(b"\x81\xb0")
        text = raw.decode("cp932", errors="orca_wave")
        used_encoding = "cp932 (errors=orca_wave)"
 
        n_unknown = text.count("�")
        print(
            f"注意: {Path(file_path).name} で \\x81\\xb0 を「〜」として復元"
            f"（最大 {n_wave} 箇所）。未知の不正バイト: {n_unknown} 箇所"
        )
 
        if n_unknown:
            for line_no, line in enumerate(text.splitlines(), start=1):
                if "�" in line:
                    print(f"  行{line_no}: {line[:120]}")
 
    df = pd.read_csv(
        io.StringIO(text),
        sep=",",
        dtype=str,
        keep_default_na=False,
    )
 
    # 列名の全角・半角を統一
    df.columns = [normalize_text(col) for col in df.columns]
 
    # 必要な列がそろっているか確認
    missing_columns = [
        col for col in SOURCE_COLUMNS
        if col not in df.columns
    ]
 
    if missing_columns:
        raise ValueError(
            f"必要な列がありません: {file_path}\n"
            f"不足している列: {missing_columns}\n"
            f"ファイルの列名（先頭40件）: {list(df.columns)[:40]}"
        )
 
    # 必要な列だけを取り出し、DBの列名に変更
    df = df[SOURCE_COLUMNS].copy()
 
    for col in SOURCE_COLUMNS:
        df[col] = df[col].map(normalize_text)
 
    # 置換文字が病名以外の列に混入していないか検証
    for col in SOURCE_COLUMNS:
        if col in FREE_TEXT_COLUMNS:
            continue
        bad = df[col].str.contains("�", regex=False)
        if bad.any():
            raise ValueError(
                f"文字化けが重要列に混入: {file_path} / 列={col} / "
                f"行(0始まり)={df.index[bad].tolist()[:10]}"
            )
 
    df.columns = DB_COLUMNS
 
    print(f"読み込み: {Path(file_path).name} ({used_encoding})")
 
    return df
 
 
# ============================================================
# 5. テーブル作成
# ============================================================
 
def create_tables(conn):
 
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
 
    # 患者・診療年月で検索しやすくする
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_orca_dx_patient_month
        ON {TABLE_NAME} (Patient_ID, billing_month)
    """)
 
    # 病名コードで検索しやすくする
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_orca_dx_code
        ON {TABLE_NAME} (diagnosis_code_1)
    """)
 
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {IMPORT_TABLE} (
            source_file TEXT PRIMARY KEY,
            file_hash TEXT NOT NULL UNIQUE,
            imported_at TEXT NOT NULL,
            source_rows INTEGER NOT NULL,
            imported_rows INTEGER NOT NULL
        )
    """)
 
    conn.commit()
 
 
# ============================================================
# 6. ファイルのハッシュ値
# ============================================================
 
def calculate_file_hash(file_path):
 
    sha256 = hashlib.sha256()
 
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)
 
    return sha256.hexdigest()
 
 
# ============================================================
# 7. 1ファイルの取り込み
# ============================================================
 
def import_diagnosis_file(conn, file_path):
 
    file_path = file_path.resolve()
    source_file = str(file_path)
 
    file_hash = calculate_file_hash(file_path)
 
    existing = conn.execute(
        f"SELECT file_hash FROM {IMPORT_TABLE} WHERE source_file = ?",
        (source_file,),
    ).fetchone()
 
    if existing is not None:
 
        if existing[0] == file_hash:
            print(f"取り込み済みのためスキップ: {file_path.name}")
            return
 
        raise RuntimeError(
            f"取り込み済みファイルの内容が変更されています。\n"
            f"ファイル: {file_path}\n"
            "既存データとの重複を避けるため、取り込みを中止しました。"
        )
 
    duplicate = conn.execute(
        f"SELECT source_file FROM {IMPORT_TABLE} WHERE file_hash = ?",
        (file_hash,),
    ).fetchone()
 
    if duplicate is not None:
        print(
            f"同じ内容のファイルが登録済みのためスキップ: {file_path.name}"
        )
        return
 
    df = read_orca_diagnosis_file(file_path)
 
    source_rows = len(df)
 
    # 元ファイルの行番号（1行目はヘッダー）
    df["source_file"] = source_file
    df["source_row_number"] = range(2, source_rows + 2)
 
    insert_cols = DB_COLUMNS + ["source_file", "source_row_number"]
 
    insert_sql = (
        f"INSERT INTO {TABLE_NAME} ({', '.join(insert_cols)}) "
        f"VALUES ({', '.join(['?'] * len(insert_cols))})"
    )
 
    rows = df[insert_cols].values.tolist()
 
    conn.execute("BEGIN")
 
    try:
 
        conn.executemany(insert_sql, rows)
 
        conn.execute(
            f"""
            INSERT INTO {IMPORT_TABLE} (
                source_file, file_hash, imported_at,
                source_rows, imported_rows
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                source_file,
                file_hash,
                datetime.now().isoformat(timespec="seconds"),
                source_rows,
                len(rows),
            ),
        )
 
        conn.commit()
 
        print(f"取り込み完了: {file_path.name}")
        print(f"  元データの行数: {source_rows:,}")
        print(f"  保存した行数: {len(rows):,}")
 
    except Exception:
        conn.rollback()
        raise
 
 
# ============================================================
# 8. メイン処理
# ============================================================
 
def main(input_dir=INPUT_DIR, db_path=DB_PATH):
 
    input_dir = Path(input_dir)
    db_path = Path(db_path)
 
    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"入力フォルダが見つかりません:\n{input_dir}"
        )
 
    db_path.parent.mkdir(parents=True, exist_ok=True)
 
    files = sorted(
        p for p in input_dir.iterdir()
        if p.is_file() and p.suffix.lower() == ".txt"
    )
 
    if not files:
        print(f"取り込み対象の.txtファイルがありません:\n{input_dir}")
        return
 
    print(f"対象ファイル数: {len(files)}")
 
    with sqlite3.connect(db_path) as conn:
 
        create_tables(conn)
 
        for file_path in files:
 
            try:
                import_diagnosis_file(conn, file_path)
 
            except Exception as e:
                print(f"\nエラー: {file_path.name}")
                print(e)
                raise
 
    print("\nすべての処理が終了しました。")
    print(f"データベース: {db_path}")
 
 
if __name__ == "__main__":
    main()