from pathlib import Path
from datetime import datetime
imoprt codecs
import hashlib
import io
import sqlite3
import unicodedata

import pandas as pd


# ============================================================
# 1. 設定
# ============================================================

# ORCAの月別ファイルを置くフォルダ
INPUT_DIR = Path(
    "/Users/muna/Hana_research/data/raw/ORCAから処方"
)

# データベースの保存先
DB_PATH = Path(
    "/Users/muna/Hana_research/data/db/orca_receipt.db"
)

# 保存するテーブル名
TABLE_NAME = "orca_receipt_details"

# 取り込み履歴を保存するテーブル名
IMPORT_TABLE = "orca_imported_files"


# ============================================================
# 2. ORCAから取り込む項目
# ============================================================

# 元データから保持する14項目
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

# 日別実績の列
DAY_COLUMNS = [f"{day}日" for day in range(1, 32)]


# データベースの列名
DB_COLUMNS = [
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
# 4. ORCAファイルの読み込み
# ============================================================
def _orca_decode_handler(err):
    """cp932 で不正な \\x81\\xb0 を「〜」として復元する。それ以外は「�」に置換。"""
    if isinstance(err, UnicodeDecodeError):
        if err.object[err.start:err.start + 2] == b"\x81\xb0":
            return ("〜", err.start + 2)
        return ("\ufffd", err.start + 1)
    raise err

codecs.register_error("orca_wave", _orca_decode_handler)


def read_orca_file(file_path):
    """
    カンマ区切りのORCAファイルを読み込む。
    文字コードは utf-8-sig → cp932 の順に試し、
    それでも失敗した場合は cp932 で不正バイトを「�」に置換して読む。
    必要な列がなければエラーにする。
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

        # 不正バイトを含む場合のフォールバック
    if text is None:
        n_wave = raw.count(b"\x81\xb0")
        text = raw.decode("cp932", errors="orca_wave")
        used_encoding = "cp932 (errors=orca_wave)"

        n_unknown = text.count("\ufffd")
        print(
            f"注意: {Path(file_path).name} で \\x81\\xb0 を「〜」として復元（最大 {n_wave} 箇所）。"
            f"未知の不正バイト: {n_unknown} 箇所"
        )

        if n_unknown:
            for line_no, line in enumerate(text.splitlines(), start=1):
                if "\ufffd" in line:
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

    # 必要な列だけを取り出す
    df = df[required_columns].copy()

    # 値の前後の空白などを整理
    for col in required_columns:
        df[col] = df[col].map(normalize_text)

    # 置換文字が名称列以外に混入していないか検証
    free_text_columns = {"明細名称", "明細正式名称", "明細単位名称"}
    critical_columns = [
        col for col in required_columns
        if col not in free_text_columns
    ]

    for col in critical_columns:
        bad = df[col].str.contains("\ufffd", regex=False)
        if bad.any():
            raise ValueError(
                f"文字化けが重要列に混入: {file_path} / 列={col} / "
                f"行(0始まり)={df.index[bad].tolist()[:10]}"
            )

    print(f"読み込み: {Path(file_path).name} ({used_encoding})")

    return df


# ============================================================
# 5. 日別実績が保存対象か判定
# ============================================================

def is_nonzero(value):
    """
    空欄、0、0.0、<br>などは保存対象外。
    それ以外は保存対象とする。
    """

    value = normalize_text(value)

    if value == "":
        return False

    # サンプル表示で空欄を表していた文字列にも対応
    if value.lower() in {"<br>", "nan", "none", "null"}:
        return False

    # 数値として0なら除外
    try:
        if float(value) == 0:
            return False
    except ValueError:
        # 数値に変換できない値は、勝手に捨てない
        pass

    return True


# ============================================================
# 6. 診療年月と日から日付を作成
# ============================================================

def make_service_date(billing_month, day):
    """
    例:
        診療年月 = 201507
        日       = 3

    結果:
        2015-07-03
    """

    billing_month = normalize_text(billing_month)
    day = int(day)

    if len(billing_month) != 6 or not billing_month.isdigit():
        raise ValueError(
            f"診療年月が不正です: {billing_month}"
        )

    year = int(billing_month[:4])
    month = int(billing_month[4:6])

    # 実在する日付か確認する
    date_value = datetime(year, month, day)

    return date_value.strftime("%Y-%m-%d")


# ============================================================
# 7. データベースとテーブルを作成
# ============================================================

def create_tables(conn):

    # 日別実績を保存するテーブル
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orca_receipt_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            billing_month TEXT NOT NULL,
            service_date TEXT NOT NULL,
            Patient_ID  TEXT NOT NULL,

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

    # 患者・日付で検索しやすくするインデックス
    conn.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_orca_patient_date
        ON orca_receipt_details (
            Patient_ID,
            service_date
        )
    """)

    # 取り込み済みファイルの管理テーブル
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orca_imported_files (
            source_file TEXT PRIMARY KEY,
            file_hash TEXT NOT NULL UNIQUE,
            imported_at TEXT NOT NULL,
            source_rows INTEGER NOT NULL,
            imported_rows INTEGER NOT NULL
        )
    """)

    conn.commit()


# ============================================================
# 8. ファイルのハッシュ値を計算
# ============================================================

def calculate_file_hash(file_path):

    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            sha256.update(chunk)

    return sha256.hexdigest()


# ============================================================
# 9. ORCAファイルを取り込む
# ============================================================

def import_orca_file(conn, file_path):

    file_path = file_path.resolve()
    source_file = str(file_path)

    # ファイルの内容を識別する
    file_hash = calculate_file_hash(file_path)

    # すでに同じパスのファイルを取り込んでいるか確認
    existing = conn.execute(
        """
        SELECT file_hash
        FROM orca_imported_files
        WHERE source_file = ?
        """,
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

    # 別の名前で同じファイルが置かれていないか確認
    duplicate = conn.execute(
        """
        SELECT source_file
        FROM orca_imported_files
        WHERE file_hash = ?
        """,
        (file_hash,),
    ).fetchone()

    if duplicate is not None:
        print(
            f"同じ内容のファイルが登録済みのためスキップ: "
            f"{file_path.name}"
        )
        return

    # ORCAデータを読み込む
    df = read_orca_file(file_path)

    source_rows = len(df)
    imported_rows = 0

    # ファイル単位でトランザクションを開始
    conn.execute("BEGIN")

    try:

        insert_sql = """
            INSERT INTO orca_receipt_details (
                billing_month,
                service_date,
                Patient_ID,
                drug_group_no,
                detail_seq,
                service_category,
                service_type,
                detail_code,
                detail_quantity,
                detail_count,
                unit_code,
                unit_name,
                detail_name,
                official_detail_name,
                drug_price_code,
                daily_value,
                source_file,
                source_row_number
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """

        batch = []
        batch_size = 10000

        # 元データの1行ずつ処理
        for row_number, (_, row) in enumerate(
            df.iterrows(),
            start=2,
        ):

            billing_month = row["診療年月"]

            # 日別実績を1日ずつ確認
            for day in range(1, 32):

                day_column = f"{day}日"
                daily_value = row[day_column]

                # 0や空欄の日は保存しない
                if not is_nonzero(daily_value):
                    continue

                # 診療年月と日から日付を作成
                service_date = make_service_date(
                    billing_month,
                    day,
                )

                # 14項目を元データから取得
                base_values = [
                    row[col]
                    for col in BASE_COLUMNS
                ]

                # 保存用データを作成
                values = (
                    base_values[0],   # billing_month
                    service_date,
                    base_values[1],   # Patient_ID
                    *base_values[2:],
                    daily_value,
                    source_file,
                    row_number,
                )

                batch.append(values)

                # 一定件数ごとにまとめて登録
                if len(batch) >= batch_size:

                    conn.executemany(
                        insert_sql,
                        batch,
                    )

                    imported_rows += len(batch)
                    batch.clear()

        # 最後に残ったデータを登録
        if batch:

            conn.executemany(
                insert_sql,
                batch,
            )

            imported_rows += len(batch)

        # 取り込み履歴を保存
        conn.execute(
            """
            INSERT INTO orca_imported_files (
                source_file,
                file_hash,
                imported_at,
                source_rows,
                imported_rows
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                source_file,
                file_hash,
                datetime.now().isoformat(timespec="seconds"),
                source_rows,
                imported_rows,
            ),
        )

        conn.commit()

        print(f"取り込み完了: {file_path.name}")
        print(f"  元データの行数: {source_rows:,}")
        print(f"  保存した日別データ数: {imported_rows:,}")

    except Exception:
        conn.rollback()
        raise


# ============================================================
# 10. メイン処理
# ============================================================

def main():

    if not INPUT_DIR.is_dir():
        raise FileNotFoundError(
            f"入力フォルダが見つかりません:\n{INPUT_DIR}"
        )

    # データベースの親フォルダを作成
    DB_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # .txtファイルを取得
    files = sorted(
        p for p in INPUT_DIR.iterdir()
        if p.is_file()
        and p.suffix.lower() == ".txt"
    )

    if not files:
        print(
            f"取り込み対象の.txtファイルがありません:\n"
            f"{INPUT_DIR}"
        )
        return

    print(f"対象ファイル数: {len(files)}")

    with sqlite3.connect(DB_PATH) as conn:

        create_tables(conn)

        for file_path in files:

            try:
                import_orca_file(
                    conn,
                    file_path,
                )

            except Exception as e:
                print(f"\nエラー: {file_path.name}")
                print(e)
                raise

    print("\nすべての処理が終了しました。")
    print(f"データベース: {DB_PATH}")


# 実行
if __name__ == "__main__":
    main()