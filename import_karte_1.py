"""
import_karte.py
AllKarte フォルダの CSV ファイルを SQLite データベースに取り込むスクリプト
"""

import sqlite3
import pandas as pd
import os
import sys
import shutil
from datetime import datetime

# -----------------------------------------------
# 設定
# -----------------------------------------------
RAW_DIR = "/Users/muna/Hana_research/data/raw/AllKarte"
DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"
TABLE_NAME = "karte"
ENCODING = "cp932"


def backup_db(db_path: str) -> None:
    """既存のDBをバックアップする（同じ階層に日時付きファイル名で保存）"""
    if not os.path.exists(db_path):
        print("\n📦 バックアップ: 既存DBなし（スキップ）")
        return

    db_dir = os.path.dirname(db_path)
    db_name = os.path.splitext(os.path.basename(db_path))[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(db_dir, f"{db_name}_backup_{timestamp}.db")

    shutil.copy2(db_path, backup_path)
    print(f"\n📦 バックアップ完了: {os.path.basename(backup_path)}")


def load_csvs(raw_dir: str) -> pd.DataFrame:
    """CSVファイルを読み込んで結合する"""
    csv_files = sorted([f for f in os.listdir(raw_dir) if f.endswith(".csv")])

    if not csv_files:
        print(f"❌ CSVファイルが見つかりません: {raw_dir}")
        sys.exit(1)

    print(f"📂 CSVファイル数: {len(csv_files)}")
    print("-" * 40)

    all_dfs = []
    errors = []

    for filename in csv_files:
        filepath = os.path.join(raw_dir, filename)
        try:
            df = pd.read_csv(filepath, encoding=ENCODING, dtype=str)
            df["source_file"] = filename  # 出所ファイル名を記録
            all_dfs.append(df)
            print(f"  ✅ {filename}: {len(df):,} 行")
        except Exception as e:
            errors.append(filename)
            print(f"  ❌ {filename}: {e}")

    if errors:
        print(f"\n⚠️  読み込み失敗: {len(errors)} ファイル → {errors}")

    if not all_dfs:
        print("❌ 読み込めたファイルがありません。処理を中止します。")
        sys.exit(1)

    combined = pd.concat(all_dfs, ignore_index=True)
    print("-" * 40)
    print(f"📊 合計: {len(combined):,} 行 × {len(combined.columns)} 列")
    return combined


def save_to_db(df: pd.DataFrame, db_path: str, table_name: str) -> None:
    """DataFrameをSQLiteに保存する"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"\n💾 DB保存中: {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"  ✅ テーブル '{table_name}' に保存完了")
    finally:
        conn.close()


def verify_db(db_path: str, table_name: str) -> None:
    """取り込み結果を検証する"""
    print(f"\n🔍 検証中...")
    conn = sqlite3.connect(db_path)
    try:
        # 総行数
        total = pd.read_sql(f"SELECT COUNT(*) as 総行数 FROM {table_name}", conn)
        print(f"  総行数: {total['総行数'][0]:,} 行")

        # 年別件数
        year_counts = pd.read_sql(
            f"""
            SELECT source_file, COUNT(*) as 件数
            FROM {table_name}
            GROUP BY source_file
            ORDER BY source_file
            """,
            conn,
        )
        print("\n  ファイル別件数:")
        for _, row in year_counts.iterrows():
            print(f"    {row['source_file']}: {row['件数']:,} 行")

    finally:
        conn.close()


def main():
    print("=" * 40)
    print("  Hana Research DB 取り込みスクリプト")
    print("=" * 40)

    # 1. 既存DBをバックアップ
    backup_db(DB_PATH)

    # 2. CSV読み込み・結合
    df = load_csvs(RAW_DIR)

    # 3. DB保存
    save_to_db(df, DB_PATH, TABLE_NAME)

    # 4. 検証
    verify_db(DB_PATH, TABLE_NAME)

    print("\n✅ すべての処理が完了しました！")


if __name__ == "__main__":
    main()
