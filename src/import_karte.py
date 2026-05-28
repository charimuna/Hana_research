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

# 削除するカラム（個人情報）
DROP_COLUMNS = ["患者氏名", "医師名", "入力者ID", "代行入力", "生年月日", "年齢"]

# カラム名の変更マップ（日本語 → 英語 ＋ ID → Patient_ID）
RENAME_COLUMNS = {
    "NO":           "record_no",
    "診療タイプ":    "visit_type",
    "診療日時":      "visit_datetime",
    "フラグ備考":    "flag_note",
    "ID":           "Patient_ID",
    "患者施設":      "facility",
    "カルテ内容":    "karte_text",
    "終了時間":      "end_time",
    "居宅療養管理指導": "home_care_guidance",
    "ご家族への連絡事項": "family_note",
    "体温":          "temperature",
    "血圧":          "blood_pressure",
    "脈拍":          "pulse",
    "脈拍整不整":    "pulse_rhythm",
    "SPO2":         "spo2",
    "SPO2備考":     "spo2_note",
    "呼吸数":        "respiratory_rate",
    "呼吸数整不整":  "resp_rhythm",
    "身長":          "height",
    "体重":          "weight",
    "更新日時":      "updated_at",
    "看護師氏名":    "nurse_name",
    "同行者看護師ID": "nurse_id",
    "診療時間（分）": "visit_duration_min",
}


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


def process_columns(df: pd.DataFrame) -> pd.DataFrame:
    """カラムの削除・リネーム・Study_ID付与を行う"""

    # 1. 個人情報カラムを削除
    cols_to_drop = [c for c in DROP_COLUMNS if c in df.columns]
    df = df.drop(columns=cols_to_drop)
    print(f"\n🗑️  削除カラム: {cols_to_drop}")

    # 2. カラム名を英語にリネーム
    df = df.rename(columns=RENAME_COLUMNS)
    renamed = {k: v for k, v in RENAME_COLUMNS.items() if k in df.columns or v in df.columns}
    print(f"✏️  リネーム完了: {len(RENAME_COLUMNS)} カラム → 英語化")

    return df


def attach_study_id(df: pd.DataFrame, db_path: str) -> pd.DataFrame:
    """study_id_linkage テーブルから Study_ID を Patient_ID をキーに付与する"""
    conn = sqlite3.connect(db_path)
    try:
        linkage = pd.read_sql("SELECT Patient_ID, Study_ID FROM study_id_linkage", conn)
    finally:
        conn.close()

    # Patient_ID の型を合わせてマージ
    df["Patient_ID"] = df["Patient_ID"].astype(str)
    linkage["Patient_ID"] = linkage["Patient_ID"].astype(str)

    before = len(df)
    df = df.merge(linkage, on="Patient_ID", how="left")

    matched = df["Study_ID"].notna().sum()
    unmatched = df["Study_ID"].isna().sum()
    print(f"🔗 Study_ID付与: {matched:,} 件マッチ / {unmatched:,} 件未マッチ（NaN）")

    # カラム順を整理: Patient_ID の直後に Study_ID を配置
    cols = list(df.columns)
    cols.remove("Study_ID")
    pid_idx = cols.index("Patient_ID")
    cols.insert(pid_idx + 1, "Study_ID")
    df = df[cols]

    return df


def save_to_db(df: pd.DataFrame, db_path: str, table_name: str) -> None:
    """DataFrameをSQLiteに保存する"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"\n💾 DB保存中: {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"  ✅ テーブル '{table_name}' に保存完了")
        print(f"  📋 カラム数: {len(df.columns)} 列")
        print(f"  📋 カラム一覧: {list(df.columns)}")
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

        # Study_ID の付与状況
        study_check = pd.read_sql(
            f"""
            SELECT
                COUNT(*) as 総件数,
                COUNT(Study_ID) as Study_ID有,
                SUM(CASE WHEN Study_ID IS NULL THEN 1 ELSE 0 END) as Study_ID無
            FROM {table_name}
            """,
            conn,
        )
        print(f"  Study_ID有: {study_check['Study_ID有'][0]:,} 件")
        print(f"  Study_ID無: {study_check['Study_ID無'][0]:,} 件")

        # ファイル別件数
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

    # 3. カラム削除・リネーム
    df = process_columns(df)

    # 4. Study_ID を付与
    df = attach_study_id(df, DB_PATH)

    # 5. DB保存
    save_to_db(df, DB_PATH, TABLE_NAME)

    # 6. 検証
    verify_db(DB_PATH, TABLE_NAME)

    print("\n✅ すべての処理が完了しました！")


if __name__ == "__main__":
    main()
