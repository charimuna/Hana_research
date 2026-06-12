import sqlite3

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

try:
    # 全テーブル名を取得
    tables = [row[0] for row in cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )]
    print(f"対象テーブル数: {len(tables)}\n")

    renamed_count = 0

    for table in tables:
        cols = cursor.execute(f"PRAGMA table_info('{table}')").fetchall()
        # col: (cid, name, type, notnull, dflt_value, pk)

        for col in cols:
            col_name = col[1]
            # patient_id / patient_ID / PATIENT_ID など大文字小文字を問わず検出
            if col_name.lower() == "patient_id" and col_name != "Patient_ID":
                # SQLiteはRENAME COLUMNをサポート（v3.25.0以降）
                cursor.execute(
                    f'ALTER TABLE "{table}" RENAME COLUMN "{col_name}" TO "Patient_ID"'
                )
                print(f"✅ {table}: '{col_name}' → 'Patient_ID'")
                renamed_count += 1

    conn.commit()

    if renamed_count == 0:
        print("ℹ️  修正が必要な列は見つかりませんでした（既に統一済みか、該当テーブルなし）")
    else:
        print(f"\n✅ 合計 {renamed_count} 列をリネームしました")

except Exception as e:
    conn.rollback()
    print(f"❌ エラーが発生しました: {e}")
    raise

finally:
    conn.close()