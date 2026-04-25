import sqlite3

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

try:
    # 現在のテーブル一覧を確認
    tables = [row[0] for row in cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )]
    print(f"現在のテーブル一覧: {tables}\n")

    if "Background_summury" in tables:
        cursor.execute(
            "ALTER TABLE Background_summury RENAME TO Background_summary"
        )
        conn.commit()
        print("✅ 'Background_summury' → 'Background_summary' にリネームしました")
    elif "Background_summary" in tables:
        print("ℹ️  既に 'Background_summary' という名前のテーブルが存在します（修正不要）")
    else:
        print("❌ 'Background_summury' テーブルが見つかりませんでした")

except Exception as e:
    conn.rollback()
    print(f"❌ エラーが発生しました: {e}")
    raise

finally:
    conn.close()