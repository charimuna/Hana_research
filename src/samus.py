import sqlite3
from pathlib import Path

# === 固定設定 (毎回入力しなくていい) ===
DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"

def get_samus_count():
    """SAMUS が 'Background_summary' の memo に含まれる行数を返す"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # SQL クエリ実行
        # 'SAMUS' を含む文字列を数える
        cursor.execute("""
            SELECT COUNT(*) 
            FROM Background_summary
            WHERE memo LIKE '%サムス%'
        """)
        
        # 結果取得
        result = cursor.fetchone()[0]
        
        print(f"SAMUS を含む行数: {result}")
        
    except sqlite3.OperationalError as e:
        print(f"エラー: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    get_samus_count()