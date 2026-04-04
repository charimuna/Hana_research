import sqlite3
import pandas as pd
import os

# 実行している場所のパスを確認
current_dir = os.path.dirname(os.path.abspath(__file__))
db_file = os.path.join(current_dir, 'Hana_research.db')
excel_file = os.path.join(current_dir, 'FreeDocument_diseaselist.xlsx')
table_name = 'DiagnosisList'

try:
    # Excelを読み込む
    df = pd.read_excel(excel_file)
    
    # データベースへ接続
    conn = sqlite3.connect(db_file)
    
    # データを書き込む（index=Falseで余計な番号を入れない）
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"成功: {table_name} テーブルが更新されました。")

except Exception as e:
    print(f"エラーが発生しました: {e}")
import sqlite3
import pandas as pd
import os

# フォルダの場所を取得
current_dir = os.path.dirname(os.path.abspath(__file__))
db_file = os.path.join(current_dir, 'Hana_research.db')
excel_file = os.path.join(current_dir, 'FreeDocument_diseaselist.xlsx')
table_name = 'DiagnosisList'

try:
    # 【修正ポイント】parse_dates で日付列を指定します
    # 列名が「作成日」であると仮定しています。実際のExcelの列名に合わせてください。
    df = pd.read_excel(excel_file, parse_dates=['作成日'])

    # SQLiteで扱いやすい文字列形式（YYYY-MM-DD）に変換
    df['作成日'] = df['作成日'].dt.strftime('%Y-%m-%d')

    conn = sqlite3.connect(db_file)
    
    # if_exists='replace' なので、古い 0000-00-00 のデータは消えて新しくなります
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"成功: {table_name} の日付を修正して更新しました。")

except Exception as e:
    print(f"エラーが発生しました: {e}")
    print("ヒント: Excelの列名が '作成日' ではない場合、コード内の名前を書き換えてください。")