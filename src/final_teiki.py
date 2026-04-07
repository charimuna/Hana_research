import pandas as pd
import glob
import os
import re

# フォルダと出力ファイル
folder_path = "/Users/muna/Hana_research/data/raw/AllKarte"
output_path = os.path.expanduser("~/Desktop/teiki_byo3.csv")

csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

all_records = []

for file in csv_files:
    try:
        df = pd.read_csv(file, encoding='cp932', header=0)

        # 列名の確認
        expected_cols = ['診療タイプ', '診療日時', 'ID', 'カルテ内容']
        if not all(col in df.columns for col in expected_cols):
            print(f"{file} の列名が期待値と異なります。スキップします。")
            continue

        # 空欄・NaNは文字列変換しても無視
        df = df.dropna(subset=['診療タイプ','診療日時','ID','カルテ内容'])

        # 文字列化・前後空白除去
        df['診療タイプ'] = df['診療タイプ'].astype(str).str.strip()
        df['カルテ内容'] = df['カルテ内容'].astype(str).str.strip()

        # 診療日時を datetime に変換（曜日部分を除去）
        df['診療日時'] = df['診療日時'].str.replace(r'\([日月火水木金土]\)', '', regex=True)
        df['診療日時'] = pd.to_datetime(df['診療日時'], format='%Y/%m/%d %H:%M', errors='coerce')
        df = df.dropna(subset=['診療日時'])  # 変換失敗は除外

        # 定期訪問のみ抽出
        teiki = df[df['診療タイプ'] == '定期訪問'].copy()
        teiki = teiki.sort_values(['ID','診療日時'], ascending=[True, False])
        teiki_latest3 = teiki.groupby('ID').head(3)

        for _, row in teiki_latest3.iterrows():
            content = row['カルテ内容']
            # 半角#、全角＃の後の病名抽出
            matches = re.findall(r'[＃#](.*?)(?:\r?\n|$)', content)
            for disease_name in matches:
                disease_name = disease_name.strip()
                if disease_name:  # 空文字や数値のみは無視
                    all_records.append({'ID': row['ID'], 'disease_name': disease_name})

    except Exception as e:
        print(f"{file} の読み込みでエラー: {e}")

if all_records:
    result_df = pd.DataFrame(all_records).drop_duplicates()
    result_df.to_csv(output_path, index=False, header=True)
    print(f"抽出結果を {output_path} に保存しました。")
else:
    print("条件に合うデータが見つかりませんでした。")