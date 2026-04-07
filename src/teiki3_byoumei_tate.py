import pandas as pd
import re

# 入出力パス
input_path = "/Users/muna/Hana_research/data/derived/teiki_byo3.csv"
output_path = "/Users/muna/Hana_research/data/derived/teiki_byou3_tate.csv"

# エンコーディング候補
encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']

# 読み込み（順番に試す）
for enc in encodings:
    try:
        df = pd.read_csv(input_path, encoding=enc)
        print(f"読み込み成功: {enc}")
        break
    except Exception as e:
        print(f"失敗: {enc}")
else:
    raise ValueError("すべてのエンコーディングで読み込み失敗")

# 必要列確認
df = df[['ID', 'disease_name']].copy()

# 分割（, 、 。すべて対応）
df['disease_name'] = df['disease_name'].apply(
    lambda x: re.split(r'[,\u3001\u3002]', str(x))
)

# explodeで縦持ち化
df = df.explode('disease_name')

# トリム＋空要素除去
df['disease_name'] = df['disease_name'].str.strip()
df = df[df['disease_name'] != ""]

# IDを数値に変換（指定通り）
df['ID'] = pd.to_numeric(df['ID'], errors='coerce')

# 重複削除（ID × disease_name）
df = df.drop_duplicates(subset=['ID', 'disease_name'])

# 保存
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("処理完了")
print(f"出力ファイル: {output_path}")