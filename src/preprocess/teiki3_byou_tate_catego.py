import pandas as pd

# パス
input_path = "/Users/muna/Hana_research/data/derived/teiki_byou3_tate_with_category.csv"
keyword_path = "/Users/muna/Hana_research/data/derived/disease_keyword_tate.csv"
output_path = "/Users/muna/Hana_research/data/derived/teiki_byou3_tate_with_category.csv"

# エンコーディング候補
encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']

# CSV読み込み関数
def read_csv_with_fallback(path):
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            print(f"読み込み成功: {path} ({enc})")
            return df
        except Exception:
            print(f"失敗: {path} ({enc})")
    raise ValueError(f"{path} の読み込みに失敗")

# 読み込み
df = read_csv_with_fallback(input_path)
kw_df = read_csv_with_fallback(keyword_path)

# 列確認（念のため）
df = df[['ID', 'disease_name']].copy()
kw_df = kw_df[['カテゴリ', 'キーワード']].copy()

# キーワードリスト（順序重要）
keywords = kw_df['キーワード'].tolist()
categories = kw_df['カテゴリ'].tolist()

# 分類関数
def classify_disease(name):
    for kw, cat in zip(keywords, categories):
        if pd.isna(name):
            return "分類不能"
        if kw in name:
            return cat
    return "分類不能"

# 分類実行
df['category'] = df['disease_name'].apply(classify_disease)

# 保存
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("処理完了")
print(f"出力: {output_path}")