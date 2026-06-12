import pandas as pd

# 入力ファイル
input_path = "/Users/muna/Hana_research/data/derived/teiki_byou3_tate_with_category.csv"

# 出力ファイル
output_path = "/Users/muna/Hana_research/data/derived/teiki_byou3_car.csv"

# CSV読み込み
df = pd.read_csv(input_path)

# 「category」に「癌」を含む行を抽出
df_cancer = df[df["category"].str.contains("癌", na=False)]

# 必要な列のみ抽出（重複はそのまま保持）
df_out = df_cancer[["ID", "disease_name"]]

# IDを数値に変換してソート
df_out["ID"] = pd.to_numeric(df_out["ID"], errors="coerce")
df_sorted = df_out.sort_values("ID")

# CSV出力（ヘッダーあり）
df_sorted.to_csv(output_path, index=False, encoding="utf-8-sig")