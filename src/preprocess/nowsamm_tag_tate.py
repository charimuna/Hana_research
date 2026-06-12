import pandas as pd

# 入力ファイル
input_path = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data(20251116).csv"

# 出力ファイル
output_path = "/Users/muna/Hana_research/data/raw/NowSamari/tag_tate.csv"

# CSV読み込み（文字コードは必要に応じて変更）
df = pd.read_csv(input_path, dtype=str, encoding="cp932")

# 列名確認（重要）
print(df.columns)

# 仮定：列名が「患者ID」「タグ」の場合
# ※違う場合はここを書き換える
id_col = "患者ID"
tag_col = "タグ"

# タグがNaNの行を除外
df = df[[id_col, tag_col]].dropna(subset=[tag_col])

# タグを分割（半角スペース）
df[tag_col] = df[tag_col].str.split(" ")

# explodeで縦持ちに
df_tate = df.explode(tag_col)

# 空タグ削除
df_tate = df_tate[df_tate[tag_col].notna() & (df_tate[tag_col] != "")]

# 開始日・終了日を追加（空欄）
df_tate["開始日"] = ""
df_tate["終了日"] = ""

# 列順を整理
df_tate = df_tate[[id_col, tag_col, "開始日", "終了日"]]

# 出力
df_tate.to_csv(output_path, index=False, encoding="utf-8-sig")

print("完了:", output_path)