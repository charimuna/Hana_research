import pandas as pd

# 入力・出力ファイル
input_path = "/Users/muna/Hana_research/data/derived/teiki_byou3_car.csv"
output_path = "/Users/muna/Hana_research/data/derived/teiki_byou3_car_uniqueID.csv"

# 読み込み
df = pd.read_csv(input_path)

# end == 1 の行（最優先で残す）
df_end1 = df[df["end"] == 1]

# end != 1 の行
df_not_end1 = df[df["end"] != 1]

# past に「疑い」「術後」「切除後」を含む行を除外
mask_exclude = df_not_end1["past"].str.contains("疑い|術後|切除後", na=False)
df_filtered = df_not_end1[~mask_exclude]

# 残すデータを結合
df_keep = pd.concat([df_end1, df_filtered], ignore_index=True)

# IDのみ抽出 → 重複削除 → 数値ソート
df_id = df_keep[["ID"]].drop_duplicates()
df_id["ID"] = pd.to_numeric(df_id["ID"], errors="coerce")
df_id_sorted = df_id.sort_values("ID")

# 出力
df_id_sorted.to_csv(output_path, index=False, encoding="utf-8-sig")