import pandas as pd

# パス
tag_path = "/Users/muna/Hana_research/data/derived/tag_tate.csv"
hot_path = "/Users/muna/Hana_research/data/derived/HOT_date_tate.csv"
output_path = "/Users/muna/Hana_research/data/derived/tag_tate_with_HOT.csv"

# 読み込み
tag_df = pd.read_csv(tag_path, dtype=str)
hot_df = pd.read_csv(hot_path, dtype=str)

# 列名統一（重要）
tag_df = tag_df.rename(columns={"患者ID": "Patient_ID"})
tag_df["Patient_ID"] = tag_df["Patient_ID"].astype(str)
hot_df["Patient_ID"] = hot_df["Patient_ID"].astype(str)

# ongoing列がなければ作成
if "ongoing" not in tag_df.columns:
    tag_df["ongoing"] = ""

# 在宅酸素タグのみ抽出
target_tag = "在宅酸素"
tag_hot = tag_df[tag_df["タグ"] == target_tag].copy()

# その他タグはそのまま保持
tag_other = tag_df[tag_df["タグ"] != target_tag].copy()

# 出力用リスト
result_rows = []

# HOTデータをIDごとに処理
hot_grouped = hot_df.groupby("Patient_ID")

for _, row in tag_hot.iterrows():
    pid = row["Patient_ID"]

    if pid not in hot_grouped.groups:
        # HOTに存在しない → ongoing=1
        new_row = row.copy()
        new_row["ongoing"] = "1"
        result_rows.append(new_row)
        continue

    hot_records = hot_grouped.get_group(pid)

    # 複数回含めて全展開
    for _, hrow in hot_records.iterrows():
        new_row = row.copy()
        new_row["開始日"] = hrow.get("start_date", "")
        new_row["終了日"] = hrow.get("end_date", "")

        if pd.isna(hrow.get("end_date")) or hrow.get("end_date") == "":
            new_row["ongoing"] = "1"
        else:
            new_row["ongoing"] = ""

        result_rows.append(new_row)

# DataFrame化
tag_hot_new = pd.DataFrame(result_rows)

# 結合（他タグ + 在宅酸素処理後）
final_df = pd.concat([tag_other, tag_hot_new], ignore_index=True)

# 保存
final_df.to_csv(output_path, index=False, encoding="utf-8-sig")

print("完了:", output_path)