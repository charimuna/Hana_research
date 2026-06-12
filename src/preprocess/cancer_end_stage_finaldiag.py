import pandas as pd

# パス
category_path = "/Users/muna/Hana_research/data/derived/teiki_byou3_tate_with_category.csv"
choice_path = "/Users/muna/Hana_research/data/derived/teiki3_end_choice.csv"
output_path = "/Users/muna/Hana_research/data/derived/cancer_end_stage_finaldiag.csv"

# --- category 読み込み ---
df_cat = pd.read_csv(category_path, encoding="utf-8")

# 【癌】完全一致で抽出
df_cat_cancer = df_cat[df_cat["category"] == "【癌】"]

# IDをユニーク化
df_cat_cancer = df_cat_cancer[["ID"]].drop_duplicates()

# --- choice 読み込み ---
df_choice = pd.read_csv(choice_path, encoding="utf-8")

# 必要列のみ
df_choice = df_choice[["patient_ID", "end_stage"]]

# --- 結合（left）---
df = pd.merge(
    df_cat_cancer,
    df_choice,
    left_on="ID",
    right_on="patient_ID",
    how="left"
)

# --- end_stage → 0/1（文字列）変換 ---
def to_flag(x):
    try:
        return 1 if str(x).strip() == "1" or float(x) == 1 else 0
    except:
        return 0

df["endstage_cancer"] = df["end_stage"].apply(to_flag)

# --- ★ここが重要：IDごとに集約（1つでも1があれば1）---
df_grouped = df.groupby("ID", as_index=False)["endstage_cancer"].max()

# --- 出力整形 ---
df_out = df_grouped.rename(columns={"ID": "patient_ID"})

# 文字列に変換
df_out["patient_ID"] = df_out["patient_ID"].astype(str)
df_out["endstage_cancer"] = df_out["endstage_cancer"].astype(str)

# --- 保存 ---
df_out.to_csv(output_path, index=False, encoding="utf-8")

print("完了:", output_path)