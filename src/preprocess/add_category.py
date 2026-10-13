import pandas as pd

# === 1. 読み込み ===
df = pd.read_csv("../data/processed/first_dis.csv", encoding="utf-8-sig")
dict_df = pd.read_csv("../data/processed/disease_keyword_tate.csv", encoding="utf-8-sig")

# === 2. 列名整理 ===
df.columns = df.columns.str.strip()
dict_df.columns = dict_df.columns.str.strip()

# === 3. 初期化 ===
df["カテゴリ"] = "分類不能"

# === 4. マッチング（部分一致）===
for _, row in dict_df.iterrows():
    category = row["カテゴリ"]
    keyword = str(row["キーワード"]).strip()

    if keyword == "" or pd.isna(keyword):
        continue

    # 部分一致でカテゴリー付与
    mask = df["病名"].str.contains(keyword, na=False, regex=False)  # ← ここ重要
    df.loc[mask, "カテゴリ"] = category

# === 5. 保存（上書き or 別名）===
df.to_csv("../data/processed/first_dis_with_category.csv", index=False, encoding="utf-8-sig")

print("Done: category added.")
