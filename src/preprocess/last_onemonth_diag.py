import pandas as pd
import sqlite3

# =========================
# ファイル
# =========================

keyword_file = "/Users/muna/Hana_research/data/processed/disease_keyword.xlsx"
term_file = "/Users/muna/Desktop/patient_hash_terms_vertical.csv"
db_file = "/Users/muna/Hana_research/data/db/Hana_Research.db"
table_name = "last_one_month_diag"

# =========================
# 分類辞書作成
# =========================

kw_df = pd.read_excel(keyword_file, header=None)

rules = []

for col_idx, col in enumerate(kw_df.columns):
    category = kw_df.iloc[0, col_idx]
    if pd.isna(category):
        continue
    for keyword in kw_df.iloc[1:, col_idx]:
        if pd.isna(keyword):
            continue
        keyword = str(keyword).strip()
        if keyword:
            rules.append({
                "keyword": keyword,
                "category": str(category).strip()
            })

# 長いキーワード優先
rules = sorted(rules, key=lambda x: len(x["keyword"]), reverse=True)
print(f"keyword数: {len(rules)}")

# =========================
# term読込
# =========================

df = pd.read_csv(term_file)
df = df.rename(columns={"term": "diagnosis"})

# =========================
# 分類
# =========================

def classify_term(term):
    diseases = [t.strip() for t in str(term).replace("，", "、").replace(",", "、").split("、")]
    categories = []
    for disease in diseases:
        matched = "分類不能"
        for rule in rules:
            if rule["keyword"] in disease:
                matched = rule["category"]
                break
        categories.append(matched)
    unique_cats = list(dict.fromkeys(categories))
    return "、".join(unique_cats)

df["category"] = df["diagnosis"].apply(classify_term)

# =========================
# 心不全フラグ
# =========================

def has_heart_failure(term):
    diseases = [t.strip() for t in str(term).replace("，", "、").replace(",", "、").split("、")]
    return 1 if any("心不全" in d for d in diseases) else 0

df["heart_failure_flg"] = df["diagnosis"].apply(has_heart_failure)

# =========================
# DB保存
# =========================

conn = sqlite3.connect(db_file)

df.to_sql(
    table_name,
    conn,
    if_exists="replace",
    index=False
)

conn.close()

# =========================
# 確認
# =========================

print(df.head(20))
print("\n分類結果")
print(df["category"].value_counts().head(30))
print(f"\n心不全あり: {df['heart_failure_flg'].sum()}件")
print(f"\nDB保存完了: {db_file} / テーブル: {table_name}")