import pandas as pd
import glob
import os
import re

# パス
folder_path = "/Users/muna/Hana_research/data/raw/AllKarte"
output_path = "/Users/muna/Hana_research/data/derived/teiki3_end_choice.csv"

# エンコーディング候補
encodings = ['utf-8', 'shift_jis', 'cp932', 'utf-8-sig']

csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

all_records = []

# キーワード辞書（正規表現）
keyword_patterns = {
    "緩和": r"(緩和ケア|緩和的|緩和)",
    "末期": r"(末期|終末期|ターミナル)",
    "看取り": r"(看取り期|看取|みとり|看取り)",
    "転移": r"(転移)",
    "BSC": r"(BSC|Best\s*supportive\s*care|best\s*supportive)",
    "播種": r"(播種)"
}

# 除外トリガー
exclude_trigger = r"(今後について|今後のこと)"

def read_csv_multi_encoding(file):
    for enc in encodings:
        try:
            return pd.read_csv(file, encoding=enc)
        except:
            continue
    print(f"読み込み失敗: {file}")
    return None

def split_sentences(text):
    # 文単位分割（句点ベース）
    sentences = re.split(r"[。．]", text)
    return [s.strip() for s in sentences if s.strip()]

def extract_keywords(text):
    found_keywords = set()

    sentences = split_sentences(text)

    for i, sentence in enumerate(sentences):

        # 除外条件チェック
        if re.search(exclude_trigger, sentence):
            window = sentences[i:i+3]  # 3文以内
            window_text = " ".join(window)
            if any(re.search(pat, window_text) for pat in keyword_patterns.values()):
                continue

        # 通常キーワード抽出
        for key, pat in keyword_patterns.items():
            if re.search(pat, sentence, re.IGNORECASE):
                found_keywords.add(key)

    return list(found_keywords)


for file in csv_files:
    df = read_csv_multi_encoding(file)
    if df is None:
        continue

    expected_cols = ['診療タイプ', '診療日時', 'ID', 'カルテ内容']
    if not all(col in df.columns for col in expected_cols):
        print(f"{file} 列不一致")
        continue

    df = df.dropna(subset=expected_cols)

    df['診療タイプ'] = df['診療タイプ'].astype(str).str.strip()
    df['カルテ内容'] = df['カルテ内容'].astype(str)

    # 日付処理
    df['診療日時'] = df['診療日時'].str.replace(r'\([日月火水木金土]\)', '', regex=True)
    df['診療日時'] = pd.to_datetime(df['診療日時'], errors='coerce')
    df = df.dropna(subset=['診療日時'])

    # 定期のみ
    teiki = df[df['診療タイプ'] == '定期訪問'].copy()

    # 新しい順
    teiki = teiki.sort_values(['ID', '診療日時'], ascending=[True, False])

    # 各患者 最新3件
    teiki_latest3 = teiki.groupby('ID').head(3)

    # 患者ごとに処理
    for pid, group in teiki_latest3.groupby('ID'):

        hit = False

        for _, row in group.sort_values('診療日時', ascending=False).iterrows():

            content = row['カルテ内容']
            keywords = extract_keywords(content)

            if keywords:
                all_records.append({
                    "patient_ID": pid,
                    "date": row['診療日時'].strftime("%Y-%m-%d"),
                    "keyword": ",".join(sorted(set(keywords))),
                    "end_stage": 1
                })
                hit = True
                break  # 最初のヒットのみ

        if not hit:
            all_records.append({
                "patient_ID": pid,
                "date": "",
                "keyword": "",
                "end_stage": 0
            })

# 保存
result_df = pd.DataFrame(all_records)
result_df.to_csv(output_path, index=False, encoding="utf-8-sig")

print("完了:", output_path)