df = pd.read_csv(input_path, encoding="cp932")

# カラム名正規化
df.columns = df.columns.str.strip()

# 必要カラムだけ抽出（存在チェック付き）
missing = [c for c in cols if c not in df.columns]
if missing:
    print("存在しないカラム:", missing)
    print("実際のカラム:", df.columns.tolist())
    raise ValueError("カラム不一致")

df = df[cols]