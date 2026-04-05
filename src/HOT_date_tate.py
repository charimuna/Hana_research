import pandas as pd
import numpy as np

# パスの設定
input_path = '/Users/muna/Hana_research/data/derived/home_oxygen_dates_extracted.csv'
output_path = '/Users/muna/Hana_research/data/derived/HOT_date_tate.csv'

# CSVの読み込み
df = pd.read_csv(input_path, dtype=str, encoding="utf-8-sig")

# 1. 初回導入エピソードの抽出
ep1 = df[['患者ID', '在宅酸素導入日', '在宅酸素終了日']].copy()
ep1.columns = ['Patient_ID', 'start_date', 'end_date']

# 2. 再導入エピソードの抽出
ep2 = df[['患者ID', '在宅酸素再導入日', '在宅酸素再終了日']].copy()
ep2.columns = ['Patient_ID', 'start_date', 'end_date']

# 3. 縦に結合
df_tate = pd.concat([ep1, ep2], axis=0, ignore_index=True)

# 4. start_dateが空の行（再導入がない場合など）を削除
df_tate = df_tate.dropna(subset=['start_date'])

# 5. ongoing 列の作成 (終了日が空なら 1, それ以外は 0)
df_tate['ongoing'] = np.where(df_tate['end_date'].isna(), 1, 0)

# 6. IDと日付でソート（見やすくするため）
df_tate = df_tate.sort_values(['Patient_ID', 'start_date']).reset_index(drop=True)

# 7. 保存
df_tate.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"変換が完了しました。保存先: {output_path}")
print(df_tate.head(10))