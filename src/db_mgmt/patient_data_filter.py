import pandas as pd
import re
from datetime import datetime

# ===== パス =====
input_path = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260419.csv"
output_path = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260419_filtered.csv"

# ===== 必須カラム =====
required_cols = [
    "患者ID", "生年月日", "性別", "診断名", "初診日", "診療ステータス",
    "訪問先区分", "施設名", "寝たきり度", "認知度", "介護認定",
    "病歴", "常備薬", "死亡日", "死亡場所", "死亡コメント",
    "タグ", "終了日", "終了コメント", "管理料設定（最終算定日）", "PS"
]

# ===== カラム確認 =====
df_check = pd.read_csv(input_path, encoding="cp932", nrows=0)
actual_cols = [c.strip() for c in df_check.columns]

missing = [c for c in required_cols if c not in actual_cols]
if missing:
    print("=== カラム不一致 ===")
    print("不足:", missing)
    print("実際のカラム:")
    for col in actual_cols:
        print(repr(col))
    raise ValueError("カラム名が一致しない")

# ===== データ読み込み =====
df = pd.read_csv(
    input_path,
    encoding="cp932",
    usecols=required_cols,
    dtype=str
)

df.columns = df.columns.str.strip()

# ===== 和暦変換 =====
era_dict = {
    "明治": (1868, datetime(1868, 1, 25)),
    "大正": (1912, datetime(1912, 7, 30)),
    "昭和": (1926, datetime(1926, 12, 25)),
    "平成": (1989, datetime(1989, 1, 8)),
    "令和": (2019, datetime(2019, 5, 1)),
}

def convert_wareki(date_str):
    if pd.isna(date_str) or str(date_str).strip() == "":
        return date_str
    try:
        m = re.match(r"(明治|大正|昭和|平成|令和)(元|\d+)年(\d+)月(\d+)日", str(date_str))
        if not m:
            return None
        era, y, mth, d = m.groups()
        y = 1 if y == "元" else int(y)

        base, start = era_dict[era]
        year = base + y - 1
        dt = datetime(year, int(mth), int(d))

        if dt < start:
            dt = start

        return dt.strftime("%Y-%m-%d")
    except:
        return None

# ===== 汎用日付変換（これが修正ポイント）=====
def convert_date(date_str):
    if pd.isna(date_str) or str(date_str).strip() == "":
        return date_str
    dt = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")

# ===== 変換 =====
df["生年月日"] = df["生年月日"].apply(convert_wareki)

for col in ["初診日", "管理料設定（最終算定日）", "死亡日", "終了日"]:
    df[col] = df[col].apply(convert_date)

# ===== 保存 =====
df.to_csv(output_path, index=False, encoding="utf-8-sig")