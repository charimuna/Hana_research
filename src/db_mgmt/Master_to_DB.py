import pandas as pd
import sqlite3
import re
from datetime import datetime

# ===== パス =====
csv_path = "/Users/muna/Hana_research/data/raw/NowSamari/patient_data_20260419.csv"
db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

# ===== 和暦 → 西暦変換 =====
era_dict = {
    "明治": (1868, datetime(1868, 1, 25)),
    "大正": (1912, datetime(1912, 7, 30)),
    "昭和": (1926, datetime(1926, 12, 25)),
    "平成": (1989, datetime(1989, 1, 8)),
    "令和": (2019, datetime(2019, 5, 1)),
}

def convert_wareki_to_seireki(date_str):
    try:
        match = re.match(r"(明治|大正|昭和|平成|令和)(元|\d+)年(\d+)月(\d+)日", date_str)
        if not match:
            return None

        era, year, month, day = match.groups()
        year = 1 if year == "元" else int(year)
        month = int(month)
        day = int(day)

        base_year, start_date = era_dict[era]
        western_year = base_year + year - 1

        dt = datetime(western_year, month, day)

        # 元号開始日前は補正
        if dt < start_date:
            dt = start_date

        return dt.strftime("%Y-%m-%d")

    except:
        return None


# ===== 年齢計算（満年齢）=====
def calc_age(birth, visit):
    try:
        b = datetime.strptime(birth, "%Y-%m-%d")
        v = datetime.strptime(visit, "%Y-%m-%d")
        age = v.year - b.year - ((v.month, v.day) < (b.month, b.day))
        return age
    except:
        return None


# ===== CSV読み込み =====
df = pd.read_csv(
    csv_path,
    encoding="cp932",
    usecols=["患者ID", "生年月日", "初診日", "性別"]
)


# カラム名（日本語そのまま）
df = df[["患者ID", "生年月日", "初診日", "性別"]]

# ===== 前処理 =====
df = df.dropna()

# 初診日フィルタ
df = df[df["初診日"] != "0000-00-00"]

# 和暦→西暦
df["Birth_Date"] = df["生年月日"].apply(convert_wareki_to_seireki)

# 不正除外
df = df.dropna(subset=["Birth_Date"])

# 初診日チェック
def valid_date(x):
    try:
        datetime.strptime(x, "%Y-%m-%d")
        return True
    except:
        return False

df = df[df["初診日"].apply(valid_date)]

# 年齢
df["Age_at_Visit"] = df.apply(
    lambda row: calc_age(row["Birth_Date"], row["初診日"]), axis=1
)

df = df.dropna(subset=["Age_at_Visit"])

# 型
df["患者ID"] = df["患者ID"].astype(int)

# ===== DB接続 =====
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# ===== UPSERT =====
sql = """
INSERT INTO Patient_Master (Patient_ID, Birth_Date, First_Visit_Date, Gender, Age_at_Visit)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(Patient_ID) DO UPDATE SET
    Birth_Date=excluded.Birth_Date,
    First_Visit_Date=excluded.First_Visit_Date,
    Gender=excluded.Gender,
    Age_at_Visit=excluded.Age_at_Visit
"""

data = list(zip(
    df["患者ID"],
    df["Birth_Date"],
    df["初診日"],
    df["性別"],
    df["Age_at_Visit"]
))

cursor.executemany(sql, data)

conn.commit()
conn.close()