import sqlite3
import pandas as pd
import numpy as np
import re

# =====================================================
# 設定
# =====================================================

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"

# =====================================================
# 接続
# =====================================================

conn = sqlite3.connect(DB_PATH)

# =====================================================
# 元データ取得
# =====================================================

sql = """
SELECT
    Study_ID,
    visit_datetime,
    record_no,
    karte_text
FROM karte
WHERE karte_text IS NOT NULL
"""

df = pd.read_sql(sql, conn)

print("loaded:", len(df))

# =====================================================
# visit_date
# =====================================================

df["visit_date"] = (
    pd.to_datetime(
        df["visit_datetime"].str.extract(r"^([0-9]{4}/[0-9]{1,2}/[0-9]{1,2})")[0],
        errors="coerce"
    )
    .dt.date
    .astype(str)
)

# =====================================================
# 正規化辞書
# =====================================================

HF_STD = {
    "hfpef": "HFpEF",
    "hfpref": "HFpEF",
    "preserved ef": "HFpEF",
    "ef正常": "HFpEF",
    "normal ef": "HFpEF",

    "hfmref": "HFmrEF",
    "hfmref ": "HFmrEF",
    "hfmref": "HFmrEF",

    "hfref": "HFrEF",
    "hfref ": "HFrEF",
    "systolic dysfunction": "HFrEF_like",
    "ef低下": "HFrEF_like",
    "収縮能低下": "HFrEF_like",
    "左室収縮能低下": "HFrEF_like",
    "左室機能低下": "HFrEF_like",
}

# =====================================================
# HF term抽出
# =====================================================

HF_PATTERNS = [
    r'\bHFpEF\b',
    r'\bHFPEF\b',
    r'\bhfpef\b',

    r'\bHFmrEF\b',
    r'\bHFMREF\b',
    r'\bhfmref\b',

    r'\bHFrEF\b',
    r'\bHFREF\b',
    r'\bhfref\b',

    r'preserved\s*EF',
    r'normal\s*EF',

    r'EF正常',
    r'EF低下',
    r'収縮能低下',
    r'左室収縮能低下',
    r'左室機能低下',
    r'systolic dysfunction',
]

HF_REGEX = re.compile(
    "|".join(HF_PATTERNS),
    flags=re.IGNORECASE
)

# =====================================================
# EF数値
# =====================================================

EF_RANGE_RE = re.compile(
    r'EF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[-－〜~]\s*(\d+(?:\.\d+)?)\s*[％%]',
    re.IGNORECASE
)

EF_VALUE_RE = re.compile(
    r'EF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]',
    re.IGNORECASE
)

EF_LT_RE = re.compile(
    r'EF\s*[＜<]\s*(\d+(?:\.\d+)?)',
    re.IGNORECASE
)

EF_GT_RE = re.compile(
    r'EF\s*[＞>]\s*(\d+(?:\.\d+)?)',
    re.IGNORECASE
)

# =====================================================
# ESC分類
# =====================================================

def classify_esc(ef):

    if pd.isna(ef):
        return None

    if ef <= 40:
        return "HFrEF"

    if ef < 50:
        return "HFmrEF"

    return "HFpEF"

# =====================================================
# 抽出
# =====================================================

rows = []

for _, r in df.iterrows():

    sid = r["Study_ID"]
    vdt = r["visit_datetime"]
    vdate = r["visit_date"]
    rec = r["record_no"]
    txt = str(r["karte_text"])

    numeric_classes = []
    term_classes = []

    # -----------------------------------------
    # EF range
    # -----------------------------------------

    for m in EF_RANGE_RE.finditer(txt):

        low = float(m.group(1))
        high = float(m.group(2))

        ef = np.median([low, high])

        cls = classify_esc(ef)

        numeric_classes.append(cls)

        rows.append([
            sid,vdt,vdate,rec,
            "EF_value",
            ef,
            m.group(0),
            None,
            None,
            "numeric",
            None,
            0
        ])

        rows.append([
            sid,vdt,vdate,rec,
            "HF_class_numeric",
            cls,
            m.group(0),
            None,
            None,
            "numeric",
            None,
            0
        ])

    # -----------------------------------------
    # EF value
    # -----------------------------------------

    for m in EF_VALUE_RE.finditer(txt):

        if "-" in m.group(0) or "－" in m.group(0):
            continue

        ef = float(m.group(1))

        cls = classify_esc(ef)

        numeric_classes.append(cls)

        rows.append([
            sid,vdt,vdate,rec,
            "EF_value",
            ef,
            m.group(0),
            None,
            None,
            "numeric",
            None,
            0
        ])

        rows.append([
            sid,vdt,vdate,rec,
            "HF_class_numeric",
            cls,
            m.group(0),
            None,
            None,
            "numeric",
            None,
            0
        ])

    # -----------------------------------------
    # HF terms
    # -----------------------------------------

    for m in HF_REGEX.finditer(txt):

        raw = m.group(0)

        key = raw.lower().strip()

        if key in HF_STD:
            std = HF_STD[key]
        elif key == "hfpef":
            std = "HFpEF"
        elif key == "hfmref":
            std = "HFmrEF"
        elif key == "hfref":
            std = "HFrEF"
        elif key == "hfref":
            std = "HFrEF"
        elif key == "hfpef":
            std = "HFpEF"
        else:
            std = raw

        term_classes.append(std)

        rows.append([
            sid,vdt,vdate,rec,
            "HF_term",
            raw,
            raw,
            raw,
            std,
            "term",
            None,
            0
        ])

        if std in ["HFpEF","HFmrEF","HFrEF","HFrEF_like"]:

            rows.append([
                sid,vdt,vdate,rec,
                "HF_class",
                std,
                raw,
                raw,
                std,
                "term",
                None,
                0
            ])

# =====================================================
# DataFrame化
# =====================================================

ef_long = pd.DataFrame(
    rows,
    columns=[
        "Study_ID",
        "visit_datetime",
        "visit_date",
        "record_no",
        "item",
        "value",
        "matched_text",
        "hf_term_raw",
        "hf_term_std",
        "ef_source",
        "final_hf_class",
        "conflict_flag"
    ]
)

# =====================================================
# conflict判定 + final_hf_class
# =====================================================

grp_cols = ["Study_ID","visit_datetime","record_no"]

for key, g in ef_long.groupby(grp_cols):

    numeric = g.loc[
        g["item"]=="HF_class_numeric",
        "value"
    ].tolist()

    term = g.loc[
        g["item"]=="HF_class",
        "value"
    ].tolist()

    final_class = None
    conflict = 0

    if len(numeric) > 0:
        final_class = numeric[0]

    elif len(term) > 0:
        final_class = term[0]

    if len(numeric) > 0 and len(term) > 0:

        if numeric[0] != term[0]:
            conflict = 1

    idx = g.index

    ef_long.loc[idx,"final_hf_class"] = final_class
    ef_long.loc[idx,"conflict_flag"] = conflict

# =====================================================
# SQLite保存
# =====================================================

cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS ef_long")

conn.commit()

ef_long.to_sql(
    "ef_long",
    conn,
    if_exists="replace",
    index=True,
    index_label="id"
)

# =====================================================
# INDEX
# =====================================================

cur.execute("""
CREATE INDEX IF NOT EXISTS idx_ef_long_sid
ON ef_long(Study_ID)
""")

cur.execute("""
CREATE INDEX IF NOT EXISTS idx_ef_long_date
ON ef_long(visit_date)
""")

cur.execute("""
CREATE INDEX IF NOT EXISTS idx_ef_long_item
ON ef_long(item)
""")

conn.commit()

print("saved:", len(ef_long))

conn.close()

print("DONE")