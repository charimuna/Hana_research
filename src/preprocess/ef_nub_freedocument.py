import sqlite3
import pandas as pd
import numpy as np
import re

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"

# ==========================================
# connect
# ==========================================

conn = sqlite3.connect(DB_PATH)

df = pd.read_sql("""
SELECT
    Study_ID,
    Date,
    document_type,
    text_data
FROM Freedocument
WHERE document_type LIKE '%初診時%'
""", conn)

conn.close()

# ==========================================
# regex
# ==========================================

BP_RE = re.compile(
    r'EF\s*\(\s*BP\s*\)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]',
    re.I
)

TEICH_RE = re.compile(
    r'EF\s*\(\s*teich\s*\)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]',
    re.I
)

EF_RANGE_RE = re.compile(
    r'EF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[-－〜~]\s*(\d+(?:\.\d+)?)\s*[％%]',
    re.I
)

EF_RE = re.compile(
    r'EF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]',
    re.I
)
EF_OPERATOR_RE = re.compile(
    r'EF\s*'
    r'([><＞＜≧≦])\s*'
    r'(\d+(?:\.\d+)?)'
    r'\s*[％%]',
    re.I
)

# ==========================================
# extract
# ==========================================

rows = []

for _, r in df.iterrows():

    sid = r["Study_ID"]
    dt = r["Date"]
    dtype = r["document_type"]

    txt = str(r["text_data"])

    found = False

    # ----------------------
    # BP
    # ----------------------

    for m in BP_RE.finditer(txt):

        rows.append({
            "Study_ID": sid,
            "Date": dt,
            "document_type": dtype,
            "EF": float(m.group(1)),
            "source": "BP",
            "matched_text": m.group(0)
        })

        found = True

    # ----------------------
    # teich
    # ----------------------

    for m in TEICH_RE.finditer(txt):

        rows.append({
            "Study_ID": sid,
            "Date": dt,
            "document_type": dtype,
            "EF": float(m.group(1)),
            "source": "teich",
            "matched_text": m.group(0)
        })

        found = True

    # ----------------------
    # range
    # ----------------------

    for m in EF_RANGE_RE.finditer(txt):

        ef = np.median([
            float(m.group(1)),
            float(m.group(2))
        ])

        rows.append({
            "Study_ID": sid,
            "Date": dt,
            "document_type": dtype,
            "EF": ef,
            "source": "range",
            "matched_text": m.group(0)
        })

        found = True

    # ----------------------
    # normal EF
    # ----------------------

    if not found:

        for m in EF_RE.finditer(txt):

            rows.append({
                "Study_ID": sid,
                "Date": dt,
                "document_type": dtype,
                "EF": float(m.group(1)),
                "source": "numeric",
                "matched_text": m.group(0)
            })

# ==========================================
# dataframe
# ==========================================

ef_df = pd.DataFrame(rows)

print("抽出件数 =", len(ef_df))

print(
    ef_df.head(20).to_string()
)

# ==========================================
# save
# ==========================================

out_csv = "/Users/muna/Hana_research/data/processed/initial_EF_extracted.csv"

ef_df.to_csv(
    out_csv,
    index=False,
    encoding="utf-8-sig"
)

print()
print("保存先:")
print(out_csv)