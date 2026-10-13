import sqlite3
import pandas as pd
import numpy as np
import re

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(DB_PATH)

# =====================================================
# load
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
        df["visit_datetime"].str.extract(
            r"^([0-9]{4}/[0-9]{1,2}/[0-9]{1,2})"
        )[0],
        errors="coerce"
    )
    .dt.date
    .astype(str)
)

# =====================================================
# HF term normalization
# =====================================================

HF_STD = {

    "hfpef":"HFpEF",
    "preserved ef":"HFpEF",
    "ef正常":"HFpEF",
    "normal ef":"HFpEF",

    "hfmref":"HFmrEF",
    "hfmref ":"HFmrEF",
    "hfmref":"HFmrEF",
    "hfmref.":"HFmrEF",
    "hfmref,":"HFmrEF",
    "hfmref/":"HFmrEF",
    "hfmref;":"HFmrEF",
    "hfmref:":"HFmrEF",
    "hfmref?":"HFmrEF",
    "hfmref!":"HFmrEF",
    "hfmref\n":"HFmrEF",
    "hfmref\r":"HFmrEF",
    "hfmref\t":"HFmrEF",
    "hfmref　":"HFmrEF",

    "hfref":"HFrEF",

    "ef低下":"HFrEF_like",
    "収縮能低下":"HFrEF_like",
    "左室収縮能低下":"HFrEF_like",
    "左室機能低下":"HFrEF_like",
    "systolic dysfunction":"HFrEF_like",
}

# =====================================================
# ESC
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
# HF term
# =====================================================

HF_REGEX = re.compile(
    r'HFpEF|HFPEF|hfpef|'
    r'HFmrEF|HFMREF|hfmref|'
    r'HFrEF|HFREF|hfref|'
    r'preserved\s*EF|'
    r'normal\s*EF|'
    r'EF正常|'
    r'EF低下|'
    r'収縮能低下|'
    r'左室収縮能低下|'
    r'左室機能低下|'
    r'systolic dysfunction',
    re.I
)

# =====================================================
# EF regex
# =====================================================

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

rows = []

# =====================================================
# extract
# =====================================================

for _, r in df.iterrows():

    sid = r["Study_ID"]
    vdt = r["visit_datetime"]
    vdate = r["visit_date"]
    rec = r["record_no"]

    txt = str(r["karte_text"])

    # ----------------------------
    # BP
    # ----------------------------

    for m in BP_RE.finditer(txt):

        ef = float(m.group(1))
        cls = classify_esc(ef)

        rows.append([
            sid,vdt,vdate,rec,
            "EF_value",
            ef,
            m.group(0),
            None,None,
            "BP",
            None,
            0
        ])

        rows.append([
            sid,vdt,vdate,rec,
            "HF_class_numeric",
            cls,
            m.group(0),
            None,None,
            "BP",
            None,
            0
        ])

    # ----------------------------
    # teich
    # ----------------------------

    for m in TEICH_RE.finditer(txt):

        ef = float(m.group(1))
        cls = classify_esc(ef)

        rows.append([
            sid,vdt,vdate,rec,
            "EF_value",
            ef,
            m.group(0),
            None,None,
            "teich",
            None,
            0
        ])

        rows.append([
            sid,vdt,vdate,rec,
            "HF_class_numeric",
            cls,
            m.group(0),
            None,None,
            "teich",
            None,
            0
        ])

    # ----------------------------
    # range
    # ----------------------------

    for m in EF_RANGE_RE.finditer(txt):

        ef = np.median([
            float(m.group(1)),
            float(m.group(2))
        ])

        cls = classify_esc(ef)

        rows.append([
            sid,vdt,vdate,rec,
            "EF_value",
            ef,
            m.group(0),
            None,None,
            "numeric",
            None,
            0
        ])

        rows.append([
            sid,vdt,vdate,rec,
            "HF_class_numeric",
            cls,
            m.group(0),
            None,None,
            "numeric",
            None,
            0
        ])

    # ----------------------------
    # normal EF
    # ----------------------------

    for m in EF_RE.finditer(txt):

        if "BP" in m.group(0):
            continue

        if "teich" in m.group(0).lower():
            continue

        ef = float(m.group(1))

        cls = classify_esc(ef)

        rows.append([
            sid,vdt,vdate,rec,
            "EF_value",
            ef,
            m.group(0),
            None,None,
            "numeric",
            None,
            0
        ])

        rows.append([
            sid,vdt,vdate,rec,
            "HF_class_numeric",
            cls,
            m.group(0),
            None,None,
            "numeric",
            None,
            0
        ])

    # ----------------------------
    # term
    # ----------------------------

    for m in HF_REGEX.finditer(txt):

        raw = m.group(0)

        key = raw.lower().strip()

        if key == "hfpef":
            std = "HFpEF"
        elif key == "hfmref":
            std = "HFmrEF"
        elif key == "hfref":
            std = "HFrEF"
        else:
            std = HF_STD.get(key, raw)

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
# dataframe
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
# final class
# =====================================================

grp = ["Study_ID","visit_datetime","record_no"]

for key, g in ef_long.groupby(grp):

    bp = g.loc[
        (g.item=="HF_class_numeric") &
        (g.ef_source=="BP"),
        "value"
    ].tolist()

    teich = g.loc[
        (g.item=="HF_class_numeric") &
        (g.ef_source=="teich"),
        "value"
    ].tolist()

    numeric = g.loc[
        (g.item=="HF_class_numeric") &
        (g.ef_source=="numeric"),
        "value"
    ].tolist()

    term = g.loc[
        g.item=="HF_class",
        "value"
    ].tolist()

    if len(bp):
        final_class = bp[0]
    elif len(teich):
        final_class = teich[0]
    elif len(numeric):
        final_class = numeric[0]
    else:
        final_class = None

    numeric_set = set()

    if len(bp):
        numeric_set.add(bp[0])

    elif len(teich):
        numeric_set.add(teich[0])

    elif len(numeric):
        numeric_set.add(numeric[0])

    term_set = set(
        x for x in term
        if x in ["HFpEF","HFmrEF","HFrEF"]
    )

    conflict = 0

    if numeric_set and term_set:

        if numeric_set.isdisjoint(term_set):
            conflict = 1

    ef_long.loc[g.index,"final_hf_class"] = final_class
    ef_long.loc[g.index,"conflict_flag"] = conflict

# =====================================================
# save
# =====================================================

cur = conn.cursor()

cur.execute(
    "DROP TABLE IF EXISTS ef_long"
)

conn.commit()

ef_long.to_sql(
    "ef_long",
    conn,
    if_exists="replace",
    index=True,
    index_label="id"
)

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