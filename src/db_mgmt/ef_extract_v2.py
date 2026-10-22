"""
EF抽出・HF分類・壁運動所見の縦持ちテーブル作成（出力: ef_long_v2）

- 出力先は新テーブル ef_long_v2。既存の ef_long は変更しない。
- ef_value（REAL）と value_text（TEXT）を分離。NFKC＋小文字化で全角・半角を区別しない。
- EF分類: <=40 HFrEF / 41-49 HFmrEF / >=50 HFpEF。範囲値は下限を採用。
- EF優先順位: EF(BP) > EF(teich・t) > 単独EF・visual・v・simpson・area（ms・bo は対象外）。
  同一優先度で分類が不一致なら ef_multi_class、最終分類は NULL。
- 最終分類の優先順位: EF数値 > 用語 > 壁運動。
- 壁運動（asynergy・壁運動、誤記含む）は item=wall_motion（なし / あり / 判定不能）。
  EF数値も用語もない記録で「なし」のみ → HFpEF（final_basis=wall_motion）。
  「なし」と他の値が混在する記録は補完しない（conflict_detail=wm_mixed）。
- dyssynchrony / asynchrony（同期不全）は対象外。
"""

# =====================================================
# 設定
# =====================================================

import re, sqlite3, unicodedata
from collections import Counter
import numpy as np
import pandas as pd

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_colwidth", 80)

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUT_TABLE = "ef_long_v2"          # 既存 ef_long には触れない
EF_MAX = 100                      # 0 < EF <= 100 のみ採用
ALLOW_EF_WITHOUT_PERCENT = True   # 「EF 65,」のような%なし表記も抽出（10〜100のみ）

# =====================================================
# load
# =====================================================

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql(
    "SELECT Study_ID, visit_datetime, record_no, karte_text "
    "FROM karte WHERE karte_text IS NOT NULL",
    conn,
)
df["visit_date"] = (
    pd.to_datetime(
        df["visit_datetime"].astype(str).str.extract(r"^([0-9]{4}/[0-9]{1,2}/[0-9]{1,2})")[0],
        errors="coerce",
    ).dt.strftime("%Y-%m-%d")
)
print("loaded:", len(df))

# =====================================================
# 正規化・パターン
# =====================================================

# ---------- 正規化・パターン ----------
def norm(s):
    return unicodedata.normalize("NFKC", str(s)).lower()

NUM = r"(\d+(?:\.\d+)?)"
SEP = r"\s*[:=]?\s*"
PCT = r"\s*%"
RNG = r"\s*[-~〜−–—‐]\s*"
EFP = r"(?<![a-z])(?:lv)?ef"

BP_RE    = re.compile(rf"{EFP}\s*\(\s*bp\s*\){SEP}{NUM}{PCT}")
TEICH_RE = re.compile(rf"{EFP}\s*\(\s*teich\w*\s*\){SEP}{NUM}{PCT}")
RANGE_RE = re.compile(rf"{EFP}{SEP}{NUM}{RNG}{NUM}{PCT}")
EF_RE    = re.compile(rf"{EFP}{SEP}{NUM}{PCT}")
NOPCT_RE = re.compile(rf"{EFP}{SEP}{NUM}(?!\d|\.\d|\s*[%/~〜\-])")

# EF(方法名)。bp・teich* は上の専用パターンで処理済み。ms・bo は対象外
METH_RE       = re.compile(rf"{EFP}\s*\(\s*([a-z]+)\s*\){SEP}{NUM}{PCT}")
METH_NOPCT_RE = re.compile(rf"{EFP}\s*\(\s*([a-z]+)\s*\){SEP}{NUM}(?!\d|\.\d|\s*[%/~〜\-])")
METHOD_MAP = {"visual": "visual", "vusal": "visual", "v": "visual",
              "simpson": "simpson", "area": "area", "t": "teich"}

# 優先度（小さいほど優先）。同一優先度で分類が不一致なら ef_multi_class
LEVEL = {"BP": 0, "teich": 1,
         "visual": 2, "simpson": 2, "area": 2,
         "numeric": 2, "numeric_nopct": 2}

def classify_esc(ef):
    if ef <= 40:
        return "HFrEF"
    if ef < 50:
        return "HFmrEF"
    return "HFpEF"

# ---------- 用語 ----------
HF_TERM_RE = re.compile(
    r"hfpef|hfmref|hfref|"
    r"preserved\s*ef|normal\s*ef|ef\s*正常|"
    r"ef\s*低下|左室収縮能\s*低下|収縮能\s*低下|左室機能\s*低下|"
    r"systolic\s*dysfunction"
)
HF_STD = {
    "hfpef": "HFpEF", "preservedef": "HFpEF", "ef正常": "HFpEF", "normalef": "HFpEF",
    "hfmref": "HFmrEF",
    "hfref": "HFrEF", "ef低下": "HFrEF", "収縮能低下": "HFrEF",
    "左室収縮能低下": "HFrEF", "左室機能低下": "HFrEF", "systolicdysfunction": "HFrEF",
}

# ---------- 壁運動 ----------
# asynergy と誤記（asynegy, asyneryg, asynrgy ...）。asynch* は同期不全なので除外
ASYN_RE = re.compile(r"asyn[a-z]{1,8}|アシナジー|壁運動")
D = r"[-ー−‐–—]"
NEG_B = re.compile(r"(?<![a-z])(?:no|without)\s*(?:(?:lv|左室|明らかな|local|regional)\s*)*$")
NEG_A = re.compile(rf"^\s*(?:\(\s*{D}\s*\)|{D}|(?:は|も)?(?:なし|無し|無|ない|なさそう)|(?:は|も|など)?認めず|(?:は|も)?認めない)")
POS_A = re.compile(
    r"^\s*(?:\(\s*\+\s*\)|\+|(?:は|も)?(?:あり|有り|有|あるも)|を?認め|"
    r"(?:gy\s*)?\(\s*(?:akinesis|diffuse|inf|ant|sept|post|lat|apical|basal|mid)|in\s+[a-z])"
)
WM_ABSENT_1 = re.compile(r"^\s*(?:の|に|は|も)?\s*(?:低下|低k|異常)?\s*(?:は|も|を)?\s*(?:なし|無し|無|ない|認めず|認めない)")
WM_ABSENT_2 = re.compile(r"^\s*(?:は|に)?\s*(?:正常|問題なし)")
WM_PRESENT  = re.compile(r"^\s*(?:の|に|は|も)?\s*(?:低下|低k|異常|不良)")

def wm_state(kw, before, after):
    if kw == "壁運動":
        if WM_ABSENT_1.match(after) or WM_ABSENT_2.match(after):
            return "なし"
        if WM_PRESENT.match(after):
            return "あり"          # 「壁運動低下」（否定語なし）はあり
        return "判定不能"
    if NEG_B.search(before) or NEG_A.match(after):
        return "なし"
    if POS_A.match(after):
        return "あり"
    return "判定不能"              # 例：明らかなasynergy（否定語なし）

def ctx(t, s, e):
    return t[max(0, s - 12):e + 15].replace("\r", " ").replace("\n", "⏎")

# =====================================================
# 抽出
# =====================================================

# ---------- 抽出 ----------
COLS = ["rid", "Study_ID", "visit_datetime", "visit_date", "record_no",
        "item", "ef_value", "value_text", "matched_text",
        "hf_term_raw", "hf_term_std", "ef_source", "ef_is_range"]
rows = []
n_ef_out_of_range = 0

def add(base, item, ef_value=None, value_text=None, matched=None,
        raw=None, std=None, src=None, rng=0):
    rows.append((*base, item, ef_value, value_text, matched, raw, std, src, rng))

for rid, r in enumerate(df.itertuples(index=False)):
    t = norm(r.karte_text)
    base = (rid, r.Study_ID, r.visit_datetime, r.visit_date, r.record_no)

    # EF数値
    hits = []
    for rx, src in ((BP_RE, "BP"), (TEICH_RE, "teich"), (EF_RE, "numeric")):
        for m in rx.finditer(t):
            hits.append((float(m.group(1)), m.group(0), src, 0))
    for m in RANGE_RE.finditer(t):
        hits.append((min(float(m.group(1)), float(m.group(2))), m.group(0), "numeric", 1))
    for m in METH_RE.finditer(t):
        src = METHOD_MAP.get(m.group(1))
        if src and m.group(1) != "teich":
            hits.append((float(m.group(2)), m.group(0), src, 0))
    if ALLOW_EF_WITHOUT_PERCENT:
        for m in NOPCT_RE.finditer(t):
            v = float(m.group(1))
            if 10 <= v <= 100:
                hits.append((v, m.group(0), "numeric_nopct", 0))
        for m in METH_NOPCT_RE.finditer(t):
            src = METHOD_MAP.get(m.group(1))
            v = float(m.group(2))
            if src and 10 <= v <= 100:
                hits.append((v, m.group(0), src, 0))
    for ef, mt, src, rng in hits:
        if not (0 < ef <= EF_MAX):
            n_ef_out_of_range += 1
            continue
        add(base, "EF_value", ef_value=ef, matched=mt, src=src, rng=rng)
        add(base, "HF_class_numeric", value_text=classify_esc(ef), matched=mt, src=src, rng=rng)

    # 用語
    for m in HF_TERM_RE.finditer(t):
        raw = m.group(0)
        std = HF_STD[re.sub(r"\s+", "", raw)]
        add(base, "HF_term", value_text=raw, matched=raw, raw=raw, std=std, src="term")
        add(base, "HF_class", value_text=std, matched=raw, raw=raw, std=std, src="term")

    # 壁運動（asynergy・壁運動）
    for m in ASYN_RE.finditer(t):
        kw = m.group(0)
        if kw.startswith("asynch"):
            continue
        s, e = m.span()
        st = wm_state(kw, t[max(0, s - 20):s], t[e:e + 20])
        add(base, "wall_motion", value_text=st, matched=ctx(t, s, e))

ef_long = pd.DataFrame(rows, columns=COLS)
print("rows:", len(ef_long), "/ records with any row:", ef_long["rid"].nunique())
print("EF out of range (excluded):", n_ef_out_of_range)

# =====================================================
# 記録単位の最終分類
# =====================================================

# ---------- 記録単位の最終分類 ----------
def decide(rid, g):
    flags = []
    final, basis = None, "none"

    num = g[g["item"] == "HF_class_numeric"]
    term = set(g.loc[g["item"] == "HF_class", "value_text"])
    wm = set(g.loc[g["item"] == "wall_motion", "value_text"])

    if len(term) > 1:
        flags.append("term_multi_class")
    if "なし" in wm and (wm - {"なし"}):
        flags.append("wm_mixed")

    if len(num):
        lv = num["ef_source"].map(LEVEL)
        top = set(num.loc[lv == lv.min(), "value_text"])
        if len(top) == 1:
            final, basis = next(iter(top)), "EF"
            if term and term != {final}:
                flags.append("ef_vs_term")
        else:
            basis = "EF_conflict"
            flags.append("ef_multi_class")
    elif term:
        if len(term) == 1:
            final, basis = next(iter(term)), "term"
        else:
            basis = "term_conflict"
    elif wm == {"なし"}:
        final, basis = "HFpEF", "wall_motion"

    return {"rid": rid, "final_hf_class": final, "final_basis": basis,
            "conflict_flag": int(bool(flags)), "conflict_detail": ";".join(flags) or None}

rec = pd.DataFrame([decide(rid, g) for rid, g in ef_long.groupby("rid", sort=False)])
ef_long = ef_long.merge(rec, on="rid", how="left")

print(rec["final_basis"].value_counts(dropna=False).to_string())
print()
print(rec["final_hf_class"].value_counts(dropna=False).to_string())
print()
print(rec["conflict_detail"].value_counts(dropna=False).to_string())

# =====================================================
# 保存
# =====================================================

# ---------- 保存（新テーブル。既存 ef_long は変更しない） ----------
out = ef_long.drop(columns=["rid"])
cur = conn.cursor()
cur.execute(f"DROP TABLE IF EXISTS {OUT_TABLE}")
conn.commit()
out.to_sql(OUT_TABLE, conn, if_exists="replace", index=True, index_label="id")
for col in ["Study_ID", "visit_date", "item"]:
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_{col} ON {OUT_TABLE}({col})")
conn.commit()
print("saved:", OUT_TABLE, len(out))
print(pd.read_sql(f"SELECT item, COUNT(*) n, typeof(ef_value) t FROM {OUT_TABLE} GROUP BY item, t", conn))

# =====================================================
# 診断：未抽出のEF表記
# =====================================================

# ---------- 診断：未抽出のEF表記 ----------
# (1) EF(方法名) 形式のうち BP / teich 以外（現行ルールでは抽出していない）
METHOD_RE = re.compile(r"(?<![a-z])(?:lv)?ef\s*\(\s*([a-z]+)\s*\)\s*[:=]?\s*\d")
ef_rids = set(ef_long.loc[ef_long["item"] == "EF_value", "rid"])
meth = Counter()
nopct_rec = 0
for rid, r in enumerate(df.itertuples(index=False)):
    t = norm(r.karte_text)
    for m in METHOD_RE.finditer(t):
        name = m.group(1)
        if name != "bp" and not name.startswith("teich"):
            meth[name] += 1
    if rid not in ef_rids:
        if any(10 <= float(m.group(1)) <= 100 for m in NOPCT_RE.finditer(t)):
            nopct_rec += 1
print("EF(方法名) のうち BP/teich 以外（ms・bo など対象外のものも含む）:")
print(pd.Series(meth).sort_values(ascending=False).head(15).to_string() or "なし")
print()
print("EF数値が1件も抽出されず、「EF 65」のような%なし表記だけがある記録:", nopct_rec)

# =====================================================
# 終了
# =====================================================

conn.close()
print("DONE")
