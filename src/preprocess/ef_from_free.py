import pandas as pd
import numpy as np
import re

# =====================================================
# Input / Output
# =====================================================

INPUT_CSV  = "/Users/muna/Hana_research/data/processed/EF_context.csv"
OUTPUT_LONG = "/Users/muna/Hana_research/data/processed/EF_extracted_long.csv"
OUTPUT_WIDE = "/Users/muna/Hana_research/data/processed/EF_wide.csv"

# =====================================================
# HF term 標準化辞書
# =====================================================

HF_STD = {
    "hfpef"                : "HFpEF",
    "preserved ef"         : "HFpEF",
    "ef正常"               : "HFpEF",
    "normal ef"            : "HFpEF",
    "hfmref"               : "HFmrEF",
    "hfref"                : "HFrEF",
    "ef低下"               : "HFrEF_like",
    "収縮能低下"            : "HFrEF_like",
    "左室収縮能低下"        : "HFrEF_like",
    "左室機能低下"          : "HFrEF_like",
    "systolic dysfunction" : "HFrEF_like",
}

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
# 生理的範囲フィルタ（フォールバック用）
# =====================================================

EF_MIN, EF_MAX = 10, 85

# =====================================================
# 正規表現
# =====================================================

BP_RE = re.compile(
    r'EF\s*(?:\(\s*(?:BP|m\.?simpson|simpson|MOD)\s*\))?\s*'
    r'[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]'
    r'\s*(?:\(\s*(?:BP|m\.?simpson|simpson|MOD)\s*\))?',
    re.I
)

TEICH_RE = re.compile(
    r'EF\s*(?:\(\s*teich(?:olz)?\s*\)|\s+teich(?:olz)?)\s*'
    r'[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]'
    r'|\bEF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]\s*\(\s*teich(?:olz)?\s*\)',
    re.I
)

EF_RANGE_RE = re.compile(
    r'EF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[-－〜~]\s*(\d+(?:\.\d+)?)\s*[％%]',
    re.I
)

EF_INEQ_RE = re.compile(
    r'EF\s*([>＞<＜][=＝]?|≥|≦|≤|≧)\s*(\d+(?:\.\d+)?)\s*([％%])?',
    re.I
)

EF_RE = re.compile(
    r'EF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]',
    re.I
)

FALLBACK_RE = re.compile(
    r'EF\D{0,15}?(\d+(?:\.\d+)?)\s*([％%]|mm\b)?',
    re.I
)

HF_REGEX = re.compile(
    r'HFpEF|HFPEF|hfpef|'
    r'HFmrEF|HFMREF|hfmref|'
    r'HFrEF|HFREF|hfref|'
    r'preserved\s*EF|'
    r'normal\s*EF|'
    r'EF正常|EF低下|'
    r'収縮能低下|左室収縮能低下|左室機能低下|'
    r'systolic dysfunction',
    re.I
)

_BP_CHECK    = re.compile(r'BP|simpson|MOD', re.I)
_TEICH_CHECK = re.compile(r'teich', re.I)

# =====================================================
# operator 正規化
# =====================================================

def normalize_operator(op_str):
    op = op_str.strip()
    if op in (">", "＞"):
        return ">"
    if op in ("<", "＜"):
        return "<"
    if op in (">=", "=>", "≥", "≧", "＞＝", "＞="):
        return ">="
    if op in ("<=", "=<", "≤", "≦", "＜＝", "＜="):
        return "<="
    return op

# =====================================================
# 優先度マッピング
# =====================================================

SOURCE_PRIORITY = {
    "BP"               : 1,
    "Simpson"          : 1,
    "MOD"              : 1,
    "teich"            : 2,
    "numeric"          : 3,
    "numeric_range"    : 3,
    "numeric_ineq"     : 3,
    "numeric_no_unit"  : 4,
    "numeric_mm_typo"  : 4,
}

# =====================================================
# 行追加ヘルパー
# =====================================================

def add_ef_rows(rows, sid, date, ef, raw, src, operator=None):
    cls = classify_esc(ef)
    rows.append([sid, date, "EF_value",         ef,  raw, None, None, src, None, 0, operator])
    rows.append([sid, date, "HF_class_numeric",  cls, raw, None, None, src, None, 0, operator])

# =====================================================
# メイン抽出ループ
# =====================================================

df   = pd.read_csv(INPUT_CSV)
rows = []

for _, r in df.iterrows():

    sid  = r["Study_ID"]
    date = r["Date"]
    txt  = str(r["context"])

    matched_spans = set()

    # 1. BP / Simpson / MOD
    for m in BP_RE.finditer(txt):
        raw = m.group(0)
        if _TEICH_CHECK.search(raw):
            continue
        if not _BP_CHECK.search(raw):
            continue
        src = "Simpson" if re.search(r'simpson|MOD', raw, re.I) else "BP"
        ef  = float(m.group(1))
        matched_spans.add(m.span())
        add_ef_rows(rows, sid, date, ef, raw, src, operator="=")

    # 2. Teicholz
    for m in TEICH_RE.finditer(txt):
        raw = m.group(0)
        val = m.group(1) or m.group(2)
        ef  = float(val)
        matched_spans.add(m.span())
        add_ef_rows(rows, sid, date, ef, raw, "teich", operator="=")

    # 3. 範囲表記
    for m in EF_RANGE_RE.finditer(txt):
        raw = m.group(0)
        if _BP_CHECK.search(raw) or _TEICH_CHECK.search(raw):
            continue
        ef = float(np.median([float(m.group(1)), float(m.group(2))]))
        matched_spans.add(m.span())
        add_ef_rows(rows, sid, date, ef, raw, "numeric_range", operator="=")

    # 4. 不等号表記
    for m in EF_INEQ_RE.finditer(txt):
        raw = m.group(0)
        if any(s <= m.start() < e for s, e in matched_spans):
            continue
        op = normalize_operator(m.group(1))
        ef = float(m.group(2))
        matched_spans.add(m.span())
        add_ef_rows(rows, sid, date, ef, raw, "numeric_ineq", operator=op)

    # 5. 素のEF数値（％付き）
    for m in EF_RE.finditer(txt):
        raw = m.group(0)
        if _BP_CHECK.search(raw) or _TEICH_CHECK.search(raw):
            continue
        after = txt[m.end():m.end()+20]
        if _BP_CHECK.search(after) or _TEICH_CHECK.search(after):
            continue
        if any(s <= m.start() < e for s, e in matched_spans):
            continue
        ef = float(m.group(1))
        matched_spans.add(m.span())
        add_ef_rows(rows, sid, date, ef, raw, "numeric", operator="=")

    # 6. フォールバック
    for m in FALLBACK_RE.finditer(txt):
        if any(s <= m.start() < e for s, e in matched_spans):
            continue
        ef = float(m.group(1))
        if not (EF_MIN <= ef <= EF_MAX):
            continue
        unit = (m.group(2) or "").strip().lower()
        if unit in ("%", "％"):
            src = "numeric"
        elif unit == "mm":
            src = "numeric_mm_typo"
        else:
            src = "numeric_no_unit"
        raw = m.group(0)
        matched_spans.add(m.span())
        add_ef_rows(rows, sid, date, ef, raw, src, operator="=")

    # 7. HF用語
    for m in HF_REGEX.finditer(txt):
        raw = m.group(0)
        key = raw.lower().strip()
        std = HF_STD.get(key, raw)
        if key == "hfpef":
            std = "HFpEF"
        elif key == "hfmref":
            std = "HFmrEF"
        elif key == "hfref":
            std = "HFrEF"
        rows.append([sid, date, "HF_term",  raw, raw, raw, std, "term", None, 0, None])
        rows.append([sid, date, "HF_class", std, raw, raw, std, "term", None, 0, None])

# =====================================================
# DataFrame 構築
# =====================================================

ef_long = pd.DataFrame(rows, columns=[
    "Study_ID",
    "Date",
    "item",
    "value",
    "matched_text",
    "hf_term_raw",
    "hf_term_std",
    "ef_source",
    "final_hf_class",
    "conflict_flag",
    "operator",
])

ef_long["source_priority"] = ef_long["ef_source"].map(SOURCE_PRIORITY)

# =====================================================
# final_hf_class / conflict_flag
# =====================================================

PRIORITY_ORDER = [
    ["BP", "Simpson"],
    ["teich"],
    ["numeric", "numeric_range", "numeric_ineq", "numeric_no_unit", "numeric_mm_typo"],
]

_SRC_RANK = {}
for _rank, _srcs in enumerate(PRIORITY_ORDER, start=1):
    for _s in _srcs:
        _SRC_RANK[_s] = _rank

for key, g in ef_long.groupby(["Study_ID", "Date"]):

    # 数値由来EF_valueを優先順に取得
    ef_vals = g[
        (g["item"] == "EF_value") &
        (g["ef_source"].isin(_SRC_RANK.keys()))
    ].copy()

    ef_vals["_rank"] = ef_vals["ef_source"].map(_SRC_RANK)
    ef_vals = ef_vals.sort_values("_rank")

    if len(ef_vals):
        best_ef     = ef_vals.iloc[0]["value"]
        final_class = classify_esc(best_ef)
    else:
        best_ef     = None
        final_class = None

    # 用語由来クラス
    term_vals = g.loc[g["item"] == "HF_class", "value"].tolist()
    term_set  = set(x for x in term_vals if x in ("HFpEF", "HFmrEF", "HFrEF"))
    hfref_like = any(x == "HFrEF_like" for x in term_vals)

    # 数値なしの場合は用語から決定
    if final_class is None:
        if len(term_set) == 1:
            final_class = term_set.pop()
        elif len(term_set) > 1:
            final_class = "未分類"
        elif hfref_like:
            final_class = "未分類"
        else:
            final_class = None

    # conflict判定
    conflict = 0
    if best_ef is not None and term_set:
        if classify_esc(best_ef) not in term_set:
            conflict = 1

    # 書き戻し
    ef_long.loc[g.index, "final_hf_class"] = final_class
    ef_long.loc[g.index, "conflict_flag"]  = conflict

# =====================================================
# long形式 保存
# =====================================================

ef_long.to_csv(OUTPUT_LONG, index=False, encoding="utf-8-sig")
print(f"[long] {len(ef_long)} rows → {OUTPUT_LONG}")
print("\n--- item ---")
print(ef_long["item"].value_counts())
print("\n--- ef_source ---")
print(ef_long["ef_source"].value_counts())
print("\n--- operator ---")
print(ef_long["operator"].value_counts(dropna=False))
print("\n--- final_hf_class ---")
print(ef_long.drop_duplicates(["Study_ID","Date","final_hf_class"])
      .groupby("final_hf_class", dropna=False).size())
print(f"\nconflict件数: {ef_long[ef_long.conflict_flag==1]['Study_ID'].nunique()} 患者")

# =====================================================
# wide化（1患者1行）
# =====================================================

WIDE_PRIORITY = {
    "BP"               : 1,
    "Simpson"          : 1,
    "teich"            : 2,
    "numeric"          : 3,
    "numeric_range"    : 3,
    "numeric_mm_typo"  : 3,
    "numeric_no_unit"  : 3,
    "numeric_ineq"     : 4,
    "term"             : 5,
}

ef_long["_wide_priority"] = ef_long["ef_source"].map(WIDE_PRIORITY)

# EF_value行を優先順にソートして1患者1行
ef_value_rows = ef_long[ef_long["item"] == "EF_value"].copy()
ef_value_rows["_has_value"] = ef_value_rows["value"].notna().astype(int)
ef_value_rows = ef_value_rows.sort_values(
    ["Study_ID", "_wide_priority", "_has_value"],
    ascending=[True, True, False]
)
best_rows = ef_value_rows.groupby("Study_ID", sort=False).first().reset_index()

# EF_valueがない患者はHF_class（term）で補完
term_only = ef_long[
    (ef_long["item"] == "HF_class") &
    (~ef_long["Study_ID"].isin(best_rows["Study_ID"]))
].copy()
term_only["_has_value"] = 0
term_only = term_only.sort_values("Study_ID")
term_only_best = term_only.groupby("Study_ID", sort=False).first().reset_index()

ef_wide = pd.concat([best_rows, term_only_best], ignore_index=True)
ef_wide = ef_wide.drop(columns=["_wide_priority", "_has_value"], errors="ignore")
ef_wide = ef_wide.sort_values("Study_ID").reset_index(drop=True)

# =====================================================
# wide形式 保存
# =====================================================

ef_wide.to_csv(OUTPUT_WIDE, index=False, encoding="utf-8-sig")
print(f"\n[wide] {len(ef_wide)} 患者 → {OUTPUT_WIDE}")
print("\n--- ef_source ---")
print(ef_wide["ef_source"].value_counts(dropna=False))
print("\n--- final_hf_class ---")
print(ef_wide["final_hf_class"].value_counts(dropna=False))
print("\n--- conflict_flag ---")
print(ef_wide["conflict_flag"].value_counts(dropna=False))