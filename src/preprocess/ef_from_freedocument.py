"""
ef_extraction.py
────────────────────────────────────────────────────────────────
Freedocument テーブルから EF 値・HF 分類を抽出し
ef_from_freedocument テーブルに書き込む。

修正点:
  1. EF_BP_RE の group 番号バグ修正（BP のみ group(2)、他は group(1)）
  2. EF の二重抽出防止（使用済みスパン管理）
  3. EF_RANGE_RE と EF_RE の重複マッチ防止
  4. HF_STD キーの空白正規化（preserved ef 等のヒット漏れ修正）
  5. final クラスを per-visit サマリテーブルに保存
  6. conflict_flag 更新を groupby 後に安全に行う
"""

import re
import sqlite3

import numpy as np
import pandas as pd

DB_PATH = "/Users/muna/Hana_research/data/db/Hana_Research.db"

# ──────────────────────────────────────────────────────────────
# HF 用語正規化
# ──────────────────────────────────────────────────────────────

HF_REGEX = re.compile(
    # HFpEF 系（スラッシュ・スペース区切りを許容）
    r"HF[/\s]?pEF|HFPEF|hfpef"
    r"|preserved\s*EF"
    r"|normal\s*EF"
    r"|EF正常"
    # HFmrEF 系
    r"|HF[/\s]?mrEF|HFMREF|hfmref"
    # HFrEF 系
    r"|HF[/\s]?rEF|HFREF|hfref"
    r"|EF低下"
    r"|収縮能低下"
    r"|左室収縮能低下"
    r"|左室機能低下"
    r"|systolic\s+dysfunction"
    # 分類不明
    r"|心不全",
    re.I,
)

# normalize_hf_key() 適用後のキーで照合
HF_STD: dict[str, str] = {
    # HFpEF 系
    "hfpef":                "HFpEF",
    "hf/pef":               "HFpEF",
    "hf pef":               "HFpEF",
    "preserved ef":         "HFpEF",
    "normal ef":            "HFpEF",
    "ef正常":               "HFpEF",
    # HFmrEF 系
    "hfmref":               "HFmrEF",
    "hf/mref":              "HFmrEF",
    "hf mref":              "HFmrEF",
    # HFrEF 系
    "hfref":                "HFrEF",
    "hf/ref":               "HFrEF",
    "hf ref":               "HFrEF",
    "ef低下":               "HFrEF_like",
    "収縮能低下":           "HFrEF_like",
    "左室収縮能低下":       "HFrEF_like",
    "左室機能低下":         "HFrEF_like",
    "systolic dysfunction": "HFrEF_like",
    # 分類不明
    "心不全":               "HF_unclassified",
}


def normalize_hf_key(raw: str) -> str:
    """マッチ文字列を辞書キー照合用に正規化する。"""
    return re.sub(r"\s+", " ", raw.lower().strip())


def lookup_hf_std(raw: str) -> str:
    """HF_STD を引き、なければ略語プレフィクスで判定する。"""
    key = normalize_hf_key(raw)
    if key in HF_STD:
        return HF_STD[key]
    # 略語フォールバック
    if "hfpef" in key:
        return "HFpEF"
    if "hfmref" in key:
        return "HFmrEF"
    if "hfref" in key:
        return "HFrEF"
    # preserved / normal EF
    if "preserved" in key or "normal ef" in key or "ef正常" in key:
        return "HFpEF"
    return "HFpEF"  # デフォルト（要件に応じて None に変更可）


# ──────────────────────────────────────────────────────────────
# EF 正規表現
#   ※ 各パターンは (数値) を group(1) に統一する。
#      BP だけ (BP|Biplane) が group(1) に入るため group(2) を使う
#      → group_idx を tuple で管理して解決する。
# ──────────────────────────────────────────────────────────────

# EF (BP|Biplane): group(1)=メソッド名, group(2)=数値
EF_BP_RE = re.compile(
    r"EF\s*\(\s*(BP|Biplane|Simpson|MOD)\s*\)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]",
    re.I,
)

# EF (Teich): group(1)=数値
EF_TEICH_RE = re.compile(
    r"EF\s*\(\s*Teich\s*\)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]",
    re.I,
)

# EF 範囲: group(1)=下限, group(2)=上限
EF_RANGE_RE = re.compile(
    r"EF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[-－〜~]\s*(\d+(?:\.\d+)?)\s*[％%]",
    re.I,
)

# EF 単独数値: group(1)=数値
EF_RE = re.compile(
    r"EF\s*[:=]?\s*(\d+(?:\.\d+)?)\s*[％%]",
    re.I,
)

# ──────────────────────────────────────────────────────────────
# EF → HF クラス分類（ESC 2021 基準）
# ──────────────────────────────────────────────────────────────

def classify_esc(ef: float | None) -> str | None:
    if ef is None:
        return None
    if ef <= 40:
        return "HFrEF"
    if ef < 50:
        return "HFmrEF"
    return "HFpEF"


# ──────────────────────────────────────────────────────────────
# DB 接続・データ取得
# ──────────────────────────────────────────────────────────────

conn = sqlite3.connect(DB_PATH)

df = pd.read_sql(
    "SELECT * FROM Freedocument WHERE document_type LIKE '%初診時%'",
    conn,
)

# ──────────────────────────────────────────────────────────────
# テキストごとの抽出ループ
# ──────────────────────────────────────────────────────────────

COLUMNS = [
    "Study_ID", "visit_date", "record_no",
    "item", "value", "matched_text",
    "ef_value", "ef_low", "ef_high",
    "ef_source", "hf_term_raw", "hf_term_std",
    "conflict_flag",
]

rows: list[list] = []


def append_ef_rows(
    sid, vdt, rec, ef: float, matched: str,
    ef_low=None, ef_high=None, source: str = "raw",
):
    """EF 数値行と数値由来 HF クラス行を rows に追加する。"""
    cls = classify_esc(ef)
    rows.append([sid, vdt, rec,
                 "EF_value",          ef,  matched,
                 ef, ef_low, ef_high, source, None, None, 0])
    rows.append([sid, vdt, rec,
                 "HF_class_numeric",  cls, matched,
                 ef, ef_low, ef_high, source, None, None, 0])


for _, r in df.iterrows():
    sid = r["Study_ID"]
    vdt = r["Date"]
    rec = r.get("record_no", None)
    txt = str(r["text_data"])

    # 使用済みスパンを記録して二重抽出を防ぐ
    used_spans: set[tuple[int, int]] = set()

    # ── 1. メソッド付き EF (BP / Simpson / Teich) ──────────────
    # (pattern, source, value_group_index)
    method_patterns = [
        (EF_BP_RE,   "BP",      2),   # group(2) が数値
        (EF_SIMP_RE, "Simpson", 1),   # group(1) が数値
        (EF_TEICH_RE,"Teich",   1),
    ]

    for pattern, source, g_idx in method_patterns:
        for m in pattern.finditer(txt):
            span = (m.start(), m.end())
            used_spans.add(span)
            ef = float(m.group(g_idx))
            append_ef_rows(sid, vdt, rec, ef, m.group(0), source=source)

    # ── 2. EF 範囲 ────────────────────────────────────────────
    for m in EF_RANGE_RE.finditer(txt):
        span = (m.start(), m.end())
        if span in used_spans:
            continue
        used_spans.add(span)
        low  = float(m.group(1))
        high = float(m.group(2))
        ef   = (low + high) / 2
        append_ef_rows(sid, vdt, rec, ef, m.group(0),
                       ef_low=low, ef_high=high, source="range")

    # ── 3. EF 単独（未使用スパンのみ） ────────────────────────
    for m in EF_RE.finditer(txt):
        span = (m.start(), m.end())
        if span in used_spans:
            continue
        used_spans.add(span)
        ef = float(m.group(1))
        append_ef_rows(sid, vdt, rec, ef, m.group(0), source="raw")

    # ── 4. HF 用語 ────────────────────────────────────────────
    for m in HF_REGEX.finditer(txt):
        raw = m.group(0)
        std = lookup_hf_std(raw)
        rows.append([sid, vdt, rec,
                     "HF_term",       raw, raw,
                     None, None, None, "term", raw, std, 0])
        rows.append([sid, vdt, rec,
                     "HF_class_term", std, raw,
                     None, None, None, "term", raw, std, 0])

# ──────────────────────────────────────────────────────────────
# DataFrame 作成
# ──────────────────────────────────────────────────────────────

ef_long = pd.DataFrame(rows, columns=COLUMNS)

# 抽出ゼロ件でも後続処理・DB 書き込みが壊れないよう早期チェック
if ef_long.empty:
    print("WARNING: 抽出結果が 0 件です。正規表現・クエリ条件を確認してください。")
    # 空テーブルを型付きで作成して終了
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ef_from_freedocument (
            Study_ID TEXT, visit_date TEXT, record_no TEXT,
            item TEXT, value TEXT, matched_text TEXT,
            ef_value REAL, ef_low REAL, ef_high REAL,
            ef_source TEXT, hf_term_raw TEXT, hf_term_std TEXT,
            conflict_flag INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ef_visit_summary (
            Study_ID TEXT, visit_date TEXT, record_no TEXT,
            numeric_class TEXT, term_class TEXT,
            final_class TEXT, conflict_flag INTEGER
        )
    """)
    conn.commit()
    conn.close()
    raise SystemExit(0)

# record_no が None の行も groupby で落とさないよう文字列 "NA" に変換
ef_long["record_no"] = ef_long["record_no"].fillna("NA").astype(str)

# ──────────────────────────────────────────────────────────────
# per-visit 集計: conflict_flag と final_class を付与
# ──────────────────────────────────────────────────────────────

GRP = ["Study_ID", "visit_date", "record_no"]

summary_records: list[dict] = []

for key, g in ef_long.groupby(GRP, sort=False, dropna=False):
    num_classes  = g.loc[g["item"] == "HF_class_numeric", "value"].dropna().tolist()
    term_classes = g.loc[g["item"] == "HF_class_term",    "value"].dropna().tolist()

    numeric_cls = num_classes[0]  if num_classes  else None
    term_cls    = term_classes[0] if term_classes else None

    conflict = int(
        bool(numeric_cls and term_cls and numeric_cls != term_cls)
    )

    final_cls = numeric_cls if numeric_cls else term_cls

    summary_records.append({
        "Study_ID":      key[0],
        "visit_date":    key[1],
        "record_no":     key[2],
        "numeric_class": numeric_cls,
        "term_class":    term_cls,
        "final_class":   final_cls,
        "conflict_flag": conflict,
    })

    ef_long.loc[g.index, "conflict_flag"] = conflict

visit_summary = pd.DataFrame(summary_records)

# ──────────────────────────────────────────────────────────────
# DB 書き込み
# ──────────────────────────────────────────────────────────────

ef_long.to_sql("ef_from_freedocument", conn, if_exists="replace", index=False)
visit_summary.to_sql("ef_visit_summary",     conn, if_exists="replace", index=False)

conn.commit()
conn.close()

print(f"DONE: ef_from_freedocument ({len(ef_long)} rows)")
print(f"DONE: ef_visit_summary     ({len(visit_summary)} rows)")