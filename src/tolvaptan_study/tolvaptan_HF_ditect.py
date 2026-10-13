"""
Hana Research: 心不全(HF=1)患者の tolvaptan 導入前 30 日における「心不全増悪日」の同定
=====================================================================================
対象   : tolvaptan_study で HF = 1 かつ ind_date あり
期間   : ind_date-30日 〜 ind_date 当日(0日)  ※当日を含む
日付   : karte.visit_datetime の日付部分（例 '2015/10/1(木) 09:00' → 2015-10-01）
症状   : weight_gain(体重増加) / edema(浮腫の増悪・出現) / dyspnea(呼吸困難の出現・増悪)
         = 3症状のうち1つでも該当した日を「増悪日」とする
除外   : 否定（なし／無し／なく／認めず／(-) 等）、条件・指示文（増悪時／出現の際は連絡 等）、
         既往・疑い・リスク記載、リンパ／薬剤性／本態性／全周性の浮腫
         ※「著明」「あり」「認める」は増悪語に含めない（状態記載のため）

出力 (OUTPUT_DIR):
  hf_worsening_long.csv     : 長形式。1行 = 患者 × 日付 × 症状（該当日すべて）
  hf_worsening_patient.csv  : 患者単位。症状ごとの 0/1・初回日・最終日・該当日数
DB への書き込みはしない（CSV 確認後に別処理で反映する）。
"""

import re
import sqlite3
import unicodedata
from pathlib import Path

import pandas as pd

DB_PATH = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
OUTPUT_DIR = Path("/Users/muna/Hana_research/src/tolvaptan_study/")

LONG_FILE = OUTPUT_DIR / "hf_worsening_long.csv"
PATIENT_FILE = OUTPUT_DIR / "hf_worsening_patient.csv"

DAYS_BEFORE = 30      # ind_date - 30日 から
DAYS_AFTER = 0        # ind_date 当日(0日)まで含める
CONTEXT_CHARS = 25    # 根拠として保存する前後文字数
NEGATION_WINDOW = 10  # 増悪語の直後に否定・条件語を探す文字数

# ============================================================
# 検索パターン（検討済みの最終版）
# ============================================================

GAP = r"[^、。,;；\r\n]{0,10}"  # 読点・句点・改行（\r\n）をまたがない
WORSEN = (r"(?:出現|増悪|悪化|増強|増加|↑|強まっ|強くな|ひどく|増え|進行"
          r"|拡大|広が|目立ってき|出てき|出た|新規)")
EDEMA = r"(?:(?<!リンパ)(?<!リンパ性)(?<!薬剤性)(?<!本態性)(?<!全周性)浮腫|むくみ)"
DYSP = r"(?:呼吸苦|呼吸困難|息切れ|息苦し[いさ]?|起座呼吸)"

NEGATION_REGEX = re.compile(
    r"(なし|無し|なく|無く|認めず|認めない|否定|ない|\(\s*-\s*\))")
CONDITION_REGEX = re.compile(
    r"(時|場合|あれば|ようなら|際|連絡|ください|下さい|有無|確認|チェック"
    r"|リスク|注意|既往|疑い|可能性|恐れ|おそれ)")

REASON_PATTERNS = {
    "weight_gain": [
        rf"体重{GAP}(?:増加|増|上昇)",
        r"WEIGHT GAIN",
        rf"(?<![A-Z])BW{GAP}(?:増加|増|UP|↑)",
    ],
    "edema": [
        rf"{EDEMA}{GAP}{WORSEN}",
        rf"(?:新た|新規)に?{GAP}{EDEMA}",
        r"(?:WORSENING|NEW)\s+EDEMA",
        r"EDEMA.{0,10}(?:WORSEN|PROGRESS|INCREAS)",
    ],
    "dyspnea": [
        rf"{DYSP}{GAP}{WORSEN}",
        rf"(?<![A-Z])DOE(?![A-Z]){GAP}{WORSEN}",
        rf"(?:新たに|新規){GAP}{DYSP}",
        r"(?:WORSENING|NEW\s+ONSET|NEW)\s+(?:DYSPNEA|ORTHOPNEA)",
    ],
}
SYMPTOMS = list(REASON_PATTERNS.keys())
SYMPTOM_REGEX = {
    s: re.compile("|".join(p), flags=re.IGNORECASE)
    for s, p in REASON_PATTERNS.items()
}


def find_positive_hits(text, regex, window=NEGATION_WINDOW):
    """否定・条件・既往を除いた陽性出現の前後文脈を list で返す（空なら陰性）。"""
    hits = []
    for m in regex.finditer(text):
        tail = re.split(r"[、。\r\n]", text[m.end(): m.end() + window])[0]
        if (NEGATION_REGEX.search(m.group(0))     # 症状語と増悪語の間の否定
                or NEGATION_REGEX.search(tail)    # 直後の否定
                or CONDITION_REGEX.search(tail)):  # 条件・指示・既往・疑い
            continue
        s = max(0, m.start() - CONTEXT_CHARS)
        e = min(len(text), m.end() + CONTEXT_CHARS)
        hits.append(text[s:e].replace("\r", " ").replace("\n", " "))
    return hits


def parse_date(series):
    """'2024/3/5' '2024-03-05 10:30' '20240305' 等から日付(Timestamp)を作る。読めなければ NaT。"""
    p = series.astype(str).str.extract(r"(\d{4})\D?(\d{1,2})\D?(\d{1,2})")
    return pd.to_datetime(
        {"year": pd.to_numeric(p[0]), "month": pd.to_numeric(p[1]),
         "day": pd.to_numeric(p[2])},
        errors="coerce",
    )


def run():
    print("#" * 70)
    print(f"# HF 増悪日の同定（ind_date-{DAYS_BEFORE}日 〜 +{DAYS_AFTER}日）")
    print("#" * 70)

    # ------------------------------------------------------------
    # 1. 対象患者: HF = 1 かつ ind_date あり
    # ------------------------------------------------------------
    conn = sqlite3.connect(DB_PATH)

    n_hf_all = pd.read_sql_query(
        "SELECT COUNT(*) AS n FROM tolvaptan_study WHERE CAST(HF AS INTEGER) = 1",
        conn)["n"].iloc[0]

    patients = pd.read_sql_query(
        """
        SELECT Patient_ID, Study_ID, ind_date
        FROM tolvaptan_study
        WHERE CAST(HF AS INTEGER) = 1
          AND ind_date IS NOT NULL
        """,
        conn,
    )
    patients["Patient_ID"] = patients["Patient_ID"].astype(str)
    patients["Study_ID"] = patients["Study_ID"].astype(str)
    patients["ind_date"] = pd.to_datetime(patients["ind_date"], errors="coerce")
    patients = patients[patients["ind_date"].notna()].copy()

    print(f"HF=1 の患者数            : {n_hf_all:,}")
    print(f"うち ind_date あり(対象) : {len(patients):,}")
    dup = patients["Patient_ID"].duplicated().sum()
    if dup:
        print(f"[警告] Patient_ID が重複する行: {dup} → カルテが重複して結合される。要確認")

    # ------------------------------------------------------------
    # 2. 対象患者のカルテ取得
    # ------------------------------------------------------------
    ids = patients["Patient_ID"].unique().tolist()
    frames = []
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        ph = ",".join(["?"] * len(chunk))
        frames.append(pd.read_sql_query(
            f"""
            SELECT Patient_ID, visit_datetime, karte_text
            FROM karte
            WHERE Patient_ID IN ({ph})
              AND karte_text IS NOT NULL AND karte_text != ''
            """,
            conn, params=chunk))
    conn.close()
    karte = pd.concat(frames, ignore_index=True)
    karte["Patient_ID"] = karte["Patient_ID"].astype(str)
    print(f"取得カルテ件数            : {len(karte):,}")

    # ------------------------------------------------------------
    # 3. 日付: visit_datetime の日付部分（例 '2015/10/1(木) 09:00'）
    # ------------------------------------------------------------
    karte["event_date"] = parse_date(karte["visit_datetime"])
    n_nat = karte["event_date"].isna().sum()
    print(f"日付なし(除外)            : {n_nat:,}")
    print("visit_datetime の例(確認用):", karte["visit_datetime"].dropna().astype(str).head(3).tolist())
    if n_nat:
        print("  読めなかった例:", karte.loc[karte["event_date"].isna(), "visit_datetime"].astype(str).head(3).tolist())

    # ------------------------------------------------------------
    # 4. 導入前30日(〜当日)のカルテに絞る
    # ------------------------------------------------------------
    karte = karte.merge(patients, on="Patient_ID", how="inner")
    karte = karte[karte["event_date"].notna()].copy()
    karte["days_from_ind"] = (karte["event_date"] - karte["ind_date"]).dt.days
    kw = karte[(karte["days_from_ind"] >= -DAYS_BEFORE)
               & (karte["days_from_ind"] <= DAYS_AFTER)].copy()
    print(f"期間内カルテ件数          : {len(kw):,}")

    # ------------------------------------------------------------
    # 5. 症状の判定（カルテ単位）
    # ------------------------------------------------------------
    kw["text_norm"] = kw["karte_text"].astype(str).map(
        lambda s: unicodedata.normalize("NFKC", s).upper())

    rows = []
    for r in kw.itertuples(index=False):
        for sym in SYMPTOMS:
            ctx = find_positive_hits(r.text_norm, SYMPTOM_REGEX[sym])
            if ctx:
                rows.append((r.Study_ID, r.Patient_ID, r.ind_date, r.event_date,
                             r.days_from_ind, sym, len(ctx), ctx[0]))
    cols = ["Study_ID", "Patient_ID", "ind_date", "event_date", "days_from_ind",
            "symptom", "n_matches", "evidence"]
    hits = pd.DataFrame(rows, columns=cols)

    # ------------------------------------------------------------
    # 6. 長形式: 患者 × 日付 × 症状（該当日すべて）
    # ------------------------------------------------------------
    key = ["Study_ID", "Patient_ID", "ind_date", "event_date", "days_from_ind", "symptom"]
    if len(hits):
        long_df = (hits.groupby(key, as_index=False)
                   .agg(n_karte=("evidence", "size"),
                        n_matches=("n_matches", "sum"),
                        evidence=("evidence", "first")))
        long_df.insert(6, "flag", 1)
        long_df["n_symptoms_that_day"] = (
            long_df.groupby(["Study_ID", "event_date"])["symptom"].transform("nunique"))
        long_df = long_df.sort_values(["Study_ID", "event_date", "symptom"])
    else:
        long_df = pd.DataFrame(columns=key + ["flag", "n_karte", "n_matches",
                                              "evidence",
                                              "n_symptoms_that_day"])

    long_df.to_csv(LONG_FILE, index=False, encoding="utf-8-sig",
                   date_format="%Y-%m-%d")

    # ------------------------------------------------------------
    # 7. 患者単位: 症状ごと 0/1・初回日・最終日・該当日数
    # ------------------------------------------------------------
    out = patients[["Study_ID", "Patient_ID", "ind_date"]].copy()
    n_k = kw.groupby("Study_ID").size().rename("n_karte_window")
    out = out.merge(n_k, on="Study_ID", how="left")
    out["n_karte_window"] = out["n_karte_window"].fillna(0).astype(int)

    for sym in SYMPTOMS:
        sub = long_df[long_df["symptom"] == sym]
        g = sub.groupby("Study_ID")["event_date"]
        agg = pd.DataFrame({f"{sym}_first_date": g.min(),
                            f"{sym}_last_date": g.max(),
                            f"{sym}_n_days": g.nunique()})
        out = out.merge(agg, on="Study_ID", how="left")
        out[f"{sym}_n_days"] = out[f"{sym}_n_days"].fillna(0).astype(int)
        out.insert(out.columns.get_loc(f"{sym}_first_date"), sym,
                   (out[f"{sym}_n_days"] > 0).astype(int))

    gany = long_df.groupby("Study_ID")["event_date"]
    any_df = pd.DataFrame({"hf_worsen_first_date": gany.min(),
                           "hf_worsen_last_date": gany.max(),
                           "hf_worsen_n_days": gany.nunique()})
    out = out.merge(any_df, on="Study_ID", how="left")
    out["hf_worsen_n_days"] = out["hf_worsen_n_days"].fillna(0).astype(int)
    out.insert(out.columns.get_loc("hf_worsen_first_date"), "hf_worsen_any",
               (out["hf_worsen_n_days"] > 0).astype(int))
    out["hf_worsen_last_days_before_ind"] = (
        out["ind_date"] - out["hf_worsen_last_date"]).dt.days

    out.to_csv(PATIENT_FILE, index=False, encoding="utf-8-sig",
               date_format="%Y-%m-%d")

    # ------------------------------------------------------------
    # 8. 結果確認
    # ------------------------------------------------------------
    print("\n" + "=" * 70)
    print("完了")
    print("=" * 70)
    print(f"長形式 : {LONG_FILE}  ({len(long_df):,} 行)")
    print(f"患者別 : {PATIENT_FILE}  ({len(out):,} 人)")
    print(f"\n期間内カルテが0件の患者: {(out['n_karte_window'] == 0).sum()} 人（増悪なしと区別して扱う）")
    print("\n症状別 該当人数 / 対象")
    for sym in SYMPTOMS:
        print(f"  {sym:12s}: {out[sym].sum():3d} / {len(out)}")
    print(f"  {'いずれか1つ':10s}: {out['hf_worsen_any'].sum():3d} / {len(out)}")
    print("\n増悪日数の分布（患者あたり）")
    print(out["hf_worsen_n_days"].value_counts().sort_index())


if __name__ == "__main__":
    run()