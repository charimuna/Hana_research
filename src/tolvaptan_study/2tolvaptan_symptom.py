"""
Hana Research: tolvaptan 導入理由の抽出; 否定語は考慮しない
=========================================
ind_date（サムスカ導入日）の 7日前 〜 30日後 のカルテから、
導入理由と考えられる記載（体重増加・浮腫・呼吸苦・水分貯留・利尿剤抵抗・
低Na血症・胸水・腹水）をキーワード検索で拾い、患者ごとに0/1化する。
 
Step2/Step3（併存疾患の抽出）と同じ設計:
  step4 : 候補カルテを抽出してCSV保存（1行 = 1カルテ×1理由）
  step5 : step4のCSVを読み込み、224人全員を母集団に患者単位の0/1表を作りCSV保存
 
複数の理由に該当する場合はすべて1にする(重複可)。
実行順序: step4 -> step5 (step5はstep4の出力に依存)
"""
 
import re
import sqlite3
from pathlib import Path
 
import pandas as pd
 
DB_PATH = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")
 
OUTPUT_DIR = Path("/Users/muna/Hana_research/src/tolvaptan_study/")
 
REASON_CANDIDATE_FILE = OUTPUT_DIR / "step4_reason_candidates.csv"
REASON_FLAG_FILE = OUTPUT_DIR / "step5_reason_flags.csv"
 
# 導入日の前後のウィンドウ（非対称: 導入前1週間 / 導入後1ヶ月）
DAYS_BEFORE = 7
DAYS_AFTER = 30
 
# --- 否定表現の設定 ---
# キーワードの直後 NEGATION_WINDOW 文字以内に否定語があれば、その出現は陰性(0)として扱う。
# 例: 「浮腫なし」「呼吸苦(-)」「浮腫（-）」はヒットしても陰性扱いになる。
# 一方「浮腫あり」「浮腫増悪」のように否定語が続かなければ陽性扱い。
NEGATION_WINDOW = 10
NEGATION_REGEX = re.compile(r"(なし|[\(（]\s*[-－]\s*[\)）])")
 
# 導入理由の検索パターン（Step2の疾患パターンと同じ書き方の正規表現）
REASON_PATTERNS = {
    "weight_gain": [
        r"体重増加",
        r"体重.{0,3}増加",
        r"体重.{0,3}増",
        r"体重上昇",
        r"WEIGHT GAIN",
    ],
    "edema": [
        r"浮腫",
        r"むくみ",
        r"下腿浮腫",
        r"EDEMA",
    ],
    "dyspnea": [
        r"呼吸苦",
        r"呼吸困難",
        r"息切れ",
        r"起座呼吸",
        r"労作時呼吸困難",
        r"\bDOE\b",
        r"DYSPNEA",
        r"ORTHOPNEA",
    ],
    "fluid_retention": [
        r"水分貯留",
        r"体液貯留",
        r"うっ血",
        r"肺うっ血",
        r"CONGESTION",
    ],
    "diuretic_resistance": [
        # 「利尿」+ 任意で「剤」/「薬」+ 抵抗・反応不良・効果不十分・効果不良・不良・無効
        # 例: 利尿抵抗/利尿剤抵抗/利尿薬抵抗、利尿反応不良/利尿剤反応不良/利尿薬反応不良、
        #     利尿効果不十分/利尿効果不良/利尿不良/利尿無効/利尿剤無効/利尿薬無効 など
        r"利尿(?:剤|薬)?(?:抵抗|反応不良|効果不十分|効果不良|不良|無効)",
        r"REFRACTORY",
        # 薬剤名を含む表現（増量・漸増・iv化/静注化 = 抵抗性を疑って対応した記載とみなす）
        r"ラシックス.{0,4}増量",
        r"ラシックス.{0,4}漸増",
        r"フロセミド.{0,4}増量",
        r"フロセミド.{0,4}漸増",
        r"ループ利尿薬.{0,4}増量",
        r"ループ利尿剤.{0,4}増量",
        r"利尿薬.{0,2}IV化",
        r"利尿剤.{0,2}IV化",
        r"利尿薬.{0,2}静注化",
        r"利尿剤.{0,2}静注化",
    ],
    "hyponatremia": [
        r"低Na血症",
        r"低ナトリウム血症",
        r"HYPONATREMIA",
    ],
    "pleural_effusion": [
        r"胸水",
        r"PLEURAL EFFUSION",
    ],
    "ascites": [
        r"腹水",
        r"ASCITES",
    ],
}
 
 
# ============================================================
# 否定表現を考慮したキーワード判定
# ============================================================
 
def _has_unnegated_match(text, combined_regex, negation_window=NEGATION_WINDOW):
    """
    text 内で combined_regex にヒットした箇所のうち、
    直後 negation_window 文字以内に否定語（なし／(-)／（-）等）が
    なければ True（陽性あり）を返す。
 
    例:「浮腫なし、呼吸苦あり」で edema を判定する場合、
    「浮腫」の直後に「なし」があるため浮腫はFalse(陰性)。
    同じ文の「呼吸苦」は直後に否定語がないため dyspnea はTrue(陽性)。
    """
    for m in combined_regex.finditer(text):
        window = text[m.end(): m.end() + negation_window]
        if not NEGATION_REGEX.search(window):
            return True
    return False
 
 
# ============================================================
# STEP 4: 導入前後のカルテから導入理由の候補を抽出
# ============================================================
 
def run_step4():
    print("\n" + "#" * 70)
    print("# STEP 4: TOLVAPTAN REASON CANDIDATES (IND_DATE -7d / +30d)")
    print("#" * 70)
 
    # ------------------------------------------------------------
    # 1. tolvaptan_study の対象患者を取得
    # ------------------------------------------------------------
 
    conn = sqlite3.connect(DB_PATH)
 
    tolvaptan = pd.read_sql_query(
        """
        SELECT
            Patient_ID,
            Study_ID,
            ind_date
        FROM tolvaptan_study
        WHERE ind_date IS NOT NULL
        """,
        conn
    )
 
    tolvaptan["Patient_ID"] = tolvaptan["Patient_ID"].astype(str)
    tolvaptan["Study_ID"] = tolvaptan["Study_ID"].astype(str)
    tolvaptan["ind_date"] = pd.to_datetime(tolvaptan["ind_date"], errors="coerce")
 
    print("=" * 70)
    print("tolvaptan_study")
    print("=" * 70)
    print(f"対象患者数: {len(tolvaptan):,}")
    print()
 
    # ------------------------------------------------------------
    # 2. 対象患者のカルテを取得
    # ------------------------------------------------------------
 
    patient_ids = tolvaptan["Patient_ID"].dropna().unique().tolist()
 
    placeholders = ",".join(["?"] * len(patient_ids))
 
    karte = pd.read_sql_query(
        f"""
        SELECT
            Patient_ID,
            Study_ID,
            visit_datetime,
            karte_text
        FROM karte
        WHERE Patient_ID IN ({placeholders})
          AND karte_text IS NOT NULL
          AND karte_text != ''
        """,
        conn,
        params=patient_ids
    )
 
    conn.close()
 
    karte["Patient_ID"] = karte["Patient_ID"].astype(str)
 
    print("=" * 70)
    print("カルテ")
    print("=" * 70)
    print(f"取得カルテ件数: {len(karte):,}")
    print()
 
    # ------------------------------------------------------------
    # 3. カルテ日付を作成
    # ------------------------------------------------------------
 
    karte["visit_date"] = pd.to_datetime(
        karte["visit_datetime"]
        .astype(str)
        .str.extract(r"^(\d{4}/\d{1,2}/\d{1,2})", expand=False),
        errors="coerce"
    )
 
    # ------------------------------------------------------------
    # 4. tolvaptan_study と結合
    # ------------------------------------------------------------
 
    tolvaptan_merge = (
        tolvaptan[["Patient_ID", "Study_ID", "ind_date"]]
        .rename(columns={"Study_ID": "Study_ID_tolva"})
    )
 
    karte = karte.merge(tolvaptan_merge, on="Patient_ID", how="inner")
 
    # ------------------------------------------------------------
    # 5. ind_date -7日 〜 +30日 のカルテだけ残す
    # ------------------------------------------------------------
 
    karte["days_from_ind"] = (karte["visit_date"] - karte["ind_date"]).dt.days
 
    karte_window = karte[
        karte["visit_date"].notna()
        & karte["ind_date"].notna()
        & (karte["days_from_ind"] >= -DAYS_BEFORE)
        & (karte["days_from_ind"] <= DAYS_AFTER)
    ].copy()
 
    print("=" * 70)
    print(f"ind_date -{DAYS_BEFORE}日 〜 +{DAYS_AFTER}日 のカルテ")
    print("=" * 70)
    print(f"件数: {len(karte_window):,}")
    print()
 
    # ------------------------------------------------------------
    # 6. カルテ本文を検索用に正規化
    # ------------------------------------------------------------
 
    karte_window["karte_text_search"] = karte_window["karte_text"].astype(str).str.upper()
 
    # ------------------------------------------------------------
    # 7. 導入理由の候補を抽出
    # ------------------------------------------------------------
 
    hits = []
 
    for reason, reason_patterns in REASON_PATTERNS.items():
 
        regex = re.compile("|".join(reason_patterns), flags=re.IGNORECASE)
 
        # 「なし」「(-)」等の否定語が直後に続く出現は除外し、
        # 陽性(否定語を伴わない)出現が1つでもある行だけをヒットとする
        mask = karte_window["karte_text_search"].apply(
            lambda text: _has_unnegated_match(text, regex)
        )
 
        tmp = karte_window.loc[
            mask,
            [
                "Study_ID_tolva",
                "Patient_ID",
                "ind_date",
                "visit_date",
                "days_from_ind",
                "visit_datetime",
                "karte_text",
            ]
        ].copy()
 
        tmp["reason"] = reason
 
        hits.append(tmp)
 
    hits = pd.concat(hits, ignore_index=True)
 
    hits = hits.rename(columns={"Study_ID_tolva": "Study_ID"})
 
    hits = hits[
        [
            "Study_ID",
            "Patient_ID",
            "ind_date",
            "reason",
            "visit_date",
            "days_from_ind",
            "visit_datetime",
            "karte_text",
        ]
    ]
 
    # ------------------------------------------------------------
    # 8. 候補カルテを保存
    # ------------------------------------------------------------
 
    hits.to_csv(REASON_CANDIDATE_FILE, index=False, encoding="utf-8-sig")
 
    # ------------------------------------------------------------
    # 9. 結果確認
    # ------------------------------------------------------------
 
    print("=" * 70)
    print("Step 4 完了")
    print("=" * 70)
 
    print("導入理由候補カルテ:")
    print(REASON_CANDIDATE_FILE)
    print()
 
    print("=" * 70)
    print("理由候補ヒット件数")
    print("=" * 70)
 
    print(hits["reason"].value_counts())
    print()
 
 
# ============================================================
# STEP 5: 224人全員を母集団として、導入理由を患者単位で0/1化
# ============================================================
 
def run_step5():
    print("\n" + "#" * 70)
    print("# STEP 5: FINAL REASON FLAGS (FROM STEP4 CSV)")
    print("#" * 70)
 
    reason_list = list(REASON_PATTERNS.keys())
 
    # ------------------------------------------------------------
    # 1. tolvaptan_studyから224人全員を取得
    # ------------------------------------------------------------
 
    conn = sqlite3.connect(DB_PATH)
 
    patients = pd.read_sql_query(
        """
        SELECT
            Patient_ID,
            Study_ID,
            ind_date
        FROM tolvaptan_study
        WHERE ind_date IS NOT NULL
        """,
        conn
    )
 
    conn.close()
 
    patients["Patient_ID"] = patients["Patient_ID"].astype(str)
    patients["Study_ID"] = patients["Study_ID"].astype(str)
 
    print("=" * 70)
    print("Step 5：母集団")
    print("=" * 70)
    print(f"tolvaptan_study対象患者数: {len(patients):,}")
    print()
 
    # ------------------------------------------------------------
    # 2. Step 4の候補を読み込む
    # ------------------------------------------------------------
 
    df = pd.read_csv(REASON_CANDIDATE_FILE)
 
    df["Study_ID"] = df["Study_ID"].astype(str)
    df["Patient_ID"] = df["Patient_ID"].astype(str)
 
    print("=" * 70)
    print("Step 4候補")
    print("=" * 70)
    print(f"候補カルテ件数: {len(df):,}")
    print(f"候補が存在する患者数: {df['Study_ID'].nunique():,}")
    print()
 
    # ------------------------------------------------------------
    # 3. 224人全員を母集団にする
    # ------------------------------------------------------------
 
    patient_info = patients.copy()
 
    # ------------------------------------------------------------
    # 4. 理由ごとに0/1化（複数該当はすべて1）
    # ------------------------------------------------------------
 
    for reason in reason_list:
        reason_patients = df.loc[df["reason"] == reason, "Study_ID"].dropna().unique()
        patient_info[reason] = patient_info["Study_ID"].isin(reason_patients).astype(int)
 
    # ------------------------------------------------------------
    # 5. 各理由の最初の候補記載日
    # ------------------------------------------------------------
 
    for reason in reason_list:
        first_date = (
            df.loc[df["reason"] == reason]
            .groupby("Study_ID")["visit_date"]
            .min()
            .rename(f"{reason}_first_date")
        )
 
        patient_info = patient_info.merge(first_date, on="Study_ID", how="left")
 
    # ------------------------------------------------------------
    # 6. 各理由の候補ヒット数
    # ------------------------------------------------------------
 
    for reason in reason_list:
        hit_count = (
            df.loc[df["reason"] == reason]
            .groupby("Study_ID")
            .size()
            .rename(f"{reason}_hit_count")
        )
 
        patient_info = patient_info.merge(hit_count, on="Study_ID", how="left")
 
        patient_info[f"{reason}_hit_count"] = (
            patient_info[f"{reason}_hit_count"].fillna(0).astype(int)
        )
 
    # ------------------------------------------------------------
    # 7. 該当理由数
    # ------------------------------------------------------------
 
    patient_info["reason_count"] = patient_info[reason_list].sum(axis=1)
 
    # ------------------------------------------------------------
    # 8. 保存
    # ------------------------------------------------------------
 
    patient_info.to_csv(REASON_FLAG_FILE, index=False, encoding="utf-8-sig")
 
    # ------------------------------------------------------------
    # 9. 結果確認
    # ------------------------------------------------------------
 
    print("=" * 70)
    print("Step 5 完了")
    print("=" * 70)
 
    print("出力ファイル:")
    print(REASON_FLAG_FILE)
    print()
 
    print(f"最終患者数: {len(patient_info):,}")
    print()
 
    print("理由別 該当人数")
    print("-" * 40)
 
    for reason in reason_list:
        n = patient_info[reason].sum()
        print(f"{reason:22s}: {n:3d} / {len(patient_info)}")
 
    print()
 
    print("8理由すべて0の患者（理由不明群）")
    print("-" * 40)
 
    none_count = (patient_info["reason_count"] == 0).sum()
    print(f"{none_count}人")
    print()
 
    print("該当理由数の分布")
    print("-" * 40)
    print(patient_info["reason_count"].value_counts().sort_index())
    print()
 
    print("先頭20例")
    print("-" * 40)
    print(
        patient_info[["Study_ID", "Patient_ID", "ind_date"] + reason_list + ["reason_count"]]
        .head(20)
        .to_string(index=False)
    )
 
 
# ============================================================
# メイン
# ============================================================
 
if __name__ == "__main__":
    run_step4()
    run_step5()
 
