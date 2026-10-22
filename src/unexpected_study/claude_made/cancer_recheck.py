# %% [markdown]
# # 母集団の再確認：karte「# 病名」による癌判定と新旧比較
# - 旧母集団: unex_study（1,281例）
# - 新母集団: Patient_Master を起点に
#   First_Visit_Date 2015-03-01〜2025-03-31 / Age_at_Visit ≥ 20 /
#   death_date あり かつ ≤ 2025-10-31 /
#   当院 karte の最終記録（医師＋訪問看護、死亡日以前）から死亡まで30日超を除外（転院・転居等）/
#   karte で癌該当を除外
# - 癌判定: 医師カルテ（visit_type ≠ 訪問看護）の「#」〜改行の1行のみ、
#   死亡日の30日前〜死亡当日
# - 新母集団は2通り
#   A: 確定（癌＋末期）のみ除外
#   B: 確定＋要確認を除外

# %%
import re
import sqlite3
import unicodedata
from datetime import date
from pathlib import Path

import pandas as pd

DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUT = Path("/Users/muna/Hana_research/data/output") / f"cancer_recheck_{date.today():%Y%m%d}"
OUT.mkdir(parents=True, exist_ok=True)

FV_START, FV_END = pd.Timestamp("2015-03-01"), pd.Timestamp("2025-03-31")
DEATH_END = pd.Timestamp("2025-10-31")
AGE_MIN = 20
WINDOW_DAYS = 30
LOST_DAYS = 30            # 死亡日 − karte最終記録日 > 30日 で除外（30日ちょうどは残す）
LCD_MISSING_EXCLUDE = False  # 死亡日以前の karte が1件もない死亡例を除外するか（件数は必ず表示）
NURSE_VISIT_TYPE = "訪問看護"

# %% [markdown]
# ## 1. 判定辞書とルール
# 1行ごとに次の順で判定し、患者単位では 確定 > 要確認 > 非該当 の最上位を採用する。
# 1. 癌の語なし → 対象外
# 2. 疑い・既往・寛解 → 非該当
# 3. 「末期」あり → 確定
# 4. 術後・治療後・療法後・切除後 あり → 進行語あれば 要確認、なければ 非該当
# 5. それ以外（癌の語のみ、癌＋進行語） → 要確認
#
# 「転移なし」「転移(-)」等は進行語「転移」として扱わない。

# %%
CANCER = re.compile(r"癌|がん|ガン|悪性腫瘍|悪性新生物|肉腫|白血病|リンパ腫|骨髄腫")
EXCLUDE = re.compile(r"疑|既往|寛解|s/o|r/o|\?", re.IGNORECASE)
TERMINAL = re.compile(r"末期")
POSTOP = re.compile(r"術後|治療後|療法後|切除後")
METS_NEG = re.compile(r"転移\s*(なし|無し|無|ない|\(\s*-\s*\)|-|陰性)")
PROGRESS = re.compile(
    r"再発|播種|転移|浸潤|進行|緩和|bsc|stage\s*(iv|4)|ステージ\s*(iv|4)",
    re.IGNORECASE,
)
HASH_LINE = re.compile(r"^\s*#(.*)$")  # NFKC後は全角＃も#になる

RANK = {"確定": 3, "要確認": 2, "非該当": 1}


def norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def classify_line(line: str):
    """1行（#以降）を判定。癌の語がなければ None。"""
    t = norm(line)
    m_c = CANCER.findall(t)
    if not m_c:
        return None
    t_prog = METS_NEG.sub("", t)
    prog = sorted({m.group(0) for m in PROGRESS.finditer(t_prog)})
    excl = sorted(set(EXCLUDE.findall(t)))
    if excl:
        cat = "非該当"
    elif TERMINAL.search(t):
        cat = "確定"
    elif POSTOP.search(t):
        cat = "要確認" if prog else "非該当"
    else:
        cat = "要確認"
    return {
        "category": cat,
        "cancer_terms": "|".join(sorted(set(m_c))),
        "exclude_terms": "|".join(excl),
        "progress_terms": "|".join(prog),
        "terminal": int(bool(TERMINAL.search(t))),
        "postop": int(bool(POSTOP.search(t))),
        "mets_negated": int(bool(METS_NEG.search(t))),
    }


def hash_lines(text: str):
    if not isinstance(text, str):
        return []
    out = []
    for raw in re.split(r"\r\n|\r|\n", text):
        m = HASH_LINE.match(norm(raw))
        if m:
            out.append(raw.strip())
    return out


def parse_karte_dt(s):
    # 例: 2016/3/31(木) 17:40
    if not isinstance(s, str):
        return pd.NaT
    s = re.sub(r"\(.\)|（.）", " ", s)
    return pd.to_datetime(s.strip(), errors="coerce")


# %% [markdown]
# ## 2. データ読み込み

# %%
con = sqlite3.connect(DB)

pm = pd.read_sql(
    "SELECT CAST(Patient_ID AS TEXT) AS Patient_ID, First_Visit_Date, Age_at_Visit "
    "FROM Patient_Master",
    con,
)
ev = pd.read_sql(
    "SELECT CAST(Patient_ID AS TEXT) AS Patient_ID, death_date FROM event",
    con,
)
old = pd.read_sql(
    "SELECT DISTINCT CAST(Patient_ID AS TEXT) AS Patient_ID FROM unex_study", con
)

for df in (pm, ev, old):
    df["Patient_ID"] = df["Patient_ID"].str.strip().str.replace(r"\.0$", "", regex=True)

pm["fv"] = pd.to_datetime(pm["First_Visit_Date"], errors="coerce")
ev["dd"] = pd.to_datetime(ev["death_date"], errors="coerce")

print("Patient_Master:", len(pm), " 重複ID:", pm["Patient_ID"].duplicated().sum())
print("First_Visit_Date 変換失敗:", (pm["fv"].isna() & pm["First_Visit_Date"].notna()).sum())
print("death_date 変換失敗:", (ev["dd"].isna() & ev["death_date"].notna()).sum())
print("旧母集団 unex_study:", len(old))

# %% [markdown]
# ## 3. 死亡日（1患者1日に集約。不一致は別CSV）

# %%
ev_nn = ev.dropna(subset=["dd"]).drop_duplicates(["Patient_ID", "dd"])
dd_conflict = ev_nn[ev_nn["Patient_ID"].duplicated(keep=False)].sort_values(["Patient_ID", "dd"])
print("死亡日が患者内で不一致:", dd_conflict["Patient_ID"].nunique(), "例")
if len(dd_conflict):
    dd_conflict.to_csv(OUT / "check_death_date_conflict.csv", index=False, encoding="utf-8-sig")
death = ev_nn.sort_values("dd").groupby("Patient_ID", as_index=False)["dd"].last()

pm = pm.merge(death, on="Patient_ID", how="left")
pm["dd_conflict"] = pm["Patient_ID"].isin(dd_conflict["Patient_ID"]).astype(int)

# %% [markdown]
# ## 4. 癌以外の基準でフロー

# %%
pm["r_fv"] = ~pm["fv"].between(FV_START, FV_END)
pm["r_age"] = ~pm["r_fv"] & (pm["Age_at_Visit"].isna() | (pm["Age_at_Visit"] < AGE_MIN))
pm["r_alive"] = ~pm["r_fv"] & ~pm["r_age"] & pm["dd"].isna()
pm["r_late"] = ~pm["r_fv"] & ~pm["r_age"] & pm["dd"].notna() & (pm["dd"] > DEATH_END)
pm["deceased"] = ~(pm["r_fv"] | pm["r_age"] | pm["r_alive"] | pm["r_late"])

# karte（全記録：医師＋訪問看護）を死亡例について読み込み
ids = tuple(pm.loc[pm["deceased"], "Patient_ID"])
q = (
    "SELECT CAST(Patient_ID AS TEXT) AS Patient_ID, record_no, visit_type, "
    "visit_datetime, karte_text FROM karte "
    f"WHERE CAST(Patient_ID AS TEXT) IN ({','.join('?' * len(ids))})"
)
k_all = pd.read_sql(q, con, params=ids)
con.close()
k_all["Patient_ID"] = k_all["Patient_ID"].str.strip().str.replace(r"\.0$", "", regex=True)
k_all["vdt"] = k_all["visit_datetime"].map(parse_karte_dt)
print("visit_datetime 変換失敗:", k_all["vdt"].isna().sum())
print("visit_type 内訳:\n", k_all["visit_type"].value_counts(dropna=False))
k_all = k_all.merge(pm[["Patient_ID", "dd"]], on="Patient_ID")
k_all["days_before_death"] = (k_all["dd"].dt.normalize() - k_all["vdt"].dt.normalize()).dt.days
print("死亡日より後の karte 記録:", int((k_all["days_before_death"] < 0).sum()), "件（最終記録日の計算から除く）")

# 最終記録日（死亡日以前）
lcd = (k_all[k_all["days_before_death"] >= 0]
       .groupby("Patient_ID", as_index=False)["vdt"].max().rename(columns={"vdt": "lcd"}))
pm = pm.merge(lcd, on="Patient_ID", how="left")

# 最終記録から死亡まで30日超（転院・転居等）
pm["gap_days"] = (pm["dd"].dt.normalize() - pm["lcd"].dt.normalize()).dt.days
pm["r_lost"] = pm["deceased"] & (pm["gap_days"] > LOST_DAYS)
pm["r_lcd_na"] = pm["deceased"] & pm["lcd"].isna() & LCD_MISSING_EXCLUDE
print("死亡例:", int(pm["deceased"].sum()))
print("  死亡日以前のkarteなし:", int((pm["deceased"] & pm["lcd"].isna()).sum()),
      "（除外する設定:" , LCD_MISSING_EXCLUDE, "）")
print("  最終記録から死亡まで30日超:", int(pm["r_lost"].sum()))
print("  gap_days 分布:\n", pm.loc[pm["deceased"], "gap_days"].describe())

dec = pm[pm["deceased"] & ~pm["r_lost"] & ~pm["r_lcd_na"]].copy()
print("癌判定前の解析候補:", len(dec))

# %% [markdown]
# ## 5. karte 抽出（医師カルテ、死亡前30日〜当日、#行のみ）

# %%
k = k_all[k_all["Patient_ID"].isin(set(dec["Patient_ID"]))
          & (k_all["visit_type"].fillna("") != NURSE_VISIT_TYPE)
          & k_all["days_before_death"].between(0, WINDOW_DAYS)].copy()

rows = []
for r in k.itertuples(index=False):
    for line in hash_lines(r.karte_text):
        res = classify_line(line)
        if res:
            rows.append({
                "Patient_ID": r.Patient_ID, "record_no": r.record_no,
                "visit_type": r.visit_type, "visit_datetime": r.visit_datetime,
                "days_before_death": r.days_before_death, "line": line, **res,
            })
hits = pd.DataFrame(rows, columns=[
    "Patient_ID", "record_no", "visit_type", "visit_datetime", "days_before_death",
    "line", "category", "cancer_terms", "exclude_terms", "progress_terms",
    "terminal", "postop", "mets_negated",
])
print("癌の語を含む#行:", len(hits), " 患者数:", hits["Patient_ID"].nunique())

# 患者単位
hits["rank"] = hits["category"].map(RANK)
pt = hits.groupby("Patient_ID", as_index=False)["rank"].max()
pt["cancer_cat"] = pt["rank"].map({v: k_ for k_, v in RANK.items()})
dec = dec.merge(pt[["Patient_ID", "cancer_cat"]], on="Patient_ID", how="left")
dec["cancer_cat"] = dec["cancer_cat"].fillna("該当行なし")
print(dec["cancer_cat"].value_counts())

# %% [markdown]
# ## 6. 新母集団 A / B と旧母集団の比較

# %%
new_a = set(dec.loc[dec["cancer_cat"] != "確定", "Patient_ID"])
new_b = set(dec.loc[~dec["cancer_cat"].isin(["確定", "要確認"]), "Patient_ID"])
old_s = set(old["Patient_ID"])
pm_s = set(pm["Patient_ID"])


def reason(pid):
    if pid not in pm_s:
        return "Patient_Masterに不在"
    r = pm.loc[pm["Patient_ID"] == pid].iloc[0]
    if r["r_fv"]:
        return "初診日が期間外"
    if r["r_age"]:
        return "20歳未満/年齢欠測"
    if r["r_alive"]:
        return "死亡日なし"
    if r["r_late"]:
        return "死亡日>2025-10-31"
    if r["r_lost"]:
        return "最終記録から死亡まで30日超"
    if r["r_lcd_na"]:
        return "死亡日以前のkarteなし"
    return "癌:" + dec.loc[dec["Patient_ID"] == pid, "cancer_cat"].iloc[0]


all_ids = sorted(old_s | new_a | new_b, key=lambda x: (len(x), x))
cmp_df = pd.DataFrame({"Patient_ID": all_ids})
cmp_df["in_old"] = cmp_df["Patient_ID"].isin(old_s).astype(int)
cmp_df["in_new_A"] = cmp_df["Patient_ID"].isin(new_a).astype(int)
cmp_df["in_new_B"] = cmp_df["Patient_ID"].isin(new_b).astype(int)
changed = cmp_df[(cmp_df["in_old"] != cmp_df["in_new_A"]) | (cmp_df["in_old"] != cmp_df["in_new_B"])].copy()
changed["reason_or_status"] = changed["Patient_ID"].map(reason)
changed = changed.merge(pm[["Patient_ID", "dd", "lcd", "gap_days", "dd_conflict"]], on="Patient_ID", how="left")

# 人数表
fl = [
    ("Patient_Master", len(pm)),
    ("除外: 初診日が期間外", int(pm["r_fv"].sum())),
    ("除外: 20歳未満/年齢欠測", int(pm["r_age"].sum())),
    ("除外: 死亡日なし", int(pm["r_alive"].sum())),
    ("除外: 死亡日>2025-10-31", int(pm["r_late"].sum())),
    ("死亡例", int(pm["deceased"].sum())),
    ("除外: 当院最終記録から死亡まで30日超", int(pm["r_lost"].sum())),
    ("除外: 死亡日以前のkarteなし", int(pm["r_lcd_na"].sum())),
    ("参考: 死亡例のうち死亡日以前のkarteなし（除外設定にかかわらず）", int((pm["deceased"] & pm["lcd"].isna()).sum())),
    ("癌判定前の解析候補", len(dec)),
    ("癌: 確定", int((dec["cancer_cat"] == "確定").sum())),
    ("癌: 要確認", int((dec["cancer_cat"] == "要確認").sum())),
    ("癌: 非該当（疑い/既往/寛解/術後のみ）", int((dec["cancer_cat"] == "非該当").sum())),
    ("新母集団A（確定のみ除外）", len(new_a)),
    ("新母集団B（確定＋要確認を除外）", len(new_b)),
    ("旧母集団（unex_study）", len(old_s)),
    ("旧のうちPatient_Masterに不在", len(old_s - pm_s)),
    ("A: 旧から抜けた", len(old_s - new_a)),
    ("A: 新たに増えた", len(new_a - old_s)),
    ("A: 共通", len(old_s & new_a)),
    ("B: 旧から抜けた", len(old_s - new_b)),
    ("B: 新たに増えた", len(new_b - old_s)),
    ("B: 共通", len(old_s & new_b)),
]
counts = pd.DataFrame(fl, columns=["item", "n"])
print(counts.to_string(index=False))

# %% [markdown]
# ## 7. 出力

# %%
counts.to_csv(OUT / "1_counts_comparison.csv", index=False, encoding="utf-8-sig")
changed.to_csv(OUT / "2_changed_ids.csv", index=False, encoding="utf-8-sig")

suspect_ids = set(dec.loc[dec["cancer_cat"] == "要確認", "Patient_ID"])
review = hits[hits["Patient_ID"].isin(suspect_ids)].drop(columns="rank").copy()
review["in_old"] = review["Patient_ID"].isin(old_s).astype(int)
review = review.sort_values(["Patient_ID", "days_before_death"], ascending=[True, False])
review.to_csv(OUT / "3_review_suspect.csv", index=False, encoding="utf-8-sig")

# 監査用（確定・非該当の根拠行）
hits[hits["category"] != "要確認"].drop(columns="rank").to_csv(
    OUT / "audit_hits_definite_and_nonapplicable.csv", index=False, encoding="utf-8-sig"
)
print("出力先:", OUT)
