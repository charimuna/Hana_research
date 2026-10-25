# %% [markdown]
# # 母集団の再確認：karte「# 病名」による癌判定と新旧比較
# - 旧母集団: unex_study（1,281例）
# - 新母集団: Patient_Master を起点に
#   First_Visit_Date 2015-03-01〜2025-03-31 /
#   Background_summary で外来（facility/visit_place）・職員・特養・visit_place NULL・行なしを除外 /
#   Age_at_Visit ≥ 20 /
#   death_date あり かつ ≤ 2025-10-31 /
#   （当院 karte 最終記録から死亡まで30日超の例は除外しない。日数は参考表示のみ）/
#   karte で癌該当を除外
# - 癌判定: 医師カルテ（visit_type ≠ 訪問看護）の「#」〜改行の1行のみ、
#   死亡日の30日前〜死亡当日。ただし 死亡日−最終医師カルテ日 > 30日 の例は
#   最終医師カルテ日の30日前〜最終医師カルテ日（緩和ケア等へ送り出した後の死亡を拾うため）
# - 新母集団: 確定を除外（2026-10-23 要確認区分を廃止し、新母集団は1通り）

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
LOST_DAYS = 30            # 参考表示用: 死亡日 − karte最終記録日 > 30日
LOST_EXCLUDE = False      # True にすると 30日超を除外（2026-10-23 除外しない方針に変更）
LCD_MISSING_EXCLUDE = False  # 死亡日以前の karte が1件もない死亡例を除外するか（件数は必ず表示）
NURSE_VISIT_TYPE = "訪問看護"
# 最終医師カルテ日（窓の起点）の計算に使う visit_type（完全一致）。#行検索の対象は変えない
ANCHOR_VISIT_TYPES = {"定期訪問", "緊急往診", "休日往診", "往診", "夜間往診", "深夜往診", "初診"}

# %% [markdown]
# ## 1. 判定辞書とルール
# 1行ごとに次の順で判定し、患者単位では 確定 > 非該当 の最上位を採用する（2026-10-23 改訂2）。
# 1. 癌の語なし → 対象外（「ガングリオン」は癌の語から除く）
# 2. 疑い・既往・寛解・s/o・r/o・? → 非該当（除外語）。ただし除外語より後ろの区切り（、,・/（））以降に、
#    除外語を含まない区間で進行語があれば確定（除外語後の進行語）。再発なし等があれば非該当のまま
# 3. 「末期」あり → 確定（末期）
# 4. その#行、またはその#行から次の#行までの本文に「再発なし」「予後良好」等 → 非該当（再発なし/予後良好）
# 5. 術後系の語あり・進行語なし → 非該当（術後のみ）
# 6. 術後系の語あり・進行語あり → 確定（術後+進行）
# 7. 癌の語＋進行語 → 確定（癌+進行）
# 8. 癌の語のみ:
#    - 癌の語がすべて前立腺癌／高分化型肉腫／有棘細胞癌 → 非該当（前立腺癌・良性扱い単独）
#    - その#行がカルテ記録内で3番目以降の#行（癌以外の病名の#行も数える） → 非該当（3番目以降の#・癌のみ）
#    - それ以外 → 確定（癌のみ）
# 患者単位の判定のあと、MANUAL_NONCANCER の ID は非該当に上書きする。
# 「転移なし」「転移(-)」等は進行語「転移」として扱わない。

# %%
CANCER_TERMS = r"癌|がん|ガン|悪性腫瘍|悪性新生物|肉腫|白血病|リンパ腫|骨髄腫"
CANCER = re.compile(CANCER_TERMS)
NOT_CANCER = re.compile(r"ガングリオン")
PROSTATE = re.compile(r"前立腺\s*(?:癌|がん|ガン|悪性腫瘍|悪性新生物)")
BENIGN_LIKE = re.compile(r"高分化型?\s*(?:脂肪)?\s*肉腫|有棘細胞\s*癌")  # 癌のみの行で単独なら非該当
EXCLUDE = re.compile(r"疑|既往|寛解|s/o|r/o|\?", re.IGNORECASE)
# 除外語の後ろの区切り以降に進行語があれば確定にするための区切り記号（NFKC後）
DELIM = re.compile(r"[、,・/()（）]")
EXCLUDE_SEG = re.compile(r"疑|既往|寛解|[sr]_o_|\?", re.IGNORECASE)
TERMINAL = re.compile(r"末期")
GOOD_PROG = re.compile(
    r"再発\s*(?:なし|無し|無|ない|認めず|認めない|みられず|所見なし|\(\s*-\s*\)|-|陰性)|無再発|予後良好"
)
POSTOP = re.compile(
    r"術後|治療後|療法後|切除後|TUR(?:[-\s]*BT|[-\s]*P)?\s*後|ESD\s*後|EMR\s*後|ope\s*後|OP\s*後|s/p"
    r"|摘出|全摘|郭清|摘除",
    re.IGNORECASE,
)
METS_NEG = re.compile(r"転移\s*(なし|無し|無|ない|\(\s*-\s*\)|-|陰性)")
PROGRESS = re.compile(
    r"再発|播種|転移|浸潤|進行|緩和|bsc|stage\s*(iv|4)|ステージ\s*(iv|4)",
    re.IGNORECASE,
)
HASH_LINE = re.compile(r"^\s*#(.*)$")  # NFKC後は全角＃も#になる
HASH_POS_CUTOFF = 3  # 記録内でこの番目以降の#行は、癌のみなら非該当

# 手動で非がんとする例（ID: 理由）
MANUAL_NONCANCER = {
    "170278": "癌あるも予後良好",
    "180076": "術後",
    "180102": "再発なし",
}

# 手動で癌末期（除外）とする例（ID: 理由）。搬送例の目視（2026-10-25）で確認
MANUAL_CANCER = {
    "150144": "目視でがん末期（搬送例の再評価）",
    "170054": "目視で肺がん末期（搬送例の再評価）",
    "230361": "目視でがん末期・緩和入院（搬送例の再評価）",
    "230284": "目視でがん末期・緩和予定入院（搬送例の再評価）",
    "160236": "目視でがん末期（境界例の再評価）",
    "230156": "目視でがん末期・緩和入院（境界例の再評価）",
    "240169": "目視でがん末期・緩和入院（境界例の再評価）",
    "230241": "目視でがん末期・同日再入院（長期ギャップ例の再評価）",
}
assert not set(MANUAL_CANCER) & set(MANUAL_NONCANCER), "MANUAL_CANCER と MANUAL_NONCANCER に同じIDがある"

RANK = {"確定": 2, "非該当": 1}


def norm(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def classify_line(line: str, block: str = "", pos: int = 1):
    """#行を判定。block はその#行の次の行から次の#行の前まで、pos は記録内の#行の順番（1始まり）。
    癌の語がなければ None。"""
    t = NOT_CANCER.sub("", norm(line))
    m_c = CANCER.findall(t)
    if not m_c:
        return None
    b = norm(block or "")
    t_prog = METS_NEG.sub("", GOOD_PROG.sub("", t))
    prog = sorted({m.group(0) for m in PROGRESS.finditer(t_prog)})
    excl = sorted(set(EXCLUDE.findall(t)))
    good = sorted({m.group(0) for m in GOOD_PROG.finditer(t)} | {m.group(0) for m in GOOD_PROG.finditer(b)})
    postop = bool(POSTOP.search(t))
    # 除外語より後ろの区切り以降で、除外語を含まない区間に進行語があるか
    rescue = []
    if excl:
        # s/o・r/o の「/」で区切らないよう退避してから分割
        tt = re.sub(r"([sr])/o", r"\1_o_", t, flags=re.IGNORECASE)
        segs = DELIM.split(tt)
        first_ex = next(i for i, sg in enumerate(segs) if EXCLUDE_SEG.search(sg))
        for sg in segs[first_ex + 1:]:
            if EXCLUDE_SEG.search(sg):
                continue
            sg_p = METS_NEG.sub("", GOOD_PROG.sub("", sg))
            rescue += [m.group(0) for m in PROGRESS.finditer(sg_p)]
    rescue = sorted(set(rescue))
    n_special = len(PROSTATE.findall(t)) + len(BENIGN_LIKE.findall(t))
    special_only = n_special >= len(m_c)
    if excl and rescue and not good:
        cat, basis = "確定", "除外語後の進行語"
    elif excl:
        cat, basis = "非該当", "除外語"
    elif TERMINAL.search(t):
        cat, basis = "確定", "末期"
    elif good:
        cat, basis = "非該当", "再発なし/予後良好"
    elif postop:
        cat, basis = ("確定", "術後+進行") if prog else ("非該当", "術後のみ")
    elif prog:
        cat, basis = "確定", "癌+進行"
    elif special_only:
        cat, basis = "非該当", "前立腺癌・良性扱い単独"
    elif pos >= HASH_POS_CUTOFF:
        cat, basis = "非該当", "3番目以降の#・癌のみ"
    else:
        cat, basis = "確定", "癌のみ"
    return {
        "category": cat,
        "basis": basis,
        "hash_pos": pos,
        "cancer_terms": "|".join(sorted(set(m_c))),
        "exclude_terms": "|".join(excl),
        "rescue_progress_terms": "|".join(rescue),
        "good_prog_terms": "|".join(good),
        "progress_terms": "|".join(prog),
        "terminal": int(bool(TERMINAL.search(t))),
        "postop": int(postop),
        "special_only": int(special_only),
        "mets_negated": int(bool(METS_NEG.search(t))),
    }


def hash_lines(text: str):
    """#行ごとに (行, その#行から次の#行までの本文, 記録内の順番) を返す"""
    if not isinstance(text, str):
        return []
    raws = re.split(r"\r\n|\r|\n", text)
    idx = [i for i, raw in enumerate(raws) if HASH_LINE.match(norm(raw))]
    out = []
    for n, i in enumerate(idx):
        j = idx[n + 1] if n + 1 < len(idx) else len(raws)
        out.append((raws[i].strip(), "\n".join(raws[i + 1:j]), n + 1))
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
bs = pd.read_sql(
    "SELECT CAST(Patient_ID AS TEXT) AS Patient_ID, visit_place, facility FROM Background_summary", con
)
old = pd.read_sql(
    "SELECT DISTINCT CAST(Patient_ID AS TEXT) AS Patient_ID FROM unex_study", con
)

for df in (pm, ev, old, bs):
    df["Patient_ID"] = df["Patient_ID"].str.strip().str.replace(r"\.0$", "", regex=True)

assert not bs["Patient_ID"].duplicated().any(), "Background_summary に1患者複数行あり"
pm = pm.merge(bs, on="Patient_ID", how="left", indicator="bs_merge")
pm["has_bs"] = pm["bs_merge"] == "both"
pm = pm.drop(columns="bs_merge")
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
def setting_excl(r):
    """起点の前で除く理由（なければ None）。上から先に当たったものを採用"""
    fac = r["facility"] if isinstance(r["facility"], str) else ""
    vp = r["visit_place"]
    if not r["has_bs"]:
        return "Background_summaryに行なし"
    if fac.strip() == "外来":
        return "外来（facility）"
    if pd.isna(vp):
        return "visit_place NULL"
    if vp == "外来":
        return "外来（visit_place）"
    if "職員" in fac:
        return "職員"
    if vp == "特別養護老人ホーム" or re.search(r"特別養護|特養|介護老人福祉施設", fac):
        return "特養"
    return None


pm["r_fv"] = ~pm["fv"].between(FV_START, FV_END)
pm["setting_excl"] = pm.apply(setting_excl, axis=1)
pm["r_setting"] = ~pm["r_fv"] & pm["setting_excl"].notna()
pre = ~pm["r_fv"] & ~pm["r_setting"]
pm["r_age"] = pre & (pm["Age_at_Visit"].isna() | (pm["Age_at_Visit"] < AGE_MIN))
pm["r_alive"] = pre & ~pm["r_age"] & pm["dd"].isna()
pm["r_late"] = pre & ~pm["r_age"] & pm["dd"].notna() & (pm["dd"] > DEATH_END)
pm["deceased"] = ~(pm["r_fv"] | pm["r_setting"] | pm["r_age"] | pm["r_alive"] | pm["r_late"])

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
pm["over_lost"] = pm["deceased"] & (pm["gap_days"] > LOST_DAYS)
pm["r_lost"] = pm["over_lost"] & LOST_EXCLUDE
pm["r_lcd_na"] = pm["deceased"] & pm["lcd"].isna() & LCD_MISSING_EXCLUDE
print("死亡例:", int(pm["deceased"].sum()))
print("  死亡日以前のkarteなし:", int((pm["deceased"] & pm["lcd"].isna()).sum()),
      "（除外する設定:" , LCD_MISSING_EXCLUDE, "）")
print("  最終記録から死亡まで30日超:", int(pm["over_lost"].sum()), "（除外する設定:", LOST_EXCLUDE, "）")
print("  gap_days 分布:\n", pm.loc[pm["deceased"], "gap_days"].describe())

dec = pm[pm["deceased"] & ~pm["r_lost"] & ~pm["r_lcd_na"]].copy()
print("癌判定前の解析候補:", len(dec))

# %% [markdown]
# ## 5. karte 抽出（医師カルテ、#行のみ）
# - 死亡日−最終医師カルテ日 ≤ 30日: 死亡日の30日前〜死亡当日（従来どおり）
# - 死亡日−最終医師カルテ日 > 30日: 最終医師カルテ日の30日前〜最終医師カルテ日
# - 最終医師カルテ日は ANCHOR_VISIT_TYPES（定期訪問・往診各種・初診）の最終記録。その他・なし・文書等は起点に使わない

# %%
doc = k_all[(k_all["visit_type"].fillna("") != NURSE_VISIT_TYPE)
            & (k_all["days_before_death"] >= 0)].copy()
ldd = (doc[doc["visit_type"].isin(ANCHOR_VISIT_TYPES)]
       .groupby("Patient_ID", as_index=False)["vdt"].max()
       .rename(columns={"vdt": "last_doc_dt"}))
dec = dec.merge(ldd, on="Patient_ID", how="left")
dec["gap_doc_days"] = (dec["dd"].dt.normalize() - dec["last_doc_dt"].dt.normalize()).dt.days
use_ldd = dec["gap_doc_days"] > WINDOW_DAYS
dec["cancer_window"] = use_ldd.map({True: "最終医師カルテ起点", False: "死亡日起点"})
dec["anchor"] = dec["dd"].dt.normalize().where(~use_ldd, dec["last_doc_dt"].dt.normalize())
print("癌判定の窓:\n", dec["cancer_window"].value_counts())
print("  最終医師カルテなし:", int(dec["last_doc_dt"].isna().sum()))

k = doc[doc["Patient_ID"].isin(set(dec["Patient_ID"]))].merge(
    dec[["Patient_ID", "anchor", "cancer_window"]], on="Patient_ID")
k["days_before_anchor"] = (k["anchor"] - k["vdt"].dt.normalize()).dt.days
k = k[k["days_before_anchor"].between(0, WINDOW_DAYS)].copy()

rows = []
for r in k.itertuples(index=False):
    for line, block, pos in hash_lines(r.karte_text):
        res = classify_line(line, block, pos)
        if res:
            rows.append({
                "Patient_ID": r.Patient_ID, "record_no": r.record_no,
                "visit_type": r.visit_type, "visit_datetime": r.visit_datetime,
                "cancer_window": r.cancer_window,
                "days_before_death": r.days_before_death,
                "days_before_anchor": r.days_before_anchor, "line": line, **res,
            })
hits = pd.DataFrame(rows, columns=[
    "Patient_ID", "record_no", "visit_type", "visit_datetime", "cancer_window",
    "days_before_death", "days_before_anchor",
    "line", "category", "basis", "hash_pos", "cancer_terms", "exclude_terms",
    "rescue_progress_terms", "good_prog_terms",
    "progress_terms", "terminal", "postop", "special_only", "mets_negated",
])
print("癌の語を含む#行:", len(hits), " 患者数:", hits["Patient_ID"].nunique())

# 患者単位
hits["rank"] = hits["category"].map(RANK)
pt = hits.groupby("Patient_ID", as_index=False)["rank"].max()
pt["cancer_cat"] = pt["rank"].map({v: k_ for k_, v in RANK.items()})
# 確定の根拠（確定行の basis を患者ごとに列挙）
bas = (hits[hits["category"] == "確定"].groupby("Patient_ID")["basis"]
       .agg(lambda s: "|".join(sorted(set(s)))).rename("cancer_basis").reset_index())
dec = dec.merge(pt[["Patient_ID", "cancer_cat"]], on="Patient_ID", how="left")
dec = dec.merge(bas, on="Patient_ID", how="left")
dec["cancer_cat"] = dec["cancer_cat"].fillna("該当行なし")
dec["manual_override"] = dec["Patient_ID"].map(MANUAL_NONCANCER)
dec.loc[dec["manual_override"].notna(), "cancer_cat"] = "非該当"
dec.loc[dec["manual_override"].notna(), "cancer_basis"] = None
dec["manual_cancer"] = dec["Patient_ID"].map(MANUAL_CANCER)
dec.loc[dec["manual_cancer"].notna(), "cancer_cat"] = "確定"
dec.loc[dec["manual_cancer"].notna(), "cancer_basis"] = "手動: " + dec.loc[dec["manual_cancer"].notna(), "manual_cancer"]
print("手動で癌末期に上書き:", int(dec["manual_cancer"].notna().sum()), "例",
      "（死亡例に見つからないID:", sorted(set(MANUAL_CANCER) - set(dec["Patient_ID"])), "）")
print("手動で非がんに上書き:", int(dec["manual_override"].notna().sum()), "例")
print(dec["cancer_cat"].value_counts())
print("確定行の根拠（行単位）:\n", hits.loc[hits["category"] == "確定", "basis"].value_counts())
print("非該当行の根拠（行単位）:\n", hits.loc[hits["category"] == "非該当", "basis"].value_counts())

# %% [markdown]
# ## 6. 新母集団と旧母集団の比較

# %%
new_a = set(dec.loc[dec["cancer_cat"] != "確定", "Patient_ID"])
old_s = set(old["Patient_ID"])
pm_s = set(pm["Patient_ID"])


def reason(pid):
    if pid not in pm_s:
        return "Patient_Masterに不在"
    r = pm.loc[pm["Patient_ID"] == pid].iloc[0]
    if r["r_fv"]:
        return "初診日が期間外"
    if r["r_setting"]:
        return "対象外: " + r["setting_excl"]
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


all_ids = sorted(old_s | new_a, key=lambda x: (len(x), x))
cmp_df = pd.DataFrame({"Patient_ID": all_ids})
cmp_df["in_old"] = cmp_df["Patient_ID"].isin(old_s).astype(int)
cmp_df["in_new"] = cmp_df["Patient_ID"].isin(new_a).astype(int)
changed = cmp_df[cmp_df["in_old"] != cmp_df["in_new"]].copy()
changed["reason_or_status"] = changed["Patient_ID"].map(reason)
changed = changed.merge(pm[["Patient_ID", "dd", "lcd", "gap_days", "dd_conflict"]], on="Patient_ID", how="left")
changed = changed.merge(dec[["Patient_ID", "last_doc_dt", "gap_doc_days", "cancer_window", "cancer_basis", "manual_override"]],
                        on="Patient_ID", how="left")

# 人数表
is_ldd = dec["cancer_window"] == "最終医師カルテ起点"
fl = [
    ("Patient_Master", len(pm)),
    ("除外: 初診日が期間外", int(pm["r_fv"].sum())),
    ("除外: 外来/職員/特養/visit_place NULL等", int(pm["r_setting"].sum())),
    ("除外: 20歳未満/年齢欠測", int(pm["r_age"].sum())),
    ("除外: 死亡日なし", int(pm["r_alive"].sum())),
    ("除外: 死亡日>2025-10-31", int(pm["r_late"].sum())),
    ("死亡例", int(pm["deceased"].sum())),
    ("除外: 当院最終記録から死亡まで30日超", int(pm["r_lost"].sum())),
    ("参考: 死亡例のうち当院最終記録から死亡まで30日超（除外設定にかかわらず）", int(pm["over_lost"].sum())),
    ("除外: 死亡日以前のkarteなし", int(pm["r_lcd_na"].sum())),
    ("参考: 死亡例のうち死亡日以前のkarteなし（除外設定にかかわらず）", int((pm["deceased"] & pm["lcd"].isna()).sum())),
    ("癌判定前の解析候補", len(dec)),
    ("  窓: 死亡日起点", int((~is_ldd).sum())),
    ("  窓: 最終医師カルテ起点（死亡まで30日超）", int(is_ldd.sum())),
    ("    うち確定", int((is_ldd & (dec["cancer_cat"] == "確定")).sum())),
    ("癌: 確定（除外）", int((dec["cancer_cat"] == "確定").sum())),
    ("  根拠に末期を含む", int(dec["cancer_basis"].fillna("").str.contains("末期").sum())),
    ("  根拠が末期以外のみ", int(((dec["cancer_cat"] == "確定")
                              & ~dec["cancer_basis"].fillna("").str.contains("末期")).sum())),
    ("  うち手動で非がんに上書き", int(dec["manual_override"].notna().sum())),
    ("癌: 非該当（疑い/既往/寛解/再発なし/術後のみ/前立腺癌・良性扱い単独/3番目以降の#）", int((dec["cancer_cat"] == "非該当").sum())),
    ("癌: 該当行なし", int((dec["cancer_cat"] == "該当行なし").sum())),
    ("新母集団（確定を除外）", len(new_a)),
    ("旧母集団（unex_study）", len(old_s)),
    ("旧のうちPatient_Masterに不在", len(old_s - pm_s)),
    ("旧から抜けた", len(old_s - new_a)),
    ("新たに増えた", len(new_a - old_s)),
    ("共通", len(old_s & new_a)),
]
counts = pd.DataFrame(fl, columns=["item", "n"])
print(counts.to_string(index=False))

# %% [markdown]
# ## 7. 出力

# %%
counts.to_csv(OUT / "1_counts_comparison.csv", index=False, encoding="utf-8-sig")
changed.to_csv(OUT / "2_changed_ids.csv", index=False, encoding="utf-8-sig")

# 目視用: 確定例の窓内の全#行（確定・非該当とも）。患者ごとの根拠を付ける
def_ids = set(dec.loc[dec["cancer_cat"] == "確定", "Patient_ID"])
review = hits[hits["Patient_ID"].isin(def_ids)].drop(columns="rank").copy()
review = review.merge(dec[["Patient_ID", "cancer_basis"]], on="Patient_ID", how="left")
review["in_old"] = review["Patient_ID"].isin(old_s).astype(int)
review = review.sort_values(["Patient_ID", "days_before_death"], ascending=[True, False])
review.to_csv(OUT / "3_review_definite.csv", index=False, encoding="utf-8-sig")

# advanced cancer（確定）の患者一覧: 1患者1行
d = hits[hits["category"] == "確定"].sort_values(["Patient_ID", "days_before_anchor"])
acl = d.groupby("Patient_ID").agg(
    n_definite_lines=("line", "size"),
    cancer_terms=("cancer_terms", lambda x: "|".join(sorted(set("|".join(x).split("|"))))),
    latest_line=("line", "first"),
    latest_visit=("visit_datetime", "first"),
    latest_days_before_anchor=("days_before_anchor", "first"),
).reset_index()
acl = dec.loc[dec["cancer_cat"] == "確定",
              ["Patient_ID", "dd", "last_doc_dt", "gap_doc_days", "cancer_window", "cancer_basis"]
              ].merge(acl, on="Patient_ID", how="left")
acl["has_terminal"] = acl["cancer_basis"].fillna("").str.contains("末期").astype(int)
acl["in_old"] = acl["Patient_ID"].isin(old_s).astype(int)
acl = acl.sort_values(["has_terminal", "cancer_basis", "Patient_ID"], ascending=[False, True, True])
acl.to_csv(OUT / "4_advanced_cancer_patients.csv", index=False, encoding="utf-8-sig")
print("advanced cancer 患者一覧:", len(acl), "例")
print(acl["cancer_basis"].value_counts().to_string())

# 監査用（非該当とした例の根拠行）
nonapp_ids = set(dec.loc[dec["cancer_cat"] == "非該当", "Patient_ID"])
hits[hits["Patient_ID"].isin(nonapp_ids)].drop(columns="rank").to_csv(
    OUT / "audit_hits_nonapplicable.csv", index=False, encoding="utf-8-sig"
)
print("出力先:", OUT)
