# %% [markdown]
# # Figure 1 フローチャート用の人数（CSVのみ）
# - 判定ロジックは cancer_recheck.py と同じものをこのファイル内で再実行する（2026-10-23）
# - 起点: Patient_Master（HMC導入患者）
# - 起点の前で除く（フローチャートには出さない）: 初診日が期間外、Background_summary で
#   facility=外来／facility に職員／特養（visit_place=特別養護老人ホーム、facility に特別養護・特養・介護老人福祉施設）／
#   visit_place=外来／visit_place が NULL／Background_summary に行なし
# - 判定順（起点以降）: 年齢 → 転帰（event.status）→ 2025-10-31より後の死亡 → advanced cancer
# - 転帰
#   死亡: status = 死亡 かつ 死亡日あり、または 死亡日あり
#   生存: status = 診療中
#   転帰不明: 終了／入院中／一時休止／待機／NULL／event に行なし／死亡だが死亡日なし
# - advanced cancer: 死亡前30日以内（死亡日−最終医師訪問日 > 30日の例は最終医師訪問前30日以内）の
#   医師カルテ病名欄（#行）に癌が記載された例。判定ルールは cancer_recheck.py の「1. 判定辞書とルール」と同一

# %%
import re
import sqlite3
import unicodedata
from datetime import date
from pathlib import Path

import pandas as pd

DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUT = Path("/Users/muna/Hana_research/data/output") / f"flowchart_{date.today():%Y%m%d}"
OUT.mkdir(parents=True, exist_ok=True)

FV_START, FV_END = pd.Timestamp("2015-03-01"), pd.Timestamp("2025-03-31")
DEATH_END = pd.Timestamp("2025-10-31")
AGE_MIN = 20
WINDOW_DAYS = 30
NURSE_VISIT_TYPE = "訪問看護"
ANCHOR_VISIT_TYPES = {"定期訪問", "緊急往診", "休日往診", "往診", "夜間往診", "深夜往診", "初診"}
ALIVE_STATUS = {"診療中"}
DEAD_STATUS = "死亡"

# %% [markdown]
# ## 1. 癌判定ルール（cancer_recheck.py と同一）

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
    "SELECT CAST(Patient_ID AS TEXT) AS Patient_ID, First_Visit_Date, Age_at_Visit FROM Patient_Master",
    con,
)
bs = pd.read_sql(
    "SELECT CAST(Patient_ID AS TEXT) AS Patient_ID, visit_place, facility FROM Background_summary", con
)
ev = pd.read_sql(
    "SELECT CAST(Patient_ID AS TEXT) AS Patient_ID, status, death_date FROM event", con
)
for df in (pm, ev, bs):
    df["Patient_ID"] = df["Patient_ID"].str.strip().str.replace(r"\.0$", "", regex=True)
pm["fv"] = pd.to_datetime(pm["First_Visit_Date"], errors="coerce")
ev["dd"] = pd.to_datetime(ev["death_date"], errors="coerce")
print("Patient_Master:", len(pm), " 重複ID:", int(pm["Patient_ID"].duplicated().sum()))
ev = ev[ev["Patient_ID"].isin(set(pm["Patient_ID"]))]  # Patient_Master 外の行（ID 0, 1 等）は除く
print("event 重複ID（Patient_Master内）:", int(ev["Patient_ID"].duplicated().sum()))
print("death_date 変換失敗:", int((ev["dd"].isna() & ev["death_date"].notna()
                                  & (ev["death_date"].astype(str).str.strip() != "")).sum()))
assert not ev["Patient_ID"].duplicated().any(), "event に1患者複数行あり。集約方法を決めてから実行"

assert not bs["Patient_ID"].duplicated().any(), "Background_summary に1患者複数行あり"
pm = pm.merge(bs, on="Patient_ID", how="left", indicator="bs_merge")
pm["has_bs"] = pm["bs_merge"] == "both"
pm = pm.drop(columns="bs_merge")
pm = pm.merge(ev[["Patient_ID", "status", "dd"]], on="Patient_ID", how="left", indicator="ev_merge")
pm["has_event"] = pm["ev_merge"] == "both"
pm = pm.drop(columns="ev_merge")

# %% [markdown]
# ## 3. 癌以外の基準でフロー

# %%
def outcome(r):
    if pd.notna(r["dd"]):
        return "死亡"
    if not r["has_event"]:
        return "転帰不明: eventなし"
    st = r["status"]
    if st in ALIVE_STATUS:
        return "生存"
    if st == DEAD_STATUS:
        return "転帰不明: 死亡だが死亡日なし"
    if pd.isna(st):
        return "転帰不明: status NULL"
    return f"転帰不明: {st}"


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


pm["outcome"] = pm.apply(outcome, axis=1)
pm["r_fv"] = ~pm["fv"].between(FV_START, FV_END)
pm["setting_excl"] = pm.apply(setting_excl, axis=1)
pm["r_setting"] = ~pm["r_fv"] & pm["setting_excl"].notna()
pm["r_age"] = (~pm["r_fv"] & ~pm["r_setting"]
               & (pm["Age_at_Visit"].isna() | (pm["Age_at_Visit"] < AGE_MIN)))
base = ~pm["r_fv"] & ~pm["r_setting"] & ~pm["r_age"]
pm["r_alive"] = base & (pm["outcome"] == "生存")
pm["r_unknown"] = base & pm["outcome"].str.startswith("転帰不明")
pm["r_late"] = base & (pm["outcome"] == "死亡") & (pm["dd"] > DEATH_END)
pm["deceased"] = base & (pm["outcome"] == "死亡") & (pm["dd"] <= DEATH_END)

# 死亡日があるのに status が死亡以外
print("死亡日あり・status≠死亡（死亡として扱う）:",
      int((pm["dd"].notna() & (pm["status"] != DEAD_STATUS)).sum()))

# %% [markdown]
# ## 4. 癌判定（死亡例のみ）

# %%
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
print("visit_datetime 変換失敗:", int(k_all["vdt"].isna().sum()))
k_all = k_all.merge(pm[["Patient_ID", "dd"]], on="Patient_ID")
k_all["days_before_death"] = (k_all["dd"].dt.normalize() - k_all["vdt"].dt.normalize()).dt.days

dec = pm[pm["deceased"]].copy()
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

k = doc.merge(dec[["Patient_ID", "anchor"]], on="Patient_ID")
k["days_before_anchor"] = (k["anchor"] - k["vdt"].dt.normalize()).dt.days
k = k[k["days_before_anchor"].between(0, WINDOW_DAYS)]

rows = []
for r in k.itertuples(index=False):
    for line, block, pos in hash_lines(r.karte_text):
        res = classify_line(line, block, pos)
        if res:
            rows.append({"Patient_ID": r.Patient_ID, "category": res["category"],
                         "basis": res["basis"]})
hits = pd.DataFrame(rows, columns=["Patient_ID", "category", "basis"])
hits["rank"] = hits["category"].map(RANK)
pt = hits.groupby("Patient_ID", as_index=False)["rank"].max()
pt["cancer_cat"] = pt["rank"].map({v: k_ for k_, v in RANK.items()})
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
dec["advanced_cancer"] = (dec["cancer_cat"] == "確定").astype(int)

# %% [markdown]
# ## 5. フローチャート人数

# %%
pm = pm.merge(dec[["Patient_ID", "cancer_window", "cancer_cat", "cancer_basis", "manual_override", "manual_cancer", "advanced_cancer"]],
              on="Patient_ID", how="left")
pm["advanced_cancer"] = pm["advanced_cancer"].fillna(0).astype(int)


def flow_group(r):
    if r["r_fv"]:
        return "0 対象外: 初診日が期間外"
    if r["r_setting"]:
        return "0 対象外: " + r["setting_excl"]
    if r["r_age"]:
        return "1 除外: 20歳未満/年齢欠測"
    if r["r_alive"]:
        return "2 除外: 生存"
    if r["r_unknown"]:
        return "3 除外: 転帰不明"
    if r["r_late"]:
        return "4 除外: 2025-10-31より後の死亡"
    if r["advanced_cancer"] == 1:
        return "5 除外: advanced cancer"
    return "6 解析対象"


pm["flow_group"] = pm.apply(flow_group, axis=1)

n_hmc = int((~pm["r_fv"] & ~pm["r_setting"]).sum())
n_age = int(pm["r_age"].sum())
n_alive = int(pm["r_alive"].sum())
n_unk = int(pm["r_unknown"].sum())
n_late = int(pm["r_late"].sum())
n_dec = int(pm["deceased"].sum())
n_ac = int((pm["deceased"] & (pm["advanced_cancer"] == 1)).sum())
n_ana = n_dec - n_ac
assert n_hmc - n_age - n_alive - n_unk - n_late == n_dec

fl = [
    ("box", "HMC導入（2015-03-01〜2025-03-31）", "Patients who received physician-led HMC", n_hmc),
    ("excl", "除外 計", "Excluded", n_age + n_alive + n_unk + n_late),
    ("excl", "  生存", "Remained alive", n_alive),
    ("excl", "  転帰不明", "Lost to follow-up or unknown outcome", n_unk),
    ("excl", "  2025-10-31より後の死亡", "Died after 31 October 2025", n_late),
    ("excl", "  20歳未満/年齢欠測", "Aged < 20 years or missing age", n_age),
    ("box", "死亡例", "Deceased patients", n_dec),
    ("excl", "除外: advanced cancer", "Advanced cancer", n_ac),
    ("box", "解析対象", "Noncancer patients included in the analysis", n_ana),
]
counts = pd.DataFrame(fl, columns=["type", "item_ja", "item_en", "n"])
print(counts.to_string(index=False))

# 内訳（参考）
pre = pd.DataFrame([("初診日が期間内", int((~pm["r_fv"]).sum()))]
                   + list(pm.loc[pm["r_setting"], "setting_excl"].value_counts().items())
                   + [("起点（HMC導入）", n_hmc)], columns=["item", "n"])
pre.insert(0, "section", "起点の前で除いた例（フローチャートに出さない）")
unk = (pm.loc[pm["r_unknown"], "outcome"].value_counts().rename_axis("item").reset_index(name="n"))
unk.insert(0, "section", "転帰不明の内訳")
ac = (dec.loc[dec["advanced_cancer"] == 1, "cancer_window"].value_counts()
      .rename_axis("item").reset_index(name="n"))
ac.insert(0, "section", "advanced cancer の窓")
basis_mt = dec.loc[dec["advanced_cancer"] == 1, "cancer_basis"].fillna("").str.contains("末期")
ac2 = pd.DataFrame({"section": "advanced cancer の根拠", "item": ["末期を含む", "末期以外のみ"],
                    "n": [int(basis_mt.sum()), int((~basis_mt).sum())]})
ana_win = (dec.loc[dec["advanced_cancer"] == 0, "cancer_window"].value_counts()
           .rename_axis("item").reset_index(name="n"))
ana_win.insert(0, "section", "解析対象の窓（最終医師カルテ起点＝死亡まで30日超）")
detail = pd.concat([pre, unk, ac, ac2, ana_win], ignore_index=True)
print(detail.to_string(index=False))

# %% [markdown]
# ## 6. 出力

# %%
counts.to_csv(OUT / "1_flowchart_counts.csv", index=False, encoding="utf-8-sig")
detail.to_csv(OUT / "2_flowchart_detail.csv", index=False, encoding="utf-8-sig")
pm[["Patient_ID", "First_Visit_Date", "Age_at_Visit", "visit_place", "facility", "status", "dd", "outcome", "flow_group",
    "cancer_window", "cancer_cat", "cancer_basis", "manual_override", "manual_cancer"]].sort_values("Patient_ID").to_csv(
    OUT / "3_flowchart_patients.csv", index=False, encoding="utf-8-sig")
print("出力先:", OUT)
