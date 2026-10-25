# %% [markdown]
# # 搬送例の特定と搬送日の推定（GGI改訂 手順2）
# - 母集団: 3_flowchart_patients.csv の flow_group == '6 解析対象'
# - 保存先: /Users/muna/Hana_research/src/unexpected_study/claude_made/transport_cases.py
# - L : 死亡日以前の最終記録日（医師記録＋文書）
# - K : L の7日前〜L の医師記録・文書で「搬送」「救急要請」「入院」を含む最新の日
# - R : K から5日以内の診療情報提供料の算定日（ORCA）
# - K_nurse : L の7日前〜死亡日の訪問看護記録で同じ語を含む最新の日（参考・目視用）
# - R_any : L の7日前〜死亡日の診療情報提供料の最新の算定日（L より後の搬送の取りこぼし対策）
# - 候補: 病院死（death_place）、または death_place が NULL で K・K_nurse・R_any のいずれかがある例
# - est_transport_date: 確定は K、それ以外は K・K_nurse・R_any の最も遅い日（なければ L）。目視の優先順位づけ用
# 改訂 2026-10-23: R_any の窓を死亡日まで拡大、訪問看護記録の検索を追加、推定搬送日の集計を追加
# - 出力: transport_cases.csv（1行1患者）、transport_k_snippets.csv（目視用）、集計CSV

# %% [1] 設定
import re
import sqlite3
from pathlib import Path

import pandas as pd

DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
POP_CSV = "/Users/muna/Hana_research/data/output/flowchart_20261025/3_flowchart_patients.csv"
OUT_DIR = Path("/Users/muna/Hana_research/data/output/transport_20261023")
OUT_DIR.mkdir(parents=True, exist_ok=True)

POP_GROUP = "6 解析対象"
DOCTOR_TYPES = ["定期訪問", "緊急往診", "休日往診", "往診", "夜間往診", "深夜往診", "初診"]
DOC_TYPES = ["文書"]
REC_TYPES = DOCTOR_TYPES + DOC_TYPES
NURSE_TYPES = ["訪問看護"]
HOSP_PLACES = ["連携医療機関・当院", "連携以外の医療機関"]
KW = ["搬送", "救急要請", "入院"]
LOOKBACK_K = 7   # K を探す範囲（L から遡る日数）
R_WINDOW = 5     # K から R までの許容日数

# 病院名の抽出（略称・総称は目視で確認する）
HOSP_RE = re.compile(r"([一-龥ぁ-んァ-ヶーA-Za-zＡ-Ｚａ-ｚ0-9０-９]{1,20}病院)")
GENERIC = {"病院", "総合病院", "大学病院", "救急病院", "市民病院", "近医病院", "当該病院", "同病院", "前医病院"}

con = sqlite3.connect(DB)

# %% [2] 母集団
pop_all = pd.read_csv(POP_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
print("flow_group の内訳:")
print(pop_all["flow_group"].value_counts(dropna=False), "\n")

pop = pop_all[pop_all["flow_group"] == POP_GROUP].copy()
assert len(pop) > 0, f"flow_group == '{POP_GROUP}' の行がありません。値を確認してください。"
pop["Patient_ID"] = pop["Patient_ID"].str.strip()
pop["dd"] = pd.to_datetime(pop["dd"], format="%Y-%m-%d", errors="coerce")
print(f"解析対象: {len(pop)} 例（dd 欠損 {pop['dd'].isna().sum()} 例）")

con.execute("DROP TABLE IF EXISTS temp.pop_ids")
con.execute("CREATE TEMP TABLE pop_ids (Patient_ID TEXT PRIMARY KEY)")
con.executemany("INSERT INTO pop_ids VALUES (?)", [(x,) for x in pop["Patient_ID"].unique()])

# %% [3] 死亡場所・死亡コメント
bs = pd.read_sql(
    """
    SELECT CAST(b.Patient_ID AS TEXT) AS Patient_ID, b.death_place, b.death_comment
    FROM Background_summary b
    JOIN pop_ids p ON CAST(b.Patient_ID AS TEXT) = p.Patient_ID
    """,
    con,
)
dup = bs["Patient_ID"].duplicated(keep=False)
if dup.any():
    print(f"注意: Background_summary に重複 {bs.loc[dup, 'Patient_ID'].nunique()} 患者。death_place が入っている行を優先します。")
    bs = bs.sort_values("death_place", na_position="last").drop_duplicates("Patient_ID")
print(f"Background_summary と結合できた患者: {len(bs)} / {len(pop)}")

# %% [4] karte（医師記録＋文書＋訪問看護）
LOAD_TYPES = REC_TYPES + NURSE_TYPES
ph = ",".join("?" * len(LOAD_TYPES))
k = pd.read_sql(
    f"""
    SELECT CAST(k.Patient_ID AS TEXT) AS Patient_ID, k.record_no, k.visit_type,
           k.visit_datetime, k.karte_text
    FROM karte k
    JOIN pop_ids p ON CAST(k.Patient_ID AS TEXT) = p.Patient_ID
    WHERE k.visit_type IN ({ph})
    """,
    con,
    params=LOAD_TYPES,
)
print("visit_type の内訳（解析対象・医師記録＋文書＋訪問看護）:")
print(k["visit_type"].value_counts(), "\n")

m = k["visit_datetime"].str.extract(
    r"(\d{4})/(\d{1,2})/(\d{1,2})(?:\([^)]*\))?\s*(?:(\d{1,2}):(\d{2}))?"
)
k["vdate"] = pd.to_datetime(m[0] + "-" + m[1] + "-" + m[2], format="%Y-%m-%d", errors="coerce")
k["vdt"] = pd.to_datetime(
    m[0] + "-" + m[1] + "-" + m[2] + " " + m[3].fillna("") + ":" + m[4].fillna(""),
    format="%Y-%m-%d %H:%M",
    errors="coerce",
)
print(f"visit_datetime の日付が読めない記録: {k['vdate'].isna().sum()} 件")
k["text"] = k["karte_text"].fillna("")

# %% [5] L（最終記録日）と K（搬送記載）
k = k.merge(pop[["Patient_ID", "dd"]], on="Patient_ID", how="inner")
k = k[k["vdate"].notna() & k["dd"].notna() & (k["vdate"] <= k["dd"])].copy()

kn = k[k["visit_type"].isin(NURSE_TYPES)].copy()   # 訪問看護
k = k[k["visit_type"].isin(REC_TYPES)].copy()      # 医師記録＋文書

L = k.groupby("Patient_ID")["vdate"].max().rename("L_date")
k = k.join(L, on="Patient_ID")

# 訪問看護: L の7日前〜死亡日（L より後の記録も含める）
kn = kn.join(L, on="Patient_ID")
kn = kn[kn["L_date"].notna()
        & (kn["vdate"] >= kn["L_date"] - pd.Timedelta(days=LOOKBACK_K))
        & (kn["vdate"] <= kn["dd"])].copy()
hit_n = kn[kn["text"].str.contains("|".join(KW), regex=True)].copy()
hit_n["kw"] = hit_n["text"].apply(lambda t: "/".join(w for w in KW if w in t))
if len(hit_n):
    _last = hit_n.groupby("Patient_ID")["vdate"].transform("max")
    _hn = hit_n[hit_n["vdate"] == _last]
    Kn = _hn.groupby("Patient_ID").agg(
        K_nurse_date=("vdate", "max"),
        K_nurse_kw=("kw", lambda s: "/".join(sorted({w for x in s for w in x.split("/")}, key=KW.index))),
    ).reset_index()
else:
    Kn = pd.DataFrame(columns=["Patient_ID", "K_nurse_date", "K_nurse_kw"])

win = k[(k["vdate"] >= k["L_date"] - pd.Timedelta(days=LOOKBACK_K)) & (k["vdate"] <= k["L_date"])].copy()
hit = win[win["text"].str.contains("|".join(KW), regex=True)].copy()
hit["kw"] = hit["text"].apply(lambda t: "/".join(w for w in KW if w in t))


def names_in(texts):
    s = {n for t in texts for n in HOSP_RE.findall(t)}
    return sorted(s - GENERIC)


def agg_k(g):
    kd = g["vdate"].max()
    gd = g[g["vdate"] == kd]
    kws = sorted({w for s in gd["kw"] for w in s.split("/")}, key=KW.index)
    hansou = g.loc[g["text"].str.contains("搬送"), "vdate"]
    return pd.Series(
        {
            "K_date": kd,
            "K_time": gd["vdt"].max(),
            "K_kw": "/".join(kws),
            "K_hansou_date": hansou.max() if len(hansou) else pd.NaT,
            "K_hit_days": g["vdate"].nunique(),
            "K_dest_hosp": "/".join(names_in(gd["text"])),
        }
    )


Kdf = hit.groupby("Patient_ID").apply(agg_k).reset_index() if len(hit) else pd.DataFrame(
    columns=["Patient_ID", "K_date", "K_time", "K_kw", "K_hansou_date", "K_hit_days", "K_dest_hosp"]
)

# 目視用スニペット（キーワード前後40字）
rows = []
for src, df_hit in [("医師・文書", hit), ("訪問看護", hit_n)]:
    for r in df_hit.itertuples():
        for w in KW:
            for mm in re.finditer(w, r.text):
                s = r.text[max(0, mm.start() - 40): mm.end() + 40].replace("\r", " ").replace("\n", " ")
                rows.append([r.Patient_ID, src, r.vdate.date(), r.visit_datetime, r.visit_type, r.record_no, w, s])
snip = pd.DataFrame(rows, columns=["Patient_ID", "source", "date", "visit_datetime", "visit_type", "record_no", "kw", "snippet"])

# %% [6] R（ORCA 診療情報提供料）
orca = pd.read_sql(
    """
    SELECT CAST(o.Patient_ID AS TEXT) AS Patient_ID, o.service_date, o.detail_name
    FROM orca_receipt_details o
    JOIN pop_ids p ON CAST(o.Patient_ID AS TEXT) = p.Patient_ID
    WHERE o.detail_name LIKE '%診療情報提供%' OR o.official_detail_name LIKE '%診療情報提供%'
    """,
    con,
)
orca["rdate"] = pd.to_datetime(orca["service_date"], format="%Y-%m-%d", errors="coerce")
print(f"診療情報提供料の算定: {len(orca)} 件 / {orca['Patient_ID'].nunique()} 患者（日付不明 {orca['rdate'].isna().sum()} 件）")

base = (
    pop[["Patient_ID", "dd"]]
    .merge(bs, on="Patient_ID", how="left")
    .merge(L.reset_index(), on="Patient_ID", how="left")
    .merge(Kdf, on="Patient_ID", how="left")
    .merge(Kn, on="Patient_ID", how="left")
)

rr = orca[["Patient_ID", "rdate"]].dropna().merge(base[["Patient_ID", "K_date", "L_date", "dd"]], on="Patient_ID")
r1 = (
    rr[rr["K_date"].notna()
       & (rr["rdate"] >= rr["K_date"])
       & (rr["rdate"] <= rr["K_date"] + pd.Timedelta(days=R_WINDOW))]
    .groupby("Patient_ID")["rdate"].min().rename("R_date")
)
r2 = (
    rr[rr["L_date"].notna()
       & (rr["rdate"] >= rr["L_date"] - pd.Timedelta(days=LOOKBACK_K))
       & (rr["rdate"] <= rr["dd"])]
    .groupby("Patient_ID")["rdate"].max().rename("R_any_date")
)
base = base.merge(r1.reset_index(), on="Patient_ID", how="left").merge(r2.reset_index(), on="Patient_ID", how="left")
base["R_after_L"] = base["R_any_date"] > base["L_date"]

# %% [7] 候補の抽出と分類
base["hosp_death"] = base["death_place"].isin(HOSP_PLACES)
base["place_null"] = base["death_place"].isna()
cand = base[
    base["hosp_death"]
    | (base["place_null"] & (base["K_date"].notna() | base["K_nurse_date"].notna() | base["R_any_date"].notna()))
].copy()


def classify(r):
    if pd.notna(r.K_date) and pd.notna(r.R_date):
        return "確定"
    if pd.notna(r.K_date):
        return "要目視_Kのみ"
    if pd.notna(r.K_nurse_date):
        return "要目視_看護Kのみ"
    if pd.notna(r.R_any_date):
        return "要目視_Rのみ"
    return "要目視_根拠なし"


cand["class"] = cand.apply(classify, axis=1)
cand["transport_date"] = cand["K_date"].where(cand["class"] == "確定")
cand["days_transport_to_death"] = (cand["dd"] - cand["transport_date"]).dt.days
cand["days_L_to_death"] = (cand["dd"] - cand["L_date"]).dt.days

# 推定搬送日（目視の優先順位づけ用）
_alt = cand[["K_date", "K_nurse_date", "R_any_date"]].max(axis=1)
cand["est_transport_date"] = cand["transport_date"].fillna(_alt).fillna(cand["L_date"])
cand["days_est_to_death"] = (cand["dd"] - cand["est_transport_date"]).dt.days

BINS = [-1, 0, 1, 7, 30, float("inf")]
LABELS = ["当日", "翌日", "2-7日", "8-30日", "31日以上"]
cand["days_bin"] = pd.cut(cand["days_transport_to_death"], bins=BINS, labels=LABELS)
cand["days_bin_L"] = pd.cut(cand["days_L_to_death"], bins=BINS, labels=LABELS)
cand["days_bin_est"] = pd.cut(cand["days_est_to_death"], bins=BINS, labels=LABELS)
neg = (cand["days_transport_to_death"] < 0).sum()
if neg:
    print(f"注意: 搬送日が死亡日より後の例 {neg} 例")

# %% [8] 転院の判定（death_comment の病院名 vs 搬送記載の病院名）
cand["death_hosp"] = cand["death_comment"].fillna("").apply(lambda t: "/".join(names_in([t])))


def transfer_flag(r):
    a = [x for x in str(r.K_dest_hosp or "").split("/") if x and x != "nan"]
    b = [x for x in str(r.death_hosp or "").split("/") if x and x != "nan"]
    if not a or not b:
        return "判定不能"
    return "一致" if any(x in y or y in x for x in a for y in b) else "不一致"


cand["transfer_flag"] = cand.apply(transfer_flag, axis=1)

# %% [9] 出力と集計
cols = [
    "Patient_ID", "dd", "death_place", "death_comment", "death_hosp",
    "L_date", "K_date", "K_time", "K_kw", "K_hansou_date", "K_hit_days", "K_dest_hosp",
    "K_nurse_date", "K_nurse_kw", "R_date", "R_any_date", "R_after_L", "class", "transport_date",
    "days_transport_to_death", "days_bin", "days_L_to_death", "days_bin_L",
    "est_transport_date", "days_est_to_death", "days_bin_est", "transfer_flag",
]
cand[cols].sort_values(["class", "Patient_ID"]).to_csv(OUT_DIR / "transport_cases.csv", index=False, encoding="utf-8-sig")
snip[snip["Patient_ID"].isin(cand["Patient_ID"])].to_csv(
    OUT_DIR / "transport_k_snippets.csv", index=False, encoding="utf-8-sig"
)

s_place = base["death_place"].value_counts(dropna=False).rename("n")
s_class = cand["class"].value_counts().rename("n")
s_days = pd.crosstab(cand["days_bin"], cand["transfer_flag"], margins=True, dropna=False)
s_days_L = pd.crosstab(cand["days_bin_L"], cand["class"], margins=True, dropna=False)
s_days_est = pd.crosstab(cand["days_bin_est"], cand["class"], margins=True, dropna=False)
s_after = pd.crosstab(cand["days_bin_L"], cand["R_after_L"].fillna(False), margins=True, dropna=False)

s_place.to_csv(OUT_DIR / "summary_death_place.csv", encoding="utf-8-sig")
s_class.to_csv(OUT_DIR / "summary_class.csv", encoding="utf-8-sig")
s_days.to_csv(OUT_DIR / "summary_days_by_transfer_confirmed.csv", encoding="utf-8-sig")
s_days_L.to_csv(OUT_DIR / "summary_daysL_by_class.csv", encoding="utf-8-sig")
s_days_est.to_csv(OUT_DIR / "summary_daysEst_by_class.csv", encoding="utf-8-sig")
s_after.to_csv(OUT_DIR / "summary_daysL_by_R_after_L.csv", encoding="utf-8-sig")

print("\n死亡場所（解析対象）:\n", s_place)
print("\n搬送候補の分類:\n", s_class)
print("\n搬送から死亡までの日数 × 転院フラグ（確定例のみ）:\n", s_days)
print("\n最終記録日から死亡までの日数 × 分類（全候補、参考）:\n", s_days_L)
print("\n推定搬送日から死亡までの日数 × 分類（目視範囲の決定用）:\n", s_days_est)
print("\n最終記録日から死亡までの日数 × L より後の算定あり:\n", s_after)

con.close()
