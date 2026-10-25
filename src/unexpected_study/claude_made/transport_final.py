# %% [markdown]
# # 搬送表の統合と index time の決定（GGI改訂 手順3-3）
# 定義（2026-10-25 確定）:
#   - 在宅医療から入院（救急・予定・外来経由・レスパイトを問わない）して死亡した例は「搬送あり」
#   - index time: 搬送ありは搬送（入院）日時、搬送なしは死亡日
#   - 搬送から死亡まで30日超は主解析から除外（exclude_cutoff30）
#   - 搬送の有無が不明な例は主解析から除外（exclude_transport_unknown）
# 搬送日の採用順:
#   1. 目視（review_transport_v1-2.xlsx, review_transport_2_manu01.xlsx）
#   2. 自動確定のうち K の語に「搬送」「救急要請」を含む例（③検証で 21/21 一致）
#   3. それ以外（未目視）は推定日を参考値とし、status で区別する
# 癌末期の除外は flowchart_counts.py の MANUAL_CANCER で行う（母集団 3_flowchart_patients.csv から外れる）
# 保存先: /Users/muna/Hana_research/src/unexpected_study/claude_made/transport_final.py

# %% [1] 設定
from pathlib import Path

import pandas as pd

OUT_DIR = Path("/Users/muna/Hana_research/data/output/transport_20261023")
POP_CSV = "/Users/muna/Hana_research/data/output/flowchart_20261025/3_flowchart_patients.csv"
POP_GROUP = "6 解析対象"
CASES_CSV = OUT_DIR / "transport_cases.csv"
REVIEW_FILES = [  # 置き場所に合わせて修正
    OUT_DIR / "review_transport_v1-2_manu01.xlsx",
    OUT_DIR / "review_transport_2_manu01.xlsx",
]
# 境界例の目視（記入済みファイルがあれば読み込む）
if (OUT_DIR / "review_transport_3_manu.xlsx").exists():
    REVIEW_FILES.append(OUT_DIR / "review_transport_3_manu.xlsx")
FINAL_CSV = OUT_DIR / "transport_final.csv"

CUTOFF = 30
BOUNDARY_MAX = 40   # 未目視の「入院」のみ確定例で、31〜40日は実際には30日以内の可能性がある

# 目視記入の上書き（2026-10-25 決定: 予定・外来経由・レスパイト入院も搬送あり）
OVERRIDE_TRANSPORT_1 = {
    "150007": "予定手術で入院",
    "190165": "レスパイト入院からそのまま死亡",
    "160222": "外来受診後に入院",
    "170327": "外来受診後に入院",
    "210091": "外来受診後に入院",
    "200295": "尿閉で入院（不明→1）",
}

# 当院の診療終了後に死亡した例（主解析から除外、2026-10-25 決定）
CARE_ENDED = {
    "210241": "訪問診療の変更（2022-09-09 最終診察）",
    # 以下、最終記録から死亡まで長期の例の目視（check_no_transport_gap _manu.xlsx、2026-10-25）
    "210010": "施設入所", "180170": "施設入所", "170292": "施設入所", "150025": "施設入所",
    "210432": "施設入所", "150082": "施設入所", "170310": "転居", "220404": "施設入所",
    "160276": "往診終了（2017-02）", "200372": "施設入所", "200237": "施設入所", "160218": "施設入所",
    "191588": "施設入所", "180240": "施設入所", "180175": "施設入所", "210383": "施設入所",
    "180067": "施設入所", "210138": "施設入所", "180352": "施設入所", "150050": "施設入所",
    "190145": "当院の診療終了（2020-10）",
}

# 最終記録から死亡まで長期の例の目視で、搬送（入院）が判明した例（2026-10-25）
# 先の目視（v1-2 等）より優先する。値: (搬送日, 搬送時刻 or None, メモ)
GAP_TRANSPORT = {
    "180090": ("2019-01-27", None, "同日搬送、入院"),
    "181041": ("2022-10-14", None, "同日搬送、入院"),
    "180297": ("2024-02-01", None, "搬送入院後、施設入所"),
    "160179": ("2017-06-14", None, "搬送後、転院"),
    "180295": ("2019-04-05", None, "搬送入院後、施設入所"),
    "190339": ("2022-01-07", None, "搬送入院"),
    "200175": ("2020-10-05", None, "搬送入院後、施設入所"),
    "160269": ("2016-12-27", None, "搬送入院"),
    "190130": ("2022-11-14", None, "搬送入院"),
    "240348": ("2025-04-13", "04:00", "病院搬送"),
    "170069": ("2018-07-27", None, "骨折で病院搬送（先の目視 0 を変更）"),
    "220210": ("2024-11-30", "11:00", "病院搬送"),
    "190210": ("2024-07-03", "19:00", "病院搬送"),
    "230241": ("2023-09-25", None, "同日再入院（がん末、MANUAL_CANCER で除外予定）"),
    "160013": ("2022-11-17", None, "同日入院"),
    "160301": ("2017-04-28", None, "同日入院（先の目視 0 を変更）"),
    "181063": ("2020-09-23", None, "同日より入院（先の目視 0 を変更）"),
    "180059": ("2019-10-09", None, "搬送入院、その後施設"),
    "170281": ("2020-07-22", None, "搬送入院、その後施設"),
}
assert not set(CARE_ENDED) & set(GAP_TRANSPORT), "CARE_ENDED と GAP_TRANSPORT に同じIDがある"

# 目視記入の日付補完（2026-10-25）
OVERRIDE_TRANSPORT_DATE = {
    "230284": "2024-09-10",  # 緩和予定入院の入院日（癌末期で母集団から除外予定）
}

# %% [2] 読み込み
pop = pd.read_csv(POP_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
pop = pop[pop["flow_group"] == POP_GROUP][["Patient_ID", "dd"]].copy()
pop["Patient_ID"] = pop["Patient_ID"].str.strip()
pop["dd"] = pd.to_datetime(pop["dd"], format="%Y-%m-%d", errors="coerce")
print(f"解析対象: {len(pop)} 例")

cases = pd.read_csv(CASES_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
for c in ["K_date", "K_time", "est_transport_date"]:
    cases[c] = pd.to_datetime(cases[c], errors="coerce")
# transport_cases.py の自動判定列は名前を変えて残す（この後で最終値を作り直すため）
cases = cases.rename(columns={"transport_date": "auto_transport_date",
                              "days_transport_to_death": "auto_days_transport_to_death"})

revs = []
for f in REVIEW_FILES:
    r = pd.read_excel(f, dtype=str)
    r["review_file"] = f.name
    revs.append(r[["Patient_ID", "review_set", "rev_transport", "rev_transport_date",
                   "rev_transport_time", "rev_memo", "review_file"]])
rev = pd.concat(revs, ignore_index=True)
rev["Patient_ID"] = rev["Patient_ID"].str.strip()
dup = rev["Patient_ID"].duplicated(keep=False)
assert not dup.any(), f"目視ファイル間で重複: {sorted(rev.loc[dup, 'Patient_ID'].unique())}"
rev["rev_transport"] = rev["rev_transport"].str.strip()

# 上書き
for pid, why in OVERRIDE_TRANSPORT_1.items():
    m = rev["Patient_ID"] == pid
    assert m.sum() == 1, f"上書き対象 {pid} が目視ファイルにない"
    rev.loc[m, "override_note"] = f"{rev.loc[m, 'rev_transport'].iloc[0]}→1: {why}"
    rev.loc[m, "rev_transport"] = "1"
for pid, d in OVERRIDE_TRANSPORT_DATE.items():
    m = rev["Patient_ID"] == pid
    assert m.sum() == 1, f"日付補完の対象 {pid} が目視ファイルにない"
    rev.loc[m, "override_note"] = rev.loc[m, "override_note"].fillna("") + f"日付補完 {d}"
    rev.loc[m, "rev_transport_date"] = d
print(f"目視: {len(rev)} 例（上書き {len(OVERRIDE_TRANSPORT_1)} 例）")
print(rev["rev_transport"].value_counts(dropna=False), "\n")

add = []
for pid, (d_, t_, memo) in GAP_TRANSPORT.items():
    m = rev["Patient_ID"] == pid
    note = f"長期ギャップ目視: {memo}"
    if m.any():
        rev.loc[m, "override_note"] = rev.loc[m, "override_note"].fillna("") + note
        rev.loc[m, ["rev_transport", "rev_transport_date", "rev_transport_time", "rev_memo"]] = ["1", d_, t_, memo]
    else:
        add.append({"Patient_ID": pid, "review_set": "⑥長期ギャップ_目視", "rev_transport": "1",
                    "rev_transport_date": d_, "rev_transport_time": t_, "rev_memo": memo,
                    "review_file": "check_no_transport_gap _manu.xlsx", "override_note": note})
rev = pd.concat([rev, pd.DataFrame(add)], ignore_index=True)
rev["rev_transport_time"] = rev["rev_transport_time"].where(
    ~rev["rev_transport_time"].fillna("").str.fullmatch(r"\d{1,2}:\d{2}"), rev["rev_transport_time"] + ":00")
print(f"長期ギャップ目視で搬送あり: {len(GAP_TRANSPORT)} 例（新規 {len(add)} 例）")
rev["rev_date"] = pd.to_datetime(rev["rev_transport_date"], format="mixed", errors="coerce")
rev["rev_time"] = pd.to_datetime(rev["rev_transport_time"], format="%H:%M:%S", errors="coerce").dt.time

# %% [3] 患者ごとに搬送の有無と搬送日を決める
df = pop.merge(cases.drop(columns=["dd"]), on="Patient_ID", how="left").merge(rev, on="Patient_ID", how="left")
df["is_candidate"] = df["class"].notna()
kw = df["K_kw"].fillna("")
auto_ok = (df["class"] == "確定") & kw.str.contains("搬送|救急要請", regex=True)


def decide(r):
    # 戻り値: (transport, transport_date, transport_time, source, status)
    if pd.notna(r.rev_transport):
        if r.rev_transport == "1":
            if pd.isna(r.rev_date):
                return (1, pd.NaT, None, "目視", "要修正_日付なし")
            return (1, r.rev_date, r.rev_time, "目視", "確定")
        if r.rev_transport == "0":
            return (0, pd.NaT, None, "目視", "確定")
        return (pd.NA, pd.NaT, None, "目視", "搬送不明")
    if not r.is_candidate:
        return (0, pd.NaT, None, "候補外", "確定")
    if r._auto_ok:
        t = r.K_time.time() if pd.notna(r.K_time) else None
        return (1, r.K_date, t, "自動_搬送語", "確定")
    return (1, r.est_transport_date, None, "推定_未目視", "未目視")


df["_auto_ok"] = auto_ok
res = df.apply(decide, axis=1, result_type="expand")
res.columns = ["transport", "transport_date", "transport_time", "transport_source", "transport_status"]
df = pd.concat([df, res], axis=1)
df["transport_date"] = pd.to_datetime(df["transport_date"])
df["days_transport_to_death"] = (df["dd"] - df["transport_date"]).dt.days

# %% [4] index time と除外フラグ
is_tr = df["transport"] == 1
df["index_type"] = "死亡"
df.loc[is_tr, "index_type"] = "搬送"
df["index_date"] = df["dd"]
df.loc[is_tr, "index_date"] = df.loc[is_tr, "transport_date"]
df["index_time"] = None
df.loc[is_tr, "index_time"] = df.loc[is_tr, "transport_time"]
df["index_time_source"] = "なし（死亡時刻は手順5で付与）"
df.loc[is_tr & (df["transport_source"] == "目視") & df["transport_time"].notna(), "index_time_source"] = "目視の搬送時刻"
df.loc[is_tr & (df["transport_source"] == "自動_搬送語") & df["transport_time"].notna(), "index_time_source"] = "搬送記載カルテの記録時刻"
df.loc[is_tr & df["transport_time"].isna(), "index_time_source"] = "なし（日単位）"

df["exclude_transport_unknown"] = df["transport_status"] == "搬送不明"
df["exclude_cutoff30"] = is_tr & (df["days_transport_to_death"] > CUTOFF)
df["flag_boundary_unreviewed"] = (
    (df["transport_status"] == "未目視")
    & df["days_transport_to_death"].between(CUTOFF + 1, BOUNDARY_MAX)
)
df["flag_date_error"] = (df["transport_status"] == "要修正_日付なし") | (df["days_transport_to_death"] < 0)
df["care_ended_note"] = df["Patient_ID"].map(CARE_ENDED)
df["exclude_care_ended"] = df["care_ended_note"].notna()
missing = sorted(set(CARE_ENDED) - set(df["Patient_ID"]))
if missing:
    print("注意: CARE_ENDED のうち母集団にないID:", missing)
df["main_analysis"] = ~(df["exclude_transport_unknown"] | df["exclude_cutoff30"] | df["flag_date_error"]
                        | df["exclude_care_ended"])

# %% [5] 出力と集計
cols = [
    "Patient_ID", "dd", "death_place", "class", "K_kw", "K_date",
    "review_set", "rev_transport", "rev_transport_date", "rev_transport_time", "rev_memo", "override_note",
    "transport", "transport_date", "transport_time", "transport_source", "transport_status",
    "days_transport_to_death", "index_type", "index_date", "index_time", "index_time_source",
    "exclude_transport_unknown", "exclude_cutoff30", "exclude_care_ended", "care_ended_note",
    "flag_boundary_unreviewed", "flag_date_error",
    "main_analysis",
]
df[cols].sort_values("Patient_ID").to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")

print("搬送の有無 × 情報源:")
print(pd.crosstab(df["transport"].astype("string").fillna("不明"), df["transport_source"], margins=True), "\n")
bins = pd.cut(df.loc[is_tr, "days_transport_to_death"], [-1, 0, 1, 7, 30, float("inf")],
              labels=["当日", "翌日", "2-7日", "8-30日", "31日以上"])
print("搬送ありの日数区分 × 状態:")
print(pd.crosstab(bins, df.loc[is_tr, "transport_status"], margins=True, dropna=False), "\n")
print(f"除外: 搬送不明 {df['exclude_transport_unknown'].sum()} 例、搬送後30日超 {df['exclude_cutoff30'].sum()} 例、"
      f"日付エラー {df['flag_date_error'].sum()} 例、当院の診療終了後の死亡 {df['exclude_care_ended'].sum()} 例")
print(f"主解析: {df['main_analysis'].sum()} 例（うち搬送あり {(df['main_analysis'] & is_tr).sum()} 例）")
print(f"注意: 未目視で搬送後31〜{BOUNDARY_MAX}日の例 {df['flag_boundary_unreviewed'].sum()} 例（30日以内の可能性あり）")
print(f"搬送ありで時刻なし: {(is_tr & df['transport_time'].isna()).sum()} 例")
if df["flag_date_error"].any():
    print("日付エラー:", df.loc[df["flag_date_error"], "Patient_ID"].tolist())
print(f"→ {FINAL_CSV}")
