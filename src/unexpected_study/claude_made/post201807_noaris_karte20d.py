# %% 2018年7月以降の死亡例：ARIS記載なし ＋ 初回ARISが死亡7日以内 → 死亡前20日間のカルテ抽出
import sqlite3, re, unicodedata
import pandas as pd
from pathlib import Path

DB  = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUT = Path("/Users/muna/Hana_research/data/output/aris_firstdate")
OUT.mkdir(parents=True, exist_ok=True)

START      = pd.Timestamp("2018-07-01")
WIN_DAY    = 20      # 出力するカルテ：死亡日−20日 〜 死亡日
LATE_DAY   = 7       # 初回ARISが死亡日の何日前以内なら抽出するか（0〜7日）
NURSE_TYPE = "訪問看護"
ARIS_RE = re.compile(r"(?<![A-Za-z])A?RIS(?![A-Za-z])|アリス\s*・?\s*ケア", re.I)

def nfkc(x):
    return unicodedata.normalize("NFKC", x) if isinstance(x, str) else ""

con = sqlite3.connect(DB)

# 母集団 1,281例 + 死亡日 + unex 0/1
pt = pd.read_sql("""
    SELECT u.Patient_ID, u.Study_ID, u.unex_death, ud.unex24, u.Unex_48 AS unex48
    FROM unex_study u
    LEFT JOIN unexpected_death ud ON CAST(ud.Patient_ID AS TEXT) = CAST(u.Patient_ID AS TEXT)
""", con)
ev = pd.read_sql("""
    SELECT DISTINCT Study_ID, death_date FROM event
    WHERE death_date IS NOT NULL AND death_date <> ''
""", con)
ev["death_date"] = pd.to_datetime(ev["death_date"], errors="coerce").dt.normalize()
ev = ev.dropna().drop_duplicates()
assert not ev["Study_ID"].duplicated().any(), "1患者に複数のdeath_date"
pt = pt.merge(ev, on="Study_ID", how="left")

tgt = pt[pt["death_date"] >= START].copy()
print("2018-07-01 以降の死亡:", len(tgt))

# 全期間のカルテ（〜死亡日）
con.execute("DROP TABLE IF EXISTS temp.ids")
con.execute("CREATE TEMP TABLE ids (Study_ID TEXT PRIMARY KEY)")
con.executemany("INSERT INTO temp.ids VALUES (?)", [(s,) for s in tgt["Study_ID"]])
kt = pd.read_sql("""
    SELECT k.Study_ID, k.record_no, k.visit_type, k.visit_datetime, k.karte_text
    FROM karte k JOIN temp.ids i ON k.Study_ID = i.Study_ID
""", con)

s = (kt["visit_datetime"].astype(str)
       .str.replace(r"\([^)]*\)", " ", regex=True)
       .str.replace(r"\s+", " ", regex=True).str.strip())
kt["rec_dt"]   = pd.to_datetime(s, format="%Y/%m/%d %H:%M", errors="coerce")
kt["rec_date"] = pd.to_datetime(s.str.extract(r"^(\d{4}/\d{1,2}/\d{1,2})")[0],
                                format="%Y/%m/%d", errors="coerce")
print("日付パース失敗:", kt["rec_date"].isna().sum())

kt = kt.merge(tgt[["Study_ID", "Patient_ID", "death_date", "unex_death", "unex24", "unex48"]],
              on="Study_ID", how="inner")
kt["days_before_death"] = (kt["death_date"] - kt["rec_date"]).dt.days
kt = kt[kt["days_before_death"] >= 0].copy()
kt["source"] = (kt["visit_type"] == NURSE_TYPE).map({True: "看護", False: "医師"})
kt["aris_hit"] = kt["karte_text"].map(nfkc).str.contains(ARIS_RE)

# 患者ごとの初回ARIS（医師・看護とも、目視確認前の自動判定）
fa = (kt[kt["aris_hit"]].sort_values("rec_dt")
        .groupby("Study_ID").head(1)[["Study_ID", "rec_dt", "days_before_death", "source"]]
        .rename(columns={"rec_dt": "first_aris_dt", "days_before_death": "first_aris_days",
                         "source": "first_aris_source"}))
tgt = tgt.merge(fa, on="Study_ID", how="left")
tgt["aris_status"] = "7日より前"
tgt.loc[tgt["first_aris_dt"].isna(), "aris_status"] = "ARISなし"
tgt.loc[tgt["first_aris_days"] <= LATE_DAY, "aris_status"] = f"{LATE_DAY}日以内"
print(pd.crosstab(tgt["aris_status"], tgt["unex24"], dropna=False, margins=True))

pick = tgt[tgt["aris_status"] != "7日より前"]
print("抽出対象:", len(pick))

# 抽出対象の死亡前20日間のカルテ
w = kt[kt["Study_ID"].isin(pick["Study_ID"]) & (kt["days_before_death"] <= WIN_DAY)]
w = w.merge(pick[["Study_ID", "aris_status", "first_aris_dt", "first_aris_days", "first_aris_source"]],
            on="Study_ID", how="left")
out = (w.sort_values(["Patient_ID", "rec_dt", "record_no"])
         [["Patient_ID", "death_date", "unex_death", "unex24", "unex48",
           "aris_status", "first_aris_dt", "first_aris_days", "first_aris_source",
           "rec_dt", "days_before_death", "source", "visit_type", "aris_hit", "record_no", "karte_text"]])
out["aris_hit"]      = out["aris_hit"].astype(int)
out["death_date"]    = out["death_date"].dt.strftime("%Y-%m-%d")
out["rec_dt"]        = out["rec_dt"].dt.strftime("%Y-%m-%d %H:%M")
out["first_aris_dt"] = out["first_aris_dt"].dt.strftime("%Y-%m-%d %H:%M")

fn = OUT / "post201807_noaris_late7d_karte20d.csv"
out.to_csv(fn, index=False, encoding="utf-8-sig")
print("出力:", fn, "/", len(out), "件 /", out["Patient_ID"].nunique(), "例")

nok = pick[~pick["Patient_ID"].isin(out["Patient_ID"])]
print("20日間にカルテなし:", len(nok))
nok[["Patient_ID", "death_date", "unex_death", "unex24", "unex48", "aris_status",
     "first_aris_dt", "first_aris_days"]].to_csv(
    OUT / "post201807_noaris_late7d_nokarte.csv", index=False, encoding="utf-8-sig")
