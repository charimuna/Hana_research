# %% 2018年6月までに死亡した例：死亡前20日間のカルテ抽出
import sqlite3
import pandas as pd
from pathlib import Path

DB  = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUT = Path("/Users/muna/Hana_research/data/output/aris_firstdate")
OUT.mkdir(parents=True, exist_ok=True)

CUTOFF  = pd.Timestamp("2018-06-30")
WIN_DAY = 20          # 死亡日−20日 〜 死亡日（当日含む）
NURSE_TYPE = "訪問看護"

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
print("母集団:", len(pt), "/ death_date欠損:", pt["death_date"].isna().sum())
print("unex24 欠損:", pt["unex24"].isna().sum(), "/ unex48 欠損:", pt["unex48"].isna().sum())

tgt = pt[pt["death_date"] <= CUTOFF].copy()
print("2018-06-30 までの死亡:", len(tgt))
print(pd.crosstab(tgt["unex24"], tgt["unex48"], dropna=False, margins=True))

# カルテ
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
kt = kt[kt["days_before_death"].between(0, WIN_DAY)].copy()
kt["source"] = (kt["visit_type"] == NURSE_TYPE).map({True: "看護", False: "医師"})

out = (kt.sort_values(["Patient_ID", "rec_dt", "record_no"])
         [["Patient_ID", "death_date", "unex_death", "unex24", "unex48",
           "rec_dt", "days_before_death", "source", "visit_type", "record_no", "karte_text"]])
out["death_date"] = out["death_date"].dt.strftime("%Y-%m-%d")
out["rec_dt"]     = out["rec_dt"].dt.strftime("%Y-%m-%d %H:%M")

fn = OUT / "pre201807_death_karte20d.csv"
out.to_csv(fn, index=False, encoding="utf-8-sig")
print("出力:", fn, "/", len(out), "件 /", out["Patient_ID"].nunique(), "例")

# カルテが1件もない例
nok = tgt[~tgt["Patient_ID"].isin(out["Patient_ID"])]
print("20日間にカルテなし:", len(nok))
nok[["Patient_ID", "death_date", "unex_death", "unex24", "unex48"]].to_csv(
    OUT / "pre201807_death_nokarte.csv", index=False, encoding="utf-8-sig")
