"""
Freedocument.text_data / karte.karte_text / karte.height から身長を抽出し、
Patient_Master.height に NULL の行だけ書き込む。

優先順位 : Freedocument.text_data -> karte.karte_text -> karte.height
各ソース内 : 日付昇順で最初に見つかった有効値を採用。範囲外の行は無視し、
            同一ソース内で次の日付の行を探す（患者をまたいでは探さない）。
有効範囲 : タグが 身長/height のとき 50-220cm（境界含む）
           タグが Ht/H のとき      102-195cm（境界含む）
           karte.height（数値列、タグなし）は 50-220cm
未抽出   : NULLのまま（書き込まない）

使い方 : まず DRY_RUN = True で実行して件数を確認 -> 問題なければ False にして再実行
"""
import math
import re
import sqlite3
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

# ===== 設定 =====
DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
DRY_RUN = False            # True: 件数表示のみ（DB無変更） / False: バックアップ後に書き込み
DECIMALS = 0              # 0=整数切り捨て(INTEGER列) / 1=小数第1位まで(REAL列)

LO, HI = 50, 220          # 身長/height タグ、および karte.height（数値列）の有効範囲
LO_HT, HI_HT = 102, 195   # Ht/H タグの有効範囲

AUDIT_CSV = "height_extracted.csv"   # 監査用（DBには出典を入れない）

# tag グループでどの表記が一致したかを判定し、範囲を切り替える
PAT = re.compile(
    r"(?<![A-Za-z])(?P<tag>ht|height|身長|h)\s*[:=]?\s*(?P<num>\d{2,3}(?:\.\d+)?)(?!\d)",
    re.IGNORECASE,
)
NARROW_TAGS = {"ht", "h"}   # この2つだけ 102-195cm。身長/height は 50-220cm


# ===== 抽出関数 =====
def floor_d(x):
    f = 10 ** DECIMALS
    v = math.floor(x * f) / f
    return int(v) if DECIMALS == 0 else float(v)


def extract(text):
    """最初のマッチのみ採用。範囲外はNone。戻り値: (値, マッチ原文)"""
    if not isinstance(text, str):
        return None
    m = PAT.search(unicodedata.normalize("NFKC", text))
    if not m:
        return None
    v = float(m.group("num"))
    lo, hi = (LO_HT, HI_HT) if m.group("tag").lower() in NARROW_TAGS else (LO, HI)
    return (floor_d(v), m.group(0)) if lo <= v <= hi else None


def from_numeric(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v):
        return None
    return (floor_d(v), str(x)) if LO <= v <= HI else None


def parse_karte_date(s):
    """'YYYY/M/DD(曜)00:00' -> datetime。曜日・時刻は無視"""
    s = s.astype(str).map(lambda x: unicodedata.normalize("NFKC", x))
    d = s.str.extract(r"(\d{4})/(\d{1,2})/(\d{1,2})")
    return pd.to_datetime(d[0] + "-" + d[1] + "-" + d[2], format="%Y-%m-%d", errors="coerce")


def to_int_id(series):
    """Patient_ID を整数に統一（変換不能はNA）"""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def first_hits(df, col, source, fn=extract):
    hits = {}
    for pid, val, dt in zip(df["Patient_ID"], df[col], df["dt"]):
        if pd.isna(pid) or pid in hits:
            continue
        r = fn(val)
        if r:
            hits[pid] = (r[0], source, r[1], dt)
    return pd.DataFrame(
        [(int(p), *v) for p, v in hits.items()],
        columns=["Patient_ID", "height_value", "table_name", "source_text", "record_date"],
    )


# ===== 1. 読み込み =====
conn = sqlite3.connect(DB)
tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
for t in ("Freedocument", "karte", "Patient_Master"):
    assert t in tables, f"テーブルが見つかりません: {t}（DBパスを確認）"

free = pd.read_sql(
    "SELECT rowid AS rid, Patients_ID AS Patient_ID, Date, text_data FROM Freedocument", conn
)
karte = pd.read_sql(
    "SELECT rowid AS rid, Patient_ID, visit_datetime, karte_text, height FROM karte", conn
)

free["Patient_ID"] = to_int_id(free["Patient_ID"])
karte["Patient_ID"] = to_int_id(karte["Patient_ID"])
print("ID変換不能  Freedocument:", free["Patient_ID"].isna().sum(),
      " karte:", karte["Patient_ID"].isna().sum())

free["dt"] = pd.to_datetime(free["Date"], errors="coerce")
karte["dt"] = parse_karte_date(karte["visit_datetime"])
print("日付パース失敗  Freedocument:", free["dt"].isna().sum(),
      " karte:", karte["dt"].isna().sum())

free = free.sort_values(["Patient_ID", "dt", "rid"])
karte = karte.sort_values(["Patient_ID", "dt", "rid"])

# ===== 2. 抽出 =====
parts = [
    first_hits(free, "text_data", "Freedocument.text_data"),
    first_hits(karte, "karte_text", "karte.karte_text"),
    first_hits(karte, "height", "karte.height", fn=from_numeric),
]
long = pd.concat(parts, ignore_index=True)
prio = {"Freedocument.text_data": 1, "karte.karte_text": 2, "karte.height": 3}
long["_p"] = long["table_name"].map(prio)
long = long.sort_values(["Patient_ID", "_p"])
long["adopted"] = (~long.duplicated("Patient_ID")).astype(int)

adopted = long[long["adopted"] == 1].drop(columns="_p").copy()
audit = long.drop(columns="_p").copy()
audit["record_date"] = pd.to_datetime(audit["record_date"]).dt.strftime("%Y-%m-%d")
audit.to_csv(AUDIT_CSV, index=False, encoding="utf-8-sig")

all_ids = set(free["Patient_ID"].dropna().astype(int)) | set(karte["Patient_ID"].dropna().astype(int))
print(f"\n総患者数(Freedocument∪karte): {len(all_ids)}")
print(f"抽出成功: {len(adopted)}  未抽出: {len(all_ids) - len(adopted)}")
print(adopted["table_name"].value_counts().to_string())

# ===== 3. Patient_Master との突合 =====
cols = {r[1]: r[2] for r in conn.execute('PRAGMA table_info("Patient_Master")')}
assert "Patient_ID" in cols, f"Patient_Master に Patient_ID がありません: {list(cols)}"
has_height = "height" in cols

master_ids = {int(r[0]) for r in conn.execute("SELECT Patient_ID FROM Patient_Master") if r[0] is not None}
if has_height:
    already = {int(r[0]) for r in conn.execute(
        "SELECT Patient_ID FROM Patient_Master WHERE height IS NOT NULL")}
else:
    already = set()

in_master = adopted[adopted["Patient_ID"].isin(master_ids)]
not_in_master = len(adopted) - len(in_master)
to_update = in_master[~in_master["Patient_ID"].isin(already)]

print(f"\nPatient_Master 行数: {len(master_ids)}  height列: {'あり' if has_height else 'なし（追加予定）'}")
print(f"抽出済みのうち Patient_Master に存在: {len(in_master)}  存在しない: {not_in_master}")
print(f"既にheightが入っている(スキップ): {len(in_master) - len(to_update)}")
print(f"書き込み対象: {len(to_update)}")

if DRY_RUN:
    print("\n[DRY_RUN] DBは変更していません。問題なければ DRY_RUN = False にして再実行してください。")
    conn.close()
    raise SystemExit

# ===== 4. バックアップ -> 書き込み =====
bak = Path(DB).with_name(f"{Path(DB).stem}_backup_{datetime.now():%Y%m%d_%H%M%S}.db")
with sqlite3.connect(bak) as dst:
    conn.backup(dst)
dst.close()
print(f"\nバックアップ作成: {bak}")

col_type = "INTEGER" if DECIMALS == 0 else "REAL"
if not has_height:
    conn.execute(f"ALTER TABLE Patient_Master ADD COLUMN height {col_type}")

caster = int if DECIMALS == 0 else float   # numpy型をそのまま渡すとBLOBになるため変換
rows = [(caster(h), int(p)) for p, h in zip(to_update["Patient_ID"], to_update["height_value"])]

before = conn.total_changes
with conn:   # 失敗時は自動ロールバック
    conn.executemany(
        "UPDATE Patient_Master SET height = ? WHERE Patient_ID = ? AND height IS NULL", rows
    )
updated = conn.total_changes - before
print(f"更新行数: {updated}（対象 {len(rows)}）")

# ===== 5. 検証 =====
n_nn, mn, mx = conn.execute(
    "SELECT COUNT(height), MIN(height), MAX(height) FROM Patient_Master"
).fetchone()
types = conn.execute("SELECT typeof(height), COUNT(*) FROM Patient_Master GROUP BY 1").fetchall()
print(f"height非NULL: {n_nn}  min={mn}  max={mx}  型別: {types}")
assert mn is None or LO <= mn, "範囲外の値が入っています"
assert mx is None or mx <= HI, "範囲外の値が入っています"
conn.close()