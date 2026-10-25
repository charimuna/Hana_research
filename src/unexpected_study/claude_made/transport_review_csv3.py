# %% [markdown]
# # 境界例の目視用CSV（GGI改訂 手順3-4）
# 対象: transport_final.csv で flag_boundary_unreviewed == True（未目視で搬送後31〜40日）
# 理由: 「入院」のみの例では、実際の搬送日が自動判定より最大10日遅かったため、30日以内に移る可能性がある
# 出力: review_transport_3.csv（記入欄付き、書式は review_transport.csv と同じ）
#       記入後は review_transport_3_manu01.xlsx として同じフォルダに保存 → transport_final.py が読み込む
# 保存先: /Users/muna/Hana_research/src/unexpected_study/claude_made/transport_review_csv3.py

# %% [1] 設定
from pathlib import Path

import pandas as pd

OUT_DIR = Path("/Users/muna/Hana_research/data/output/transport_20261023")
FINAL_CSV = OUT_DIR / "transport_final.csv"
CASES_CSV = OUT_DIR / "transport_cases.csv"
SNIP_CSV = OUT_DIR / "transport_k_snippets.csv"
REVIEW_CSV = OUT_DIR / "review_transport_3.csv"
SNIP_MAX = 2000

# %% [2] 対象の抽出
fin = pd.read_csv(FINAL_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
flag = fin["flag_boundary_unreviewed"].astype(str).str.lower() == "true"
ids = set(fin.loc[flag, "Patient_ID"])
print(f"境界例: {len(ids)} 例")

cases = pd.read_csv(CASES_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
rev = cases[cases["Patient_ID"].isin(ids)].copy()
assert len(rev) == len(ids), "transport_cases.csv に見つからない境界例がある"
rev["review_set"] = "⑤境界_31-40日_未目視"

# %% [3] スニペットを患者ごとにまとめる（日付順）
snip = pd.read_csv(SNIP_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
snip = snip.sort_values(["Patient_ID", "date", "visit_datetime"])
snip["line"] = "[" + snip["source"] + " " + snip["visit_datetime"].astype(str) + " " + snip["kw"] + "] " + snip["snippet"].astype(str)
snip_agg = (
    snip.groupby("Patient_ID")
    .agg(n_snippets=("line", "size"), snippets=("line", lambda s: " | ".join(s)[:SNIP_MAX]))
    .reset_index()
)
rev = rev.merge(snip_agg, on="Patient_ID", how="left")
rev["n_snippets"] = rev["n_snippets"].fillna(0).astype(int)

# %% [4] 記入欄を付けて出力
for c in ["rev_transport", "rev_transport_date", "rev_transport_time", "rev_memo"]:
    rev[c] = ""
cols = [
    "review_set", "Patient_ID", "dd", "death_place", "death_comment", "class",
    "L_date", "K_date", "K_kw", "K_nurse_date", "K_nurse_kw", "R_date", "R_any_date",
    "est_transport_date", "days_est_to_death", "n_snippets", "snippets",
    "rev_transport", "rev_transport_date", "rev_transport_time", "rev_memo",
]
rev[cols].sort_values("Patient_ID").to_csv(REVIEW_CSV, index=False, encoding="utf-8-sig")
print(rev["class"].value_counts())
print(f"→ {REVIEW_CSV}")
