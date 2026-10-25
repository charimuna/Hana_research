# %% [markdown]
# # 搬送例の目視用CSV（GGI改訂 手順3）
# 入力: transport_cases.py の出力（transport_cases.csv, transport_k_snippets.csv）
# 対象:
#   ① 未確定（class != 確定）で、推定搬送日から死亡まで30日以内
#   ② 要目視_根拠なし で、推定搬送日から死亡まで31日以上（推定搬送日が L で代用されている例）
#   ③ 確定例のうち搬送から死亡まで30日以内から無作為に30例（自動判定の検証用、シード固定）
# 出力: review_transport.csv（記入欄付き）
# 保存先: /Users/muna/Hana_research/src/unexpected_study/claude_made/transport_review_csv.py

# %% [1] 設定
from pathlib import Path

import pandas as pd

OUT_DIR = Path("/Users/muna/Hana_research/data/output/transport_20261023")
CASES_CSV = OUT_DIR / "transport_cases.csv"
SNIP_CSV = OUT_DIR / "transport_k_snippets.csv"
REVIEW_CSV = OUT_DIR / "review_transport.csv"

CUTOFF = 30          # 搬送後の区切り（日）
N_VALIDATE = 30      # ③の抽出数
SEED = 20261023      # ③の乱数シード
SNIP_MAX = 2000      # 1セルに入れるスニペットの最大文字数

# %% [2] 読み込み
cases = pd.read_csv(CASES_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
snip = pd.read_csv(SNIP_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
print(f"候補: {len(cases)} 例")
print(cases["class"].value_counts(), "\n")

# %% [3] 対象の抽出
is_conf = cases["class"] == "確定"
s1 = cases[~is_conf & (cases["days_est_to_death"] <= CUTOFF)].copy()
s1["review_set"] = "①未確定_30日以内"

s2 = cases[(cases["class"] == "要目視_根拠なし") & (cases["days_est_to_death"] > CUTOFF)].copy()
s2["review_set"] = "②根拠なし_31日以上"

conf30 = cases[is_conf & (cases["days_transport_to_death"] <= CUTOFF)]
s3 = conf30.sample(n=min(N_VALIDATE, len(conf30)), random_state=SEED).copy()
s3["review_set"] = "③確定_検証抽出"

rev = pd.concat([s1, s2, s3], ignore_index=True)
assert not rev["Patient_ID"].duplicated().any(), "同じ患者が複数の区分に入っています"

# %% [4] スニペットを患者ごとにまとめる（日付順）
snip = snip.sort_values(["Patient_ID", "date", "visit_datetime"])
snip["line"] = "[" + snip["source"] + " " + snip["visit_datetime"].astype(str) + " " + snip["kw"] + "] " + snip["snippet"].astype(str)
snip_agg = (
    snip.groupby("Patient_ID")
    .agg(n_snippets=("line", "size"), snippets=("line", lambda s: " | ".join(s)[:SNIP_MAX]))
    .reset_index()
)
rev = rev.merge(snip_agg, on="Patient_ID", how="left")
rev["n_snippets"] = rev["n_snippets"].fillna(0).astype(int)

# %% [5] 記入欄を付けて出力
rev["rev_transport"] = ""        # あり／なし／不明
rev["rev_transport_date"] = ""   # YYYY-MM-DD
rev["rev_transport_time"] = ""   # HH:MM（分かれば）
rev["rev_memo"] = ""             # 搬送前の状況など

cols = [
    "review_set", "Patient_ID", "dd", "death_place", "death_comment", "class",
    "L_date", "K_date", "K_kw", "K_nurse_date", "K_nurse_kw", "R_date", "R_any_date",
    "est_transport_date", "days_est_to_death", "n_snippets", "snippets",
    "rev_transport", "rev_transport_date", "rev_transport_time", "rev_memo",
]
rev = rev[cols].sort_values(["review_set", "Patient_ID"])
rev.to_csv(REVIEW_CSV, index=False, encoding="utf-8-sig")

print("目視対象:")
print(rev["review_set"].value_counts().sort_index())
print(f"合計 {len(rev)} 例 → {REVIEW_CSV}")
