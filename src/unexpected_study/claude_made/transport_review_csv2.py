# %% [markdown]
# # 搬送例の追加目視用CSV（GGI改訂 手順3-2）
# 対象: 確定例のうち、搬送から死亡まで30日以内で、K の語に「搬送」「救急要請」を含まない例（「入院」のみ）
#       ③で目視済みの例は除く
# 理由: ③の検証で「入院」のみの確定例は 3/9 しか搬送日が一致しなかったため
# 入力: transport_cases.csv, transport_k_snippets.csv, review_transport_v1-2.xlsx（目視済み）
# 出力: review_transport_2.csv（記入欄付き、書式は review_transport.csv と同じ）
# 保存先: /Users/muna/Hana_research/src/unexpected_study/claude_made/transport_review_csv2.py

# %% [1] 設定
from pathlib import Path

import pandas as pd

OUT_DIR = Path("/Users/muna/Hana_research/data/output/transport_20261023")
CASES_CSV = OUT_DIR / "transport_cases.csv"
SNIP_CSV = OUT_DIR / "transport_k_snippets.csv"
DONE_XLSX = OUT_DIR / "review_transport_v1-2.xlsx"   # 目視済みファイルの置き場所に合わせて修正
REVIEW_CSV = OUT_DIR / "review_transport_2.csv"

CUTOFF = 30
SNIP_MAX = 2000

# %% [2] 読み込み
cases = pd.read_csv(CASES_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
snip = pd.read_csv(SNIP_CSV, dtype={"Patient_ID": str}, encoding="utf-8-sig")
done = pd.read_excel(DONE_XLSX, dtype=str)
done_ids = set(done["Patient_ID"].str.strip())

# %% [3] 対象の抽出
kw = cases["K_kw"].fillna("")
target = cases[
    (cases["class"] == "確定")
    & (cases["days_transport_to_death"] <= CUTOFF)
    & ~kw.str.contains("搬送|救急要請", regex=True)
].copy()
print(f"「入院」のみの確定例（30日以内）: {len(target)} 例")
print(f"  うち目視済み: {target['Patient_ID'].isin(done_ids).sum()} 例")
rev = target[~target["Patient_ID"].isin(done_ids)].copy()
rev["review_set"] = "④確定_入院のみ"
print(f"  今回の目視対象: {len(rev)} 例")

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
for c in ["rev_transport", "rev_transport_date", "rev_transport_time", "rev_memo"]:
    rev[c] = ""

cols = [
    "review_set", "Patient_ID", "dd", "death_place", "death_comment", "class",
    "L_date", "K_date", "K_kw", "K_nurse_date", "K_nurse_kw", "R_date", "R_any_date",
    "est_transport_date", "days_est_to_death", "n_snippets", "snippets",
    "rev_transport", "rev_transport_date", "rev_transport_time", "rev_memo",
]
rev[cols].sort_values("Patient_ID").to_csv(REVIEW_CSV, index=False, encoding="utf-8-sig")
print(f"→ {REVIEW_CSV}")
