import pandas as pd

df = pd.read_csv("../data/processed/初診時病名リスト.csv", encoding="utf-8-sig")

df["作成日"] = pd.to_datetime(df["作成日"], errors="coerce")

min_dates = df.groupby("患者ID")["作成日"].transform("min")
df_oldest = df[df["作成日"] == min_dates].copy()

split_pattern = r"[、，；;]"
df_oldest["病名リスト"] = df_oldest["項目名"].str.split(split_pattern)

df_long = df_oldest.explode("病名リスト")

df_long["病名リスト"] = df_long["病名リスト"].str.strip()
df_long = df_long[df_long["病名リスト"] != ""]
df_long = df_long.dropna(subset=["病名リスト"])

df_result = df_long[["患者ID", "作成日", "病名リスト"]].rename(
    columns={"病名リスト": "病名"}
)

df_result.to_csv("first_dis.csv", index=False, encoding="utf-8-sig")