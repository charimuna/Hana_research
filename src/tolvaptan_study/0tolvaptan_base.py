#!/usr/bin/env python3
"""
Hana Research: Background_summary.memo から「サムスカ導入」の導入日を抽出して tolvaptan_study を作る

抽出の方針（書式の揺れを吸収する）
  - 全角/半角（数字・記号・カナ・空白）は NFKC で正規化して判定。
    ー ‐ − – — ― などのハイフン類は "-" とみなす
  - キーワード: 「サムスカ導入」「サムスカ 導入」「サムスカ導入日」（間の空白は0〜2文字）
  - キーワードと日付の間: 空白、: = → ( [ 「 『 【 < , 、 - と、それらの閉じ括弧を最大6文字まで許容
      例) サムスカ導入(2020/4/1)  サムスカ導入（２０２０／４／１）  サムスカ導入 2020-04-01
          【サムスカ導入】2020/4/1  サムスカ導入日: 2020年4月1日
  - 日付の形式: YYYY/MM/DD  YYYY-MM-DD  YYYY.MM.DD  YYYY年M月D日  YYYYMMDD
                YYYY/MM  YYYY-MM  YYYY年M月（月のみ → 1日で補い date_precision="month"）
  - 「サムスカ導入 2020/4 5mg」のように月の後ろに数字が続くだけの場合は、5を日とは読まない
  - 日付として不正（0000-00-00, 2020/02/31, 年が範囲外）は採用しない
  - 1つの memo に複数あれば、有効なものの中で最も早い日付を導入日にする（同日なら日精度を優先）
  - raw_match には、元の memo にあった表記をそのまま残す

使い方（tolvaptan_study を最新の内容で一新する）
  python make_tolvaptan_study.py --test                      # 書式ごとの自己テスト（DBは使わない）
  python make_tolvaptan_study.py --csv tol.sama.2026.csv     # 確認のみ（DBは変更しない）
  python make_tolvaptan_study.py --csv tol.sama.2026.csv --write   # バックアップ後に一新

  対象患者
    - Background_summary の memo から導入日が取れた患者
    - --csv を付けると、CSV（重要メモ列）の患者も加える。Background_summary で取れなかった患者は
      重要メモから同じ関数で導入日を抽出し、取れなければ ind_date は空欄のまま残す
  Study_ID は Patient_Master の値を使う（Background_summary と食い違えば一覧表示）。
  新しい表は Patient_ID を PRIMARY KEY にし、EF列（ef_*）は空欄で作る。
  （EF列は作り直しで消える。EFは別の処理で入れ直す）
  確認のみの実行でも、旧テーブルとの差（消える患者・日付が変わる患者）を表示する。
"""

import argparse
import re
import sqlite3
import sys
import unicodedata
from datetime import date, datetime
from pathlib import Path

import pandas as pd

DEFAULT_DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
DEFAULT_OUT = "/Users/muna/Hana_research/src/tolvaptan_study/"

YEAR_MIN, YEAR_MAX = 2000, 2035  # 導入日として許容する年

# ---------------- 書式の定義（ここを編集して探索） ----------------
KEYWORD = r"サムスカ\s{0,2}(?:導入日|導入)"
GAP = r"[\s:=→(\[「『【<、,\-)\]」』】>]{0,6}"

KW_RE = re.compile(KEYWORD + GAP)
# YYYY<sep>M[<sep>D]（月だけも可）。日は区切りに直接つながる場合のみ日とみなす
DATE_RE = re.compile(
    r"(?P<y>\d{4})\s{0,2}[/\-.年]\s{0,2}(?P<m>\d{1,2})(?!\d)"
    r"(?:(?:\s{0,2}月\s{0,2}|[/\-.])(?P<d>\d{1,2})(?!\d)\s{0,2}日?)?"
)
DATE8_RE = re.compile(r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})(?!\d)")

_DASHES = "‐‑‒–—―−ー"


def _normalize_with_map(s):
    """NFKC正規化（ハイフン類は '-' に統一）。正規化後の各文字が元文字列の何文字目かも返す"""
    out, idx = [], []
    for i, ch in enumerate(s):
        for c in unicodedata.normalize("NFKC", ch):
            out.append("-" if c in _DASHES else c)
            idx.append(i)
    return "".join(out), idx


def _to_date(y, m, d):
    """有効なら (date, precision)、不正なら None"""
    if not (YEAR_MIN <= y <= YEAR_MAX and 1 <= m <= 12):
        return None
    if d is None:
        return date(y, m, 1), "month"
    try:
        return date(y, m, d), "day"
    except ValueError:
        return None


def extract_tolvaptan_date(memo):
    """
    memo からサムスカ導入日を抽出する。
    戻り値: ind_date(YYYY-MM-DD) / date_precision(day|month) / raw_match(元の表記) / n_candidates
    不正な日付だけが見つかった場合は ind_date=None のまま raw_match に元の表記を入れる。
    """
    empty = {"ind_date": None, "date_precision": None, "raw_match": None, "n_candidates": 0}
    if memo is None or (isinstance(memo, float) and pd.isna(memo)):
        return empty

    original = str(memo)
    text, idx = _normalize_with_map(original)

    valid, invalid = [], []
    for km in KW_RE.finditer(text):
        dm = DATE_RE.match(text, km.end()) or DATE8_RE.match(text, km.end())
        if not dm:
            continue
        d = dm.group("d")
        parsed = _to_date(int(dm.group("y")), int(dm.group("m")), int(d) if d else None)
        end = dm.end()
        if end < len(text) and text[end] in ")]】」』>":  # 閉じ括弧も raw_match に含める
            end += 1
        raw = original[idx[km.start()]: idx[end - 1] + 1].strip()
        (valid if parsed else invalid).append((parsed, raw))

    if valid:
        # 最も早い日付。同日なら日精度を優先
        (dt, precision), raw = min(valid, key=lambda v: (v[0][0], v[0][1] != "day"))
        return {
            "ind_date": dt.strftime("%Y-%m-%d"),
            "date_precision": precision,
            "raw_match": raw,
            "n_candidates": len(valid) + len(invalid),
        }
    if invalid:
        return {"ind_date": None, "date_precision": None,
                "raw_match": invalid[0][1], "n_candidates": len(invalid)}
    return empty


# ---------------- 自己テスト ----------------
TEST_CASES = [
    # (memo, ind_date, precision)
    ("サムスカ導入(2020/4/1)", "2020-04-01", "day"),
    ("サムスカ導入2020/4/1", "2020-04-01", "day"),
    ("サムスカ導入（2020/04/01）", "2020-04-01", "day"),
    ("サムスカ導入（２０２０／０４／０１）", "2020-04-01", "day"),
    ("サムスカ導入 2020-04-01", "2020-04-01", "day"),
    ("サムスカ導入　2020-4-1", "2020-04-01", "day"),
    ("サムスカ 導入 2020.4.1", "2020-04-01", "day"),
    ("サムスカ導入日:2020年4月1日", "2020-04-01", "day"),
    ("サムスカ導入日： ２０２０年４月１日", "2020-04-01", "day"),
    ("ｻﾑｽｶ導入　２０２０／４／１", "2020-04-01", "day"),
    ("【サムスカ導入】2020/4/1", "2020-04-01", "day"),
    ("サムスカ導入→2020-04-01から", "2020-04-01", "day"),
    ("サムスカ導入 2020ー04ー01", "2020-04-01", "day"),   # 長音記号をハイフン代わりに入力
    ("サムスカ導入 2020−04−01", "2020-04-01", "day"),    # マイナス記号
    ("サムスカ導入20200401", "2020-04-01", "day"),
    ("サムスカ導入(2020/4/1) 15mg", "2020-04-01", "day"),
    ("サムスカ導入2020/4/15mg", "2020-04-15", "day"),
    ("サムスカ導入(2020/4)", "2020-04-01", "month"),
    ("サムスカ導入 2020-04", "2020-04-01", "month"),
    ("サムスカ導入２０２０年４月", "2020-04-01", "month"),
    ("サムスカ導入 2020/4 5mg", "2020-04-01", "month"),  # 5 は日ではない
    ("既往あり。サムスカ導入(2021/3/2)。再開: サムスカ導入2019/5", "2019-05-01", "month"),  # 複数→最も早い
    ("サムスカ導入2020/4/1 サムスカ導入2020/4", "2020-04-01", "day"),                    # 同月なら日精度優先
    ("サムスカ導入(0000-00-00)", None, None),
    ("サムスカ導入 2020/02/31", None, None),
    ("サムスカ導入 2020/13/01", None, None),
    ("サムスカ導入 1999/05/01", None, None),
    ("サムスカ導入予定", None, None),
    ("サムスカ 2020/4/1", None, None),                  # 「導入」がない記載は対象外
    ("トルバプタン導入 2020/4/1", None, None),
    ("", None, None),
]


def run_tests():
    bad = 0
    for memo, exp_date, exp_prec in TEST_CASES:
        r = extract_tolvaptan_date(memo)
        ok = r["ind_date"] == exp_date and r["date_precision"] == exp_prec
        bad += not ok
        print(f"{'OK ' if ok else 'NG '} {memo!r:55s} -> {r['ind_date']} {r['date_precision']} "
              f"raw={r['raw_match']!r}")
    # raw_match は元の表記のまま
    raw = extract_tolvaptan_date("備考 ｻﾑｽｶ導入　２０２０／４／１ 以降")["raw_match"]
    assert raw == "ｻﾑｽｶ導入　２０２０／４／１", raw
    assert extract_tolvaptan_date(None)["ind_date"] is None
    assert extract_tolvaptan_date(float("nan"))["ind_date"] is None
    print(f"\n{len(TEST_CASES)} 件中 NG {bad} 件")
    return bad == 0


# ---------------- 本体 ----------------
NEW_COLS = ["Patient_ID", "Study_ID", "ind_date", "date_precision", "raw_match",
            "ef_visit_date", "ef_value", "ef_matched_text", "ef_final_hf_class"]

CREATE_SQL = """
CREATE TABLE tolvaptan_study_new (
    Patient_ID INTEGER PRIMARY KEY,
    Study_ID TEXT,
    ind_date TEXT,
    date_precision TEXT,
    raw_match TEXT,
    ef_visit_date TEXT,
    ef_value TEXT,
    ef_matched_text TEXT,
    ef_final_hf_class TEXT
)
"""


def snippet(memo):
    text, _ = _normalize_with_map(str(memo))
    i = text.find("サムスカ")
    return text[max(0, i - 10): i + 40].replace("\n", " ") if i >= 0 else None


def read_csv_memos(csv_path):
    """CSV（cp932）から Patient_ID と 重要メモ（O列）を読む"""
    df_csv = pd.read_csv(csv_path, encoding="cp932")
    df_csv = df_csv.rename(columns={"患者ID": "Patient_ID"})
    memo_col = df_csv.columns[14]  # O列
    if memo_col != "重要メモ":
        sys.exit(f"ERROR: O列が「重要メモ」ではありません。実際の列名: {memo_col}")
    df_csv["Patient_ID"] = pd.to_numeric(df_csv["Patient_ID"], errors="coerce").astype("Int64")
    n_rows = len(df_csv)
    df_csv = df_csv.dropna(subset=["Patient_ID"])
    n_dup = int(df_csv["Patient_ID"].duplicated().sum())
    df_csv = df_csv.drop_duplicates(subset=["Patient_ID"])
    print(f"CSV: {n_rows:,}行 / Patient_ID が数値でない行を除外後 {len(df_csv):,}人"
          f"（重複Patient_IDを除去: {n_dup}）")
    return df_csv[["Patient_ID", "重要メモ"]].rename(columns={"重要メモ": "csv_memo"})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out-dir", default=DEFAULT_OUT)
    ap.add_argument("--csv", default=None,
                    help="tol.sama CSV（cp932）。指定すると CSV の患者も対象に加える")
    ap.add_argument("--test", action="store_true", help="自己テストだけ実行して終了")
    ap.add_argument("--write", action="store_true",
                    help="tolvaptan_study を一新する（指定しなければ確認のみ）")
    args = ap.parse_args()

    if args.test:
        sys.exit(0 if run_tests() else 1)
    if not run_tests():
        sys.exit("自己テストにNGがあるため中止します")
    print()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db, isolation_level=None)

    # ---------------- 1. Background_summary の memo から抽出 ----------------
    bg = pd.read_sql("""
        SELECT Patient_ID, Study_ID AS Study_ID_bg, memo
        FROM Background_summary
        WHERE memo IS NOT NULL
    """, conn)
    bg["Patient_ID"] = pd.to_numeric(bg["Patient_ID"], errors="coerce").astype("Int64")
    res = bg["memo"].apply(extract_tolvaptan_date)
    for col in ["ind_date", "date_precision", "raw_match", "n_candidates"]:
        bg[col] = res.apply(lambda x, c=col: x[c])

    # 「サムスカ」の記載があるのに導入日が取れなかった memo（0人が期待値）
    bg["snippet"] = bg["memo"].apply(snippet)
    unparsed = bg[bg["snippet"].notna() & bg["ind_date"].isna()][
        ["Patient_ID", "Study_ID_bg", "raw_match", "snippet"]]
    unparsed_file = out_dir / "tolvaptan_unparsed_memos.csv"
    unparsed.to_csv(unparsed_file, index=False, encoding="utf-8-sig")
    print(f"Background_summary: memo あり {len(bg):,}人 / 導入日を抽出 "
          f"{int(bg['ind_date'].notna().sum())}人")
    print(f"  「サムスカ」の記載はあるが導入日を取れなかった: {len(unparsed)}人"
          f"（{unparsed_file}）")
    if len(unparsed):
        print(unparsed.head(20).to_string(index=False))

    bg_ok = bg[bg["ind_date"].notna()].copy()
    n_dup_bg = int(bg_ok["Patient_ID"].duplicated().sum())
    if n_dup_bg:
        print(f"  注意: Background_summary で Patient_ID が重複: {n_dup_bg}行"
              "（最も早い導入日を採用）")
        bg_ok = bg_ok.sort_values(["Patient_ID", "ind_date"]).drop_duplicates("Patient_ID")
    bg_ok["source"] = "Background_summary"
    rows = bg_ok[["Patient_ID", "ind_date", "date_precision", "raw_match",
                  "n_candidates", "source"]].copy()

    # ---------------- 2. CSV の患者を加える（任意） ----------------
    if args.csv:
        csv_df = read_csv_memos(args.csv)
        r2 = csv_df["csv_memo"].apply(extract_tolvaptan_date)
        for col in ["ind_date", "date_precision", "raw_match", "n_candidates"]:
            csv_df[col] = r2.apply(lambda x, c=col: x[c])

        in_bg = csv_df["Patient_ID"].isin(set(rows["Patient_ID"]))
        csv_only = csv_df[~in_bg].copy()
        csv_only["source"] = csv_only["ind_date"].notna().map(
            {True: "CSV重要メモ", False: "CSV（導入日なし）"})
        print(f"  CSVの患者 {len(csv_df):,}人 = Background_summaryで取得済み {int(in_bg.sum())}人"
              f" + CSVのみ {len(csv_only)}人"
              f"（うち重要メモから導入日を抽出 {int(csv_only['ind_date'].notna().sum())}人）")
        rows = pd.concat([rows, csv_only[rows.columns]], ignore_index=True)

    rows["Patient_ID"] = rows["Patient_ID"].astype("Int64")
    assert rows["Patient_ID"].is_unique, "Patient_ID が重複しています"

    # ---------------- 3. Study_ID は Patient_Master から ----------------
    pm = pd.read_sql('SELECT Patient_ID, Study_ID FROM Patient_Master', conn)
    pm["Patient_ID"] = pd.to_numeric(pm["Patient_ID"], errors="coerce").astype("Int64")
    rows = rows.merge(pm, on="Patient_ID", how="left")

    cmp_bg = rows.merge(bg[["Patient_ID", "Study_ID_bg"]].drop_duplicates("Patient_ID"),
                        on="Patient_ID", how="left")
    mism = cmp_bg[cmp_bg["Study_ID_bg"].notna() & cmp_bg["Study_ID"].notna()
                  & (cmp_bg["Study_ID_bg"] != cmp_bg["Study_ID"])]
    if len(mism):
        print(f"\n注意: Background_summary と Patient_Master で Study_ID が違う患者: {len(mism)}人"
              "（Patient_Master を採用）")
        print(mism[["Patient_ID", "Study_ID_bg", "Study_ID"]].head(10).to_string(index=False))
    n_nosid = int(rows["Study_ID"].isna().sum())
    if n_nosid:
        print(f"注意: Patient_Master に Study_ID がない患者: {n_nosid}人"
              f"（例 {rows.loc[rows['Study_ID'].isna(), 'Patient_ID'].head(5).tolist()}）"
              "\n  → --write 後に fill_study_id.py で付与できます")
    dup_sid = rows["Study_ID"].dropna().duplicated().sum()
    if dup_sid:
        sys.exit(f"ERROR: Study_ID が重複しています: {dup_sid}")

    rows = rows.sort_values(["ind_date", "Patient_ID"], na_position="last").reset_index(drop=True)

    # ---------------- 4. 結果の確認 ----------------
    n_ind = int(rows["ind_date"].notna().sum())
    print(f"\n新しい tolvaptan_study: {len(rows)}行 / 導入日あり {n_ind}人")
    print(rows["date_precision"].value_counts(dropna=False).to_string())
    n_multi = int((rows["n_candidates"] > 1).sum())
    if n_multi:
        print(f"「サムスカ導入」が複数ある memo: {n_multi}人（最も早い日付を採用）")
    print(rows.head(10).to_string(index=False))

    preview_file = out_dir / "tolvaptan_study_preview.csv"
    rows.to_csv(preview_file, index=False, encoding="utf-8-sig")
    print(f"\n確認用CSV: {preview_file}")

    # ---------------- 5. 旧テーブルとの比較 ----------------
    old_cols = {r[1] for r in conn.execute("PRAGMA table_info(tolvaptan_study)")}
    if old_cols:
        sel = "Patient_ID, ind_date" if "ind_date" in old_cols else "Patient_ID, NULL AS ind_date"
        old = pd.read_sql(f"SELECT {sel} FROM tolvaptan_study", conn)
        old["Patient_ID"] = pd.to_numeric(old["Patient_ID"], errors="coerce").astype("Int64")
        print(f"\n旧テーブル: {len(old)}行 / 導入日あり {int(old['ind_date'].notna().sum())}人"
              f" → 新テーブル: {len(rows)}行 / 導入日あり {n_ind}人")
        old_ind = old[old["ind_date"].notna()]
        cmp = old_ind.merge(rows[["Patient_ID", "ind_date"]], on="Patient_ID",
                            how="left", suffixes=("_old", "_new"))
        dropped = cmp[cmp["ind_date_new"].isna()]
        changed = cmp[cmp["ind_date_new"].notna() & (cmp["ind_date_old"] != cmp["ind_date_new"])]
        print(f"  旧テーブルで導入日があったのに、新テーブルで導入日がない: {len(dropped)}人")
        if len(dropped):
            print(dropped[["Patient_ID", "ind_date_old"]].head(20).to_string(index=False))
        print(f"  導入日が変わる: {len(changed)}人")
        if len(changed):
            print(changed.head(20).to_string(index=False))
        print(f"  旧テーブルのEF列は新テーブルでは空欄になります"
              f"（旧テーブルのEF値あり: "
              f"{int(pd.read_sql('SELECT COUNT(*) AS n FROM tolvaptan_study WHERE ef_value IS NOT NULL', conn)['n'][0]) if 'ef_value' in old_cols else 0}人）")

    # ---------------- 6. DB保存 ----------------
    if not args.write:
        print("\n確認のみ（DBは変更していません）。内容がよければ --write を付けて実行してください。")
        conn.close()
        return

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{args.db}.bak_before_tolvaptan_rebuild_{stamp}"
    dst = sqlite3.connect(backup_path)
    conn.backup(dst)
    dst.close()
    print(f"\nバックアップ: {backup_path}")

    records = []
    for r in rows.itertuples(index=False):
        records.append((
            int(r.Patient_ID),
            None if pd.isna(r.Study_ID) else str(r.Study_ID),
            None if pd.isna(r.ind_date) else str(r.ind_date),
            None if pd.isna(r.date_precision) else str(r.date_precision),
            None if pd.isna(r.raw_match) else str(r.raw_match),
            None, None, None, None,
        ))

    conn.execute("BEGIN")
    try:
        conn.execute("DROP TABLE IF EXISTS tolvaptan_study_new")
        conn.execute(CREATE_SQL)
        conn.executemany(
            "INSERT INTO tolvaptan_study_new VALUES (?,?,?,?,?,?,?,?,?)", records)
        n_new = conn.execute("SELECT COUNT(*) FROM tolvaptan_study_new").fetchone()[0]
        if n_new != len(rows):
            raise RuntimeError(f"件数不一致: {n_new} != {len(rows)}")
        conn.execute("DROP TABLE IF EXISTS tolvaptan_study")
        conn.execute("ALTER TABLE tolvaptan_study_new RENAME TO tolvaptan_study")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        print("ロールバックしました。DBは変更されていません。", file=sys.stderr)
        raise

    chk = conn.execute("""
        SELECT COUNT(*), SUM(ind_date IS NOT NULL), SUM(Study_ID IS NULL)
        FROM tolvaptan_study""").fetchone()
    print(f"完了：tolvaptan_study を一新しました（{chk[0]}行 / 導入日あり {chk[1]}人 / "
          f"Study_ID なし {chk[2]}人）")
    print("次: EF列（ef_*）は空欄です。EFの入れ直しが必要です。")
    conn.close()


if __name__ == "__main__":
    main()