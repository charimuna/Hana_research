#!/usr/bin/env python3
"""
Hana Research: tolvaptan_study に HF / Cancer / CKD / Cirrhosis / HypoNa を 0/1 で追加する

（以前のノートブック tolvaptan_disease_flags.ipynb の .py 化。抽出ロジックは変更していない）

対象: tolvaptan_study で ind_date がある全員
カルテ: ind_date の前後30日（-30日〜+30日）
病名の拾い方: カルテ内の `# 病名`（全角＃・半角#・♯、全角英数字に対応。# から次の # または行末まで）

列名と病名カテゴリの対応
  HF        : 心不全（HFpEF等の略語含む）
  Cancer    : がん（腫瘍・リンパ腫・白血病を含む。既往・術後・寛解は1にしない）
  CKD       : 腎不全（CKD・慢性腎臓病・腎機能障害を含む）
  Cirrhosis : 肝硬変
  HypoNa    : 低Na（SIADHを含む）

追加で、以下10人は Study_ID で照合し、カルテ抽出の結果に関わらず該当列を1にする（OR）。
Patient_ID は確認用（食い違えば警告。一致しなくても Study_ID を優先して進める）。
  低Na     P866511 170010 -> HypoNa
  心アミロイド P976287 170235 -> HF
  蛸壺     P376019 180120 -> HF
  低Na     P331449 190008 -> HypoNa
  ネフローゼ  P480294 190235 -> CKD
  ASHF    P026619 200067 -> HF
  がん     P935620 220194 -> Cancer
  がん     P474675 230169 -> Cancer
  がん     P246501 250238 -> Cancer
  がん     P951086 230249 -> Cancer
  上記のうち P246501 / P951086 は、自動抽出で HF=1 になっていても HF を 0 にする（HFを消す）
  ※ 低アルブミン P185844 170266 は4病名のどれにも分類しない（対象外）

使い方
  python add_disease_flags.py            # 確認のみ（DBは変更しない）。CSVに根拠行を出力
  python add_disease_flags.py --write     # バックアップ後に tolvaptan_study へ列を追加/更新
"""

import argparse
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

DEFAULT_DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
DEFAULT_OUT = "/Users/muna/Hana_research/src/tolvaptan_study/"
WINDOW_DAYS = 30

DISEASE_PATTERNS = {
    "HF": [
        r"心不全",
        r"(?<![A-Z0-9])(?:CHF|ADHF|HFPEF|HFREF|HFMREF|HF)(?![A-Z0-9])",
    ],
    "Cancer": [
        r"癌",
        r"がん",
        r"ガン(?!マ)",
        r"悪性腫瘍",
        r"(?<!良性)腫瘍",
        r"リンパ腫",
        r"白血病",
    ],
    "CKD": [
        r"腎不全",
        r"(?<![A-Z0-9])CKD(?![A-Z0-9])",
        r"慢性腎臓病",
        r"腎機能障害",
    ],
    "Cirrhosis": [
        r"肝硬変",
    ],
    "HypoNa": [
        r"低\s*NA(?![A-Z])",
        r"低ナトリウム",
        r"SIADH",
        r"HYPONATREMIA",
    ],
}

CANCER_HISTORY_PATTERN = re.compile(
    r"既往|術後|手術後|切除後|寛解|治癒|根治|(?<![A-Z])S/P(?![A-Z])"
)

DISEASES = list(DISEASE_PATTERNS.keys())
DISEASE_REGEX = {d: re.compile("|".join(p)) for d, p in DISEASE_PATTERNS.items()}

HASH_SEG = re.compile(r"#([^#\r\n]*)")

# ---------------- 手動で追加する10人（Study_ID照合。Patient_IDは確認用） ----------------
MANUAL_FLAGS = [
    # (label, Study_ID, Patient_ID, disease_column)
    ("低Na", "P866511", 170010, "HypoNa"),
    ("心アミロイド", "P976287", 170235, "HF"),
    ("蛸壺", "P376019", 180120, "HF"),
    ("低Na", "P331449", 190008, "HypoNa"),
    ("ネフローゼ", "P480294", 190235, "CKD"),
    ("ASHF", "P026619", 200067, "HF"),
    ("がん", "P935620", 220194, "Cancer"),
    ("がん", "P474675", 230169, "Cancer"),
    ("がん", "P246501", 250238, "Cancer"),
    ("がん", "P951086", 230249, "Cancer"),
    # 低アルブミン P185844 170266 はどの病名にも分類しない（対象外）
]

# 手動で 0 に戻す（HF を消す）。OR の後に適用する
# (label, Study_ID, Patient_ID, disease_column)
MANUAL_CLEARS = [
    ("がん→HF消去", "P246501", 250238, "HF"),
    ("がん→HF消去", "P951086", 230249, "HF"),
]


def normalize(text):
    t = unicodedata.normalize("NFKC", str(text))
    t = t.replace("♯", "#").replace("＃", "#")
    return t.upper()


def build_flags(conn):
    """tolvaptan_study(ind_dateあり全員) に対して 0/1 表と根拠行を作る"""
    patients = pd.read_sql_query(
        "SELECT Patient_ID, Study_ID, ind_date FROM tolvaptan_study WHERE ind_date IS NOT NULL",
        conn,
    )
    patients["Patient_ID"] = pd.to_numeric(patients["Patient_ID"], errors="coerce").astype("Int64")
    patients["ind_date"] = pd.to_datetime(patients["ind_date"], errors="coerce")
    n_bad = int(patients["ind_date"].isna().sum())
    print(f"対象患者数: {len(patients)}（ind_date を日付に直せない行: {n_bad}）")

    patient_ids = [str(p) for p in patients["Patient_ID"].dropna().unique()]
    placeholders = ",".join(["?"] * len(patient_ids))
    karte = pd.read_sql_query(
        f"""
        SELECT Patient_ID, visit_datetime, karte_text
        FROM karte
        WHERE Patient_ID IN ({placeholders})
          AND karte_text IS NOT NULL
          AND karte_text != ''
        """,
        conn,
        params=patient_ids,
    )
    karte["Patient_ID"] = pd.to_numeric(karte["Patient_ID"], errors="coerce").astype("Int64")
    print(f"取得カルテ件数: {len(karte):,}")

    ymd = karte["visit_datetime"].astype(str).str.extract(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})")
    karte["visit_date"] = pd.to_datetime(ymd[0] + "-" + ymd[1] + "-" + ymd[2], errors="coerce")
    print("visit_date を読めなかったカルテ:", int(karte["visit_date"].isna().sum()))

    karte = karte.merge(patients[["Patient_ID", "Study_ID", "ind_date"]], on="Patient_ID", how="inner")
    karte["days_from_ind"] = (karte["visit_date"] - karte["ind_date"]).dt.days
    karte_window = karte[karte["days_from_ind"].between(-WINDOW_DAYS, WINDOW_DAYS)].copy()
    print(f"-{WINDOW_DAYS}日〜+{WINDOW_DAYS}日のカルテ: {len(karte_window):,} 件 "
          f"/ {karte_window['Patient_ID'].nunique()} 人")

    seg_rows = []
    for rec in karte_window.itertuples(index=False):
        for line in re.split(r"[\r\n]+", normalize(rec.karte_text)):
            if "#" not in line:
                continue
            for seg in HASH_SEG.findall(line):
                seg = seg.strip()
                if seg:
                    seg_rows.append({
                        "Study_ID": rec.Study_ID, "Patient_ID": rec.Patient_ID,
                        "ind_date": rec.ind_date, "visit_date": rec.visit_date,
                        "days_from_ind": rec.days_from_ind, "segment": seg, "line": line.strip(),
                    })
    segments = pd.DataFrame(seg_rows)
    print(f"# の病名欄: {len(segments):,} 件 / "
          f"{segments['Patient_ID'].nunique() if len(segments) else 0} 人")

    evidence_parts = []
    for disease, regex in DISEASE_REGEX.items():
        if segments.empty:
            break
        hit = segments[segments["segment"].str.contains(regex, na=False)].copy()
        hit["disease"] = disease
        hit["counted"] = 1
        hit["exclude_reason"] = ""
        if disease == "Cancer":
            is_hist = hit["segment"].str.contains(CANCER_HISTORY_PATTERN, na=False)
            hit.loc[is_hist, "counted"] = 0
            hit.loc[is_hist, "exclude_reason"] = "既往・術後・寛解等"
        evidence_parts.append(hit)
    evidence = (
        pd.concat(evidence_parts, ignore_index=True) if evidence_parts
        else pd.DataFrame(columns=list(segments.columns) + ["disease", "counted", "exclude_reason"])
    )

    flags = patients[["Study_ID", "Patient_ID", "ind_date"]].copy()
    counted = evidence[evidence["counted"] == 1] if len(evidence) else evidence
    for d in DISEASES:
        sub = counted[counted["disease"] == d] if len(counted) else counted
        ids = set(sub["Patient_ID"]) if len(sub) else set()
        flags[d] = flags["Patient_ID"].isin(ids).astype(int)

    assert len(flags) == len(patients), "患者数が変わっています"
    assert flags["Patient_ID"].is_unique, "Patient_ID が重複しています"
    return flags, evidence


def apply_manual_flags(flags):
    """10人の追加フラグを Study_ID で照合して OR する"""
    sid_to_pid = dict(zip(flags["Study_ID"], flags["Patient_ID"]))
    applied, not_found, mismatched = [], [], []
    for label, sid, pid_expected, disease in MANUAL_FLAGS:
        if sid not in sid_to_pid:
            not_found.append((label, sid, pid_expected, disease))
            continue
        pid_actual = sid_to_pid[sid]
        if pd.notna(pid_actual) and int(pid_actual) != pid_expected:
            mismatched.append((label, sid, pid_expected, int(pid_actual), disease))
        mask = flags["Study_ID"] == sid
        before = int(flags.loc[mask, disease].iloc[0])
        flags.loc[mask, disease] = 1
        applied.append((label, sid, disease, before))

    for label, sid, pid_expected, disease in MANUAL_CLEARS:
        if sid not in sid_to_pid:
            print(f"  消去対象 {label} {sid} は tolvaptan_study にありません")
            continue
        mask = flags["Study_ID"] == sid
        before = int(flags.loc[mask, disease].iloc[0])
        flags.loc[mask, disease] = 0
        print(f"  - {label:8s} {sid} -> {disease} を 0 にする"
              f"{'（元々1→0に変更）' if before == 1 else '（元々0。変化なし）'}")

    print(f"\n手動追加フラグ: {len(MANUAL_FLAGS)}人指定 / 適用 {len(applied)}人 "
          f"/ tolvaptan_studyに見つからず {len(not_found)}人")
    for label, sid, disease, before in applied:
        mark = "（元々0→1に変更）" if before == 0 else "（元々1。変化なし）"
        print(f"  - {label:8s} {sid} -> {disease:9s} {mark}")
    if not_found:
        print("  見つからなかった（tolvaptan_studyにこの Study_ID がない）:")
        for label, sid, pid, disease in not_found:
            print(f"    {label:8s} {sid} (Patient_ID想定 {pid}) -> {disease}")
    if mismatched:
        print("  注意: 画像のPatient_IDと現在のPatient_Masterが食い違う（Study_IDは優先して適用済み）:")
        for label, sid, pid_exp, pid_act, disease in mismatched:
            print(f"    {label:8s} {sid}: 画像={pid_exp} 現在={pid_act} -> {disease}")
    return flags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out-dir", default=DEFAULT_OUT)
    ap.add_argument("--write", action="store_true",
                    help="指定しなければ確認のみ（DBは変更しない）")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db, isolation_level=None)

    flags, evidence = build_flags(conn)
    flags = apply_manual_flags(flags)

    print("\n病名別 該当人数（手動追加後）")
    for d in DISEASES:
        print(f"  {d:10s}: {flags[d].sum():4d} / {len(flags)}")

    flags_file = out_dir / "tolvaptan_disease_flags.csv"
    evidence_file = out_dir / "tolvaptan_disease_evidence.csv"
    flags.to_csv(flags_file, index=False, encoding="utf-8-sig")
    evidence.to_csv(evidence_file, index=False, encoding="utf-8-sig")
    print(f"\n確認用CSV: {flags_file}")
    print(f"根拠CSV  : {evidence_file}")

    if not args.write:
        print("\n確認のみ（DBは変更していません）。内容がよければ --write を付けて実行してください。")
        conn.close()
        return

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{args.db}.bak_before_disease_flags_{stamp}"
    dst = sqlite3.connect(backup_path)
    conn.backup(dst)
    dst.close()
    print(f"\nバックアップ: {backup_path}")

    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(tolvaptan_study)")}
    conn.execute("BEGIN")
    try:
        for d in DISEASES:
            if d not in existing_cols:
                conn.execute(f'ALTER TABLE tolvaptan_study ADD COLUMN "{d}" INTEGER')
        conn.executemany(
            f'UPDATE tolvaptan_study SET {", ".join(f"{d} = ?" for d in DISEASES)} '
            f"WHERE Patient_ID = ?",
            [tuple(int(v) for v in row[DISEASES]) + (int(row.Patient_ID),)
             for _, row in flags.iterrows()],
        )
        n_updated = conn.execute(
            f'SELECT COUNT(*) FROM tolvaptan_study WHERE "{DISEASES[0]}" IS NOT NULL'
        ).fetchone()[0]
        if n_updated != len(flags):
            raise RuntimeError(f"更新件数が一致しません: {n_updated} != {len(flags)}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        print("ロールバックしました。DBは変更されていません。", file=sys.stderr)
        raise

    print(f"完了：tolvaptan_study に {', '.join(DISEASES)} を追加/更新しました（{len(flags)}人）")
    conn.close()


if __name__ == "__main__":
    main()