"""
corrections.py — 元データ（電子カルテ等）の誤りに対する修正を記録し、DB更新後に再適用する

配置: src/db_mgmt/corrections.py
記録先: data/raw/corrections/corrections.csv（utf-8-sig）
※ 死亡日（event.death_date / last_confirmed_date）は death_date_override.csv で管理するため対象外

■ 記録（各プロジェクトの ipynb / スクリプトから）
    import sys
    sys.path.append("/Users/muna/Hana_research/src/db_mgmt")
    from corrections import record_correction

    record_correction(
        project="tolvaptan", table="lab_clean", patient_id=150123,
        key_date="2019/04/02", key_item="Na", column="value_num",
        old_value=13.5, new_value=135, reason="桁誤り（検査伝票で確認）",
    )

■ 適用（全テーブル更新 → sync_study_id.py → 本スクリプト）
    python src/db_mgmt/corrections.py            # ドライラン（DBは変更しない）
    python src/db_mgmt/corrections.py --commit   # DBバックアップ後に適用

■ 判定（1行ごと）
    applied / to_apply   現在値 = old_value → new_value に書き換え
    already_applied      現在値 = new_value（適用済み、または元データ側が修正済み）
    mismatch             現在値が old_value とも new_value とも違う → 適用しない（警告）
    not_found            対象行なし → 適用しない（警告）
    multi_match          対象行が2行以上 → 適用しない（警告）
    duplicate_in_csv     同じ対象への記録がCSVに2件以上 → すべて適用しない（警告）
    invalid              表名・列名・キー不足など → 適用しない（警告）
"""

import argparse
import csv
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]          # .../Hana_research
DB_PATH = ROOT / "data/db/Hana_Research.db"
CSV_PATH = ROOT / "data/raw/corrections/corrections.csv"
OUT_DIR = ROOT / "data/output"   # バックアップは DB と同じフォルダの backup/ に作成

FIELDS = [
    "correction_id", "recorded_at", "project", "table_name",
    "Patient_ID", "Study_ID", "key_date", "key_item",
    "column_name", "old_value", "new_value", "reason",
]

# 行の特定: (ID列, 日付列, 項目列)。None はその要素を使わない
TABLE_KEYS = {
    "Patient_Master":       ("Patient_ID", None, None),
    "event":                ("Patient_ID", None, None),
    "Background_summary":   ("Patient_ID", None, None),
    "Freedocument":         ("Patient_ID", "Date", "document_type"),
    "first_diag":           ("Patient_ID", "date", "diagnosis"),
    "last_one_month_diag":  ("Patient_ID", None, "diagnosis"),
    "intervention_history": ("Patient_ID", "start_date", "category"),
    "karte":                ("Patient_ID", "visit_datetime", None),
    "lab_raw":              ("Patient_ID", "sample_date", "item_name"),
    "lab_clean":            ("Patient_ID", "sample_date", "item_name"),
    "orca_diagnoses":       ("Patient_ID", "start_date", "diagnosis_name"),
    "orca_receipt_details": ("Patient_ID", "service_date", "detail_name"),
    "tolvaptan_study":      ("Patient_ID", None, None),
    "unex_study":           ("Patient_ID", None, None),
    "unexpected_death":     ("Patient_ID", None, None),
    "ef_long_v2":           ("Study_ID", "visit_datetime", "item"),   # Patient_ID 列なし
}

FORBIDDEN = {
    ("event", "death_date"): "死亡日は death_date_override.csv で管理",
    ("event", "last_confirmed_date"): "生存確認日は death_date_override.csv で管理",
}
FORBIDDEN_COLS = {"Patient_ID", "Study_ID"}   # ID自体は修正対象にしない


# ---------------------------------------------------------------- 共通
def _blank(v):
    return v is None or str(v).strip() == ""


def _s(v):
    """CSV に書く文字列。None は空欄"""
    return "" if v is None else str(v).strip()


def _norm(v):
    """比較用。空欄=NULL、数値は数値として比較（1 と 1.0 を同一視）"""
    if _blank(v):
        return None
    s = str(v).strip()
    try:
        return ("n", float(s))
    except ValueError:
        return ("s", s)


def _id_text(pid):
    s = str(pid).strip()
    try:
        return str(int(float(s)))
    except ValueError:
        return s


def _columns(con, table):
    return [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]


def _validate(con, table, column, pid, sid, key_date, key_item):
    """問題があればメッセージを返す。問題なければ None"""
    if table not in TABLE_KEYS:
        return f"未登録の表: {table}"
    if (table, column) in FORBIDDEN:
        return FORBIDDEN[(table, column)]
    if column in FORBIDDEN_COLS:
        return f"{column} は修正対象外"
    if column not in _columns(con, table):
        return f"{table} に列 {column} がない"
    id_col, date_col, item_col = TABLE_KEYS[table]
    if id_col == "Patient_ID" and _blank(pid):
        return "Patient_ID が空"
    if id_col == "Study_ID" and _blank(sid):
        return f"{table} は Study_ID で特定するため Study_ID が必要"
    if date_col and _blank(key_date):
        return f"key_date（{date_col}）が空"
    if item_col and _blank(key_item):
        return f"key_item（{item_col}）が空"
    return None


def _find(con, table, column, pid, sid, key_date, key_item):
    """キーに一致する行の (rowid, 現在値) を返す"""
    id_col, date_col, item_col = TABLE_KEYS[table]
    where, params = [], []
    if id_col == "Patient_ID":
        where.append("CAST(Patient_ID AS TEXT) = ?")
        params.append(_id_text(pid))
    else:
        where.append("Study_ID = ?")
        params.append(str(sid).strip())
    if date_col:
        where.append(f'"{date_col}" = ?')
        params.append(str(key_date).strip())
    if item_col:
        where.append(f'"{item_col}" = ?')
        params.append(str(key_item).strip())
    sql = f'SELECT rowid, "{column}" FROM "{table}" WHERE ' + " AND ".join(where)
    return con.execute(sql, params).fetchall()


def _find_with_fallback(con, table, column, pid, sid, key_date, key_item, new_value):
    """
    通常はキーで検索。修正対象がキー列（日付・項目）自体の場合、
    適用済みなら旧キーでは見つからないので新キーで再検索する
    戻り値: (rows, found_by_new_key)
    """
    rows = _find(con, table, column, pid, sid, key_date, key_item)
    if rows:
        return rows, False
    _, date_col, item_col = TABLE_KEYS[table]
    if column == date_col:
        return _find(con, table, column, pid, sid, new_value, key_item), True
    if column == item_col:
        return _find(con, table, column, pid, sid, key_date, new_value), True
    return [], False


def _target_key(table, pid, sid, key_date, key_item, column):
    id_col = TABLE_KEYS.get(table, ("Patient_ID",))[0]
    ident = _id_text(pid) if id_col == "Patient_ID" else _s(sid)
    return (table, ident, _s(key_date), _s(key_item), column)


def _read_csv(csv_path):
    if not Path(csv_path).exists():
        return []
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------- 記録
def record_correction(project, table, column, old_value, new_value, reason,
                      patient_id=None, study_id=None, key_date=None, key_item=None,
                      db_path=DB_PATH, csv_path=CSV_PATH):
    """
    修正を corrections.csv に1行追記する（DBは変更しない）

    key_date / key_item は DB に入っている文字列をそのまま渡す
    （例: karte.visit_datetime は "2016/3/31(木) 17:40"）
    old_value / new_value の None・空欄は NULL を意味する
    """
    if _blank(reason):
        raise ValueError("reason（修正理由）は必須")

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        msg = _validate(con, table, column, patient_id, study_id, key_date, key_item)
        if msg:
            raise ValueError(msg)

        rows, _ = _find_with_fallback(con, table, column, patient_id, study_id,
                                      key_date, key_item, new_value)
        if not rows:
            raise ValueError("DB に対象行が見つからない（キーの値を確認）")
        if len(rows) > 1:
            print(f"[警告] 対象行が {len(rows)} 行ある。適用時は multi_match で適用されない")

        # ID の補完（Patient_ID が正、Study_ID は参照用）
        id_col = TABLE_KEYS[table][0]
        if id_col == "Patient_ID":
            pid = _id_text(patient_id)
            r = con.execute(f'SELECT Study_ID FROM "{table}" WHERE CAST(Patient_ID AS TEXT) = ? '
                            f'AND Study_ID IS NOT NULL LIMIT 1', [pid]).fetchone()
            sid = r[0] if r else (_s(study_id) or "")
        else:
            sid = str(study_id).strip()
            pid = _id_text(patient_id) if not _blank(patient_id) else ""
            if not pid:
                for t in ("Patient_Master", "event"):
                    r = con.execute(f'SELECT Patient_ID FROM "{t}" WHERE Study_ID = ? LIMIT 1',
                                    [sid]).fetchone()
                    if r:
                        pid = _id_text(r[0])
                        break
    finally:
        con.close()

    # CSV 内の重複チェック
    key = _target_key(table, pid, sid, key_date, key_item, column)
    for r in _read_csv(csv_path):
        if _target_key(r["table_name"], r["Patient_ID"], r["Study_ID"], r["key_date"],
                       r["key_item"], r["column_name"]) == key:
            raise ValueError(f"同じ対象の記録が既にある（correction_id={r['correction_id']}）。"
                             "CSV を直接編集すること")

    row = {
        "correction_id": datetime.now().strftime("%Y%m%d") + "-" + uuid.uuid4().hex[:8],
        "recorded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "project": project,
        "table_name": table,
        "Patient_ID": pid,
        "Study_ID": sid,
        "key_date": _s(key_date),
        "key_item": _s(key_item),
        "column_name": column,
        "old_value": _s(old_value),
        "new_value": _s(new_value),
        "reason": _s(reason),
    }

    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not csv_path.exists() or csv_path.stat().st_size == 0
    # 新規は BOM 付き、追記は BOM なし（途中に BOM が入るのを防ぐ）
    with open(csv_path, "a", encoding="utf-8-sig" if is_new else "utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if is_new:
            w.writeheader()
        w.writerow(row)
    print(f"記録: {row['correction_id']}  {table}.{column}  {row['old_value']!r} → {row['new_value']!r}")
    return row["correction_id"]


# ---------------------------------------------------------------- 適用
def apply_corrections(commit=False, db_path=DB_PATH, csv_path=CSV_PATH, out_dir=OUT_DIR):
    records = _read_csv(csv_path)
    if not records:
        print(f"記録なし: {csv_path}")
        return

    # CSV 内の重複
    counts = {}
    for r in records:
        k = _target_key(r["table_name"], r["Patient_ID"], r["Study_ID"],
                        r["key_date"], r["key_item"], r["column_name"])
        counts[k] = counts.get(k, 0) + 1

    if commit:
        bdir = Path(db_path).parent / "backup"
        bdir.mkdir(parents=True, exist_ok=True)
        bpath = bdir / f"Hana_Research_before_corrections_{datetime.now():%Y%m%d_%H%M%S}.db"
        src, dst = sqlite3.connect(db_path), sqlite3.connect(bpath)
        src.backup(dst)
        dst.close()
        src.close()
        print(f"バックアップ: {bpath}")

    con = sqlite3.connect(db_path)
    results = []
    try:
        for r in records:
            t, col = r["table_name"], r["column_name"]
            res = dict(r, status="", n_rows="", current_value="", note="")

            msg = _validate(con, t, col, r["Patient_ID"], r["Study_ID"], r["key_date"], r["key_item"])
            if msg:
                res.update(status="invalid", note=msg)
                results.append(res)
                continue

            k = _target_key(t, r["Patient_ID"], r["Study_ID"], r["key_date"], r["key_item"], col)
            if counts[k] > 1:
                res.update(status="duplicate_in_csv")
                results.append(res)
                continue

            rows, by_new = _find_with_fallback(con, t, col, r["Patient_ID"], r["Study_ID"],
                                               r["key_date"], r["key_item"], r["new_value"])
            res["n_rows"] = len(rows)
            if not rows:
                res["status"] = "not_found"
            elif len(rows) > 1:
                res["status"] = "multi_match"
            else:
                rowid, cur = rows[0]
                res["current_value"] = _s(cur)
                if by_new or _norm(cur) == _norm(r["new_value"]):
                    res["status"] = "already_applied"
                elif _norm(cur) == _norm(r["old_value"]):
                    if commit:
                        newv = None if _blank(r["new_value"]) else r["new_value"]
                        con.execute(f'UPDATE "{t}" SET "{col}" = ? WHERE rowid = ?', [newv, rowid])
                        res["status"] = "applied"
                    else:
                        res["status"] = "to_apply"
                else:
                    res["status"] = "mismatch"
                    res["note"] = "元データが old_value と異なる（元データ側の修正・変更の可能性）"
            results.append(res)

        if commit:
            con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    # 結果出力
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    mode = "commit" if commit else "dryrun"
    out = Path(out_dir) / f"corrections_result_{mode}_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS + ["status", "n_rows", "current_value", "note"])
        w.writeheader()
        w.writerows(results)

    summary = {}
    for x in results:
        summary[x["status"]] = summary.get(x["status"], 0) + 1
    print(f"[{mode}] 記録 {len(results)} 件: " + ", ".join(f"{k}={v}" for k, v in sorted(summary.items())))
    warn = [x for x in results if x["status"] in
            ("mismatch", "not_found", "multi_match", "duplicate_in_csv", "invalid")]
    for x in warn:
        print(f"  [警告] {x['status']:<16} {x['correction_id']}  {x['table_name']}.{x['column_name']}  "
              f"PID={x['Patient_ID']}  {x['note']}")
    print(f"結果: {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="corrections.csv の修正を研究DBに再適用する")
    ap.add_argument("--commit", action="store_true", help="DBバックアップ後に実際に書き換える")
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--csv", default=str(CSV_PATH))
    a = ap.parse_args()
    apply_corrections(commit=a.commit, db_path=a.db, csv_path=a.csv)
