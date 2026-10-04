#!/usr/bin/env python3
"""
Hana Research: 研究DB上の旧 Study_ID を、リンケージDBの新 Study_ID に一括変換する

方針
  - Patient_ID 列を持つテーブル:
        Patient_ID -> リンケージ -> 新 Study_ID を直接付与する（旧Study_IDは見ない）。
        旧Study_IDが壊れていても（Patient_Master にない / Patient_ID と食い違い）影響しない。
        Study_ID が NULL の行も、Patient_ID がリンケージにあれば新IDで埋める。
  - Patient_ID 列を持たないテーブル（Freedocument, ef_long など）:
        旧Study_ID -> (Patient_Master) -> Patient_ID -> リンケージ -> 新Study_ID
  - 対応するStudy_IDがない Patient_ID（既定: 150000）は、Patient_ID 列/Study_ID 列を
    持つ全テーブルから該当行を削除する（--drop-patient-id で変更可）
  - 新旧IDは同じ範囲で重なり得るため、2段階で書き換える（旧 -> TMP_MIG_新 -> 新）
  - リンケージDBは読み取り専用で開く（このスクリプトからは一切変更しない）
  - リンケージにない患者の行（新IDを付けられない行）は、既定では中止。
    --null-unlinked を付けると、それらの Study_ID を NULL にして続行する
  - 既定はドライラン（すべて実行して検証し、最後にロールバック）。--commit で確定

使い方
  python migrate_study_id.py                  # ドライラン（DBは変わらない）
  python migrate_study_id.py --commit         # バックアップ後に本番実行
  オプション: --null-unlinked（リンケージにない行のStudy_IDをNULL化）/
              --delete-unlinked（リンケージにない行を行ごと削除）
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from urllib.parse import quote

DEFAULT_DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
DEFAULT_LINKAGE = "/Volumes/linkage_secure/hana_linkage.db"
TMP_PREFIX = "TMP_MIG_"


def q(name):
    return '"' + name.replace('"', '""') + '"'


def fail(msg):
    raise SystemExit(f"ERROR: {msg}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--linkage", default=DEFAULT_LINKAGE)
    ap.add_argument("--commit", action="store_true",
                    help="指定しなければドライラン（最後にロールバック）")
    ap.add_argument("--drop-patient-id", type=int, action="append",
                    help="対応する Study_ID がないため全テーブルから行を削除する "
                         "Patient_ID（複数指定可。既定: 150000）")
    ap.add_argument("--null-unlinked", action="store_true",
                    help="リンケージにない患者の行の Study_ID を NULL にして続行する")
    ap.add_argument("--delete-unlinked", action="store_true",
                    help="リンケージにない患者の行（Study_IDが入っているもの）を行ごと削除して続行する")
    args = ap.parse_args()

    if not os.path.isfile(args.db):
        fail(f"研究DBが見つかりません: {args.db}")
    if not os.path.isfile(args.linkage):
        fail(f"リンケージDBが見つかりません（ボリューム未マウント？）: {args.linkage}")

    mode = "本番実行(--commit)" if args.commit else "ドライラン（DBは変更されません）"
    print(f"モード: {mode}")
    print(f"研究DB     : {args.db}")
    print(f"リンケージ : {args.linkage} (読み取り専用)")

    conn = sqlite3.connect(f"file:{quote(args.db)}?mode=rw", uri=True,
                           isolation_level=None)
    conn.execute("ATTACH DATABASE ? AS lk",
                 (f"file:{quote(args.linkage)}?mode=ro",))
    try:
        run(conn, args)
    finally:
        conn.close()


def run(conn, args):
    allow_unlinked = args.null_unlinked or args.delete_unlinked
    # ---------------- Study_ID / Patient_ID 列を持つテーブルを自動検出 ----------------
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM main.sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    targets = []       # (table, study_col, patient_col or None)  ※Study_ID列あり
    drop_targets = []  # (table, patient_col or None, study_col or None)
    for t in tables:
        cols = [r[1] for r in conn.execute(f"PRAGMA main.table_info({q(t)})")]
        low = {c.lower(): c for c in cols}
        if "study_id" in low:
            targets.append((t, low["study_id"], low.get("patient_id")))
        if "study_id" in low or "patient_id" in low:
            drop_targets.append((t, low.get("patient_id"), low.get("study_id")))
    if not any(t == "Patient_Master" for t, _, _ in targets):
        fail("Patient_Master に Study_ID 列がありません")
    print("\nStudy_ID を持つテーブル:")
    for t, c, p in targets:
        n = conn.execute(f"SELECT COUNT(*) FROM main.{q(t)}").fetchone()[0]
        how = "Patient_ID から付与" if p else "旧Study_IDから対応付け"
        print(f"  - {t:28s} {n:>10,} 行  ({how})")

    # ---------------- リンケージ / 削除対象 ----------------
    lk = {int(p): s for p, s in conn.execute(
        "SELECT Patient_ID, Study_ID FROM lk.linkage")}
    if not lk:
        fail("リンケージが空です")

    drop_ids = sorted(set(args.drop_patient_id or [150000]))
    in_link = [p for p in drop_ids if p in lk]
    if in_link:
        fail(f"削除対象の Patient_ID がリンケージに存在します: {in_link}（削除せず中止）")

    # Patient_Master: 旧Study_ID -> Patient_ID（Patient_IDを持たないテーブル用）
    drop_old = set()
    old_to_pid = {}
    for p, old in conn.execute('SELECT Patient_ID, Study_ID FROM main."Patient_Master"'):
        if p is None:
            continue
        p = int(p)
        if p in drop_ids:
            if old is not None:
                drop_old.add(old)
            continue
        if old is None:
            continue
        if old in old_to_pid:
            fail(f"Patient_Master の旧 Study_ID が重複: {old} "
                 f"(Patient_ID {old_to_pid[old]} と {p})")
        old_to_pid[old] = p
    mapping = [(o, lk[p], p) for o, p in old_to_pid.items() if p in lk]
    conn.execute("DROP TABLE IF EXISTS temp._sid_map")
    conn.execute("CREATE TEMP TABLE _sid_map ("
                 "old_id TEXT PRIMARY KEY, new_id TEXT NOT NULL, "
                 "Patient_ID INTEGER NOT NULL)")
    conn.executemany("INSERT INTO temp._sid_map VALUES (?,?,?)", mapping)
    print(f"\nリンケージ: {len(lk):,}人 / 旧→新の対応表(Patient_Master経由): {len(mapping):,}人")

    # ---------------- 削除条件 ----------------
    drop_old_list = sorted(drop_old)
    old_ph = ",".join("?" * len(drop_old_list))
    id_ph = ",".join("?" * len(drop_ids))

    def drop_expr(pc, sc, a=""):
        """削除対象行なら1になる式（NULLを返さない）と、そのパラメータ"""
        terms, params = [], []
        if pc:
            terms.append(f"COALESCE(CAST({a}{q(pc)} AS INTEGER),-1) IN ({id_ph})")
            params += drop_ids
        if sc and drop_old_list:
            terms.append(f"COALESCE({a}{q(sc)},'') IN ({old_ph})")
            params += drop_old_list
        return ("(" + " OR ".join(terms) + ")", params) if terms else ("0", [])

    def count_drop_rows():
        total = 0
        for t, pc, sc in drop_targets:
            e, pr = drop_expr(pc, sc)
            total += conn.execute(
                f"SELECT COUNT(*) FROM main.{q(t)} WHERE {e}", pr).fetchone()[0]
        return total

    n_drop = count_drop_rows()
    print(f"削除対象 Patient_ID: {drop_ids}（旧Study_ID: {drop_old_list or 'なし'}）"
          f" / 該当行 合計 {n_drop:,} 行")

    # ---------------- 事前チェック ----------------
    pids_sub = "SELECT Patient_ID FROM lk.linkage"
    problems = []
    notes = []
    need_change = n_drop
    unlinked_rows = {}      # table -> 新IDを付けられない（Study_IDあり）行数
    orphan_distinct = {}    # Patient_IDなしテーブル: 対応付けできない旧IDの種類数
    for t, c, p in targets:
        T = q(t)
        de, dp = drop_expr(p, c, "x.")
        keep = f"NOT {de}"
        sc = f"x.{q(c)}"
        if p:
            cp = f"CAST(x.{q(p)} AS INTEGER)"
            n_unl = conn.execute(
                f"SELECT COUNT(*) FROM main.{T} AS x WHERE {keep} AND {sc} IS NOT NULL "
                f"AND ({cp} IS NULL OR {cp} NOT IN ({pids_sub}))", dp).fetchone()[0]
            if n_unl:
                unlinked_rows[t] = n_unl
                ex = [r[0] for r in conn.execute(
                    f"SELECT DISTINCT x.{q(p)} FROM main.{T} AS x WHERE {keep} "
                    f"AND {sc} IS NOT NULL AND ({cp} IS NULL OR {cp} NOT IN ({pids_sub})) "
                    f"LIMIT 5", dp)]
                msg = f"{t}: リンケージにない患者の行が {n_unl:,}行 (Patient_ID例={ex})"
                (notes if allow_unlinked else problems).append(msg)
            n_mis = conn.execute(
                f"SELECT COUNT(*) FROM main.{T} AS x "
                f"JOIN temp._sid_map m ON m.old_id = {sc} "
                f"WHERE {keep} AND {cp} != m.Patient_ID", dp).fetchone()[0]
            if n_mis:
                notes.append(f"{t}: Patient_ID と旧Study_IDが食い違う行 {n_mis:,}行"
                             "（Patient_IDを優先して新IDを付与）")
            need_change += conn.execute(
                f"SELECT COUNT(*) FROM main.{T} AS x WHERE {keep} AND ("
                f"({cp} IN ({pids_sub}) AND ({sc} IS NULL OR {sc} != "
                f"(SELECT l.Study_ID FROM lk.linkage l WHERE l.Patient_ID = {cp}))) "
                f"OR ({sc} IS NOT NULL AND ({cp} IS NULL OR {cp} NOT IN ({pids_sub}))))",
                dp).fetchone()[0]
        else:
            orph = f"{keep} AND {sc} IS NOT NULL AND {sc} NOT IN (SELECT old_id FROM temp._sid_map)"
            n_o, d_o = conn.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT {sc}) FROM main.{T} AS x WHERE {orph}",
                dp).fetchone()
            if n_o:
                orphan_distinct[t] = d_o
                unlinked_rows[t] = n_o
                ex = [r[0] for r in conn.execute(
                    f"SELECT DISTINCT {sc} FROM main.{T} AS x WHERE {orph} LIMIT 5", dp)]
                msg = (f"{t}: Patient_Master/リンケージで対応付けできない Study_ID が "
                       f"{d_o:,}種類 {n_o:,}行 (例={ex})")
                (notes if allow_unlinked else problems).append(msg)
            need_change += conn.execute(
                f"SELECT COUNT(*) FROM main.{T} AS x WHERE {keep} AND {sc} IN "
                f"(SELECT old_id FROM temp._sid_map WHERE old_id != new_id)",
                dp).fetchone()[0]
            need_change += n_o if allow_unlinked else 0
    if problems:
        fail("事前チェックで問題が見つかりました（DBは変更していません）:\n  - "
             + "\n  - ".join(problems)
             + "\n  → これらの行の Study_ID を NULL にして続行するなら --null-unlinked、"
               "行ごと削除するなら --delete-unlinked を付けて再実行してください")
    print("事前チェック: OK")
    for n in notes:
        print(f"  注意: {n}")
    if allow_unlinked and unlinked_rows:
        if args.delete_unlinked:
            print("  --delete-unlinked: 上記のリンケージにない行は行ごと削除します")
        else:
            print("  --null-unlinked: 上記のリンケージにない行の Study_ID は NULL にします")


    if need_change == 0:
        print("\n変更が必要な行はありません（移行済み）。何もしません。")
        return

    # ---------------- バックアップ（本番のみ） ----------------
    backup_path = None
    if args.commit:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{args.db}.bak_before_study_id_migration_{stamp}"
        dst = sqlite3.connect(backup_path)
        try:
            conn.backup(dst, name="main")
        finally:
            dst.close()
        print(f"バックアップ: {backup_path}")

    # ---------------- 変換（1トランザクション） ----------------
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 対応するStudy_IDがない患者の行を全テーブルから削除
        print("\n削除（対応するStudy_IDがない患者）:")
        for t, pc, sc in drop_targets:
            e, pr = drop_expr(pc, sc)
            n = conn.execute(f"DELETE FROM main.{q(t)} WHERE {e}", pr).rowcount
            if n:
                print(f"  - {t:28s} {n:>10,} 行 削除")

        if args.delete_unlinked:
            print("\n削除（リンケージにない患者の行）:")
            for t, c, p in targets:
                T, C = q(t), q(c)
                if p:
                    cp = f"CAST({T}.{q(p)} AS INTEGER)"
                    cond = (f"{C} IS NOT NULL AND ({cp} IS NULL "
                            f"OR {cp} NOT IN ({pids_sub}))")
                else:
                    cond = (f"{C} IS NOT NULL AND {C} NOT IN "
                            f"(SELECT old_id FROM temp._sid_map)")
                n = conn.execute(f"DELETE FROM main.{T} WHERE {cond}").rowcount
                if n:
                    print(f"  - {t:28s} {n:>10,} 行 削除")

        before = {}
        for t, c, _ in targets:
            before[t] = (
                conn.execute(f"SELECT COUNT(*) FROM main.{q(t)}").fetchone()[0],
                conn.execute(f"SELECT COUNT(DISTINCT {q(c)}) FROM main.{q(t)}").fetchone()[0])

        L = len(TMP_PREFIX)
        for t, c, p in targets:
            T, C = q(t), q(c)
            # 段階1: 新IDに中間接頭辞を付けて書き込む
            if p:
                cp = f"CAST({T}.{q(p)} AS INTEGER)"
                conn.execute(
                    f"UPDATE main.{T} SET {C} = ? || "
                    f"(SELECT l.Study_ID FROM lk.linkage l WHERE l.Patient_ID = {cp}) "
                    f"WHERE {cp} IN ({pids_sub})", (TMP_PREFIX,))
            else:
                conn.execute(
                    f"UPDATE main.{T} SET {C} = ? || "
                    f"(SELECT m.new_id FROM temp._sid_map m WHERE m.old_id = {T}.{C}) "
                    f"WHERE {C} IN (SELECT old_id FROM temp._sid_map)", (TMP_PREFIX,))
            # 新IDを付けられなかった行（旧IDのまま残っているもの）はNULLにする
            # （--null-unlinked なしの場合は事前チェックで中止済みなので0行）
            conn.execute(
                f"UPDATE main.{T} SET {C} = NULL "
                f"WHERE {C} IS NOT NULL AND substr({C}, 1, ?) != ?", (L, TMP_PREFIX))
            # 段階2: 中間接頭辞を外す
            conn.execute(
                f"UPDATE main.{T} SET {C} = substr({C}, ?) "
                f"WHERE substr({C}, 1, ?) = ?", (L + 1, L, TMP_PREFIX))

        # ---------------- 事後検証 ----------------
        errs = []
        print("\n検証結果:")
        print(f"  {'table':28s} {'rows':>10s} {'distinct ID':>12s} {'ID NULL':>9s}  結果")
        for t, c, p in targets:
            T, C = q(t), q(c)
            rows = conn.execute(f"SELECT COUNT(*) FROM main.{T}").fetchone()[0]
            dist = conn.execute(f"SELECT COUNT(DISTINCT {C}) FROM main.{T}").fetchone()[0]
            n_null = conn.execute(
                f"SELECT COUNT(*) FROM main.{T} WHERE {C} IS NULL").fetchone()[0]
            b_rows, b_dist = before[t]
            ok = True
            if rows != b_rows:
                errs.append(f"{t}: 行数が変化 {b_rows}->{rows}")
                ok = False
            tmp_left = conn.execute(
                f"SELECT COUNT(*) FROM main.{T} WHERE substr({C},1,?)=?",
                (L, TMP_PREFIX)).fetchone()[0]
            if tmp_left:
                errs.append(f"{t}: 中間値(TMP_MIG_)が {tmp_left}行 残っている")
                ok = False
            not_new = conn.execute(
                f"SELECT COUNT(*) FROM main.{T} WHERE {C} IS NOT NULL "
                f"AND {C} NOT IN (SELECT Study_ID FROM lk.linkage)").fetchone()[0]
            if not_new:
                errs.append(f"{t}: リンケージにない Study_ID が {not_new}行")
                ok = False
            if p:
                cp = f"CAST(x.{q(p)} AS INTEGER)"
                bad = conn.execute(
                    f"SELECT COUNT(*) FROM main.{T} AS x "
                    f"JOIN lk.linkage l ON l.Study_ID = x.{C} "
                    f"WHERE {cp} != l.Patient_ID").fetchone()[0]
                if bad:
                    errs.append(f"{t}: Patient_ID と新Study_IDがリンケージと不一致 {bad}行")
                    ok = False
                miss = conn.execute(
                    f"SELECT COUNT(*) FROM main.{T} AS x WHERE x.{C} IS NULL "
                    f"AND {cp} IN ({pids_sub})").fetchone()[0]
                if miss:
                    errs.append(f"{t}: リンケージにいる患者なのに Study_ID が NULL の行 {miss}")
                    ok = False
            else:
                expected = b_dist - (0 if args.delete_unlinked
                                     else orphan_distinct.get(t, 0))
                if dist != expected:
                    errs.append(f"{t}: Study_ID種類数が不一致 期待{expected} 実際{dist}")
                    ok = False
            print(f"  {t:28s} {rows:>10,} {dist:>12,} {n_null:>9,}  {'OK' if ok else 'NG'}")
        if errs:
            raise RuntimeError("検証NG:\n  - " + "\n  - ".join(errs))

        if args.commit:
            conn.execute("COMMIT")
            print("\nコミットしました。")
        else:
            conn.execute("ROLLBACK")
            print("\nドライランのためロールバックしました（DBは変更されていません）。"
                  "\n問題なければ --commit を付けて再実行してください。")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        print("\nロールバックしました。DBは変更されていません。", file=sys.stderr)
        raise

    if args.commit:
        print("\n次にやること:")
        print("  - step4/step5 などDB由来のCSV出力は旧IDのままなので再生成する")
        print("  - 旧IDで共有したファイルがあれば回収・差し替えを検討する")
        print(f"  - バックアップ({backup_path})には旧IDが残っているので、"
              "保管先と削除時期を決める")


if __name__ == "__main__":
    main()