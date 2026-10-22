#!/usr/bin/env python3
"""
sync_study_id.py — 研究DBの Study_ID をリンケージDB（正本）に同期する

いつ実行するか
  DBへの取り込み・作り直しを行うスクリプトを実行した後、毎回これを実行する。
  何度実行しても結果は同じ（冪等）。行は一切削除しない。

処理
  0. Freedocument.Patients_ID を Patient_ID に改名（未改名のときのみ）
  1. 研究DB内の古い対応表 study_id_linkage を削除（存在するときのみ）
  2. Patient_ID 列があり Study_ID 列がないテーブルに Study_ID 列を追加
  3. Patient_ID 列があるテーブル: Patient_ID -> リンケージ -> Study_ID
       リンケージにない Patient_ID（ダミーID等）の行は Study_ID = NULL
  4. Patient_ID 列がないテーブル（ef_long, ef_long_v2 等）:
       現 Study_ID -> Patient_ID（リンケージ＋手順3前の各表の対応）-> リンケージの Study_ID
       対応付けできない Study_ID は変更せず報告する
  5. 検証。既定はドライラン（最後にロールバック）、--commit で確定
  リンケージDBは読み取り専用で開く。

使い方
  python sync_study_id.py            # ドライラン（DBは変わらない）
  python sync_study_id.py --commit   # 確定（実行前に --db のバックアップを取っておくこと）
"""

import argparse
import os
import sqlite3
import sys
import time
from urllib.parse import quote

DEFAULT_DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
DEFAULT_LINKAGE = "/Volumes/linkage_secure/hana_linkage.db"
OLD_LINKAGE_TABLE = "study_id_linkage"
RENAMES = {"Freedocument": ("Patients_ID", "Patient_ID")}


def q(name):
    return '"' + name.replace('"', '""') + '"'


def fail(msg):
    raise SystemExit(f"ERROR: {msg}")


def columns(conn, t):
    return [r[1] for r in conn.execute(f"PRAGMA main.table_info({q(t)})")]


def find(cols, name):
    low = {c.lower(): c for c in cols}
    return low.get(name.lower())


def user_tables(conn):
    return [r[0] for r in conn.execute(
        "SELECT name FROM main.sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--linkage", default=DEFAULT_LINKAGE)
    ap.add_argument("--commit", action="store_true",
                    help="指定しなければドライラン（最後にロールバック）")
    ap.add_argument("--journal-mode", default=None,
                    help="SQLiteのjournal_mode（通常は指定不要）")
    args = ap.parse_args()

    if not os.path.isfile(args.db):
        fail(f"研究DBが見つかりません: {args.db}")
    if not os.path.isfile(args.linkage):
        fail(f"リンケージDBが見つかりません（ボリューム未マウント？）: {args.linkage}")

    print(f"モード     : {'本番実行(--commit)' if args.commit else 'ドライラン（DBは変更されません）'}")
    print(f"研究DB     : {args.db}")
    print(f"リンケージ : {args.linkage}（読み取り専用）")

    conn = sqlite3.connect(f"file:{quote(args.db)}?mode=rw", uri=True,
                           isolation_level=None)
    if args.journal_mode:
        conn.execute(f"PRAGMA journal_mode={args.journal_mode}")
    conn.execute("ATTACH DATABASE ? AS lk", (f"file:{quote(args.linkage)}?mode=ro",))
    try:
        run(conn, args)
    finally:
        conn.close()


def run(conn, args):
    t0 = time.time()
    lk_n = conn.execute("SELECT COUNT(*) FROM lk.linkage").fetchone()[0]
    if lk_n == 0:
        fail("リンケージが空です")
    dup = conn.execute("SELECT COUNT(*) - COUNT(DISTINCT Study_ID) FROM lk.linkage").fetchone()[0]
    if dup:
        fail("リンケージの Study_ID に重複があります")
    print(f"リンケージ : {lk_n:,}人")

    conn.execute("BEGIN IMMEDIATE")
    try:
        # ---------- 0. 列名の統一 ----------
        for t, (old, new) in RENAMES.items():
            if t in user_tables(conn):
                cols = columns(conn, t)
                if old in cols and find(cols, new) is None:
                    conn.execute(f"ALTER TABLE main.{q(t)} RENAME COLUMN {q(old)} TO {q(new)}")
                    print(f"\n改名: {t}.{old} -> {new}")

        # ---------- 1. 古い対応表の削除 ----------
        if OLD_LINKAGE_TABLE in user_tables(conn):
            conn.execute(f"DROP TABLE main.{q(OLD_LINKAGE_TABLE)}")
            print(f"削除: テーブル {OLD_LINKAGE_TABLE}（リンケージDBが正本）")

        # ---------- 対象テーブルの検出 ----------
        pid_tables, sid_only = [], []   # (table, pid_col, sid_col) / (table, sid_col)
        added = []
        for t in user_tables(conn):
            cols = columns(conn, t)
            pc, sc = find(cols, "Patient_ID"), find(cols, "Study_ID")
            if pc and not sc:
                conn.execute(f"ALTER TABLE main.{q(t)} ADD COLUMN Study_ID TEXT")
                sc = "Study_ID"
                added.append(t)
            if pc:
                pid_tables.append((t, pc, sc))
            elif sc:
                sid_only.append((t, sc))
        if added:
            print("Study_ID 列を追加: " + ", ".join(added))

        rows_before = {t: conn.execute(f"SELECT COUNT(*) FROM main.{q(t)}").fetchone()[0]
                       for t, *_ in pid_tables + sid_only}

        # ---------- 手順3前の Study_ID -> Patient_ID 対応（Patient_IDなしの表用） ----------
        conn.execute("DROP TABLE IF EXISTS temp._sid_pid")
        conn.execute("CREATE TEMP TABLE _sid_pid (sid TEXT, pid INTEGER)")
        conn.execute("INSERT INTO temp._sid_pid SELECT Study_ID, Patient_ID FROM lk.linkage")
        for t, pc, sc in pid_tables:
            conn.execute(
                f"INSERT INTO temp._sid_pid SELECT DISTINCT {q(sc)}, CAST({q(pc)} AS INTEGER) "
                f"FROM main.{q(t)} WHERE {q(sc)} IS NOT NULL AND {q(pc)} IS NOT NULL")
        conn.execute("DROP TABLE IF EXISTS temp._sid_map")
        conn.execute("CREATE TEMP TABLE _sid_map AS SELECT DISTINCT sid, pid FROM temp._sid_pid")
        amb = conn.execute("SELECT sid FROM temp._sid_map GROUP BY sid HAVING COUNT(*) > 1 "
                           "LIMIT 5").fetchall()
        if amb:
            fail(f"同じ Study_ID が複数の Patient_ID に付いています（例: {[a[0] for a in amb]}）。中止します")
        conn.execute("CREATE UNIQUE INDEX temp._ix_sid_map ON _sid_map(sid)")

        # ---------- 3. Patient_ID のある表 ----------
        print("\nPatient_ID から付与:")
        print(f"  {'table':24s} {'rows':>11s} {'更新行':>10s} {'NULL(対象外ID)':>14s} {'秒':>6s}")
        lk_sid = "(SELECT l.Study_ID FROM lk.linkage l WHERE l.Patient_ID = CAST({a}{p} AS INTEGER))"
        for t, pc, sc in pid_tables:
            s = time.time()
            T, P, S = q(t), q(pc), q(sc)
            n = conn.execute(
                f"UPDATE main.{T} SET {S} = {lk_sid.format(a=T + '.', p=P)} "
                f"WHERE {S} IS NOT {lk_sid.format(a=T + '.', p=P)}").rowcount
            n_null = conn.execute(f"SELECT COUNT(*) FROM main.{T} WHERE {S} IS NULL").fetchone()[0]
            print(f"  {t:24s} {rows_before[t]:>11,} {n:>10,} {n_null:>14,} {time.time()-s:>6.0f}")

        # ---------- 4. Patient_ID のない表 ----------
        if sid_only:
            print("\nStudy_ID から対応付け（Patient_ID 列なし）:")
        unmapped = {}
        for t, sc in sid_only:
            T, S = q(t), q(sc)
            n = conn.execute(
                f"UPDATE main.{T} SET {S} = (SELECT l.Study_ID FROM temp._sid_map m "
                f"JOIN lk.linkage l ON l.Patient_ID = m.pid WHERE m.sid = {T}.{S}) "
                f"WHERE {S} IN (SELECT m.sid FROM temp._sid_map m JOIN lk.linkage l "
                f"ON l.Patient_ID = m.pid WHERE m.sid != l.Study_ID)").rowcount
            um = conn.execute(
                f"SELECT COUNT(DISTINCT {S}), COUNT(*) FROM main.{T} WHERE {S} IS NOT NULL "
                f"AND {S} NOT IN (SELECT Study_ID FROM lk.linkage)").fetchone()
            unmapped[t] = um
            print(f"  {t:24s} {rows_before[t]:>11,} 行 / 更新 {n:,} 行 / "
                  f"リンケージにないID {um[0]:,}種類 {um[1]:,}行（変更せず）")

        # ---------- 5. 検証 ----------
        errs = []
        for t, pc, sc in pid_tables:
            T, P, S = q(t), q(pc), q(sc)
            rows = conn.execute(f"SELECT COUNT(*) FROM main.{T}").fetchone()[0]
            if rows != rows_before[t]:
                errs.append(f"{t}: 行数が変化 {rows_before[t]} -> {rows}")
            bad = conn.execute(
                f"SELECT COUNT(*) FROM main.{T} AS x WHERE x.{S} IS NOT "
                f"{lk_sid.format(a='x.', p=P)}").fetchone()[0]
            if bad:
                errs.append(f"{t}: リンケージと一致しない行 {bad}")
        for t, sc in sid_only:
            rows = conn.execute(f"SELECT COUNT(*) FROM main.{q(t)}").fetchone()[0]
            if rows != rows_before[t]:
                errs.append(f"{t}: 行数が変化 {rows_before[t]} -> {rows}")
        if errs:
            raise RuntimeError("検証NG:\n  - " + "\n  - ".join(errs))
        print("\n検証: OK（行数不変・Patient_ID のある全表でリンケージと完全一致）")

        if args.commit:
            conn.execute("COMMIT")
            print(f"コミットしました。（{time.time()-t0:.0f}秒）")
        else:
            conn.execute("ROLLBACK")
            print(f"ドライランのためロールバックしました。（{time.time()-t0:.0f}秒）"
                  "\n問題なければ --commit を付けて再実行してください。")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        print("\nロールバックしました。DBは変更されていません。", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
