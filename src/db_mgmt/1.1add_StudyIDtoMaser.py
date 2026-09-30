import sqlite3

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
conn = sqlite3.connect(db_path)
try:
    # Study_ID の重複を防ぐ(NULLは複数あってもよい)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_background_summary_study_id "
        "ON Background_summary(Study_ID)"
    )

    conn.execute("BEGIN")

    # 既存の最大番号(P000123 -> 123)
    max_no = conn.execute(
        "SELECT MAX(CAST(SUBSTR(Study_ID, 2) AS INTEGER)) "
        "FROM Background_summary WHERE Study_ID GLOB 'P[0-9]*'"
    ).fetchone()[0] or 0

    # 未付与の患者(Patient_ID の昇順)
    targets = [r[0] for r in conn.execute(
        "SELECT Patient_ID FROM Background_summary "
        "WHERE Study_ID IS NULL ORDER BY Patient_ID"
    )]

    updates = [(f"P{max_no + i:06d}", pid) for i, pid in enumerate(targets, start=1)]
    conn.executemany(
        "UPDATE Background_summary SET Study_ID = ? "
        "WHERE Patient_ID = ? AND Study_ID IS NULL",
        updates,
    )

    # 検証
    n_null = conn.execute(
        "SELECT COUNT(*) FROM Background_summary WHERE Study_ID IS NULL"
    ).fetchone()[0]
    if n_null != 0:
        raise RuntimeError(f"Study_ID が未付与のまま: {n_null}件")

    conn.commit()
    print(f"付与: {len(updates)}件 (P{max_no + 1:06d} ~ P{max_no + len(updates):06d})")
except Exception:
    conn.rollback()
    raise
finally:
    conn.close()