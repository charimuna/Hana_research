import sqlite3
from pathlib import Path

DB = "/Users/muna/Hana_research/data/db/Hana_Research.db"
OUT = Path("/Users/muna/Hana_research/docs/schema.md")

con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
tables = [r[0] for r in con.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]

lines = ["# Hana_Research.db スキーマ辞書", "", f"DB: `{DB}`", ""]
for t in tables:
    n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
    lines += [f"## {t}（{n:,}行）", "", "目的: ", "",
              "| 列 | 型 | PK | 意味・コード |", "|---|---|---|---|"]
    for cid, name, typ, notnull, dflt, pk in con.execute(f'PRAGMA table_info("{t}")'):
        lines.append(f"| {name} | {typ} | {'○' if pk else ''} |  |")
    lines.append("")
con.close()

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(lines), encoding="utf-8")
print(f"{len(tables)} tables -> {OUT}")