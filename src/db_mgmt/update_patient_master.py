import sqlite3
from pathlib import Path

db_path = Path("/Users/muna/Hana_research/data/db/Hana_Research.db")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 不足IDを追加
cursor.execute("""
INSERT INTO Patient_master (Patient_ID)
SELECT DISTINCT f.Patients_ID
FROM Freedocument f
LEFT JOIN Patient_master p
ON f.Patients_ID = p.Patient_ID
WHERE p.Patient_ID IS NULL
""")

conn.commit()
conn.close()

print("Done: Patient_master updated.")