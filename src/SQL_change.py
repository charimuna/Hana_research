import sqlite3

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
SELECT COUNT(DISTINCT patient_ID)
FROM Patient_Master;
""")

print(cursor.fetchone())

conn.close()