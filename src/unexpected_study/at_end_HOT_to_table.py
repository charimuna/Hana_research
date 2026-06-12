import sqlite3

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
ALTER TABLE intervention_history
ADD COLUMN at_end INTEGER
""")

conn.commit()

import sqlite3

db_path = "/Users/muna/Hana_research/data/db/Hana_Research.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
ALTER TABLE intervention_history
ADD COLUMN at_end INTEGER
""")

conn.commit()

cursor.execute("""
UPDATE intervention_history
SET at_end = 1
WHERE category = '在宅酸素'
AND Patient_ID IN (

    SELECT ih1.Patient_ID
    FROM intervention_history ih1
    JOIN (
        -- 各患者の最終イベント日
        SELECT 
            Patient_ID,
            MAX(COALESCE(end_date, start_date)) AS last_date
        FROM intervention_history
        WHERE category = '在宅酸素'
        GROUP BY Patient_ID
    ) last
    ON ih1.Patient_ID = last.Patient_ID
    AND COALESCE(ih1.end_date, ih1.start_date) = last.last_date

    WHERE ih1.category = '在宅酸素'
      AND (ih1.end_date IS NULL OR ih1.end_date = '')
)
""")

conn.commit()
conn.close()