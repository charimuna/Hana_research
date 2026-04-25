Hana_research/
├── data/
│   ├── raw/                # 元データ（PDF, CSV）
│   ├── derived/            # 整形済データ（解析用に前処理済み）
│   └── db/                 # SQLite データベース
├── src/                    # データ処理・DB構築用スクリプト
└── README.md               # 本ファイル

## Data Preparation

Place original data files in:/Users/muna/Hana_research/data/db/Hana_Research.db
HOT導入については，　/Users/muna/Hana_research/data/derived/HOT_date_tate.csv　がデータ元で　
data/raw/　

## Workflow　きちんと入れていく

Step 1. Create database

python src/create_db.py　

Step 2. Process data

python src/process_data.py

Step 3. Run analysis

python src/analysis.py