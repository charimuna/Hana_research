Hana_research/
├── data/
│   ├── raw/                # 元データ（PDF, CSV）
│   ├── derived/            # 整形済データ（解析用に前処理済み）
│   └── db/                 # SQLite データベース
├── src/                    # データ処理・DB構築用スクリプト
└── README.md               # 本ファイル

## Data Preparation

Place original data files in:/Users/muna/Hana_research/data/db/Hana_Research.db
HOT導入については，　/Users/muna/Hana_research/data/derived/HOT_date_tate.csv　がデータ元で　/Users/muna/Hana_research/src/at_end_HOT_to_table.py　でHOT最後まで導入をdbのテーブルにいれている
/Users/muna/Hana_research/src/unex_1281data_to_db.py　で　1281例の症例をdbにいれている

data/raw/　
/Users/muna/Hana_research/data/processed/予期せぬ死亡_n1281_脳血.csv

Step 3. Run analysis
/Users/muna/Hana_research/src/unex_study_table1.py
/Users/muna/Hana_research/src/unex_study_figure2.py