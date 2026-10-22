# Hana_Research.db スキーマ辞書

DB: `/Users/muna/Hana_research/data/db/Hana_Research.db`

## Background_summary（4,915行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | INTEGER |  |  |
| PS | TEXT |  |  |
| visit_place | TEXT |  |  |
| facility | TEXT |  |  |
| JABC | TEXT |  |  |
| ninchido | TEXT |  |  |
| Kaiyodo | TEXT |  |  |
| memo | TEXT |  |  |　ここにフリーで色々書いている，既往歴，現病歴が
| tag | TEXT |  |  |
| Study_ID | TEXT |  |  |

## Freedocument（6,878行）

目的: 初診時，退院時のまとめとそのほかPDFで取り込んだ書類

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patients_ID | INTEGER |  |  |「＝Patient_ID」
| Date | TEXT |  |  |
| document_type | TEXT |  |  |
| text_data | TEXT |  |  |　現病歴，既往歴が書いてある
| Study_ID | TEXT |  |  |

## Patient_Master（5,373行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | INTEGER | ○ |  |
| Birth_Date | TEXT |  |  |
| First_Visit_Date | TEXT |  |  |
| Gender | TEXT |  |  |
| Age_at_Visit | INTEGER |  |  |
| Study_ID | TEXT |  |  |
| height | INTEGER |  |  |

## disease_keyword_long（589行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| id | INTEGER | ○ |  |
| category | TEXT |  |  |
| keyword | TEXT |  |  |

## ef_long（148,124行）

目的: 使用しない

| 列 | 型 | PK | 意味・コード | 
|---|---|---|---|
| id | INTEGER |  |  |
| Study_ID | TEXT |  |  |
| visit_datetime | TEXT |  |  |
| visit_date | TEXT |  |  |
| record_no | TEXT |  |  |
| item | TEXT |  |  |
| value | TEXT |  |  |
| matched_text | TEXT |  |  |
| hf_term_raw | TEXT |  |  |
| hf_term_std | TEXT |  |  |
| ef_source | TEXT |  |  |
| final_hf_class | TEXT |  |  |
| conflict_flag | INTEGER |  |  |

## ef_long_v2（197,504行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| id | INTEGER |  |  |
| Study_ID | TEXT |  |  |
| visit_datetime | TEXT |  |  |
| visit_date | TEXT |  |  |
| record_no | TEXT |  |  |
| item | TEXT |  |  |
| ef_value | REAL |  |  |
| value_text | TEXT |  |  |
| matched_text | TEXT |  |  |
| hf_term_raw | TEXT |  |  |
| hf_term_std | TEXT |  |  |
| ef_source | TEXT |  |  |
| ef_is_range | INTEGER |  |  |
| final_hf_class | TEXT |  |  |
| final_basis | TEXT |  |  |
| conflict_flag | INTEGER |  |  |
| conflict_detail | TEXT |  |  |

## event（15,139行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | TEXT |  |  |
| status | TEXT |  |  |
| death_date | TEXT |  |  |
| last_confirmed_date | TEXT |  |  |
| endstage_car | TEXT |  |  |
| Study_ID | TEXT |  |  |

## first_diag（34,664行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | INTEGER |  |  |
| date | TEXT |  |  |
| diagnosis | TEXT |  |  |
| category | TEXT |  |  |
| heart_failure | INTEGER |  |  |
| Study_ID | TEXT |  |  |

## intervention_history（6,030行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | TEXT |  |  |
| category | TEXT |  |  |
| start_date | TEXT |  |  |
| end_date | TEXT |  |  |
| ongoing | INTEGER |  |  |
| at_end | INTEGER |  |  |
| Study_ID | TEXT |  |  |

## karte（440,349行）

目的: 自由記載のカルテ

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| record_no | TEXT |  |  |
| visit_type | TEXT |  |  |
| visit_datetime | TEXT |  |  |　2016/3/31(木) 17:40　形式
| flag_note | TEXT |  |  |
| Patient_ID | TEXT |  |  |
| Study_ID | TEXT |  |  |
| facility | TEXT |  |  |
| karte_text | TEXT |  |  |　ここにカルテに書いてあることがのっている．
| end_time | TEXT |  |  |
| home_care_guidance | TEXT |  |  |
| family_note | TEXT |  |  |
| temperature | TEXT |  |  |
| blood_pressure | TEXT |  |  |
| pulse | TEXT |  |  |
| pulse_rhythm | TEXT |  |  |
| spo2 | TEXT |  |  |
| spo2_note | TEXT |  |  |
| respiratory_rate | TEXT |  |  |
| resp_rhythm | TEXT |  |  |
| height | TEXT |  |  |
| weight | TEXT |  |  |
| updated_at | TEXT |  |  |
| nurse_name | TEXT |  |  |
| nurse_id | TEXT |  |  |
| visit_duration_min | TEXT |  |  |
| source_file | TEXT |  |  |

## lab_clean（2,393,570行）

目的: 採血データー

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| raw_id | INTEGER |  |  |
| Patient_ID | TEXT |  |  |
| sample_date | TEXT |  |  |
| item_name | TEXT |  |  |
| item_code | TEXT |  |  |
| value_raw | TEXT |  |  |
| unit | TEXT |  |  |
| hl_flag | TEXT |  |  |
| ref_low | TEXT |  |  |
| ref_high | TEXT |  |  |
| comment | TEXT |  |  |
| lab_company | TEXT |  |  |
| facility | TEXT |  |  |
| source_file | TEXT |  |  |
| imported_at | TEXT |  |  |
| sample_date_parsed | TIMESTAMP |  |  | 2018-02-05 00:00:00　サンプル．　使わない
| value_num | REAL |  |  |

## lab_raw（4,847,153行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| raw_id | INTEGER | ○ |  |
| Patient_ID | TEXT |  |  |
| sample_date | TEXT |  |  |
| item_name | TEXT |  |  |
| item_code | TEXT |  |  |
| value_raw | TEXT |  |  |
| unit | TEXT |  |  |
| hl_flag | TEXT |  |  |
| ref_low | TEXT |  |  |
| ref_high | TEXT |  |  |
| comment | TEXT |  |  |
| lab_company | TEXT |  |  |
| facility | TEXT |  |  |
| source_file | TEXT |  |  |
| imported_at | TEXT |  |  |

## last_one_month_diag（53,921行）

目的: 

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | INTEGER |  |  |
| diagnosis | TEXT |  |  |
| category | TEXT |  |  |
| heart_failure_flg | INTEGER |  |  |

## orca_diagnoses（1,960,277行）

目的: ORCAから吸い上げた病名の一覧

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| id | INTEGER | ○ |  |
| medical_institution_id | TEXT |  |  |
| billing_month | TEXT |  |  | YYYYMMの形式
| record_id | TEXT |  |  |
| receipt_type_code | TEXT |  |  |
| inout_category | TEXT |  |  |
| Patient_ID | TEXT |  |  |
| orca_patient_id | TEXT |  |  |
| sex | TEXT |  |  |
| birth_date | TEXT |  |  |
| dept_code | TEXT |  |  |
| receden_dept_code | TEXT |  |  |
| start_date | TEXT |  |  |　YYYYMMDDの形式
| seq_no | TEXT |  |  |
| diagnosis_name | TEXT |  |  |
| name_char_count | TEXT |  |  |
| suspected_flag | TEXT |  |  |
| main_diagnosis_flag | TEXT |  |  |
| chronic_category | TEXT |  |  |
| outcome_category | TEXT |  |  |
| outcome_date | TEXT |  |  |
| code_count | TEXT |  |  |
| diagnosis_code_1 | TEXT |  |  |
| diagnosis_code_2 | TEXT |  |  |
| diagnosis_code_3 | TEXT |  |  |
| source_file | TEXT |  |  |
| source_row_number | INTEGER |  |  |

## orca_receipt_details（6,373,467行）

目的: ORCAから落とした検査，内服，処置等のデーター

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| id | INTEGER | ○ |  |
| billing_month | TEXT |  |  |
| service_date | TEXT |  |  |
| Patient_ID | TEXT |  |  |
| drug_group_no | TEXT |  |  |
| detail_seq | TEXT |  |  |
| service_category | TEXT |  |  |
| service_type | TEXT |  |  |
| detail_code | TEXT |  |  |
| detail_quantity | TEXT |  |  |
| detail_count | TEXT |  |  |
| unit_code | TEXT |  |  |
| unit_name | TEXT |  |  |
| detail_name | TEXT |  |  |
| official_detail_name | TEXT |  |  |
| drug_price_code | TEXT |  |  |
| daily_value | TEXT |  |  |
| source_file | TEXT |  |  |
| source_row_number | INTEGER |  |  |

## study_id_linkage（5,237行）

目的: 使用しない．

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | INTEGER |  |  |
| Study_ID | TEXT |  |  |

## tolvaptan_study（214行）

目的: tolvaptan新規導入した人の研究　追加例あり．

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | INTEGER | ○ |  |
| Study_ID | TEXT |  |  |
| ind_date | TEXT |  |  |
| date_precision | TEXT |  |  |
| raw_match | TEXT |  |  |
| ef_visit_date | TEXT |  |  |
| ef_value | TEXT |  |  |
| ef_matched_text | TEXT |  |  |
| ef_final_hf_class | TEXT |  |  |
| HF | INTEGER |  |  |
| Cancer | INTEGER |  |  |
| CKD | INTEGER |  |  |
| Cirrhosis | INTEGER |  |  |
| HypoNa | INTEGER |  |  |
| pre_weight | REAL |  |  |
| pre_weight_date | TEXT |  |  |
| HFonset | TEXT |  |  |
| weight_gain | INTEGER |  |  |
| edema | INTEGER |  |  |
| dyspnea | INTEGER |  |  |
| コメント | TEXT |  |  |

## unex_study（1,281行）

目的: 予期せぬ死亡の患者研究

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | INTEGER |  |  |
| age | INTEGER |  |  |
| gender_male1 | INTEGER |  |  |
| VISITplace_Home1 | INTEGER |  |  |
| ADL | TEXT |  |  |
| Death | TEXT |  |  |
| unex_death | INTEGER |  |  |
| HF01 | INTEGER |  |  |
| ins | INTEGER |  |  |
| 認知症 | INTEGER |  |  |
| 認知機能障害 | INTEGER |  |  |
| 脳血管疾患 | INTEGER |  |  |
| 呼吸器疾患 | INTEGER |  |  |
| 心臓疾患 | INTEGER |  |  |
| 腎疾患 | INTEGER |  |  |
| フォロー | INTEGER |  |  |
| 中心静脈 | REAL |  |  |
| EN | INTEGER |  |  |
| Unex_48 | REAL |  |  |
| 尿カテ | INTEGER |  |  |
| 透析 | INTEGER |  |  |
| lastHOT | INTEGER |  |  |
| age85under1 | INTEGER |  |  |
| score | INTEGER |  |  |
| score_Group | TEXT |  |  |
| Dementia*ADL | INTEGER |  |  |
| HOT*Resp | INTEGER |  |  |
| Follow90up1 | INTEGER |  |  |
| age4bun | INTEGER |  |  |
| Study_ID | TEXT |  |  |

## unexpected_death（1,557行）

目的:

| 列 | 型 | PK | 意味・コード |
|---|---|---|---|
| Patient_ID | TEXT |  |  |
| unex24 | INTEGER |  |  |
| Study_ID | TEXT |  |  |


## 共通ルール
- 解析対象: First_Visit_Date 2015-03-01〜2025-03-31、Age_at_Visit ≥20、
  death_date あり かつ ≤2025-10-31、endstage_car = '癌末期' を除外
- 結合キー: 原則 Study_ID
- 日付: TEXT 型 YYYY-MM-DD
- CSV出力: utf-8-sig、出力先 /Users/muna/Hana_research/data/output/
- 「Study_IDがない表は Patient_ID で結合」と追記
- 共通ルールに「結合時は CAST して型を揃える」と追記