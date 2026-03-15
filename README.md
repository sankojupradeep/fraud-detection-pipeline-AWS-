# 🔍 Fraud Detection Batch Pipeline
 
> An end-to-end, event-driven batch pipeline on AWS that automatically ingests synthetic transaction data, applies multi-rule fraud detection logic using PySpark, and exposes results for analysis via Athena — triggered automatically on every S3 upload.
 
---
 
## 📌 Project Summary
 
| Detail | Value |
|--------|-------|
| **Domain** | Fintech / Fraud Detection |
| **Pipeline Type** | Batch (Incremental Load) |
| **Records Processed** | 50,000 transactions per run |
| **Trigger** | Event-driven (S3 upload → EventBridge → Lambda) |
| **Cloud** | AWS (S3, Glue, Lambda, EventBridge, Athena) |
 
---
 
## 🏗️ Architecture
 
```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA GENERATION                              │
│                  Python Faker (50K transactions)                    │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         RAW ZONE (S3)                               │
│         s3://fraud-detection-raw-zone/raw/transactions/             │
│                  Partitioned by year/month/day                      │
└────────────────────────────┬────────────────────────────────────────┘
                             │  S3 Object Created Event
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    AMAZON EVENTBRIDGE RULE                          │
│              Listens for uploads to raw/transactions/               │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      AWS LAMBDA FUNCTION                            │
│         fraud-pipeline-trigger — Orchestrates Glue jobs            │
└────────────┬────────────────────────────────────┬───────────────────┘
             │                                    │
             ▼                                    ▼ (after Job 1 succeeds)
┌────────────────────────┐            ┌───────────────────────────────┐
│  GLUE JOB 1            │            │  GLUE JOB 2                   │
│  Transformation-1      │───────────▶│  Transformation-2             │
│  Clean + Cast + Dedup  │            │  Fraud Rules + Scoring        │
└────────────────────────┘            └───────────────────────────────┘
             │                                    │
             ▼                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PROCESSED ZONE (S3)                            │
│    cleaned/date_partition=YYYY-MM-DD/part-00000.parquet             │
│    fraud_scored/date_partition=YYYY-MM-DD/part-00000.parquet        │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       AMAZON ATHENA                                 │
│          Serverless SQL queries on Parquet in S3                   │
│                  Database: fraud_detection_db                       │
└─────────────────────────────────────────────────────────────────────┘
```

---
 
## 🛠️ Tech Stack
 
| Layer | Technology |
|-------|-----------|
| Data Generation | Python 3.11, Faker library |
| Raw Storage | AWS S3 (Data Lake — Raw Zone) |
| Processed Storage | AWS S3 (Data Lake — Processed Zone) |
| ETL & Transformation | AWS Glue 4.0 (PySpark) |
| Orchestration | AWS Lambda + Amazon EventBridge |
| Query Layer | Amazon Athena |
| IAM & Security | AWS IAM Roles & Policies |
| Format | CSV (raw) → Parquet (processed) |
 
---
 
## 🔄 Pipeline Flow — Step by Step
 
### Step 1 — Data Generation
- 50,000 synthetic transactions generated using **Python Faker**
- Fields: transaction ID, user ID, amount, merchant, country, timestamp
- **2% fraud rate injected** with patterns: high amounts, high-risk countries
- Output uploaded as **CSV** to S3 raw zone
 
### Step 2 — Event-Based Auto Trigger
- S3 upload fires an **Object Created** event
- **EventBridge rule** catches the event (filtered by bucket + prefix)
- Invokes **Lambda function** automatically — no manual trigger needed
 
### Step 3 — Glue Job 1: Clean & Transform
- Reads raw CSV from Glue Data Catalog
- Casts types, removes duplicates on `transaction_id`
- Drops nulls on critical fields
- Adds `date_partition` and `hour_partition` columns
- Writes clean **Parquet** to processed zone
- Uses **dynamic partition overwrite** — incremental, idempotent
 
### Step 4 — Glue Job 2: Fraud Detection Rules
- Reads today's cleaned Parquet partition only
- Applies 4 fraud detection rules using PySpark window functions
- Calculates weighted fraud score per transaction
- Flags transactions with score ≥ 40 as fraudulent
- Merges with existing day's scored data (incremental)
- Writes single Parquet file per day using `coalesce(1)`
 
### Step 5 — Analytics via Athena
- Glue Crawler auto-discovers schema from Parquet
- Athena queries run directly on S3 — no database server needed
- Partition-aware queries minimize data scanned = near-zero cost
 
---
 
## 🚨 Fraud Detection Rules
 
| Rule | Logic | Score Weight |
|------|-------|-------------|
| **Geo Anomaly** | Transaction country is high-risk (NG, RU, BR, KP) AND differs from user's home country | +40 |
| **High Amount** | Transaction amount > $3,000 | +30 |
| **Velocity Check** | User made > 5 transactions within 10-minute sliding window | +20 |
| **Merchant Abuse** | Same user transacted > 3 times at same merchant with amount > $500 | +10 |
 
> Transactions with **fraud score ≥ 40** are flagged as fraudulent.
 
---
 
## 📁 Project Structure
 
```
fraud-detection-pipeline/
│
├── data_generator/
│   └── generate_transactions.py       # Faker synthetic data generator
│
├── glue_jobs/
│   ├── transformation1_clean.py       # Glue ETL — clean + cast + dedup
│   └── transformation2_fraud_rules.py # Glue ETL — fraud rules + scoring
│
├── lambda/
│   └── fraud_pipeline_trigger.py      # Lambda orchestrator function
│
├── athena/
│   └── fraud_queries.sql              # 10 advanced analytics queries
│
└── README.md
```
 
---
 
## 📊 Incremental Load Strategy
 
Each pipeline run uses **dynamic partition overwrite**:
 
```
Run 1 (Morning)  → writes → date_partition=2026-03-15/ (10K records)
Run 2 (Noon)     → writes → date_partition=2026-03-15/ (20K records — merged)
Run 3 (Evening)  → writes → date_partition=2026-03-15/ (50K records — merged)
 
Next Day         → writes → date_partition=2026-03-16/ (new file — old untouched)
```
 
- Historical partitions are **never modified**
- Each day's data is **deduplicated** on `transaction_id`
- Single `part-00000.parquet` file per day via `coalesce(1)`
- Safe to re-run without creating duplicates (**idempotent**)
 
---
