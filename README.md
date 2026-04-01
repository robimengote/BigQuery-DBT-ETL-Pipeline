\# ☕ Amantes Café — BigQuery DBT ETL Pipeline



> A production-grade, end-to-end data pipeline for a café point-of-sale system — built with Apache Airflow, dbt, Google BigQuery, and Power BI.



!\[Pipeline Status](https://img.shields.io/badge/status-active-brightgreen)

!\[Python](https://img.shields.io/badge/Python-3.11-blue)

!\[dbt](https://img.shields.io/badge/dbt-1.11.7-orange)

!\[Airflow](https://img.shields.io/badge/Airflow-2.x-red)

!\[BigQuery](https://img.shields.io/badge/BigQuery-GCP-blue)



\---



\## 📌 Overview



A \*\*live, production-grade data pipeline\*\* built on a real-world café 

point-of-sale (POS) dataset — the same data source used across this 

portfolio's pipeline projects.



This is not a static or simulated project. The pipeline runs on a schedule, 

processes real transactions, handles late-arriving and malformed data, and serves a live Power BI dashboard backed by Google BigQuery.



The focus is the \*\*engineering layer\*\*: automated orchestration, incremental 

loading, multi-layer data validation, and recovery flows for bad data — all 

wired together into a single, observable pipeline.



\*\*Key engineering decisions demonstrated:\*\*

\- Incremental dbt models with late-arriving row support via `is\_reprocessed` flag

\- Quarantine layer that isolates bad rows without halting the pipeline

\- MERGE-based idempotency so the pipeline is safely re-runnable

\- dbt schema tests as a circuit breaker with BigQuery audit tables for precise recovery



\---



\## 🏗️ Architecture

```

┌─────────────────────────────────────────────────────────┐

│                    POS System (Raw CSV)                  │

└─────────────────────────┬───────────────────────────────┘

&#x20;                         ↓

&#x20;                Python ETL (Extraction \& Validation)

&#x20;                ↙                          ↘

&#x20;         Good Rows                       Bad Rows

&#x20;             ↓                               ↓

&#x20;     GCS (Data Lake)              Quarantine Table (BigQuery)

&#x20;             ↓                               ↓

&#x20;  staging\_fact\_sales                  Repair row manually

&#x20;             ↓                               ↓

&#x20;         dbt run                    load\_fixed\_rows RPC (is\_reprocessed = TRUE)

&#x20;             ↓                               ↓

&#x20;         dbt test                   delete\_fixed\_rows RPC

&#x20;             ↓                               ↓

&#x20;  fact\_sales2026 + Dims             Re-run dbt run

&#x20;             ↓                               ↓

&#x20;     Power BI Dashboard             reset\_reprocessed\_flag RPC (is\_reprocessed = FALSE)

```



\---



\## 🛠️ Tech Stack

&#x20;

| Layer | Tool |

|---|---|

| Orchestration | Apache Airflow (Dockerized) |

| Extraction \& Load | Python (pandas, google-cloud-bigquery) |

| Data Lake | Google Cloud Storage (GCS) |

| Data Warehouse | Google BigQuery |

| Transformation | dbt (dbt-bigquery, dbt-utils) |

| Data Quality | dbt schema tests + audit tables |

| Visualization | Power BI |

| Containerization | Docker + Docker Compose |

| Version Control | Git + GitHub |

&#x20;

\---

&#x20;

\## 📁 Project Structure

&#x20;

```

BigQuery-DBT-ETL-Pipeline/

├── dags/                          # Airflow DAG definitions

│   └── amantes\_pipeline.py        # Main pipeline DAG

├── amantes\_dbt/                   # dbt project

│   ├── models/

│   │   ├── facts/

│   │   │   ├── fact\_sales2026.sql # Incremental fact table

│   │   │   └── schema.yml         # dbt schema tests

│   │   └── dimensions/

│   │       └── \_dimensions.yml    # Dimension table tests

│   ├── seeds/                     # Static dimension CSVs

│   ├── macros/                    # dbt macros

│   ├── dbt\_project.yml

│   └── packages.yml               # dbt\_utils dependency

├── docker-compose.yaml            # Airflow + dbt container setup

├── requirements.txt               # Python dependencies

├── profiles\_guide.yml             # dbt profiles setup guide

├── .gitignore

└── README.md

```

&#x20;

\---

&#x20;

\## ⚙️ Pipeline Features

&#x20;

\### Incremental Loading

The fact table uses dbt's incremental strategy — only new rows are processed on each run, keeping costs low and performance high on BigQuery.

&#x20;

```sql

WHERE (

&#x20;   CAST(payment\_time AS DATETIME) > (SELECT MAX(payment\_time) FROM {{ this }})

&#x20;   OR is\_reprocessed = TRUE

)

```

&#x20;

\### Quarantine Layer

Rows that fail validation during extraction are routed to a quarantine table instead of being silently dropped. Once fixed, they are reinserted into staging with `is\_reprocessed = TRUE` so dbt picks them up on the next run regardless of their original timestamp.

&#x20;

\### Idempotency

A `MERGE`-based deduplication stored procedure ensures the pipeline can be safely re-run for the same date without producing duplicate rows in BigQuery.

&#x20;

\### Multi-layer Data Validation

| Layer | Tool | What it catches |

|---|---|---|

| Extraction | Python quarantine logic | Malformed rows, unknown products |

| Staging | MERGE dedup procedure | Duplicate rows |

| Transformation | dbt schema tests | Nulls, broken FK relationships, invalid measures |

| Recovery | dbt audit tables + stored procedures | Precise identification and removal of bad rows |

&#x20;

\### dbt Schema Tests

```yaml

\- not\_null

\- dbt\_utils.unique\_combination\_of\_columns  # composite key

\- dbt\_utils.expression\_is\_true             # business rules

&#x20; - payment\_type\_key != 0  (severity: error)

&#x20; - order\_type\_key != 0    (severity: error)

&#x20; - product\_key != -1      (severity: warn)

&#x20; - quantity > 0           (severity: error)

```

&#x20;

\### Recovery Stored Procedures

| Procedure | Purpose |

|---|---|

| `dedup\_staging\_fact\_sales` | Removes duplicates after ETL load |

| `load\_fixed\_quarantine\_rows` | Reinserts fixed rows with `is\_reprocessed = TRUE` |

| `reset\_reprocessed\_flag` | Resets flag after dbt processes rows |

| `dbt\_recovery\_int` | Fixes broken rows under INT columns and sets `is\_reprocessed = TRUE`|

| `dbt\_recovery\_str` | Fixes broken rows under STRING columns and sets `is\_reprocessed = TRUE` |

\---

&#x20;

\## 🌟 Star Schema

&#x20;

The data model follows a classic Star Schema with `fact\_sales2026` at the center, surrounded by seven dimension tables. The model view below was generated directly from Power BI.

&#x20;

!\[Star Schema - Power BI Model View](bi\_model.jpg)

&#x20;

\---

&#x20;

\## 🚀 Getting Started

&#x20;

\### Prerequisites

\- Docker + Docker Compose

\- Google Cloud Platform account

\- GCP Service Account with BigQuery and GCS permissions

\- dbt CLI (optional for local runs)

&#x20;

\### 1. Clone the repo

```bash

git clone https://github.com/robimengote/BigQuery-DBT-ETL-Pipeline.git

cd BigQuery-DBT-ETL-Pipeline

```

&#x20;

\### 2. Set up environment variables

```bash

cp .env.example .env

\# Fill in your GCP project ID, dataset, and GCS bucket

```

&#x20;

\### 3. Set up dbt profiles

```bash

\# Follow the guide in profiles\_guide.yml

\# Create \~/.dbt/profiles.yml with your BigQuery credentials

```

&#x20;

\### 4. Add your GCP service account key

```bash

mkdir keys/

\# Place your service account JSON key in keys/

```

&#x20;

\### 5. Start Airflow

```bash

docker-compose up -d

```

&#x20;

\### 6. Install dbt dependencies

```bash

docker exec -it <airflow\_container> bash

cd /opt/airflow/amantes\_dbt

dbt deps

```

&#x20;

\### 7. Access Airflow UI

```

http://localhost:8080

```

&#x20;

\---

&#x20;

\## 🧪 Running dbt Manually

&#x20;

```bash

\# Run the fact table model

dbt run --select fact\_sales2026

&#x20;

\# Run schema tests

dbt test --select fact\_sales2026

&#x20;

\# Run everything

dbt run \&\& dbt test

```

&#x20;

\---

&#x20;

\## 📊 Power BI Dashboard

&#x20;

The Power BI dashboard connects directly to BigQuery and provides:

\- Daily and monthly sales trends

\- Revenue breakdown by category and sub-category

\- Payment type and order type distribution

\- Product performance analysis

!\[Live Power BI Dashboard](bi\_dashboard.jpg)

&#x20;

\---

&#x20;

\## 🔐 Security Notes

&#x20;

\- GCP service account keys are stored in `keys/` which is excluded from version control via `.gitignore`

\- `profiles.yml` is never committed — use `profiles\_guide.yml` as a reference

\- `.env` is excluded from version control

&#x20;

\---

&#x20;

\## 📝 License

&#x20;

MIT License — feel free to use this as a reference for your own pipeline projects.

