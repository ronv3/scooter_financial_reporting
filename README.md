# BsC Thesis practical part — Data Pipeline for Calculating Scooter Fleet Financials

This repository contains the **practical implementation** for a Bachelor’s thesis demo on **automating financial accounting using data models**.

The demo uses **synthetic scooter fleet data** (rides, heartbeat snapshots, scooter master data, account mappings) and builds a **scheduleable analytics pipeline** to calculate fleet financials (starting with depreciation) and later produce **journal-ready double-entry accounting outputs**.

---

## Introduction

### What this project demonstrates
This project focuses on the **data modeling and transformation** part of accounting automation (not real-time ingestion).  
The source data is generated once as CSV files and loaded as dbt seeds, after which the pipeline can be run repeatedly via Airflow.

The core idea is:

- **Operational source-like data** (rides + daily heartbeat + static master)
- → **dbt staging / transformation models**
- → **financial marts** (depreciation, fleet financial state)
- → **journal-ready accounting outputs** (double-entry lines)

### Tech stack (used in this practical part)
- **Python** — mock source data generation scripts
- **DuckDB** — analytical database / warehouse engine (file-based)
- **dbt (dbt-duckdb)** — transformations, tests, model layering
- **Apache Airflow** — orchestration and scheduling
- **Docker Compose** — local reproducible environment
- **Postgres** — Airflow metadata database (not the analytical warehouse)

> Note: In this setup, **DuckDB is the analytical database**, while **Postgres is only used by Airflow internally** for metadata (DAG runs, task state, users, etc.).

---

## Architecture (short overview)

### Logical layers in the pipeline
The project follows a common analytics engineering pattern:

- **Raw / Source layer** (`dbt seeds`)
  - `rides.csv`
  - `scooter_heartbeat.csv`
  - `scooters_master.csv`
  - `account_mapping.csv`

- **Staging layer** (`stg_*`)
  - typing, normalization, lightweight cleaning

- **Intermediate layer** (`int_*`)
  - joins and business logic basis (e.g. daily scooter financial basis)

- **Mart layer** (`mart_*`)
  - finance-ready outputs (depreciation, fleet financial state, journal lines)

### Runtime architecture (Docker)
- **Airflow webserver + scheduler** run in containers
- **Airflow metadata** is stored in **Postgres**
- **dbt** runs inside the Airflow container
- **DuckDB** is used via a mounted file (e.g. `duckdb/thesis.duckdb`)
- **Source CSVs** are stored locally and loaded with `dbt seed`

---

## Project Structure (overview)

```text
.
├── airflow/                # Airflow Dockerfile, DAGs, logs, plugins
├── compose.yml             # Docker Compose setup
├── data/                   # Source CSV files (generated mock data)
├── dbt/                    # dbt project (models, seeds, tests, profiles)
├── duckdb/                 # DuckDB database file location (persisted)
├── exports/                # Exported outputs (e.g. journal CSVs)
└── README.md
```

---

## Setup Guide

### 1) Make sure you are in the project root
```bash
cd .../scooter_fleet_depreciation
```

### 2) Create `.env` from `.env.example`
You may replace credentials and ports with your own values.

```bash
cp .env.example .env
```

### 3) Make sure Docker daemon is running
Start Docker Desktop (or your Docker engine) before continuing.

## First-time startup

### 4) Build containers
```bash
docker compose build
```

### 5) Initialize Airflow metadata DB and create admin user
```bash
docker compose up airflow-init
```

### 6) Start Airflow webserver and scheduler
```bash
docker compose up -d airflow-webserver airflow-scheduler
```

## Regular startup (after first run)

```bash
docker compose up -d
```

## Basic verification checks

### Check containers are running
```bash
docker compose ps
```

You should see at least:
- `airflow-postgres`
- `airflow-webserver`
- `airflow-scheduler`

### Check dbt can connect to DuckDB
```bash
docker compose exec airflow-webserver bash -lc "cd /opt/dbt && dbt debug --profiles-dir /opt/dbt"
```

Expected result:
- `adapter type: duckdb`
- connection test `OK`

### Load source CSVs into DuckDB (dbt seeds)
```bash
docker compose exec airflow-webserver bash -lc "cd /opt/dbt && dbt seed --full-refresh --profiles-dir /opt/dbt"
```

This creates the raw/source tables in DuckDB (typically in a schema like `dev_raw` or `main_raw`, depending on your dbt profile/target schema configuration).

### (Optional) Query DuckDB locally to verify tables
If DuckDB CLI is installed locally (using homebrew, try `brew install duckdb`):

```bash
duckdb duckdb/thesis.duckdb
```

Then inside DuckDB:

```sql
SELECT table_schema, table_name
FROM information_schema.tables
ORDER BY table_schema, table_name;
```

You should see seeded tables such as:
- `data_lake.rides`
- `data_lake.scooter_heartbeat`
- `data_lake.scooters_master`
- `data_lake.account_mapping`

## Airflow UI

Open Airflow in your browser:

- **http://localhost:8080** (or the port set in `.env`)

Login credentials are defined in your `.env` file:
- `AIRFLOW_ADMIN_USERNAME`
- `AIRFLOW_ADMIN_PASSWORD`

## Notes / Troubleshooting

### Why is there no DuckDB container?
DuckDB is file-based and embedded (similar to SQLite usage).  
dbt/Python opens the file directly (e.g. `duckdb/thesis.duckdb`).

### `SHOW TABLES;` returns no rows in DuckDB CLI
Your seeded tables may be created in a schema like `data_lake` / `data_warehouse`, not in the default schema. Query with schema-qualified names, e.g.:

```sql
SELECT COUNT(*) FROM data_lake.rides;
```

or inspect all schemas:

```sql
SELECT schema_name FROM information_schema.schemata;
```

## Next steps (project roadmap)
- Build dbt staging models (`stg_*`)
- Build intermediate financial basis models (`int_*`)
- Build marts for:
  - depreciation
  - running fleet financial state
  - journal-ready double-entry lines
- Add Airflow DAG to orchestrate:
  - `dbt seed` → `dbt run` → `dbt test`
- (Optional later) Connect Apache Superset for dashboards
