# CRM Data Platform (Medallion Architecture)

This repository implements a **modern data pipeline** for a CRM system, following the **Medallion Architecture** (Bronze → Silver → Gold) using **ClickHouse**, **dbt**, and **Airflow**.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)  
2. [Technologies Used](#technologies-used)  
3. [Project Structure](#project-structure)  
4. [Setup Instructions](#setup-instructions)  
5. [DBT Usage](#dbt-usage)  
6. [Airflow Usage](#airflow-usage)  
7. [Data Quality & Testing](#data-quality--testing)  
8. [Notes & Best Practices](#notes--best-practices)

---

## Architecture Overview
    +------------------+
    |   Raw Data (CSV) |
    +--------+---------+
             |
             v
    +------------------+
    |  Bronze Layer    |  --> Raw tables loaded into ClickHouse (`crm_raw`)
    +--------+---------+
             |
             v
    +------------------+
    |  Silver Layer    |  --> Staging dbt models (`crm_staging`)
    +--------+---------+
             |
             v
    +------------------+
    |  Gold Layer      |  --> Business-ready marts (`crm_gold`)
    +------------------+
             |
             v
    +------------------+
    |  Airflow DAGs    |  --> Orchestration & scheduling
    +------------------+

**Layers explained:**

- **Bronze:** Raw, unmodified data. Directly ingested from CSV files.  
- **Silver:** Cleaned, standardized staging tables with transformations applied.  
- **Gold:** Business-ready fact and dimension tables, ready for analytics and dashboards.  

---

## Technologies Used

- **ClickHouse** – High-performance columnar database for large-scale analytics.  
- **dbt** – Transformations, testing, and version control for SQL models.  
- **Airflow** – Orchestration and scheduling for pipelines.  
- **Python** – Scripts for ingestion and utility functions.  

---

## Project Structure

**Layers explained:**

- **Bronze:** Raw, unmodified data. Directly ingested from CSV files.  
- **Silver:** Cleaned, standardized staging tables with transformations applied.  
- **Gold:** Business-ready fact and dimension tables, ready for analytics and dashboards.  

---

## Technologies Used

- **ClickHouse** – High-performance columnar database for large-scale analytics.  
- **dbt** – Transformations, testing, and version control for SQL models.  
- **Airflow** – Orchestration and scheduling for pipelines.  
- **Python** – Scripts for ingestion and utility functions.  

---

## Project Structure

**Layers explained:**

- **Bronze:** Raw, unmodified data. Directly ingested from CSV files.  
- **Silver:** Cleaned, standardized staging tables with transformations applied.  
- **Gold:** Business-ready fact and dimension tables, ready for analytics and dashboards.  

---

## Technologies Used

- **ClickHouse** – High-performance columnar database for large-scale analytics.  
- **dbt** – Transformations, testing, and version control for SQL models.  
- **Airflow** – Orchestration and scheduling for pipelines.  
- **Python** – Scripts for ingestion and utility functions.  

---

## Project Structure

**Layers explained:**

- **Bronze:** Raw, unmodified data. Directly ingested from CSV files.  
- **Silver:** Cleaned, standardized staging tables with transformations applied.  
- **Gold:** Business-ready fact and dimension tables, ready for analytics and dashboards.  

---

## Technologies Used

- **ClickHouse** – High-performance columnar database for large-scale analytics.  
- **dbt** – Transformations, testing, and version control for SQL models.  
- **Airflow** – Orchestration and scheduling for pipelines.  
- **Python** – Scripts for ingestion and utility functions.  

---

## Project Structure
ytmc-task/
│
├── clickhouse/ # ClickHouse DDLs and setup scripts
│ ├── 01_create_databases.sql
│ ├── 02_create_bronze_tables.sql
│ └── setup.py # Python script to create DBs and tables
│
├── ingestion/ # CSV ingestion scripts
│ └── load_csv.py
│
├── crm_dbt/ # dbt project
│ ├── models/
│ │ ├── staging/ # Silver layer models
│ │ └── gold/ # Gold layer models
│ ├── dbt_project.yml
│
├── airflow/ # Airflow DAGs
│ └──dags/
│
├── README.md
└── requirements.txt

---

## Setup Instructions

### 1. Clone the repo
```bash
git clone <repo-url>
cd crm-data-platform
```
### 2. Start and build docker
```bash
docker-compose build
docker-compose up -d
```
### 3. Setup ClickHouse DBs and tables
```bash
python clickhouse/setup.py
```

## DBT Usage
Run inside the docker container
### 1. Initialize dbt project
```dbt init crm_dbt```
### 2. Run Silver Layer (staging)
```dbt run --models staging```
### 3. Run Gold Layer (marts)
```dbt run --models gold```
### 4. Run tests
```dbt test```
### 5. Optional: run models + tests together
```dbt build```

## Airflow Usage
### 1. Start Airflow environment
```
docker compose up airflow-init
docker compose up
```
### 2. Access Airflow UI

```URL: http://localhost:8080```

### 3. DAGs
```
load_bronze_dag → ingests CSVs into Bronze layer

dbt_silver_dag → runs dbt staging models

gold_layer_dag → runs dbt marts
```
### 4. Trigger DAGs
```airflow dags trigger <dag_id>```
### 5. Monitor DAGs

Use the Airflow UI to check task status, logs, and retries

## Notes

- All type conversions and transformations happen in Silver layer.
- Gold layer should consume only staging tables.
- DBT profiles.yml can be versioned and shared with the customer for reproducibility.
- Always run dbt test after transformations to ensure data quality.