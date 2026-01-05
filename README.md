📌 Project Overview

This project implements a production-style Retail Data Warehouse ETL pipeline using Oracle Database, Python, and Apache Airflow.

The goal is to:

Extract transactional retail data from an Oracle source system

Validate data quality at every stage

Transform raw business attributes into surrogate-key–based dimensions

Load clean, analytics-ready data into a Target Data Warehouse (DW)

This project follows real-world Data Engineering best practices, including incremental loads, data validation, orchestration, and schema separation.

🏗️ Architecture Overview
Oracle OLTP (Source)
        |
        v
Daily Extraction (CSV Snapshots)
        |
        v
Data Validation (Files + Tables)
        |
        v
Transformation (Set-based Logic)
        |
        v
Oracle Target Data Warehouse (Star Schema)
        |
        v
Analytics / Reporting

🧱 Data Layers Explained
1️⃣ Source Layer (Oracle – OLTP)

Tables like:

fact_sales

dim_store_master

dim_product

dim_distributor

dim_date

Data is read-only

No transformations happen here

2️⃣ Extract Layer (Files)

Daily snapshot files are generated using SQL joins and stored as pipe-delimited CSVs:

/opt/airflow/data_extracts/
├── incoming/
│   └── sales_snapshot_YYYYMMDD_HHMM.csv
├── current/
├── archive/


Why files?

Decouples source from warehouse

Allows reprocessing

Mimics real enterprise pipelines

3️⃣ Validation Layer (Data Quality)

Validation is applied at two levels:

🔹 File Validation

Mandatory column checks

Numeric column validation

Flag column validation (Y/N)

Minimum row count

Pipe (|) delimiter handling

🔹 Table Validation

Row count thresholds

NULL checks on critical columns

Duplicate primary key checks

Freshness check for fact tables

If validation fails → pipeline stops immediately.

4️⃣ Target Data Warehouse (Oracle – DW)

A Star Schema is implemented under a separate schema (target_dw).

Dimension Tables

dim_store_dw

dim_store_chain_dw

dim_product_dw

dim_category

dim_sub_category

dim_manufacturer

dim_distributor_dw

dim_date_dw

Fact Table

fact_sales_dw

Key Concepts Used

Surrogate keys

Business keys

Foreign key constraints

Incremental dimension loads

🔄 Incremental Dimension Load Logic (Set-Based)

Each dimension follows this pattern:

Read latest incoming file

Extract unique business keys

Fetch existing keys from DW into a dictionary (cache)

Identify new records only

Insert only new records

Update cache

Map surrogate keys back to main dataset

This approach is:

Fast

Scalable

Industry standard

🧪 Manufacturer & Category Handling

Manufacturers are derived logically based on product category using controlled mappings.

Example:

Grocery → Nestlé, Tata Consumer, Britannia

BabyCare → Johnson & Johnson, P&G

PersonalCare → HUL, Dabur

This mimics real master data enrichment.

⏱️ Orchestration (Apache Airflow)
Key DAGs
🔹 Extraction DAG

Generates daily snapshot files

🔹 Validation DAG

Validates extracted files

Validates source tables

🔹 Target DW Load DAG
load_dim_store_dw
    >> load_dim_product_dw
    >> load_dim_distributor_dw
    >> load_dim_date_dw
    >> load_fact_sales_dw


Retry logic enabled

Fail-fast on data issues

Fully automated (no manual runs required)

📦 Technologies Used
Category         |	Tools
Database      	 | Oracle Database
Orchestration	   | Apache Airflow
Language	       | Python
Libraries	       | pandas, oracledb
Containerization |	Docker
Scheduling	Cron via Airflow
▶️ How to Run the Project
1️⃣ Start Airflow
docker-compose up -d

2️⃣ Verify Containers
docker ps

3️⃣ Open Airflow UI
http://localhost:8080

4️⃣ Trigger DAGs (in order)

Extract pipeline

Validation pipeline

Target DW load pipeline

🛡️ Error Handling

Common issues handled:

Invalid numeric values (DPY-4004)

Empty or malformed files

Missing foreign keys

Duplicate business keys

Partial data loads

Pipelines fail safely with clear logs.

📊 Outcome

After completion:
Clean star-schema data in Oracle DW

Fully validated, analytics-ready tables

Re-runnable, auditable pipeline
