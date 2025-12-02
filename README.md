# databricks-lakehouse-etl
Lakehouse-style ETL pipeline using PySpark and Delta (bronze/silver/gold)
Databricks Lakehouse ETL Pipeline

Building a Medallion-architecture ETL pipeline using PySpark, Delta Lake, and Databricks-style modular components.

## Overview

This project demonstrates a complete Lakehouse-style ETL workflow using PySpark and Delta Lake.
It follows Databricks best practices, with separate bronze, silver, and gold layers and modularized transformation logic.

The goal is to show how a real production pipeline would be designed:

Clean separation of ingestion, transformation, and aggregation
Delta-backed storage for reliability and ACID guarantees
Parameterized Spark utilities
Data quality checks
Currency normalization + business logic
Simple reproducible dataset (customers + orders)

This project is meant to simulate the type of work done in a modern analytics platform.

## Architecture (Medallion Model)

raw (CSV files)
    ↓
bronze (raw → normalized with metadata)
    ↓
silver (cleaned, validated, joined, enriched)
    ↓
gold (aggregates + business KPIs)

Bronze

Ingest raw CSV files
Add ingestion timestamp
Normalize timestamp fields
Add source-file lineage
Partition the orders Delta table by date

Silver

Remove duplicates
Clean email + create full name
Normalize currencies to USD
Standardize status fields
Prepare analytic-ready fact/dimension tables

Gold

Join customers + orders
Compute daily revenue, customer count, order volume, AOV
Partition by order_date
Produce final metric tables used for reporting or dashboards

## Project Structure

databricks-lakehouse-etl/
├── data/
│   ├── raw/
│   │   ├── customers.csv
│   │   └── orders.csv
│   └── lakehouse/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
├── src/
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   ├── gold_aggregates.py
│   ├── validate_data_quality.py
│   ├── config.py
│   └── spark_utils.py
│
├── notebooks/        ← placeholder for Databricks exported notebooks
├── requirements.txt  ← pyspark, delta-spark, pandas
└── README.md

## Dataset Used

customers.csv

Contains basic customer profile fields.

orders.csv

Contains transaction-level order data including amount, currency, timestamps, and status.

The project converts all amounts to USD using simple FX mappings and extracts several analytic features.

## ETL Flow Summary

1. Bronze Layer – Raw Ingestion

Script: bronze_ingest.py

Reads raw CSV files
Converts strings to timestamp/date
Adds metadata (ingest_ts, source_file)
Writes to Delta tables
Orders table is partitioned by order_date

2. Silver Layer – Cleaned & Enriched Data

Script: silver_transform.py

Cleans emails
Creates full name
Removes duplicates
Applies currency FX conversion
Harmonizes status fields (uppercase)
Writes dimension + fact tables in Delta format

3. Gold Layer – Business Metrics

Script: gold_aggregates.py

Outputs daily KPIs:
Total revenue in USD
Average order value
Active customers per day
Total orders per day
Metrics grouped by order_date + country

4. Data Quality Checks

Script: validate_data_quality.py

Validates:
No NULLs in key fields
Record count sanity checks
Schema checks for expected fields

This simulates basic pipeline reliability in production systems.

## Tech Stack

Component	       Technology
Compute	             PySpark
Storage	            Delta Lake
ETL Framework	 Python + modular Spark code
Medallion 
Architecture	     Bronze → Silver → Gold
Utility Layer	 Custom SparkSession builder + config paths
Deployment	     Local execution / Databricks-ready
Data Quality	 Custom validation module

## How to Run Locally
Note: Running PySpark locally requires a working Java 11 installation.

The project can still be explored and understood without running the code.

1. Install dependencies
pip install -r requirements.txt

2. Run Bronze ingestion
python src/bronze_ingest.py

3. Run Silver transformations
python src/silver_transform.py

4. Run Gold metrics aggregation
python src/gold_aggregates.py

5. Run Data Quality checks
python src/validate_data_quality.py


Delta tables will appear in:

data/lakehouse/bronze
data/lakehouse/silver
data/lakehouse/gold
