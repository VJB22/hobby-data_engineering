# Hobby (Data Engineering) Data Pipeline and Dashboard

## Overview
This project is an end-to-end data engineering pipeline built in Python to demonstrate core skills for a junior data engineering role. It ingests raw e-commerce and sales data (via Kaggle API), processes it through a **bronze → silver → gold** medallion architecture, and outputs analytics through an interactive Streamlit dashboard.

The goal is to showcase practical skills in **data ingestion, ETL, data modeling, orchestration, and visualization**.

---

## Tech Stack
- **Python** (pandas, pyarrow)
- **DuckDB** (single-file OLAP warehouse)
- **KaggleHub** (dataset download)
- **Streamlit** (dashboard and KPIs)
- **pytest** (basic data quality checks)

---

## Architecture
**Data flow:**  
Raw Kaggle datasets → Bronze (raw tables) → Silver (cleaned fact/dimension tables) → Gold (aggregates & KPIs) → Dashboard

---

## Folder Structure
## Repository Layout

- **src/**
  - `utils_duck.py` — DB connection helpers  
  - `bronze_ingest.py` — raw Kaggle → bronze tables  
  - `silver_transform.py` — bronze → silver fact/dim  
  - `gold_aggregate.py` — silver → gold marts  
  - `pipeline.py` — orchestrates bronze → silver → gold  

- **app/**
  - `dashboard.py` — Streamlit dashboard  

- **tests/**
  - `test_quality.py` — pytest data quality checks  

- **data/** (ignored in Git, except `.gitkeep`)  
  - `raw/` — raw CSVs (Kaggle API)  
  - `bronze/` — parquet files  
  - `silver/` — transformed tables  
  - `gold/` — aggregates  
  - `lakehouse.duckdb` — DuckDB file (local only, gitignored)  

- `requirements.txt` — dependencies  
- `README.md` — project overview  
- `.gitignore` — git ignore rules  
---

## Clone the repository and create a virtual environment:

git clone https://github.com/<your-username>/hobby-data_engineering.git
cd hobby-data_engineering

# Create venv
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

--- 

## Dashboard Preview

<img width="1069" height="959" alt="Screenshot 2025-08-29 202734" src="https://github.com/user-attachments/assets/5906320f-ed10-4998-b48a-4ead16d017b3" />

--- 

## Testing
Run data quality checks:
pytest -q
The tests verify that core tables exist and basic quality conditions are met (e.g., no negative fares).

---
 
## What This Project Demonstrates
- Building an ETL pipeline from raw CSV to analytics-ready tables
- Designing a star schema (fact and dimension tables)
- Implementing the medallion architecture (bronze / silver / gold)
- Automating workflows with Prefect
- Creating an interactive dashboard with Streamlit
- Writing and running basic data quality tests

--- 

## Cloud Mapping (Optional)
- This pipeline can be extended to Azure:
- Azure Blob Storage → raw and bronze layers
- Azure Data Factory → orchestration
- Azure Synapse Analytics → silver and gold tables
-  Power BI → dashboards

-  
License
MIT License
