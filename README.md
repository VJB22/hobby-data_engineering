# Ride-Share Data Pipeline and Dashboard

## Overview
This project is an end-to-end data engineering pipeline built in Python to demonstrate core skills for a junior data engineering role. It ingests raw ride-share trip data, processes it through a bronze → silver → gold data lake pattern, and outputs analytics through an interactive dashboard.

The goal is to showcase practical skills in ETL, data modeling, orchestration, and visualization.

---

## Tech Stack
- Python (pandas, pyarrow)  
- DuckDB (single-file OLAP warehouse)  
- Prefect (workflow orchestration)  
- Streamlit (dashboard and KPIs)  
- pytest (basic data quality checks)  

---

## Architecture
**Data flow:**  
Raw CSV → Bronze (parquet) → Silver (fact and dimension tables) → Gold (aggregates) → Dashboard

---

## Folder Structure

data/raw → raw CSV (ride trips)
data/bronze → parquet (renamed columns)
data/silver → transformed tables (DuckDB)
data/gold → marts (daily and hourly summaries)

src/etl.py → ETL pipeline (bronze → silver → gold)
src/flow.py → Prefect orchestration
app/dashboard.py → Streamlit dashboard
tests/ → data quality tests

---

## Clone the repository and create a virtual environment:

git clone https://github.com/<your-username>/rides-pipeline.git
cd rides-pipeline
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
Place a small CSV file into data/raw/rides.csv.

Expected columns:
tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
trip_distance, fare_amount, tip_amount, PULocationID, DOLocationID

Run the ETL pipeline:
python src/etl.py
Launch the dashboard:
streamlit run app/dashboard.py

--- 

## Dashboard Preview
Insert a screenshot here to show the dashboard with KPIs and charts.

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
