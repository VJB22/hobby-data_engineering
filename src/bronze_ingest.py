# -*- coding: utf-8 -*-
"""
Created on Fri Aug 29 17:59:11 2025

@author: baroc
"""

import os, kagglehub
from utils_duck import connect, run_sql

def _path_for_sql(p: str) -> str:
    return p.replace("\\", "/").replace("'", "''")

def _find_file(root: str, prefer_exts=(".csv", ".tsv", ".txt", ".xlsx", ".xls")) -> str | None:
    candidates = [
        "Ecommerce Sales Dataset.csv",
        "E-commerce Sales Dataset.csv",
        "ecommerce_sales_dataset.csv",
        "EcommerceSales.csv",
    ]
    for c in candidates:
        p = os.path.join(root, c)
        if os.path.exists(p):
            return 
        
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.lower().endswith(prefer_exts):
                return os.path.join(dirpath, f)
    return None

def ingest():
    con = connect()

    sales_dir = kagglehub.dataset_download("kyanyoga/sample-sales-data")
    sales_csv = _path_for_sql(os.path.join(sales_dir, "sales_data_sample.csv"))
    run_sql(con, "DROP TABLE IF EXISTS bronze.sales_raw;")
    run_sql(con, f"""
        CREATE OR REPLACE TABLE bronze.sales_raw AS
        SELECT * FROM read_csv_auto(
            '{sales_csv}',
            header = TRUE,
            all_varchar = 1,
            ignore_errors = TRUE,
            sample_size = -1
        );
    """)

    ecom_dir = kagglehub.dataset_download("thedevastator/unlock-profits-with-e-commerce-sales-data")
    ecom_file = _find_file(ecom_dir)
    if not ecom_file:
        raise FileNotFoundError(f"No CSV/XLSX found under: {ecom_dir}")

    ecom_file_sql = _path_for_sql(ecom_file)
    run_sql(con, "DROP TABLE IF EXISTS bronze.ecommerce_raw;")

    if ecom_file.lower().endswith((".csv", ".tsv", ".txt")):
        run_sql(con, f"""
            CREATE OR REPLACE TABLE bronze.ecommerce_raw AS
            SELECT * FROM read_csv_auto(
                '{ecom_file_sql}',
                header = TRUE,
                all_varchar = 1,
                sample_size = -1
            );
        """)
    else:

        run_sql(con, "INSTALL 'excel';")
        run_sql(con, "LOAD 'excel';")
        run_sql(con, f"""
            CREATE OR REPLACE TABLE bronze.ecommerce_raw AS
            SELECT * FROM read_excel('{ecom_file_sql}', sheet=1);
        """)

    con.close()
