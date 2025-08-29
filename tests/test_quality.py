# -*- coding: utf-8 -*-
"""
Created on Fri Aug 29 18:04:32 2025

@author: baroc
"""

import duckdb
from utils_duck import DB_PATH
import os
import time

def test_db_path_resolves_and_exists():
    assert isinstance(DB_PATH, str) and DB_PATH, "DB_PATH should be a non-empty string"
    assert os.path.isabs(DB_PATH), f"DB_PATH should be absolute, got: {DB_PATH}"
    assert os.path.exists(DB_PATH), f"DB file not found at DB_PATH: {DB_PATH}"
    assert os.path.isfile(DB_PATH), f"DB_PATH is not a file: {DB_PATH}"
    assert os.path.getsize(DB_PATH) > 0, "DB file is empty (size = 0)"

def test_duckdb_can_open_read_only():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        version = con.execute("PRAGMA version").fetchone()[0]
    assert isinstance(version, str) and len(version) > 0, "DuckDB version not returned"


def test_connected_to_expected_file():
    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute("SELECT 1").fetchall()
    assert rows[0][0] == 1

def test_required_schemas_exist():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        schemas = {r[0] for r in con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()}
    for sch in ("bronze", "silver", "gold"):
        assert sch in schemas, f"Missing schema: {sch}"

def test_required_tables_exist():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        rows = con.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze','silver','gold')
        """).fetchall()
    existing = {(s, t) for s, t in rows}
    expected = {
        ("bronze", "sales_raw"),
        ("bronze", "ecommerce_raw"),
        ("silver", "sales_clean"),
        ("silver", "ecommerce_clean"),
        ("gold",   "sales_kpi"),
    }
    missing = expected - existing
    assert not missing, f"Missing tables: {sorted(list(missing))}"


def test_silver_sales_has_rows():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        n = con.execute("SELECT COUNT(*) FROM silver.sales_clean").fetchone()[0]
    assert n > 0, "silver.sales_clean is empty"

def test_silver_ecommerce_has_order_ids():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        n_nulls = con.execute("""
            SELECT COUNT(*) 
            FROM silver.ecommerce_clean 
            WHERE order_id IS NULL OR TRIM(CAST(order_id AS VARCHAR)) = ''
        """).fetchone()[0]
    assert n_nulls == 0, f"silver.ecommerce_clean has {n_nulls} NULL/blank order_id"

def test_gold_sales_kpi_has_rows_and_nonnegative_totals():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        rows = con.execute("SELECT COUNT(*) FROM gold.sales_kpi").fetchone()[0]
        negs = con.execute("SELECT COUNT(*) FROM gold.sales_kpi WHERE total_sales < 0").fetchone()[0]
    assert rows > 0, "gold.sales_kpi is empty"
    assert negs == 0, "gold.sales_kpi has negative totals"

def test_bronze_tables_exist():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        n = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='bronze' AND table_name IN ('sales_raw','ecommerce_raw')
        """).fetchone()[0]
    assert n == 2, "expected bronze.sales_raw and bronze.ecommerce_raw to exist"

def test_sales_numeric_sanity():

    with duckdb.connect(DB_PATH, read_only=True) as con:
        bad = con.execute("""
            SELECT COUNT(*) FROM silver.sales_clean
            WHERE TRY_CAST(quantityordered AS INT) < 0
               OR TRY_CAST(priceeach AS DOUBLE) < 0
        """).fetchone()[0]
    assert bad == 0, "found negative quantityordered or priceeach in silver.sales_clean"

def test_sales_orderdate_parseable_ratio():

    with duckdb.connect(DB_PATH, read_only=True) as con:
        total, ok = con.execute("""
            SELECT 
              COUNT(*) AS total,
              SUM(
                CASE 
                  WHEN COALESCE(
                    TRY_STRPTIME(orderdate, '%m/%d/%Y %H:%M'),
                    TRY_STRPTIME(orderdate, '%m/%d/%Y %H:%M:%S'),
                    TRY_STRPTIME(orderdate, '%m/%d/%Y')
                  ) IS NOT NULL THEN 1 ELSE 0 
                END
              ) AS ok
            FROM silver.sales_clean
        """).fetchone()
    assert total == 0 or ok / total >= 0.95, f"only {ok}/{total} orderdate values parse with US formats"

def test_ecommerce_order_id_duplicate_rate():

    with duckdb.connect(DB_PATH, read_only=True) as con:
        tot = con.execute("SELECT COUNT(*) FROM silver.ecommerce_clean").fetchone()[0]
        dups = con.execute("""
            WITH c AS (
              SELECT order_id, COUNT(*) n
              FROM silver.ecommerce_clean
              GROUP BY order_id
            )
            SELECT COALESCE(SUM(n-1),0) FROM c WHERE n > 1
        """).fetchone()[0]
    assert tot == 0 or (dups / tot) <= 0.10, f"duplicate rate too high: {dups}/{tot}"

def test_gold_region_cardinality_reasonable():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        regions = con.execute("SELECT COUNT(DISTINCT region) FROM gold.sales_kpi").fetchone()[0]
    assert 1 <= regions <= 1000, f"unexpected region cardinality: {regions}"

def test_gold_order_count_matches_silver_rows():

    with duckdb.connect(DB_PATH, read_only=True) as con:
        gold_sum = con.execute("SELECT COALESCE(SUM(order_count),0) FROM gold.sales_kpi").fetchone()[0]
        silver_rows = con.execute("SELECT COUNT(*) FROM silver.ecommerce_clean").fetchone()[0]
    assert gold_sum == silver_rows, f"gold order_count sum {gold_sum} != silver rows {silver_rows}"

def test_no_null_regions_in_gold():
    with duckdb.connect(DB_PATH, read_only=True) as con:
        n = con.execute("""
            SELECT COUNT(*) FROM gold.sales_kpi
            WHERE region IS NULL OR TRIM(region) = ''
        """).fetchone()[0]
    assert n == 0, f"gold.sales_kpi has {n} NULL/blank regions"

def test_gold_totals_not_extreme_outliers():

    with duckdb.connect(DB_PATH, read_only=True) as con:
        p99 = con.execute("SELECT quantile_cont(total_sales, 0.99) FROM gold.sales_kpi").fetchone()[0]
    assert p99 is None or p99 < 1e9, f"p99 total_sales looks extreme: {p99}"
    

def test_perf_gold_kpi_sum_under_2s():
    """
    Basic performance guard: summing total_sales from gold should be quick.
    Tweak threshold to your environment if needed.
    """
    start = time.perf_counter()
    with duckdb.connect(DB_PATH, read_only=True) as con:
        _ = con.execute("SELECT SUM(total_sales) FROM gold.sales_kpi").fetchone()[0]
    elapsed = time.perf_counter() - start
    assert elapsed < 2.0, f"SUM(total_sales) too slow: {elapsed:.3f}s"

def test_perf_silver_counts_under_2s():
    """
    Counting big Silver tables should also be quick.
    """
    start = time.perf_counter()
    with duckdb.connect(DB_PATH, read_only=True) as con:
        _ = con.execute("SELECT COUNT(*) FROM silver.sales_clean").fetchone()[0]
        _ = con.execute("SELECT COUNT(*) FROM silver.ecommerce_clean").fetchone()[0]
    elapsed = time.perf_counter() - start
    assert elapsed < 2.0, f"Silver COUNT(*) too slow: {elapsed:.3f}s"