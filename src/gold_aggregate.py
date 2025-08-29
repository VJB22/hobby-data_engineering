# -*- coding: utf-8 -*-
"""
Created on Fri Aug 29 18:01:03 2025

@author: baroc
"""

from utils_duck import connect

def _find_col(con, table, candidates):
    cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    for c in candidates:
        if c in cols:
            return c

    for c in candidates:
        for col in cols:
            if c in col:
                return col
    return None

def build():
    con = connect()
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")


    table = "silver.ecommerce_clean"
    region_col = _find_col(con, table, [
        "region", "state", "province", "area", "market", "zone"
    ])
    amount_col = _find_col(con, table, [
        "amount", "sales", "sales_amount", "selling_price", "unit_price",
        "price", "total", "total_amount", "grand_total", "revenue"
    ])

    if region_col is None:

        region_expr = "'UNKNOWN'"
    else:
        region_expr = f"UPPER(COALESCE({region_col}, 'UNKNOWN'))"

    if amount_col is None:

        amount_expr = "1.0"
    else:

        amount_expr = (
            f"TRY_CAST(REPLACE(REPLACE({amount_col}, ',', ''), '€', '') AS DOUBLE)"
        )


    con.execute(f"""
        CREATE OR REPLACE TABLE gold.sales_kpi AS
        SELECT region, 
               SUM(amount_num) AS total_sales,
               COUNT(*) AS order_count
        FROM (
            SELECT
                {region_expr} AS region,
                {amount_expr} AS amount_num
            FROM {table}
        )
        GROUP BY region
        ORDER BY region;
    """)

    con.close()