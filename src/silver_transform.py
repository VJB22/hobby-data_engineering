# -*- coding: utf-8 -*-
"""
Created on Fri Aug 29 18:00:33 2025

@author: baroc
"""

import re
from utils_duck import connect

def _snake(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[ \-\/]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s or s[0].isdigit():
        s = f"col_{s}"
    return s

def _select_with_aliases(con, table_fullname: str):

    cols = con.execute(f"PRAGMA table_info('{table_fullname}')").fetchall()
    aliases = {}
    select_parts = []
    for _, colname, *_ in cols:
        alias = _snake(colname)
        aliases[colname] = alias
        select_parts.append(f'"{colname}" AS {alias}')  
    return ", ".join(select_parts), aliases

def _column_exists(con, table_fullname: str, col: str) -> bool:
    cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table_fullname}')").fetchall()]
    return col in cols

def transform():
    con = connect()


    sel_sales, _ = _select_with_aliases(con, "bronze.sales_raw")
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS silver;
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.sales_clean AS
        SELECT {sel_sales}
        FROM bronze.sales_raw;
    """)


    sel_ecom, ecom_alias = _select_with_aliases(con, "bronze.ecommerce_raw")
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.ecommerce_clean AS
        SELECT {sel_ecom}
        FROM bronze.ecommerce_raw;
    """)

\
    had_order_id = any(k.lower() == "order id" for k in ecom_alias.keys())
    has_order_id_snake = "order_id" in ecom_alias.values()
    if had_order_id and not has_order_id_snake:
        src_alias = _snake("Order ID")

        if not _column_exists(con, "silver.ecommerce_clean", "order_id"):
            con.execute("""
                ALTER TABLE silver.ecommerce_clean
                ADD COLUMN order_id VARCHAR;
            """)
            con.execute(f"""
                UPDATE silver.ecommerce_clean
                SET order_id = CAST({src_alias} AS VARCHAR);
            """)

    con.close()
