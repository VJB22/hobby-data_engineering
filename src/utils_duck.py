# -*- coding: utf-8 -*-
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "lakehouse.duckdb"

def connect(read_only: bool = False):
    """Open a connection to DuckDB and ensure schemas exist."""
    con = duckdb.connect(str(DB_PATH), read_only=read_only)
    if not read_only:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    return con

def run_sql(con, sql: str):
    """Run a SQL statement on an existing connection."""

    return con.execute(sql)
