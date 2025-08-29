# -*- coding: utf-8 -*-
"""
Created on Fri Aug 29 18:01:23 2025

@author: baroc
"""

from prefect import flow, task
from bronze_ingest import ingest
from silver_transform import transform
from gold_aggregate import build

@task
def bronze(): ingest()

@task
def silver(): transform()

@task
def gold(): build()

@flow
def etl():
    bronze()
    silver()
    gold()

if __name__ == "__main__":
    etl()
