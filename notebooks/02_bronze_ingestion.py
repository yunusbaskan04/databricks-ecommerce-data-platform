# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC This notebook ingests the Olist e-commerce CSV files into the Bronze layer
# MAGIC as Unity Catalog managed Delta tables.
# MAGIC
# MAGIC ## What we do
# MAGIC
# MAGIC - Read raw CSV files from the Unity Catalog Volume.
# MAGIC - Apply an explicit schema per source file (no `inferSchema`).
# MAGIC - Capture values that do not match the schema in `_rescued_data`.
# MAGIC - Add ingestion metadata: `_ingested_at` and `_source_file`.
# MAGIC - Write each dataset as a managed table `workspace.olist.bronze_<name>`.
# MAGIC
# MAGIC ## Why explicit schemas?
# MAGIC
# MAGIC `inferSchema` scans the data and guesses types, which can silently change
# MAGIC between runs (e.g. zip code prefixes losing leading zeros as integers).
# MAGIC An explicit schema acts as a contract with the source: unexpected values
# MAGIC are rescued instead of being dropped or reshaping the table.
# MAGIC
# MAGIC ## Why managed tables?
# MAGIC
# MAGIC Managed tables are governed by Unity Catalog (permissions, lineage,
# MAGIC discoverability) and are addressed by name instead of storage path.
# MAGIC
# MAGIC ## Load strategy
# MAGIC
# MAGIC The source is a full CSV snapshot, so each table is written with
# MAGIC `mode("overwrite")`. Bronze becomes append-only once incremental file
# MAGIC ingestion (Auto Loader) is in place.
# MAGIC
# MAGIC Schemas and ingestion logic live in `src/olist_platform` (`schemas.py`,
# MAGIC `bronze.py`); this notebook only orchestrates them.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```text
# MAGIC Olist CSV Files (UC Volume)
# MAGIC       ↓
# MAGIC Spark DataFrames (explicit schema + metadata columns)
# MAGIC       ↓
# MAGIC workspace.olist.bronze_<name> (UC managed Delta tables)
# MAGIC ```

# COMMAND ----------

import os
import sys

# Make the repo's src/ package importable when running from the workspace.
sys.path.insert(0, os.path.abspath("../src"))

from olist_platform.bronze import BRONZE_SOURCES, load_to_bronze, validation_metrics
from olist_platform.config import CATALOG, SCHEMA

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

for source in BRONZE_SOURCES:
    load_to_bronze(spark, source)
    print(f"{source.file_name} -> {source.target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC For each Bronze table: row count, rows with a non-null `_rescued_data`
# MAGIC (values that did not fit the schema), and null counts for every
# MAGIC timestamp column from the source schema.

# COMMAND ----------

validation_rows = [
    (source.entity, metric, value)
    for source in BRONZE_SOURCES
    for metric, value in validation_metrics(spark, source).items()
]

display(spark.createDataFrame(validation_rows, "table_name STRING, metric STRING, value BIGINT"))
