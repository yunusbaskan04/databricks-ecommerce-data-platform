# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Clean, Grain-Specific Tables
# MAGIC
# MAGIC This notebook turns Bronze tables into cleaned Silver tables that downstream
# MAGIC consumers can trust without extra filtering.
# MAGIC
# MAGIC ## What we do
# MAGIC
# MAGIC - Read the Bronze UC tables (`workspace.olist.bronze_*`).
# MAGIC - Apply data-quality rules (ADR-0002): rows with a null key, rescued data,
# MAGIC   a duplicate key, or an order item without an order are moved to
# MAGIC   `silver_quarantine` together with the reason.
# MAGIC - Build one table per grain (ADR-0001):
# MAGIC   - `silver_orders`: one row per order, with customer attributes, item totals
# MAGIC     and payments aggregated **before** the join to avoid row multiplication.
# MAGIC   - `silver_order_items`: one row per order item, with order status/date and
# MAGIC     the product category (Portuguese and English, `"unknown"` when missing).
# MAGIC
# MAGIC Transformation logic lives in `src/olist_platform/silver.py` and is unit
# MAGIC tested in `tests/test_silver.py`; this notebook only reads and writes.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```text
# MAGIC workspace.olist.bronze_*
# MAGIC       ↓  quality rules
# MAGIC       ├── workspace.olist.silver_quarantine   (rejected rows + reason)
# MAGIC       ↓  joins + aggregation
# MAGIC workspace.olist.silver_orders        (1 row per order)
# MAGIC workspace.olist.silver_order_items   (1 row per order item)
# MAGIC ```

# COMMAND ----------

import os
import sys

# Make the repo's src/ package importable when running from the workspace.
sys.path.insert(0, os.path.abspath("../src"))

from olist_platform.config import table_name
from olist_platform.silver import ENTITY_KEYS, build_silver

# COMMAND ----------

bronze = {entity: spark.table(table_name("bronze", entity)) for entity in ENTITY_KEYS}

silver = build_silver(bronze)

# COMMAND ----------

outputs = {
    "orders": silver.orders,
    "order_items": silver.order_items,
    "quarantine": silver.quarantine,
}

for entity, df in outputs.items():
    target = table_name("silver", entity)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"-> {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC - `silver_orders` must have exactly one row per `order_id`.
# MAGIC - `silver_order_items` must have exactly one row per (`order_id`, `order_item_id`).
# MAGIC - Rejected rows per entity and reason.

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table(table_name("silver", "orders"))
items = spark.table(table_name("silver", "order_items"))

grain_checks = spark.createDataFrame(
    [
        ("silver_orders", orders.count(), orders.select("order_id").distinct().count()),
        (
            "silver_order_items",
            items.count(),
            items.select("order_id", "order_item_id").distinct().count(),
        ),
    ],
    "table_name STRING, row_count BIGINT, distinct_keys BIGINT",
)
display(grain_checks)

# COMMAND ----------

display(
    spark.table(table_name("silver", "quarantine"))
    .groupBy("entity", "_dq_reason")
    .agg(F.count(F.lit(1)).alias("rejected_rows"))
    .orderBy("entity", "_dq_reason")
)
