# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Analytics
# MAGIC
# MAGIC Creates business-oriented metric tables from the Silver layer for the
# MAGIC Databricks SQL dashboard.
# MAGIC
# MAGIC ## What we do
# MAGIC
# MAGIC - `gold_daily_sales`: revenue, freight, orders and items sold per day.
# MAGIC - `gold_category_sales`: revenue and orders per product category (English).
# MAGIC - `gold_order_status`: order count and payments collected per status.
# MAGIC
# MAGIC ## Revenue definition (ADR-0003)
# MAGIC
# MAGIC Revenue = `sum(price)` over items of orders whose status is **not**
# MAGIC `canceled` or `unavailable`. Freight is reported separately.
# MAGIC `gold_order_status` keeps every status so the cancellation share stays visible.
# MAGIC
# MAGIC ## Why read two Silver tables?
# MAGIC
# MAGIC Each aggregate runs at its own grain: item metrics from `silver_order_items`,
# MAGIC order metrics from `silver_orders`. That is why order counts are
# MAGIC `countDistinct("order_id")` on items but a plain `count` on orders.
# MAGIC
# MAGIC Aggregation logic lives in `src/olist_platform/gold.py` and is unit tested in
# MAGIC `tests/test_gold.py`.

# COMMAND ----------

import os
import sys

# Make the repo's src/ package importable when running from the workspace.
sys.path.insert(0, os.path.abspath("../src"))

from olist_platform.config import table_name
from olist_platform.gold import build_category_sales, build_daily_sales, build_order_status

# COMMAND ----------

silver_orders = spark.table(table_name("silver", "orders"))
silver_order_items = spark.table(table_name("silver", "order_items"))

gold_tables = {
    "daily_sales": build_daily_sales(silver_order_items),
    "category_sales": build_category_sales(silver_order_items),
    "order_status": build_order_status(silver_orders),
}

for entity, df in gold_tables.items():
    target = table_name("gold", entity)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"-> {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Total revenue must be identical whether it is summed per day or per category.

# COMMAND ----------

from pyspark.sql import functions as F

revenue_by_day = (
    spark.table(table_name("gold", "daily_sales")).agg(F.sum("total_revenue")).first()[0]
)
revenue_by_category = (
    spark.table(table_name("gold", "category_sales")).agg(F.sum("total_revenue")).first()[0]
)

print("Revenue (daily):   ", revenue_by_day)
print("Revenue (category):", revenue_by_category)
assert revenue_by_day == revenue_by_category, "Gold revenue totals do not reconcile"
