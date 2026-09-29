# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Processing
# MAGIC
# MAGIC This notebook demonstrates incremental data processing with Delta Lake.
# MAGIC
# MAGIC Instead of rebuilding the entire dataset, new or changed records are
# MAGIC merged into an existing Delta table.
# MAGIC
# MAGIC Key concepts:
# MAGIC
# MAGIC - Incremental loading
# MAGIC - Delta MERGE
# MAGIC - Upsert
# MAGIC - Idempotent processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental MERGE
# MAGIC
# MAGIC This notebook demonstrates the basic incremental processing pattern
# MAGIC using Delta Lake MERGE.
# MAGIC
# MAGIC A simulated incremental batch is compared with the existing Bronze
# MAGIC Delta table using `order_id`.
# MAGIC
# MAGIC - Matched records are updated.
# MAGIC - Unmatched records are inserted.
# MAGIC
# MAGIC The current implementation uses a simulated batch to demonstrate
# MAGIC the MERGE and upsert pattern. A realistic incremental ingestion
# MAGIC strategy will be implemented in the next stage.

# COMMAND ----------

BRONZE_PATH = "/Volumes/workspace/default/olist_data/bronze/orders"

orders_bronze = spark.read.format("delta").load(BRONZE_PATH)

display(orders_bronze.limit(10))

# COMMAND ----------

print("Bronze row count:", orders_bronze.count())

# COMMAND ----------

incremental_batch = (
    orders_bronze
    .limit(100)
)

display(incremental_batch)

# COMMAND ----------

orders_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col

new_orders = (
    orders_bronze
    .limit(5)
    .withColumn(
        "order_id",
        concat(lit("incremental_"), col("order_id"))
    )
)

display(new_orders)

# COMMAND ----------

from delta.tables import DeltaTable

target = DeltaTable.forPath(
    spark,
    BRONZE_PATH
)

(
    target.alias("target")
    .merge(
        new_orders.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# COMMAND ----------

updated_orders = spark.read.format("delta").load(BRONZE_PATH)

print(
    "New row count:",
    updated_orders.count()
)

# COMMAND ----------

display(
    updated_orders
    .filter(col("order_id").startswith("incremental_"))
)