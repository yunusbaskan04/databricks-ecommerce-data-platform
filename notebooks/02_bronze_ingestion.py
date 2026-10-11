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

DATA_PATH = "/Volumes/workspace/default/olist_data"

CATALOG = "workspace"
SCHEMA = "olist"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/olist_data/

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType,
)

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_unique_id", StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType()),
])

# Column names keep the source spelling ("lenght") on purpose.
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("product_category_name", StringType()),
    StructField("product_name_lenght", IntegerType()),
    StructField("product_description_lenght", IntegerType()),
    StructField("product_photos_qty", IntegerType()),
    StructField("product_weight_g", IntegerType()),
    StructField("product_length_cm", IntegerType()),
    StructField("product_height_cm", IntegerType()),
    StructField("product_width_cm", IntegerType()),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", DecimalType(10, 2)),
    StructField("freight_value", DecimalType(10, 2)),
])

PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("payment_sequential", IntegerType()),
    StructField("payment_type", StringType()),
    StructField("payment_installments", IntegerType()),
    StructField("payment_value", DecimalType(10, 2)),
])

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp


def load_to_bronze(file_name, table_name, schema):
    source_path = f"{DATA_PATH}/{file_name}"
    target_table = f"{CATALOG}.{SCHEMA}.bronze_{table_name}"

    df = (
        spark.read
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(schema)
        .csv(source_path)
        .select(
            "*",
            current_timestamp().alias("_ingested_at"),
            col("_metadata.file_path").alias("_source_file"),
        )
    )

    (
        df.write
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    print(f"{table_name} -> {target_table} completed")

# COMMAND ----------

BRONZE_SOURCES = [
    ("olist_orders_dataset.csv", "orders", ORDERS_SCHEMA),
    ("olist_customers_dataset.csv", "customers", CUSTOMERS_SCHEMA),
    ("olist_products_dataset.csv", "products", PRODUCTS_SCHEMA),
    ("olist_order_items_dataset.csv", "order_items", ORDER_ITEMS_SCHEMA),
    ("olist_order_payments_dataset.csv", "payments", PAYMENTS_SCHEMA),
]

for file_name, table_name, schema in BRONZE_SOURCES:
    load_to_bronze(file_name, table_name, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC For each Bronze table: row count, rows with a non-null `_rescued_data`
# MAGIC (values that did not fit the schema), and null counts for every
# MAGIC timestamp column from the source schema.

# COMMAND ----------

from pyspark.sql.functions import count, lit, sum as spark_sum

validation_rows = []

for _, table_name, schema in BRONZE_SOURCES:
    target_table = f"{CATALOG}.{SCHEMA}.bronze_{table_name}"
    timestamp_columns = [
        field.name
        for field in schema.fields
        if isinstance(field.dataType, TimestampType)
    ]

    aggregations = [
        count(lit(1)).alias("row_count"),
        count("_rescued_data").alias("rescued_rows"),
    ] + [
        spark_sum(col(column).isNull().cast("int")).alias(f"null_{column}")
        for column in timestamp_columns
    ]

    metrics = spark.table(target_table).agg(*aggregations).first().asDict()

    for metric, value in metrics.items():
        validation_rows.append((table_name, metric, int(value or 0)))

validation_df = spark.createDataFrame(
    validation_rows,
    "table_name STRING, metric STRING, value BIGINT",
)

display(validation_df)
