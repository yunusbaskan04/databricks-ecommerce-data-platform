"""Bronze ingestion: raw CSV snapshot -> UC managed Delta table."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType

from olist_platform import schemas
from olist_platform.config import raw_file_path, table_name


@dataclass(frozen=True)
class BronzeSource:
    file_name: str
    entity: str
    schema: StructType

    @property
    def target_table(self) -> str:
        return table_name("bronze", self.entity)


BRONZE_SOURCES = [
    BronzeSource("olist_orders_dataset.csv", "orders", schemas.ORDERS_SCHEMA),
    BronzeSource("olist_customers_dataset.csv", "customers", schemas.CUSTOMERS_SCHEMA),
    BronzeSource("olist_products_dataset.csv", "products", schemas.PRODUCTS_SCHEMA),
    BronzeSource("olist_order_items_dataset.csv", "order_items", schemas.ORDER_ITEMS_SCHEMA),
    BronzeSource("olist_order_payments_dataset.csv", "payments", schemas.PAYMENTS_SCHEMA),
    BronzeSource(
        "product_category_name_translation.csv",
        "category_translation",
        schemas.CATEGORY_TRANSLATION_SCHEMA,
    ),
]


def read_raw_csv(spark: SparkSession, source: BronzeSource) -> DataFrame:
    """Read one source file with its explicit schema and ingestion metadata."""
    return (
        spark.read.option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(source.schema)
        .csv(raw_file_path(source.file_name))
        .select(
            "*",
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
        )
    )


def load_to_bronze(spark: SparkSession, source: BronzeSource) -> None:
    """Overwrite the Bronze table with the current full CSV snapshot."""
    read_raw_csv(spark, source).write.mode("overwrite").saveAsTable(source.target_table)


def validation_metrics(spark: SparkSession, source: BronzeSource) -> dict[str, int]:
    """Row count, rescued rows and nulls per timestamp column of a Bronze table."""
    timestamp_columns = [
        field.name for field in source.schema.fields if isinstance(field.dataType, TimestampType)
    ]
    aggregations = [
        F.count(F.lit(1)).alias("row_count"),
        F.count("_rescued_data").alias("rescued_rows"),
    ] + [
        F.sum(F.col(column).isNull().cast("int")).alias(f"null_{column}")
        for column in timestamp_columns
    ]
    metrics = spark.table(source.target_table).agg(*aggregations).first().asDict()
    return {metric: int(value or 0) for metric, value in metrics.items()}
