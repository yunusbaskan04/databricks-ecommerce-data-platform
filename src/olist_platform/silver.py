"""Silver transformations: Bronze -> clean, grain-specific tables.

All functions are pure DataFrame -> DataFrame so they can be unit tested
locally. Data-quality rules follow ADR-0002; the grain split follows ADR-0001.
"""

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

METADATA_COLUMNS = ("_rescued_data", "_ingested_at", "_source_file")
UNKNOWN_CATEGORY = "unknown"

# Primary key of every Bronze entity Silver consumes.
ENTITY_KEYS = {
    "orders": ("order_id",),
    "customers": ("customer_id",),
    "products": ("product_id",),
    "order_items": ("order_id", "order_item_id"),
    "payments": ("order_id", "payment_sequential"),
    "category_translation": ("product_category_name",),
}


@dataclass(frozen=True)
class QualityResult:
    valid: DataFrame
    quarantine: DataFrame


@dataclass(frozen=True)
class SilverOutputs:
    orders: DataFrame
    order_items: DataFrame
    quarantine: DataFrame


# Helper columns added by the rules themselves, plus metadata stored separately.
_NON_RECORD_COLUMNS = ("_dq_reason", "_rank", "_has_order", "_ingested_at", "_source_file")


def _to_quarantine(rejected: DataFrame, entity: str) -> DataFrame:
    """Reshape rejected rows (carrying _dq_reason) into the shared quarantine schema."""
    data_columns = [c for c in rejected.columns if c not in _NON_RECORD_COLUMNS]
    return rejected.select(
        F.lit(entity).alias("entity"),
        "_dq_reason",
        F.to_json(F.struct(*data_columns)).alias("record"),
        "_ingested_at",
        "_source_file",
    )


def _split(df: DataFrame, entity: str, reason: Column) -> tuple[DataFrame, DataFrame]:
    """Split rows on a reason expression that is null for valid rows."""
    flagged = df.withColumn("_dq_reason", reason)
    valid = flagged.filter(F.col("_dq_reason").isNull()).drop("_dq_reason")
    rejected = flagged.filter(F.col("_dq_reason").isNotNull())
    return valid, _to_quarantine(rejected, entity)


def apply_quality_rules(df: DataFrame, entity: str) -> QualityResult:
    """Apply the row-level and duplicate rules of ADR-0002 to one Bronze entity."""
    keys = ENTITY_KEYS[entity]

    # Stage 1: row-level rules; a row gets the first reason it fails.
    any_key_null = F.lit(False)
    for key in keys:
        any_key_null = any_key_null | F.col(key).isNull()
    row_reason = F.when(any_key_null, F.lit("null_key")).when(
        F.col("_rescued_data").isNotNull(), F.lit("rescued_data")
    )
    valid, rejected_rows = _split(df, entity, row_reason)

    # Stage 2: keep one row per key among rows that passed stage 1.
    latest_first = Window.partitionBy(*keys).orderBy(
        F.col("_ingested_at").desc(), F.col("_source_file").desc()
    )
    ranked = valid.withColumn("_rank", F.row_number().over(latest_first))
    duplicate_reason = F.when(F.col("_rank") > 1, F.lit("duplicate_key"))
    valid, rejected_duplicates = _split(ranked, entity, duplicate_reason)

    return QualityResult(
        valid=valid.drop("_rank"),
        quarantine=rejected_rows.unionByName(rejected_duplicates),
    )


def reject_orphan_items(items: DataFrame, orders: DataFrame) -> QualityResult:
    """Move items whose order is not a valid Silver order to quarantine."""
    order_ids = orders.select("order_id").withColumn("_has_order", F.lit(True))
    joined = items.join(order_ids, on="order_id", how="left")
    reason = F.when(F.col("_has_order").isNull(), F.lit("orphan_order"))
    valid, rejected = _split(joined, "order_items", reason)
    return QualityResult(valid=valid.drop("_has_order"), quarantine=rejected)


def aggregate_payments(payments: DataFrame) -> DataFrame:
    """One row per order: collapse payments before joining to avoid row multiplication."""
    return payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("total_payment_value"),
        F.count(F.lit(1)).alias("payment_count"),
    )


def build_silver_orders(
    orders: DataFrame, customers: DataFrame, items: DataFrame, payments: DataFrame
) -> DataFrame:
    """silver_orders: exactly one row per order (order grain)."""
    item_totals = items.groupBy("order_id").agg(
        F.count(F.lit(1)).alias("item_count"),
        F.sum("price").alias("items_value"),
        F.sum("freight_value").alias("freight_value"),
    )
    return (
        orders.join(customers.drop(*METADATA_COLUMNS), on="customer_id", how="left")
        .join(item_totals, on="order_id", how="left")
        .join(aggregate_payments(payments), on="order_id", how="left")
        .select(
            "order_id",
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "order_status",
            "order_purchase_timestamp",
            F.to_date("order_purchase_timestamp").alias("order_date"),
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            F.coalesce("item_count", F.lit(0)).alias("item_count"),
            "items_value",
            "freight_value",
            "total_payment_value",
            F.coalesce("payment_count", F.lit(0)).alias("payment_count"),
            "_ingested_at",
        )
    )


def build_silver_order_items(
    items: DataFrame,
    silver_orders: DataFrame,
    products: DataFrame,
    category_translation: DataFrame,
) -> DataFrame:
    """silver_order_items: one row per order item, enriched with order and product attributes."""
    order_attributes = silver_orders.select(
        "order_id", "customer_unique_id", "order_status", "order_date"
    )
    product_attributes = products.select("product_id", "product_category_name")
    translations = category_translation.select(
        "product_category_name", "product_category_name_english"
    )
    category = F.coalesce("product_category_name", F.lit(UNKNOWN_CATEGORY))
    return (
        items.join(order_attributes, on="order_id", how="inner")
        .join(product_attributes, on="product_id", how="left")
        .join(translations, on="product_category_name", how="left")
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "customer_unique_id",
            "order_status",
            "order_date",
            "shipping_limit_date",
            "price",
            "freight_value",
            category.alias("product_category_name"),
            F.coalesce("product_category_name_english", category).alias(
                "product_category_name_english"
            ),
            "_ingested_at",
        )
    )


def build_silver(bronze: dict[str, DataFrame]) -> SilverOutputs:
    """Run quality rules on every Bronze entity and build the Silver tables."""
    checked = {entity: apply_quality_rules(bronze[entity], entity) for entity in ENTITY_KEYS}
    orders = checked["orders"].valid
    items = reject_orphan_items(checked["order_items"].valid, orders)

    silver_orders = build_silver_orders(
        orders,
        checked["customers"].valid,
        items.valid,
        checked["payments"].valid,
    )
    silver_order_items = build_silver_order_items(
        items.valid,
        silver_orders,
        checked["products"].valid,
        checked["category_translation"].valid,
    )

    quarantine = items.quarantine
    for result in checked.values():
        quarantine = quarantine.unionByName(result.quarantine)

    return SilverOutputs(
        orders=silver_orders,
        order_items=silver_order_items,
        quarantine=quarantine,
    )
