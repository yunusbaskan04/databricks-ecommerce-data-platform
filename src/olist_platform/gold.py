"""Gold aggregations: Silver -> business metrics for the dashboard.

Revenue follows ADR-0003: sum of item ``price`` over orders whose status is not
in ``REVENUE_EXCLUDED_STATUSES``. Item-level metrics read ``silver_order_items``;
order-level metrics read ``silver_orders``, so each aggregate runs at its grain.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REVENUE_EXCLUDED_STATUSES = ("canceled", "unavailable")


def revenue_items(silver_order_items: DataFrame) -> DataFrame:
    """Items that count towards revenue (booked revenue)."""
    return silver_order_items.filter(~F.col("order_status").isin(*REVENUE_EXCLUDED_STATUSES))


def build_daily_sales(silver_order_items: DataFrame) -> DataFrame:
    return (
        revenue_items(silver_order_items)
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count(F.lit(1)).alias("items_sold"),
            F.sum("price").alias("total_revenue"),
            F.sum("freight_value").alias("total_freight"),
            F.round(F.avg("price"), 2).alias("average_item_price"),
        )
    )


def build_category_sales(silver_order_items: DataFrame) -> DataFrame:
    return (
        revenue_items(silver_order_items)
        .groupBy("product_category_name_english")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("product_id").alias("unique_products"),
            F.count(F.lit(1)).alias("items_sold"),
            F.sum("price").alias("total_revenue"),
            F.round(F.avg("price"), 2).alias("average_item_price"),
        )
        .withColumnRenamed("product_category_name_english", "product_category")
    )


def build_order_status(silver_orders: DataFrame) -> DataFrame:
    """All statuses, including those excluded from revenue, at order grain."""
    return silver_orders.groupBy("order_status").agg(
        F.count(F.lit(1)).alias("total_orders"),
        F.sum("items_value").alias("total_items_value"),
        F.sum("total_payment_value").alias("total_payment_value"),
    )
