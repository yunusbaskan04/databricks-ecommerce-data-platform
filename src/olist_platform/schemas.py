"""Explicit Bronze schemas: the contract with each Olist source file."""

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_timestamp", TimestampType()),
        StructField("order_approved_at", TimestampType()),
        StructField("order_delivered_carrier_date", TimestampType()),
        StructField("order_delivered_customer_date", TimestampType()),
        StructField("order_estimated_delivery_date", TimestampType()),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("customer_unique_id", StringType()),
        StructField("customer_zip_code_prefix", StringType()),
        StructField("customer_city", StringType()),
        StructField("customer_state", StringType()),
    ]
)

# Column names keep the source spelling ("lenght") on purpose.
PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType()),
        StructField("product_category_name", StringType()),
        StructField("product_name_lenght", IntegerType()),
        StructField("product_description_lenght", IntegerType()),
        StructField("product_photos_qty", IntegerType()),
        StructField("product_weight_g", IntegerType()),
        StructField("product_length_cm", IntegerType()),
        StructField("product_height_cm", IntegerType()),
        StructField("product_width_cm", IntegerType()),
    ]
)

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("order_item_id", IntegerType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("shipping_limit_date", TimestampType()),
        StructField("price", DecimalType(10, 2)),
        StructField("freight_value", DecimalType(10, 2)),
    ]
)

PAYMENTS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("payment_sequential", IntegerType()),
        StructField("payment_type", StringType()),
        StructField("payment_installments", IntegerType()),
        StructField("payment_value", DecimalType(10, 2)),
    ]
)

CATEGORY_TRANSLATION_SCHEMA = StructType(
    [
        StructField("product_category_name", StringType()),
        StructField("product_category_name_english", StringType()),
    ]
)
