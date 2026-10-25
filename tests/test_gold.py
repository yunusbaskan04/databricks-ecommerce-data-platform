from datetime import date
from decimal import Decimal

import pytest

from olist_platform.gold import (
    REVENUE_EXCLUDED_STATUSES,
    build_category_sales,
    build_daily_sales,
    build_order_status,
)

DAY = date(2018, 1, 10)

ITEMS_SCHEMA = (
    "order_id STRING, order_item_id INT, product_id STRING, order_status STRING, "
    "order_date DATE, price DECIMAL(10,2), freight_value DECIMAL(10,2), "
    "product_category_name_english STRING"
)


@pytest.fixture
def items(spark):
    return spark.createDataFrame(
        [
            ("o1", 1, "p1", "delivered", DAY, Decimal("10.00"), Decimal("1.00"), "toys"),
            ("o1", 2, "p2", "delivered", DAY, Decimal("20.00"), Decimal("2.00"), "toys"),
            ("o2", 1, "p1", "shipped", DAY, Decimal("10.00"), Decimal("1.00"), "toys"),
            ("o3", 1, "p3", "canceled", DAY, Decimal("500.00"), Decimal("9.00"), "toys"),
            ("o4", 1, "p4", "unavailable", DAY, Decimal("700.00"), Decimal("9.00"), "toys"),
        ],
        ITEMS_SCHEMA,
    )


def test_revenue_rule_is_pinned():
    # Changing this changes the headline dashboard number: update ADR-0003 first.
    assert REVENUE_EXCLUDED_STATUSES == ("canceled", "unavailable")


def test_daily_sales_excludes_canceled_and_unavailable(items):
    row = build_daily_sales(items).first()
    assert row.total_revenue == Decimal("40.00")
    assert row.total_freight == Decimal("4.00")
    assert row.total_orders == 2
    assert row.items_sold == 3


def test_daily_sales_counts_orders_not_items(items):
    # o1 has two items but is one order.
    assert build_daily_sales(items).first().total_orders == 2


def test_category_sales(items):
    row = build_category_sales(items).first()
    assert row.product_category == "toys"
    assert row.total_revenue == Decimal("40.00")
    assert row.unique_products == 2


def test_order_status_keeps_all_statuses(spark):
    orders = spark.createDataFrame(
        [
            ("o1", "delivered", Decimal("30.00"), Decimal("33.00")),
            ("o2", "delivered", Decimal("10.00"), Decimal("11.00")),
            ("o3", "canceled", Decimal("500.00"), None),
        ],
        "order_id STRING, order_status STRING, items_value DECIMAL(10,2), "
        "total_payment_value DECIMAL(10,2)",
    )
    by_status = {r.order_status: r for r in build_order_status(orders).collect()}
    assert by_status["delivered"].total_orders == 2
    assert by_status["delivered"].total_payment_value == Decimal("44.00")
    assert by_status["canceled"].total_orders == 1
