from datetime import datetime
from decimal import Decimal

import pytest

from olist_platform import schemas
from olist_platform.silver import apply_quality_rules, build_silver

PURCHASED = datetime(2018, 1, 10, 9, 30)


@pytest.fixture
def bronze(bronze_df):
    """A small but complete Bronze snapshot covering every ADR-0002 rule."""
    return {
        "orders": bronze_df(
            schemas.ORDERS_SCHEMA,
            [
                {
                    "order_id": "o1",
                    "customer_id": "c1",
                    "order_status": "delivered",
                    "order_purchase_timestamp": PURCHASED,
                },
                {
                    "order_id": "o2",
                    "customer_id": "c2",
                    "order_status": "canceled",
                    "order_purchase_timestamp": PURCHASED,
                },
                {"order_id": None, "customer_id": "c1", "order_status": "delivered"},
                {"order_id": "o3", "customer_id": "c1", "_rescued_data": '{"x": "bad"}'},
            ],
        ),
        "customers": bronze_df(
            schemas.CUSTOMERS_SCHEMA,
            [
                {"customer_id": "c1", "customer_unique_id": "u1", "customer_state": "SP"},
                {"customer_id": "c2", "customer_unique_id": "u2", "customer_state": "RJ"},
            ],
        ),
        "products": bronze_df(
            schemas.PRODUCTS_SCHEMA,
            [
                {"product_id": "p1", "product_category_name": "beleza_saude"},
                {"product_id": "p2", "product_category_name": None},
                {"product_id": "p3", "product_category_name": "pc_gamer"},
            ],
        ),
        "order_items": bronze_df(
            schemas.ORDER_ITEMS_SCHEMA,
            [
                {
                    "order_id": "o1",
                    "order_item_id": 1,
                    "product_id": "p1",
                    "price": Decimal("10.00"),
                    "freight_value": Decimal("2.00"),
                },
                {
                    "order_id": "o1",
                    "order_item_id": 2,
                    "product_id": "p2",
                    "price": Decimal("20.00"),
                    "freight_value": Decimal("3.00"),
                },
                {
                    "order_id": "o1",
                    "order_item_id": 3,
                    "product_id": "p3",
                    "price": Decimal("5.00"),
                    "freight_value": Decimal("1.00"),
                },
                {
                    "order_id": "ghost",
                    "order_item_id": 1,
                    "product_id": "p1",
                    "price": Decimal("99.00"),
                    "freight_value": Decimal("0.00"),
                },
            ],
        ),
        "payments": bronze_df(
            schemas.PAYMENTS_SCHEMA,
            [
                {"order_id": "o1", "payment_sequential": 1, "payment_value": Decimal("30.00")},
                {"order_id": "o1", "payment_sequential": 2, "payment_value": Decimal("11.00")},
            ],
        ),
        "category_translation": bronze_df(
            schemas.CATEGORY_TRANSLATION_SCHEMA,
            [
                {
                    "product_category_name": "beleza_saude",
                    "product_category_name_english": "health_beauty",
                }
            ],
        ),
    }


@pytest.fixture
def silver(bronze):
    return build_silver(bronze)


def test_silver_orders_has_one_row_per_order(silver):
    rows = silver.orders.collect()
    assert sorted(r.order_id for r in rows) == ["o1", "o2"]


def test_payments_are_aggregated_not_multiplied(silver):
    o1 = silver.orders.filter("order_id = 'o1'").first()
    assert o1.total_payment_value == Decimal("41.00")
    assert o1.payment_count == 2
    assert o1.item_count == 3
    assert o1.items_value == Decimal("35.00")


def test_order_without_items_or_payments_stays_valid(silver):
    o2 = silver.orders.filter("order_id = 'o2'").first()
    assert o2.item_count == 0
    assert o2.payment_count == 0
    assert o2.total_payment_value is None


def test_silver_order_items_has_one_row_per_item(silver):
    keys = [(r.order_id, r.order_item_id) for r in silver.order_items.collect()]
    assert sorted(keys) == [("o1", 1), ("o1", 2), ("o1", 3)]


def test_category_fallbacks(silver):
    by_product = {r.product_id: r for r in silver.order_items.collect()}
    assert by_product["p1"].product_category_name_english == "health_beauty"
    assert by_product["p2"].product_category_name == "unknown"
    assert by_product["p2"].product_category_name_english == "unknown"
    # No translation exists: fall back to the Portuguese name.
    assert by_product["p3"].product_category_name_english == "pc_gamer"


def test_items_carry_order_attributes(silver):
    item = silver.order_items.filter("order_item_id = 1").first()
    assert item.order_status == "delivered"
    assert item.order_date == PURCHASED.date()


def test_quarantine_reasons(silver):
    reasons = sorted((r.entity, r._dq_reason) for r in silver.quarantine.collect())
    assert reasons == [
        ("order_items", "orphan_order"),
        ("orders", "null_key"),
        ("orders", "rescued_data"),
    ]


def test_quarantine_record_keeps_rejected_row(silver):
    orphan = silver.quarantine.filter("_dq_reason = 'orphan_order'").first()
    assert '"order_id":"ghost"' in orphan.record
    assert orphan._source_file == "test.csv"


def test_duplicate_keeps_latest_ingestion(bronze_df):
    orders = bronze_df(
        schemas.ORDERS_SCHEMA,
        [
            {"order_id": "o1", "order_status": "shipped", "_ingested_at": datetime(2026, 1, 1)},
            {"order_id": "o1", "order_status": "delivered", "_ingested_at": datetime(2026, 1, 2)},
        ],
    )
    result = apply_quality_rules(orders, "orders")

    assert [r.order_status for r in result.valid.collect()] == ["delivered"]
    assert [r._dq_reason for r in result.quarantine.collect()] == ["duplicate_key"]
