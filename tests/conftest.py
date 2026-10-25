import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

INGESTED_AT = datetime(2026, 10, 25, 12, 0, 0)

_METADATA_FIELDS = [
    StructField("_rescued_data", StringType()),
    StructField("_ingested_at", TimestampType()),
    StructField("_source_file", StringType()),
]


@pytest.fixture(scope="session")
def spark():
    # Make Spark's Python workers use the same interpreter as the tests.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("olist-platform-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def bronze_df(spark):
    """Build a Bronze-shaped DataFrame from dicts; missing fields become null."""

    def build(schema: StructType, rows: list[dict]) -> DataFrame:
        full_schema = StructType(schema.fields + _METADATA_FIELDS)
        defaults = {"_ingested_at": INGESTED_AT, "_source_file": "test.csv"}
        tuples = [
            tuple({**defaults, **row}.get(field.name) for field in full_schema.fields)
            for row in rows
        ]
        return spark.createDataFrame(tuples, full_schema)

    return build
