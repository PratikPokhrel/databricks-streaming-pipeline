# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Tests for Debezium/Kafka Bronze Ingestion
# MAGIC 
# MAGIC Tests the core transformation logic for parsing Debezium CDC events.
# MAGIC 
# MAGIC **Note**: This file tests the transformation helpers (`parse_debezium_topic`,
# MAGIC `debezium_envelope_schema`) directly since the pipeline reads from Kafka.
# MAGIC To enable full end-to-end testing, consider refactoring the pipeline to
# MAGIC read from a configurable source (table or Kafka).

# COMMAND ----------

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
)
from pyspark.pipelines.testing import test_spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions
# MAGIC 
# MAGIC These replicate the core transformation logic from my_transformation.py

# COMMAND ----------

def debezium_envelope_schema(row_schema):
    """Schema for Debezium change event envelope."""
    source_schema = StructType([
        StructField("table", StringType()),
        StructField("ts_ms", LongType()),
        StructField("lsn", LongType()),
    ])
    return StructType([
        StructField("before", row_schema),
        StructField("after", row_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ])

def parse_debezium_topic(raw_kafka_df, row_schema, date_fields=None):
    """Parse Debezium envelope and convert date fields."""
    date_fields = date_fields or []
    envelope_schema = debezium_envelope_schema(row_schema)

    parsed = (
        raw_kafka_df
        .select(F.get_json_object(F.col("value").cast("string"), "$.payload").alias("payload_json"))
        .select(F.from_json(F.col("payload_json"), envelope_schema).alias("envelope"))
        .filter("envelope.op != 'd'")
        .select("envelope.after.*", F.col("envelope.op").alias("__op"))
    )

    for field_name in date_fields:
        parsed = parsed.withColumn(
            field_name, F.expr(f"date_add(to_date('1970-01-01'), {field_name})")
        )
    return parsed.withColumn("_bronze_loaded_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Debezium Envelope Parsing

# COMMAND ----------

def test_parse_debezium_creates_and_updates(test_spark):
    """
    Verify that parse_debezium_topic correctly extracts data from Debezium
    'create' (c) and 'update' (u) operations.
    """
    # Define customer row schema
    customer_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("signup_date", IntegerType()),
        StructField("phone", StringType()),
        StructField("date_of_birth", IntegerType()),
        StructField("loyalty_tier", StringType()),
        StructField("updated_at", StringType()),
    ])
    
    # Create mock Kafka-like DataFrame with Debezium payloads
    json_data = [
        # Create operation
        ("""{"payload": {
            "before": null,
            "after": {
                "customer_id": 1,
                "name": "Alice Johnson",
                "email": "alice@example.com",
                "signup_date": 19358,
                "phone": "555-0100",
                "date_of_birth": 7305,
                "loyalty_tier": "gold",
                "updated_at": "2023-01-01T10:00:00Z"
            },
            "source": {"table": "customers", "ts_ms": 1704067200000, "lsn": 123},
            "op": "c",
            "ts_ms": 1704067200000
        }}""",),
        # Update operation
        ("""{"payload": {
            "before": {"customer_id": 2, "name": "Bob"},
            "after": {
                "customer_id": 2,
                "name": "Bob Smith",
                "email": "bob@example.com",
                "signup_date": 19400,
                "phone": "555-0200",
                "date_of_birth": 10957,
                "loyalty_tier": "silver",
                "updated_at": "2023-02-12T15:30:00Z"
            },
            "source": {"table": "customers", "ts_ms": 1704067300000, "lsn": 124},
            "op": "u",
            "ts_ms": 1704067300000
        }}""",),
    ]
    
    raw_df = test_spark.createDataFrame(json_data, ["value"])
    
    # Parse the Debezium messages
    result_df = parse_debezium_topic(
        raw_df, 
        customer_schema, 
        date_fields=["signup_date", "date_of_birth"]
    )
    
    rows = result_df.collect()
    
    # Verify both records are present
    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
    
    # Verify first customer (create operation)
    alice = [r for r in rows if r.customer_id == 1][0]
    assert alice.name == "Alice Johnson"
    assert alice.email == "alice@example.com"
    assert str(alice.signup_date) == "2023-01-01"
    assert str(alice.date_of_birth) == "1990-01-01"
    assert alice.loyalty_tier == "gold"
    assert alice._bronze_loaded_at is not None
    
    # Verify second customer (update operation)
    bob = [r for r in rows if r.customer_id == 2][0]
    assert bob.name == "Bob Smith"
    assert str(bob.signup_date) == "2023-02-12"
    assert str(bob.date_of_birth) == "2000-01-01"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Delete Operation Filtering

# COMMAND ----------

def test_parse_debezium_filters_deletes(test_spark):
    """
    Verify that delete operations (op='d') are filtered out.
    """
    customer_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("signup_date", IntegerType()),
        StructField("phone", StringType()),
        StructField("date_of_birth", IntegerType()),
        StructField("loyalty_tier", StringType()),
        StructField("updated_at", StringType()),
    ])
    
    json_data = [
        # Create
        ("""{"payload": {
            "after": {
                "customer_id": 1, "name": "Alice", "email": "alice@example.com",
                "signup_date": 19358, "phone": "555-0100", "date_of_birth": 7305,
                "loyalty_tier": "gold", "updated_at": "2023-01-01T10:00:00Z"
            },
            "source": {"table": "customers", "ts_ms": 1704067200000, "lsn": 123},
            "op": "c", "ts_ms": 1704067200000
        }}""",),
        # Delete (should be filtered out)
        ("""{"payload": {
            "before": {"customer_id": 999, "name": "Deleted User"},
            "after": null,
            "source": {"table": "customers", "ts_ms": 1704067250000, "lsn": 125},
            "op": "d", "ts_ms": 1704067250000
        }}""",),
        # Update
        ("""{"payload": {
            "after": {
                "customer_id": 2, "name": "Bob", "email": "bob@example.com",
                "signup_date": 19400, "phone": "555-0200", "date_of_birth": 10957,
                "loyalty_tier": "silver", "updated_at": "2023-02-12T15:30:00Z"
            },
            "source": {"table": "customers", "ts_ms": 1704067300000, "lsn": 126},
            "op": "u", "ts_ms": 1704067300000
        }}""",),
    ]
    
    raw_df = test_spark.createDataFrame(json_data, ["value"])
    result_df = parse_debezium_topic(raw_df, customer_schema, date_fields=["signup_date", "date_of_birth"])
    
    rows = result_df.collect()
    
    # Only create and update should remain (delete filtered out)
    assert len(rows) == 2, f"Expected 2 rows (delete filtered), got {len(rows)}"
    assert all(r.customer_id in [1, 2] for r in rows), "Unexpected customer_ids found"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Snapshot Reads

# COMMAND ----------

def test_parse_debezium_handles_snapshot_reads(test_spark):
    """
    Verify that initial snapshot reads (op='r') are processed correctly.
    """
    customer_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("signup_date", IntegerType()),
        StructField("phone", StringType()),
        StructField("date_of_birth", IntegerType()),
        StructField("loyalty_tier", StringType()),
        StructField("updated_at", StringType()),
    ])
    
    json_data = [
        ("""{"payload": {
            "after": {
                "customer_id": 100, "name": "Charlie Brown", "email": "charlie@example.com",
                "signup_date": 19000, "phone": "555-0300", "date_of_birth": 8000,
                "loyalty_tier": "bronze", "updated_at": "2022-01-01T00:00:00Z"
            },
            "source": {"table": "customers", "ts_ms": 1640995200000, "lsn": 1},
            "op": "r", "ts_ms": 1640995200000
        }}""",),
    ]
    
    raw_df = test_spark.createDataFrame(json_data, ["value"])
    result_df = parse_debezium_topic(raw_df, customer_schema, date_fields=["signup_date", "date_of_birth"])
    
    rows = result_df.collect()
    
    assert len(rows) == 1
    assert rows[0].customer_id == 100
    assert rows[0].name == "Charlie Brown"
    assert rows[0].email == "charlie@example.com"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Tables Without Date Fields

# COMMAND ----------

def test_parse_debezium_handles_no_date_fields(test_spark):
    """
    Verify that tables without date fields (e.g., products with price)
    are processed correctly without date conversion.
    """
    product_schema = StructType([
        StructField("product_id", IntegerType()),
        StructField("sku", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("category_id", IntegerType()),
        StructField("price", DoubleType()),
        StructField("updated_at", StringType()),
    ])
    
    json_data = [
        ("""{"payload": {
            "after": {
                "product_id": 101, "sku": "SKU-12345", "name": "Widget Pro",
                "category": "Electronics", "category_id": 5, "price": 299.99,
                "updated_at": "2023-03-15T12:00:00Z"
            },
            "source": {"table": "products", "ts_ms": 1678881600000, "lsn": 200},
            "op": "c", "ts_ms": 1678881600000
        }}""",),
    ]
    
    raw_df = test_spark.createDataFrame(json_data, ["value"])
    result_df = parse_debezium_topic(raw_df, product_schema, date_fields=[])
    
    rows = result_df.collect()
    
    assert len(rows) == 1
    assert rows[0].product_id == 101
    assert rows[0].sku == "SKU-12345"
    assert rows[0].name == "Widget Pro"
    assert rows[0].price == 299.99
    assert rows[0]._bronze_loaded_at is not None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Date Field Conversion Accuracy

# COMMAND ----------

def test_date_field_conversion_accuracy(test_spark):
    """
    Verify that Debezium date field conversion (days since epoch)
    produces correct date values.
    """
    schema_with_date = StructType([
        StructField("id", IntegerType()),
        StructField("event_date", IntegerType()),
    ])
    
    # Test known date conversions:
    # 0 -> 1970-01-01
    # 19358 -> 2023-01-01
    # 10957 -> 2000-01-01
    json_data = [
        ("""{"payload": {
            "after": {"id": 1, "event_date": 0},
            "source": {"table": "events", "ts_ms": 1000000, "lsn": 1},
            "op": "c", "ts_ms": 1000000
        }}""",),
        ("""{"payload": {
            "after": {"id": 2, "event_date": 19358},
            "source": {"table": "events", "ts_ms": 1000000, "lsn": 2},
            "op": "c", "ts_ms": 1000000
        }}""",),
        ("""{"payload": {
            "after": {"id": 3, "event_date": 10957},
            "source": {"table": "events", "ts_ms": 1000000, "lsn": 3},
            "op": "c", "ts_ms": 1000000
        }}""",),
    ]
    
    raw_df = test_spark.createDataFrame(json_data, ["value"])
    result_df = parse_debezium_topic(raw_df, schema_with_date, date_fields=["event_date"])
    
    rows = sorted(result_df.collect(), key=lambda r: r.id)
    
    assert str(rows[0].event_date) == "1970-01-01"
    assert str(rows[1].event_date) == "2023-01-01"
    assert str(rows[2].event_date) == "2000-01-01"