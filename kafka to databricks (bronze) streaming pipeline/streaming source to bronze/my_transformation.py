# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze layer: Kafka/Debezium CDC ingestion
# MAGIC
# MAGIC Reads directly from Kafka topics produced by the Debezium Postgres (Neon)
# MAGIC connector, parses the Debezium change-event envelope, and lands bronze
# MAGIC tables with the SAME columns/names silver.py already expects - this is a
# MAGIC drop-in replacement for the old Auto Loader/file-landing bronze source
# MAGIC for these 11 tables, since they're now all CDC-sourced via Debezium.
# MAGIC
# MAGIC IMPORTANT prerequisite: the Debezium connector must be configured with
# MAGIC `"decimal.handling.mode": "double"` - without it, NUMERIC/DECIMAL columns
# MAGIC (price, amount, unit_price, discount_pct) arrive as base64-encoded bytes,
# MAGIC not plain numbers, and will silently parse incorrectly.
# MAGIC
# MAGIC What this does NOT yet handle: deletes (op = 'd'). Debezium emits a delete
# MAGIC event with `after: null`, so those rows are currently filtered out rather
# MAGIC than propagated - a known gap, not an oversight, flagged for later.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
)

# Use DLT pipeline configuration (Serverless-compatible)
KAFKA_BOOTSTRAP_SERVERS = spark.conf.get("kafka_bootstrap_servers", "7.tcp.ngrok.io:24168")
TOPIC_PREFIX = spark.conf.get("kafka_topic_prefix", "neon.public")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debezium envelope helpers
# MAGIC
# MAGIC Every Debezium change event has the same outer shape - before/after (both
# MAGIC using the TABLE's own row schema), a source block, and an op code
# MAGIC ('c'=create, 'u'=update, 'd'=delete, 'r'=snapshot read). Only the inner row
# MAGIC schema differs per table, so this only needs to be defined once.

# COMMAND ----------


def debezium_envelope_schema(row_schema):
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
    """
    raw_kafka_df: the result of spark.readStream.format("kafka")...load()
    row_schema:   StructType matching the table's own columns (Debezium's
                  raw representation - e.g. dates as day-offset ints)
    date_fields:  column names that are Debezium `io.debezium.time.Date`
                  logical types (int32 = days since 1970-01-01), which need
                  explicit conversion - a plain cast would misinterpret them
    """
    date_fields = date_fields or []
    envelope_schema = debezium_envelope_schema(row_schema)

    parsed = (
        raw_kafka_df
        .select(F.get_json_object(F.col("value").cast("string"), "$.payload").alias("payload_json"))
        .select(F.from_json(F.col("payload_json"), envelope_schema).alias("envelope"))
        # 'r' = initial snapshot read, 'c' = insert, 'u' = update. Deletes ('d')
        # are filtered out here - see the module docstring above.
        .filter("envelope.op != 'd'")
        .select("envelope.after.*", F.col("envelope.op").alias("__op"))
    )

    # Apply all date conversions at once to avoid nested execution plan
    date_conversions = {
        field_name: F.expr(f"date_add(to_date('1970-01-01'), {field_name})")
        for field_name in date_fields
    }
    date_conversions["_bronze_loaded_at"] = F.current_timestamp()
    
    return parsed.withColumns(date_conversions)


def read_debezium_topic(table_name):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", f"{TOPIC_PREFIX}.{table_name}")
        .option("startingOffsets", "earliest")
        .load()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-table row schemas
# MAGIC
# MAGIC One StructType per table, matching exactly the columns silver.py's
# MAGIC `dlt.read_stream("bronze_<table>")` calls already expect. `date_fields`
# MAGIC lists which columns need the day-offset conversion; everything else
# MAGIC (including ZonedTimestamp columns like updated_at/created_at, which
# MAGIC arrive as ISO-8601 strings) can be cast normally downstream in silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables to ingest
# MAGIC
# MAGIC All 11 tables, same pattern as `customers`.

# COMMAND ----------

TABLE_DEFINITIONS = {
    "customers": {
        "schema": StructType([
            StructField("customer_id", IntegerType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("signup_date", IntegerType()),
            StructField("phone", StringType()),
            StructField("date_of_birth", IntegerType()),
            StructField("loyalty_tier", StringType()),
            StructField("updated_at", StringType()),
        ]),
        "date_fields": ["signup_date", "date_of_birth"],
    },
    "products": {
        "schema": StructType([
            StructField("product_id", IntegerType()),
            StructField("sku", StringType()),
            StructField("name", StringType()),
            StructField("category", StringType()),
            StructField("category_id", IntegerType()),
            StructField("price", DoubleType()),
            StructField("updated_at", StringType()),
        ]),
        "date_fields": [],
    },
    "orders": {
        "schema": StructType([
            StructField("order_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("channel_id", StringType()),
            StructField("product", StringType()),
            StructField("amount", DoubleType()),
            StructField("status", StringType()),
            StructField("created_at", StringType()),
            StructField("updated_at", StringType()),
        ]),
        "date_fields": [],
    },
    "order_items": {
        "schema": StructType([
            StructField("order_item_id", IntegerType()),
            StructField("order_id", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("discount_pct", DoubleType()),
        ]),
        "date_fields": [],
    },
    "addresses": {
        "schema": StructType([
            StructField("address_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("address_type", StringType()),
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("zip_code", StringType()),
            StructField("country", StringType()),
            StructField("created_at", StringType()),
        ]),
        "date_fields": [],
    },
    "order_status_history": {
        "schema": StructType([
            StructField("history_id", IntegerType()),
            StructField("order_id", IntegerType()),
            StructField("old_status", StringType()),
            StructField("new_status", StringType()),
            StructField("changed_at", StringType()),
        ]),
        "date_fields": [],
    },
    "reviews": {
        "schema": StructType([
            StructField("review_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("product_id", IntegerType()),
            StructField("rating", IntegerType()),
            StructField("review_text", StringType()),
            StructField("created_at", StringType()),
        ]),
        "date_fields": [],
    },
    "payments": {
        "schema": StructType([
            StructField("payment_id", IntegerType()),
            StructField("order_id", IntegerType()),
            StructField("amount", DoubleType()),
            StructField("method", StringType()),
            StructField("status", StringType()),
            StructField("paid_at", StringType()),
        ]),
        "date_fields": [],
    },
    "shipments": {
        "schema": StructType([
            StructField("shipment_id", IntegerType()),
            StructField("order_id", IntegerType()),
            StructField("carrier", StringType()),
            StructField("tracking_number", StringType()),
            StructField("shipped_at", StringType()),
            StructField("delivered_at", StringType()),
            StructField("ship_status", StringType()),
        ]),
        "date_fields": [],
    },
    "categories": {
        "schema": StructType([
            StructField("category_id", IntegerType()),
            StructField("category_name", StringType()),
            StructField("parent_category_id", IntegerType()),
        ]),
        "date_fields": [],
    },
    "channels": {
        "schema": StructType([
            StructField("channel_id", StringType()),
            StructField("channel_name", StringType()),
            StructField("channel_type", StringType()),
        ]),
        "date_fields": [],
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate one bronze table per Debezium topic
# MAGIC
# MAGIC Same factory-function-per-loop-iteration pattern as the Auto Loader
# MAGIC version of bronze.py, for the same reason: each call to
# MAGIC `make_bronze_kafka_table` gets its own local `table_name`/`definition`
# MAGIC binding, so the generated functions don't all collide on the last
# MAGIC loop value.

# COMMAND ----------


def make_bronze_kafka_table(table_name, definition):
    @dlt.table(
        name=f"bronze_{table_name}",
        comment=f"CDC bronze table for {table_name}, streamed from Debezium via Kafka.",
    )
    def _bronze_table():
        raw = read_debezium_topic(table_name)
        return parse_debezium_topic(raw, definition["schema"], definition["date_fields"])
    return _bronze_table


for table_name, definition in TABLE_DEFINITIONS.items():
    make_bronze_kafka_table(table_name, definition)