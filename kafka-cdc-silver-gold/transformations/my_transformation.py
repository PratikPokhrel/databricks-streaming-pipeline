# Databricks notebook source
# MAGIC %md
# MAGIC # Silver + Gold: full pipeline (all 11 tables)
# MAGIC
# MAGIC Bronze lives in a SEPARATE pipeline (`bronze_kafka.py`, its own
# MAGIC continuous job) - continuous jobs can't have task dependencies, so
# MAGIC bronze and this pipeline run as two independent continuous jobs, each
# MAGIC picking up new data via normal Structured Streaming incremental reads,
# MAGIC not job-level sequencing.
# MAGIC
# MAGIC Because of that pipeline boundary: every read of a `bronze_*` table
# MAGIC uses `spark.readStream.table("<bronze_catalog>.<bronze_schema>.bronze_x")`
# MAGIC (cross-pipeline), NOT `dlt.read_stream("bronze_x")` (same-pipeline only).
# MAGIC Everything WITHIN this file - silver tables feeding gold tables - uses
# MAGIC plain `dlt.read()`/`dlt.read_stream()`, since silver and gold live in
# MAGIC the same pipeline here.
# MAGIC
# MAGIC Design rules carried through from earlier:
# MAGIC - Silver = standardization + SCD2/SCD1 history + data quality. No
# MAGIC   joins, no business logic, no surrogate keys.
# MAGIC - Gold = surrogate keys (keyed on natural key + valid_from, NOT a
# MAGIC   tracked business column - avoids the tier-reversion collision bug),
# MAGIC   point-in-time joins for facts, current-state joins for aggregates.

import dlt
from pyspark.sql import functions as F

BRONZE_CATALOG = spark.conf.get("bronze_catalog", "main")
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "silver")


def _bronze_table(name):
    return f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## CUSTOMERS: silver (SCD2) + dim_customers

# COMMAND ----------


@dlt.view(name="customers_cleaned")
def customers_cleaned():
    df = spark.readStream.table(_bronze_table("bronze_customers"))
    return df.select(
        F.col("customer_id"),
        F.trim(F.col("name")).alias("customer_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.col("signup_date"),
        F.col("phone"),
        F.col("date_of_birth"),
        F.coalesce(F.col("loyalty_tier"), F.lit("Bronze")).alias("loyalty_tier"),
        F.col("updated_at").cast("timestamp"),
    )


dlt.create_streaming_table(
    name="silver_customers",
    comment="Standardized customers with SCD2 history. Surrogate key lives in gold.",
    expect_all_or_drop={"has_email": "email IS NOT NULL"},
    expect_all={"valid_signup_date": "signup_date <= current_date()"},
)

dlt.apply_changes(
    target="silver_customers",
    source="customers_cleaned",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["loyalty_tier", "customer_name"],
)


@dlt.table(
    name="dim_customers",
    comment="Customer dimension with gold-generated surrogate key and is_current flag.",
)
def dim_customers():
    df = dlt.read("silver_customers")
    return (
        df.withColumnRenamed("__START_AT", "valid_from")
        .withColumnRenamed("__END_AT", "valid_to")
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn(
            "customer_sk",
            F.sha2(F.concat_ws("||", F.col("customer_id"), F.col("valid_from").cast("string")), 256),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRODUCTS: silver (SCD2) + dim_products

# COMMAND ----------


@dlt.view(name="products_cleaned")
def products_cleaned():
    df = spark.readStream.table(_bronze_table("bronze_products"))
    return df.select(
        F.col("product_id"),
        F.col("sku"),
        F.trim(F.col("name")).alias("product_name"),
        F.col("category"),
        F.col("category_id"),
        F.col("price").cast("decimal(10,2)"),
        F.col("updated_at").cast("timestamp"),
    )


dlt.create_streaming_table(
    name="silver_products",
    comment="Standardized products with SCD2 history. Surrogate key lives in gold.",
    expect_all_or_drop={"has_sku": "sku IS NOT NULL"},
    expect_all={"valid_price": "price > 0"},
)

dlt.apply_changes(
    target="silver_products",
    source="products_cleaned",
    keys=["product_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["price"],
)


@dlt.table(
    name="dim_products",
    comment="Product dimension with gold-generated surrogate key and is_current flag.",
)
def dim_products():
    df = dlt.read("silver_products")
    return (
        df.withColumnRenamed("__START_AT", "valid_from")
        .withColumnRenamed("__END_AT", "valid_to")
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn(
            "product_sk",
            F.sha2(F.concat_ws("||", F.col("product_id"), F.col("valid_from").cast("string")), 256),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ORDERS: silver (SCD1 - latest version wins, no history needed)

# COMMAND ----------


@dlt.view(name="orders_cleaned")
def orders_cleaned():
    df = spark.readStream.table(_bronze_table("bronze_orders"))
    return df.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("channel_id"),
        F.col("product"),
        F.col("amount").cast("decimal(10,2)"),
        F.col("status").alias("order_status"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),
    )


dlt.create_streaming_table(
    name="silver_orders",
    comment="Standardized orders - latest version per order_id via apply_changes.",
    expect_all_or_drop={"valid_order_id": "order_id IS NOT NULL"},
    expect_all={"valid_amount": "amount > 0", "has_customer": "customer_id IS NOT NULL"},
)

dlt.apply_changes(
    target="silver_orders",
    source="orders_cleaned",
    keys=["order_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ORDER_ITEMS, ADDRESSES, ORDER_STATUS_HISTORY, REVIEWS
# MAGIC Insert-only facts - typed and quality-checked, no dedup/history needed.

# COMMAND ----------


@dlt.table(
    name="silver_order_items",
    comment="Standardized order line items - typed, quality-checked.",
)
@dlt.expect_or_drop("valid_order_item_id", "order_item_id IS NOT NULL")
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect("valid_unit_price", "unit_price > 0")
def silver_order_items():
    df = spark.readStream.table(_bronze_table("bronze_order_items"))
    return df.select(
        F.col("order_item_id"),
        F.col("order_id"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price").cast("decimal(10,2)"),
        F.coalesce(F.col("discount_pct"), F.lit(0)).alias("discount_pct"),
    )


@dlt.table(
    name="silver_addresses",
    comment="Standardized addresses - typed, quality-checked.",
)
@dlt.expect_or_drop("has_customer", "customer_id IS NOT NULL")
def silver_addresses():
    df = spark.readStream.table(_bronze_table("bronze_addresses"))
    return df.select(
        F.col("address_id"),
        F.col("customer_id"),
        F.coalesce(F.col("address_type"), F.lit("shipping")).alias("address_type"),
        F.col("street"),
        F.col("city"),
        F.col("state"),
        F.col("zip_code"),
        F.col("country"),
        F.col("created_at").cast("timestamp"),
    )


@dlt.table(
    name="silver_order_status_history",
    comment="Standardized order status history - typed.",
)
@dlt.expect_or_drop("has_order_id", "order_id IS NOT NULL")
def silver_order_status_history():
    df = spark.readStream.table(_bronze_table("bronze_order_status_history"))
    return df.select(
        F.col("history_id"),
        F.col("order_id"),
        F.col("old_status"),
        F.col("new_status"),
        F.col("changed_at").cast("timestamp"),
    )


@dlt.table(
    name="silver_reviews",
    comment="Standardized reviews - typed, quality-checked.",
)
@dlt.expect("valid_rating", "rating BETWEEN 1 AND 5")
def silver_reviews():
    df = spark.readStream.table(_bronze_table("bronze_reviews"))
    return df.select(
        F.col("review_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("rating").cast("int"),
        F.col("review_text"),
        F.col("created_at").cast("timestamp"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## PAYMENTS, SHIPMENTS, CATEGORIES, CHANNELS
# MAGIC Simple reference/status tables - insert-only, typed and quality-checked.

# COMMAND ----------


@dlt.table(
    name="silver_payments",
    comment="Standardized payments - typed, quality-checked.",
)
@dlt.expect_or_drop("has_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_amount", "amount > 0")
def silver_payments():
    df = spark.readStream.table(_bronze_table("bronze_payments"))
    return df.select(
        F.col("payment_id"),
        F.col("order_id"),
        F.col("amount").cast("decimal(10,2)"),
        F.col("method"),
        F.coalesce(F.col("status"), F.lit("completed")).alias("payment_status"),
        F.col("paid_at").cast("timestamp"),
    )


@dlt.table(
    name="silver_shipments",
    comment="Standardized shipments - typed, quality-checked.",
)
@dlt.expect_or_drop("has_order_id", "order_id IS NOT NULL")
def silver_shipments():
    df = spark.readStream.table(_bronze_table("bronze_shipments"))
    return df.select(
        F.col("shipment_id"),
        F.col("order_id"),
        F.col("carrier"),
        F.col("tracking_number"),
        F.col("shipped_at").cast("timestamp"),
        F.col("delivered_at").cast("timestamp"),
        F.col("ship_status"),
    )


@dlt.table(name="silver_categories", comment="Standardized categories - typed.")
def silver_categories():
    df = spark.readStream.table(_bronze_table("bronze_categories"))
    return df.select(
        F.col("category_id"),
        F.col("category_name"),
        F.col("parent_category_id"),
    )


@dlt.table(name="silver_channels", comment="Standardized channels - typed.")
def silver_channels():
    df = spark.readStream.table(_bronze_table("bronze_channels"))
    return df.select(
        F.col("channel_id"),
        F.col("channel_name"),
        F.col("channel_type"),
    )


@dlt.table(name="dim_channels", comment="Channel dimension - simple pass-through, no history.")
def dim_channels():
    return dlt.read("silver_channels")


@dlt.table(
    name="dim_date",
    comment="Date dimension spanning the observed order date range.",
)
def dim_date():
    orders = dlt.read("silver_orders")
    bounds = orders.select(
        F.min(F.to_date("created_at")).alias("min_date"),
        F.max(F.to_date("created_at")).alias("max_date"),
    )
    return (
        bounds.select(
            F.explode(
                F.sequence(F.col("min_date"), F.col("max_date"), F.expr("interval 1 day"))
            ).alias("full_date")
        )
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD FACTS: point-in-time joins against SCD2 dimensions

# COMMAND ----------


@dlt.table(
    name="gold_orders_enriched",
    comment="Orders enriched with the customer profile ACTIVE AT THE TIME the "
            "order was placed - point-in-time join, not current tier.",
)
def gold_orders_enriched():
    orders = dlt.read("silver_orders")
    customers = dlt.read("dim_customers")
    channels = dlt.read("dim_channels")

    return (
        orders.alias("o")
        .join(
            customers.alias("c"),
            (F.col("o.customer_id") == F.col("c.customer_id"))
            & (F.col("o.created_at") >= F.col("c.valid_from"))
            & (F.col("c.valid_to").isNull() | (F.col("o.created_at") < F.col("c.valid_to"))),
            "left",
        )
        .join(channels.alias("ch"), F.col("o.channel_id") == F.col("ch.channel_id"), "left")
        .select(
            F.col("o.order_id"),
            F.col("o.customer_id"),
            F.col("c.customer_sk"),
            F.col("c.customer_name"),
            F.col("c.loyalty_tier"),
            F.col("o.product"),
            F.col("o.amount"),
            F.col("o.order_status"),
            F.col("ch.channel_name"),
            F.col("o.created_at").alias("order_created_at"),
            F.to_date("o.created_at").alias("order_date"),
        )
    )


@dlt.table(
    name="gold_order_items_enriched",
    comment="Order line items enriched with the product price and customer "
            "ACTIVE AT THE TIME of the order - point-in-time joins.",
)
def gold_order_items_enriched():
    order_items = dlt.read("silver_order_items")
    orders = dlt.read("silver_orders")
    products = dlt.read("dim_products")
    customers = dlt.read("dim_customers")

    return (
        order_items.alias("oi")
        .join(orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"), "left")
        .join(
            products.alias("p"),
            (F.col("oi.product_id") == F.col("p.product_id"))
            & (F.col("o.created_at") >= F.col("p.valid_from"))
            & (F.col("p.valid_to").isNull() | (F.col("o.created_at") < F.col("p.valid_to"))),
            "left",
        )
        .join(
            customers.alias("c"),
            (F.col("o.customer_id") == F.col("c.customer_id"))
            & (F.col("o.created_at") >= F.col("c.valid_from"))
            & (F.col("c.valid_to").isNull() | (F.col("o.created_at") < F.col("c.valid_to"))),
            "left",
        )
        .select(
            F.col("oi.order_item_id"),
            F.col("oi.order_id"),
            F.col("oi.product_id"),
            F.col("p.product_sk"),
            F.col("o.customer_id"),
            F.col("c.customer_sk"),
            F.col("p.price").alias("price_at_time_of_order"),
            F.col("oi.quantity"),
            F.col("oi.discount_pct"),
            (
                F.col("oi.quantity")
                * F.col("p.price")
                * (F.lit(1) - F.coalesce(F.col("oi.discount_pct"), F.lit(0)))
            ).alias("line_total"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD AGGREGATES: current-state joins - NOT point-in-time

# COMMAND ----------


@dlt.table(
    name="gold_customer_order_summary",
    comment="Per-customer order totals using the customer's CURRENT profile - "
            "deliberately not point-in-time, since this answers 'who is this "
            "customer today', not history.",
)
def gold_customer_order_summary():
    orders = dlt.read("gold_orders_enriched")
    customers_current = dlt.read("dim_customers").filter("is_current = true")

    agg = orders.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("total_spend"),
        F.avg("amount").alias("avg_order_value"),
    )

    return (
        agg.alias("a")
        .join(customers_current.alias("c"), "customer_id", "left")
        .select(
            F.col("a.customer_id"),
            F.col("c.customer_sk"),
            F.col("c.customer_name"),
            F.col("c.loyalty_tier"),
            F.col("a.total_orders"),
            F.col("a.total_spend"),
            F.col("a.avg_order_value"),
        )
    )


@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregated by order date and channel.",
)
def gold_daily_revenue():
    orders = dlt.read("gold_orders_enriched")
    return orders.groupBy("order_date", "channel_name").agg(
        F.count("order_id").alias("num_orders"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_order_value"),
    )