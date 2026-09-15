# 🚀 Databricks Streaming Pipeline - Kafka CDC to Lakehouse

A production-ready, end-to-end streaming data pipeline that ingests Change Data Capture (CDC) events from Kafka using Debezium, transforms them through medallion architecture (Bronze → Silver → Gold), and serves analytics-ready datasets.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Pipeline Details](#pipeline-details)
- [Data Quality & Testing](#data-quality--testing)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Configuration](#configuration)

---

## 🎯 Overview

This project demonstrates a **real-time data lakehouse** implementation on Databricks using:

- **Kafka** as the event streaming platform
- **Debezium** for change data capture (CDC) from operational databases
- **Spark Declarative Pipelines (Lakeflow)** for incremental, streaming transformations
- **Delta Lake** for ACID transactions and time travel
- **Medallion Architecture** (Bronze/Silver/Gold) for data quality layers

### Use Case
Sync transactional data from a Postgres database (customers, orders, products, etc.) into Databricks for real-time analytics, maintaining full history with SCD Type 2 and creating analytics-ready dimensional models.

---

## 🏗️ Architecture

```
┌─────────────────┐
│  Postgres DB    │
│  (Operational)  │
└────────┬────────┘
         │ CDC
         ▼
┌─────────────────┐
│    Debezium     │
│  (CDC Capture)  │
└────────┬────────┘
         │ Kafka Topics
         ▼
┌─────────────────────────────────────────────────────────────┐
│                    DATABRICKS LAKEHOUSE                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ BRONZE LAYER (Raw Ingestion)                          │ │
│  │ • Parse Debezium envelope                             │ │
│  │ • Convert date fields (day offset → dates)            │ │
│  │ • Filter deletes (op='d')                             │ │
│  │ • Auto Loader from Kafka                              │ │
│  └─────────────────────┬─────────────────────────────────┘ │
│                        │                                    │
│                        ▼                                    │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ SILVER LAYER (Cleansed & Standardized)                │ │
│  │ • Data quality rules (expectations)                   │ │
│  │ • SCD Type 2 (customers, products)                    │ │
│  │ • SCD Type 1 (orders)                                 │ │
│  │ • Trim/normalize/coalesce                             │ │
│  └─────────────────────┬─────────────────────────────────┘ │
│                        │                                    │
│                        ▼                                    │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ GOLD LAYER (Analytics-Ready)                          │ │
│  │ • Surrogate keys (SHA-256 hash)                       │ │
│  │ • Dimensional models (star schema)                    │ │
│  │ • Point-in-time joins for facts                       │ │
│  │ • Aggregations & business metrics                     │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│   Dashboards    │
│   & Analytics   │
└─────────────────┘
```

---

## ✨ Features

### 🔥 Real-Time Streaming
- **Continuous processing** of CDC events from Kafka
- **Incremental ingestion** using Spark Structured Streaming
- **Exactly-once semantics** with Delta Lake

### 📊 Medallion Architecture
- **Bronze**: Raw, immutable data with Debezium metadata
- **Silver**: Cleansed, typed, deduplicated data with history
- **Gold**: Business-ready dimensional models (facts & dimensions)

### 🛡️ Data Quality
- **Built-in expectations** at every layer
- **Automated data quality checks** (nulls, ranges, formats)
- **Drop invalid records** or quarantine for review

### 📈 Slowly Changing Dimensions (SCD)
- **SCD Type 2** for customers and products (full history)
- **SCD Type 1** for orders (latest state only)
- **Automated tracking** with `apply_changes()`

### 🧪 Unit Testing
- **pytest-based test suite** for transformation logic
- **Mock Debezium payloads** for isolated testing
- **TestPipeline API** for end-to-end validation

### 🔄 CDC Support
- Parse **Debezium envelope** (before/after/op/source)
- Handle **create/update/snapshot/delete** operations
- Convert **Postgres date types** (day offset → proper dates)

---

## 📁 Project Structure

```
databricks-streaming-pipeline/
│
├── kafka to databricks (bronze) streaming pipeline/
│   ├── transformations/
│   │   └── my_transformation.py       # Bronze layer: Kafka → Delta
│   └── tests/
│       └── test_kafka_ingestion.py    # Unit tests for Debezium parsing
│
├── kafka-cdc-silver-gold/
│   └── transformations/
│       └── my_transformation.py       # Silver + Gold layers (11 tables)
│
└── README.md                          # This file
```

### Key Files

| File | Purpose |
|------|---------|
| `kafka to databricks (bronze) streaming pipeline/transformations/my_transformation.py` | Ingests Debezium CDC events from Kafka, parses envelope, converts dates, creates bronze tables |
| `kafka-cdc-silver-gold/transformations/my_transformation.py` | Reads bronze tables, applies SCD2/SCD1, creates silver tables, generates dimensional models (gold) |
| `kafka to databricks (bronze) streaming pipeline/tests/test_kafka_ingestion.py` | Unit tests for Debezium parsing, date conversion, operation filtering |

---

## 🔧 Prerequisites

### Infrastructure
- **Databricks Workspace** (DBR 18.1+)
- **Kafka Cluster** (Confluent, MSK, self-hosted)
- **Debezium Connector** for your source database
- **Unity Catalog** enabled (recommended)

### Access & Permissions
- **Kafka bootstrap servers** accessible from Databricks
- **Unity Catalog**: CREATE SCHEMA, CREATE TABLE permissions
- **Cluster or Serverless compute** with network access to Kafka

### Source Data
- Postgres database with tables: `customers`, `orders`, `products`, `order_items`, `addresses`, etc.
- Debezium CDC configured with topic prefix `neon.public`

---

## 🚀 Quick Start

### 1️⃣ Clone This Repository

```bash
# In Databricks Repos
git clone https://github.com/PratikPokhrel/databricks-streaming-pipeline.git
```

### 2️⃣ Configure Kafka Connection

Update the pipeline parameters with your Kafka details:

```python
# In pipeline settings
kafka_bootstrap_servers = "your-kafka-server:9092"
kafka_topic_prefix = "neon.public"  # Or your Debezium topic prefix
```

### 3️⃣ Create the Bronze Pipeline

1. Go to **Workflows** → **Lakeflow Pipelines** → **Create Pipeline**
2. Set **Name**: `Bronze CDC Ingestion`
3. Set **Notebook/File Path**: `/databricks-streaming-pipeline/kafka to databricks (bronze) streaming pipeline/transformations/my_transformation.py`
4. Set **Target Schema**: `main.bronze` (or your catalog.schema)
5. Enable **Serverless** (or select a cluster)
6. Set **Mode**: Triggered or Continuous
7. Add **Configuration**:
   ```
   kafka_bootstrap_servers: your-server:9092
   kafka_topic_prefix: neon.public
   ```
8. Click **Create** → **Start**

### 4️⃣ Create the Silver/Gold Pipeline

1. **After** bronze tables are populated, create another pipeline:
2. Set **Name**: `Silver + Gold Transformations`
3. Set **Notebook/File Path**: `/databricks-streaming-pipeline/kafka-cdc-silver-gold/transformations/my_transformation.py`
4. Set **Target Schema**: `main.silver` (creates silver_* and dim_* tables here)
5. Add **Configuration**:
   ```
   bronze_catalog: main
   bronze_schema: bronze
   ```
6. Click **Create** → **Start**

### 5️⃣ Verify the Data

```sql
-- Check bronze layer
SELECT * FROM main.bronze.bronze_customers LIMIT 10;

-- Check silver layer (SCD2)
SELECT * FROM main.silver.silver_customers 
WHERE customer_id = 1 
ORDER BY __START_AT;

-- Check gold dimension
SELECT * FROM main.silver.dim_customers 
WHERE is_current = true 
LIMIT 10;

-- Check fact table
SELECT * FROM main.silver.fact_order_items LIMIT 10;
```

---

## 📊 Pipeline Details

### Bronze Layer Tables

| Table | Source | Description |
|-------|--------|-------------|
| `bronze_customers` | `neon.public.customers` | Raw customer data with date conversion |
| `bronze_products` | `neon.public.products` | Raw product catalog |
| `bronze_orders` | `neon.public.orders` | Raw order headers |
| `bronze_order_items` | `neon.public.order_items` | Raw order line items |
| `bronze_addresses` | `neon.public.addresses` | Customer addresses |
| `bronze_channels` | `neon.public.channels` | Sales channels |
| `bronze_categories` | `neon.public.categories` | Product categories |
| `bronze_suppliers` | `neon.public.suppliers` | Supplier information |
| `bronze_inventory` | `neon.public.inventory` | Inventory levels |
| `bronze_order_status_history` | `neon.public.order_status_history` | Order status changes |
| `bronze_reviews` | `neon.public.reviews` | Product reviews |

**Key transformations:**
- Parse Debezium `payload.after` field
- Filter out deletes (`op != 'd'`)
- Convert date fields from day-offset (e.g., `signup_date`, `date_of_birth`)
- Add `_bronze_loaded_at` timestamp

### Silver Layer Tables

| Table | Type | Description | SCD Type |
|-------|------|-------------|----------|
| `silver_customers` | Dimension | Standardized customers with history | SCD2 |
| `silver_products` | Dimension | Standardized products with price history | SCD2 |
| `silver_orders` | Fact | Order headers (latest state) | SCD1 |
| `silver_order_items` | Fact | Order line items | Insert-only |
| `silver_addresses` | Dimension | Customer addresses | Insert-only |
| `silver_channels` | Dimension | Sales channels | SCD1 |
| `silver_categories` | Dimension | Product categories | SCD1 |
| `silver_suppliers` | Dimension | Suppliers | SCD1 |
| `silver_inventory` | Fact | Inventory snapshots | Insert-only |
| `silver_order_status_history` | Fact | Order status transitions | Insert-only |
| `silver_reviews` | Fact | Product reviews | Insert-only |

**Key transformations:**
- Data type casting and standardization
- `TRIM()`, `LOWER()`, `COALESCE()` for normalization
- Data quality expectations (null checks, range validation)
- SCD2 tracking with `__START_AT`, `__END_AT` columns

### Gold Layer Tables

| Table | Type | Description |
|-------|------|-------------|
| `dim_customers` | Dimension | Customer dimension with surrogate key |
| `dim_products` | Dimension | Product dimension with surrogate key |
| `dim_channels` | Dimension | Channel dimension |
| `dim_categories` | Dimension | Category dimension |
| `dim_suppliers` | Dimension | Supplier dimension |
| `fact_order_items` | Fact | Denormalized order line items with surrogate keys |
| `fact_inventory_snapshot` | Fact | Daily inventory levels with product SK |
| `fact_reviews` | Fact | Product reviews with customer/product SK |
| `agg_daily_sales_by_channel` | Aggregate | Daily sales rollup by channel |
| `agg_customer_lifetime_value` | Aggregate | Customer LTV and metrics |

**Key transformations:**
- Generate surrogate keys: `SHA2(natural_key || valid_from, 256)`
- Point-in-time joins for fact tables
- Current-state joins for aggregates
- Derived metrics (order_total, discount_amount, margin, etc.)

---

## 🧪 Data Quality & Testing

### Built-in Expectations

```python
# Drop records with missing critical fields
expect_all_or_drop={"has_email": "email IS NOT NULL"}

# Log violations but don't drop
expect_all={"valid_signup_date": "signup_date <= current_date()"}

# Inline expectations
@dlt.expect("valid_rating", "rating BETWEEN 1 AND 5")
```

### Run Unit Tests

```bash
# From the Lakeflow Pipeline Editor, click "Run tests"
# Or use the unit testing panel

# Tests verify:
# ✅ Debezium envelope parsing
# ✅ Date field conversion (19358 → 2023-01-01)
# ✅ Delete operation filtering (op='d')
# ✅ Snapshot read handling (op='r')
# ✅ Tables without date fields
```

### View Test Results

Test file: `kafka to databricks (bronze) streaming pipeline/tests/test_kafka_ingestion.py`

---

## 📈 Monitoring & Troubleshooting

### Check Pipeline Health

```sql
-- Event log for pipeline runs
SELECT * FROM event_log('<pipeline_id>')
ORDER BY timestamp DESC;

-- Data quality metrics
SELECT 
  flow_name,
  SUM(num_output_rows) as rows_processed,
  SUM(num_dropped_rows) as rows_dropped
FROM event_log('<pipeline_id>')
WHERE event_type = 'flow_progress'
GROUP BY flow_name;
```

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `kafka.common.KafkaException` | Kafka connectivity | Verify `kafka_bootstrap_servers` and network access |
| `Table not found: bronze_customers` | Bronze pipeline not running | Start bronze pipeline first |
| `ParseException` in Debezium | Schema mismatch | Check source table schema vs. expected schema |
| Date field shows numbers | Conversion not applied | Verify date field is in `date_fields` list |
| No new data arriving | Offset lag | Check Kafka consumer lag, Debezium connector status |

### Debug Queries

```sql
-- Check latest bronze ingestion
SELECT 
  MAX(_bronze_loaded_at) as last_load,
  COUNT(*) as total_rows
FROM main.bronze.bronze_customers;

-- Check SCD2 history
SELECT 
  customer_id,
  customer_name,
  loyalty_tier,
  __START_AT,
  __END_AT
FROM main.silver.silver_customers
WHERE customer_id = 123
ORDER BY __START_AT;

-- Check surrogate keys
SELECT 
  customer_sk,
  customer_id,
  valid_from,
  is_current
FROM main.silver.dim_customers
WHERE customer_id = 123;
```

---

## ⚙️ Configuration

### Pipeline Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `kafka_bootstrap_servers` | Kafka broker endpoints | (required) | `7.tcp.ngrok.io:24168` |
| `kafka_topic_prefix` | Debezium topic prefix | `neon.public` | `mydb.public` |
| `bronze_catalog` | Catalog for bronze tables | `main` | `prod` |
| `bronze_schema` | Schema for bronze tables | `silver` | `bronze` |

### Kafka Options

```python
# Additional Kafka settings (optional)
kafka_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": "...",
}
```

### Performance Tuning

```python
# Optimize for large-scale ingestion
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is open source and available under the MIT License.

---

## 🙏 Acknowledgments

- **Databricks** for Lakeflow Spark Declarative Pipelines
- **Debezium** for CDC capture
- **Delta Lake** for reliable lakehouse storage

---

## 📧 Contact

**Pratik Pokhrel**  
GitHub: [@PratikPokhrel](https://github.com/PratikPokhrel)  
Email: pratikpokhrel51@gmail.com

---

**Built with ❤️ on Databricks**
