# Module 4: Data Integration & ETL Pipelines

**Duration:** 3 hours
 **Prerequisites:** Completion of Modules 1–3 (PySpark Fundamentals, DataFrames, and SQL Basics)
 **What You’ll Build:** A production-ready **ETL pipeline** for banking datasets that can read raw sources (CSV, Parquet, JSON), validate and evolve schemas, process data incrementally (CDC), enforce data quality checks, and output transformed data into a structured zone for downstream analytics.

------

## 🎯 Module Overview

Banks rely on daily data pipelines to consolidate millions of records from different sources—customers, accounts, transactions, branches, and loans. This module shows you how to:

- Connect PySpark to **multiple banking data sources** (local files or S3).
- Apply **explicit schemas** and handle **schema evolution** gracefully.
- Implement **incremental loads** using Change Data Capture (CDC).
- Build **data quality checks** to prevent bad data from entering downstream reports.
- Orchestrate, monitor, and scale your pipelines for **enterprise readiness**.

By the end, you will have a hands-on ETL pipeline that mirrors what real financial institutions deploy in production.

------

## 📖 Functions Reference Table

Here’s a quick reference of functions, methods, and features we’ll be using throughout this module. Keep this table handy—it acts as your **ETL cheat sheet**.

| Category              | Functions / Methods                                          | Purpose                                                      |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **Data Reads**        | `spark.read.csv()`, `spark.read.parquet()`, `spark.read.json()` | Ingest raw datasets in different formats (CSV, Parquet, JSON). |
| **Data Writes**       | `df.write.parquet()`, `df.write.mode("append")`, `partitionBy()` | Persist transformed data in optimized Parquet format.        |
| **Schema Handling**   | `StructType`, `StructField`, `StringType`, `IntegerType`, `DoubleType`, `TimestampType` | Define strict schemas for banking data.                      |
| **Schema Evolution**  | `.withColumn()`, `.drop()`, `.na.fill()`, conditional try/except wrapper | Add missing columns, drop extra columns, or fill defaults when schema changes. |
| **Incremental Loads** | `.filter(col("date") > last_processed)`, checkpoints, batch IDs | Implement CDC to process only new records.                   |
| **Watermarks**        | `.withWatermark("eventTime", "1 day")`                       | Handle late-arriving data (streaming context).               |
| **Data Quality**      | `.count()`, `when(col.isNull())`, custom `BankingDataQualityValidator` | Ensure completeness, validity, and accuracy of data.         |
| **Performance**       | `.repartition()`, `.coalesce()`, `.cache()`, `.explain()`    | Optimize execution, reduce shuffle, debug query plans.       |
| **Orchestration**     | Airflow operators, Spark submit jobs, cron jobs              | Schedule and manage ETL execution in production.             |
| **Monitoring**        | Logging, metrics (counts, nulls, elapsed time), alerts       | Track pipeline health and detect failures early.             |



------

## 1. ETL in Banking — What & Why

### 🔎 What is ETL?

**ETL** stands for **Extract, Transform, Load**.
 It’s the backbone of data engineering — the process of moving data from **where it’s generated** (raw operational systems) to **where it’s needed** (analytics, dashboards, compliance reports, risk models).

In banking, ETL is everywhere. Banks process **millions of daily transactions**, and the quality of their ETL pipelines directly affects:

- **Customer experience** (e.g., mobile banking app showing real-time balances).
- **Compliance** (e.g., generating regulatory reports for central banks).
- **Risk management** (e.g., detecting fraud, monitoring credit risk).

------

### 🏦 ETL in a Day-in-the-Life of a Bank

Let’s imagine a typical day in a commercial bank:

1. **Overnight Batches**
   - At 2 AM, the bank runs a batch ETL job to collect the previous day’s **transactions**.
   - Data is cleaned, aggregated, and written into a reporting database.
   - By morning, executives have updated dashboards showing total deposits, withdrawals, and transfers.
2. **Real-Time Analytics**
   - During the day, ETL pipelines stream **card transactions** to a fraud detection model.
   - If an unusual purchase pattern appears (say, a charge in another country), the system can flag or block the card instantly.
3. **Regulatory Reporting**
   - At month-end, ETL pipelines pull **loans, accounts, and risk data** into structured tables.
   - Reports are submitted to the central bank to prove compliance with credit exposure limits.

👉 Without ETL, these flows would be **manual, error-prone, and impossible to scale**.

------

### 📊 Example Scenarios

- **Nightly Transactions ETL:**
   Consolidates 2 million transaction rows into summaries: daily volume, fraud cases, average transaction size.
- **Customer 360 View:**
   Joins customers, accounts, and loan tables into a **single view of a customer** across products.
- **Regulatory Compliance:**
   ETL pipelines enforce schema, validate completeness, and prepare data for government-mandated formats.

------

### ⚠️ Challenges in Banking ETL

ETL in banking isn’t just about moving data — it must meet strict requirements:

- **High Volume:** Millions of rows daily, growing year over year.
- **High Velocity:** Data arrives in near real-time (ATMs, POS systems, online banking).
- **Variety:** Mix of CSV, Parquet, JSON, databases, and APIs.
- **Data Quality:** Missing or inconsistent values can cause compliance issues.
- **Compliance:** Pipelines must meet **auditability and lineage** requirements.

------

✅ **Why this matters for you as a learner**
 By the end of this module, you’ll see how to **design ETL pipelines** that handle these challenges. We’ll start small (local CSV and Parquet files), then add schema enforcement, incremental processing, and quality checks — all with runnable PySpark code.

------

## 2. Data Source Integration & First Reads

### 🔧 Spark Session Setup

Before we load any data, we need a Spark session that supports both **local files** and (optionally) **AWS S3**.

👉 Copy and run this as `spark_setup.py` (or directly in your PySpark notebook):

```python
from pyspark.sql import SparkSession

def create_spark_session(use_s3=False):
    builder = (
        SparkSession.builder
        .appName("Banking_ETL_Pipeline")
        .config("spark.sql.adaptive.enabled", "true")   # Enable adaptive query execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    
    if use_s3:
        # Add S3 connector JARs and credentials provider
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.jars.packages", 
                    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        )
        print("✅ Spark session created with S3 support")
    else:
        print("✅ Spark session created for local files")

    spark = builder.getOrCreate()
    print(f"📊 Spark version: {spark.version}")
    return spark

# Create Spark session (default = local mode)
spark = create_spark_session(use_s3=False)
```

------

### 📂 Dataset Paths

Now we define where Spark should look for the data.

- If you’re working **locally**, files should be in the `banking_dataset/datasets/` folder.
- If you’re using **S3**, change `use_s3=True` and set your bucket name.

```python
def get_dataset_paths(use_s3=False):
    if use_s3:
        bucket_name = "banking-analytics-dataset"
        base_path = f"s3a://{bucket_name}"
        return {
            "customers": f"{base_path}/raw-data/csv/customers.csv",
            "accounts": f"{base_path}/raw-data/csv/accounts.csv",
            "transactions": f"{base_path}/raw-data/csv/transactions.csv",
            "loans": f"{base_path}/raw-data/csv/loans.csv",
            "branches": f"{base_path}/raw-data/csv/branches.csv",
            "products": f"{base_path}/raw-data/csv/products.csv"
        }, "s3"
    else:
        base_path = "banking_dataset/datasets"
        return {
            "customers": f"{base_path}/customers.csv",
            "accounts": f"{base_path}/accounts.csv",
            "transactions": f"{base_path}/transactions.parquet",
            "loans": f"{base_path}/loans.csv",
            "branches": f"{base_path}/branches.json",
            "products": f"{base_path}/products.csv"
        }, "local"

dataset_paths, source_type = get_dataset_paths(use_s3=False)
print("📍 Using data source:", source_type.upper())
for name, path in dataset_paths.items():
    print(f"{name}: {path}")
```

------

### 📥 Reading Raw Datasets

We’ll load each dataset into a Spark DataFrame. Notice how the code adapts depending on file format (CSV, Parquet, JSON).

```python
from pyspark.sql.functions import col

def read_raw_banking_data(spark, dataset_paths, source_type):
    datasets = {}

    # Customers (CSV)
    customers_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(dataset_paths["customers"])
    )
    print(f"👥 Customers: {customers_df.count():,} rows")
    datasets["customers"] = customers_df

    # Accounts (CSV)
    accounts_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(dataset_paths["accounts"])
    )
    print(f"💰 Accounts: {accounts_df.count():,} rows")
    datasets["accounts"] = accounts_df

    # Transactions (Parquet locally, CSV on S3)
    if source_type == "s3":
        transactions_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(dataset_paths["transactions"])
        )
    else:
        transactions_df = spark.read.parquet(dataset_paths["transactions"])
    print(f"💳 Transactions: {transactions_df.count():,} rows")
    datasets["transactions"] = transactions_df

    # Loans (CSV)
    loans_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(dataset_paths["loans"])
    )
    print(f"📄 Loans: {loans_df.count():,} rows")
    datasets["loans"] = loans_df

    # Branches (JSON locally, CSV on S3)
    if source_type == "s3":
        branches_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(dataset_paths["branches"])
        )
    else:
        branches_df = spark.read.json(dataset_paths["branches"])
    print(f"🏢 Branches: {branches_df.count():,} rows")
    datasets["branches"] = branches_df

    # Products (CSV)
    products_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(dataset_paths["products"])
    )
    print(f"📦 Products: {products_df.count():,} rows")
    datasets["products"] = products_df

    return datasets

# Load all datasets
raw_data = read_raw_banking_data(spark, dataset_paths, source_type)
```

------

### 🔍 Exploring Data Structures

Once the DataFrames are loaded, it’s critical to explore them: check schemas, sample rows, and null values.

```python
from pyspark.sql.functions import count, when

def analyze_data_structure(datasets):
    for name, df in datasets.items():
        print(f"\n=== {name.upper()} ===")
        print(f"Rows: {df.count():,}")
        df.printSchema()
        df.show(5, truncate=False)

        # Null analysis
        nulls = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        print("Null Value Counts:")
        nulls.show()

# Run the analysis
analyze_data_structure(raw_data)
```

------

### ✅ What You’ve Achieved

By the end of this section, you have:

- A **flexible Spark session** that can work with either **local files** or **S3**.
- All **raw banking datasets** loaded into Spark DataFrames.
- A first look at the data structures, sample rows, and missing values.

This is the **foundation** for the next part: **Schema Management & Evolution**, where we’ll enforce strict banking schemas and handle changes gracefully.

------

## 3. Schema Management & Evolution

### 🔎 Why Schema Management Matters

In banking, schema consistency is not optional:

- **Compliance:** Regulators require strict definitions of fields (e.g., account balance must always be a `Double`).
- **Integration:** Joining data from multiple systems only works if schemas align.
- **Reliability:** Inconsistent or evolving schemas can cause production pipeline failures.

Example:
 If a new column `risk_rating` suddenly appears in the `loans` file, your pipeline should not crash. Instead, it should:

- Detect the schema change.
- Add the missing column (with a default) if required.
- Drop extra columns not used downstream.

------

### 🏗️ Defining Explicit Schemas

Let’s create **banking-specific schemas** for our main datasets using `StructType` and `StructField`.

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

# Customers schema
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Accounts schema
account_schema = StructType([
    StructField("account_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("created_at", TimestampType(), True)
])

# Transactions schema
transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("account_id", IntegerType(), False),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("status", StringType(), True)
])

# Loans schema
loan_schema = StructType([
    StructField("loan_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("loan_date", TimestampType(), True),
    StructField("status", StringType(), True)
])
```

------

### 📥 Reading Data with Explicit Schemas

Instead of letting Spark infer types, we now **apply schemas explicitly**.

```python
# Load with explicit schemas
customers_df = (
    spark.read.option("header", "true")
    .schema(customer_schema)
    .csv(dataset_paths["customers"])
)

accounts_df = (
    spark.read.option("header", "true")
    .schema(account_schema)
    .csv(dataset_paths["accounts"])
)

transactions_df = (
    spark.read.schema(transaction_schema)
    .parquet(dataset_paths["transactions"])
)

loans_df = (
    spark.read.option("header", "true")
    .schema(loan_schema)
    .csv(dataset_paths["loans"])
)

# Verify
print("✅ Explicit schemas applied")
customers_df.printSchema()
accounts_df.printSchema()
transactions_df.printSchema()
loans_df.printSchema()
```

------

### ⚡ Handling Schema Evolution

Real banking systems change over time. Let’s build a **function that detects schema mismatches** and adapts automatically:

```python
def handle_schema_evolution(expected_schema, df, required_columns):
    """
    Aligns DataFrame with expected schema.
    - Adds missing columns with default values
    - Drops extra columns
    """
    # Add missing columns
    for field in expected_schema.fields:
        if field.name not in df.columns:
            default_value = None
            if isinstance(field.dataType, IntegerType):
                default_value = 0
            elif isinstance(field.dataType, DoubleType):
                default_value = 0.0
            elif isinstance(field.dataType, StringType):
                default_value = "UNKNOWN"
            elif isinstance(field.dataType, TimestampType):
                default_value = None
            df = df.withColumn(field.name, lit(default_value))
            print(f"➕ Added missing column: {field.name}")

    # Drop extra columns
    for col_name in df.columns:
        if col_name not in [f.name for f in expected_schema.fields]:
            df = df.drop(col_name)
            print(f"🗑️ Dropped unexpected column: {col_name}")

    return df.select([col.name for col in expected_schema.fields])
```

Usage:

```python
from pyspark.sql.functions import lit

# Example: incoming transactions data has changed
transactions_evolved = handle_schema_evolution(transaction_schema, transactions_df, transaction_schema.fields)
transactions_evolved.printSchema()
```

------

### ✅ What You’ve Achieved

- Defined **strict schemas** for key banking datasets.
- Loaded raw data **with explicit schema enforcement**.
- Implemented a **schema evolution handler** that automatically adapts to changes.

This ensures your pipeline won’t break when data providers add or remove fields.

------

## 4. Incremental Processing (CDC)

### 🚀 Why CDC (Change Data Capture)?

Full reloads are wasteful. With **CDC**, your pipeline processes **only new or updated rows** since the last successful run. This:

- cuts runtime & cost,
- reduces cluster load,
- makes downstream dashboards fresher, faster.

In our banking context, we’ll run CDC on **transactions** keyed by `transaction_date`.

------

### 🧭 Design (simple & robust)

We’ll implement a **file-based checkpoint** that stores the **last processed timestamp**. Each run:

1. Read last processed timestamp (if none, treat as first run).
2. Load the `transactions` dataset.
3. Filter rows where `transaction_date` > last processed timestamp.
4. Add ETL metadata (`etl_batch_id`, `etl_processed_at`, `source_system`).
5. Write to a **processed zone** in Parquet, partitioned by date.
6. Update the checkpoint to the new max `transaction_date`.

> Works locally out of the box; can be extended to S3 by switching paths you already set earlier.

------

### 🗂️ Paths used (same structure as earlier sections)

- Raw transactions (local): `banking_dataset/datasets/transactions.parquet`
- Processed transactions (local): `banking_dataset/datasets/processed/transactions/`
- CDC checkpoint (local): `banking_dataset/datasets/checkpoints/transactions_cdc_state.json`

Feel free to adjust folder names, but keep them consistent throughout the course.

------

### ✅ CDC utilities

Create a new file (or a new cell) named `cdc_transactions.py` and paste the whole block:

```python
import json, os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, date_format, max as spark_max
from pyspark.sql.types import TimestampType

# -------------------------------
# 0) Spark session (local by default)
# -------------------------------
spark = (
    SparkSession.builder
    .appName("Banking_ETL_CDC_Transactions")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
print("✅ Spark started:", spark.version)

# -------------------------------
# 1) Paths (LOCAL)
#    If you want S3, switch paths to s3a://... and reuse your S3-enabled session
# -------------------------------
RAW_TRANSACTIONS_PATH = "banking_dataset/datasets/transactions.parquet"
PROCESSED_TRANSACTIONS_PATH = "banking_dataset/datasets/processed/transactions/"
CDC_STATE_FILE = "banking_dataset/datasets/checkpoints/transactions_cdc_state.json"

# Ensure folders exist for local mode (processed + checkpoints)
os.makedirs(os.path.dirname(CDC_STATE_FILE), exist_ok=True)
os.makedirs(PROCESSED_TRANSACTIONS_PATH, exist_ok=True)

# -------------------------------
# 2) CDC state helpers
# -------------------------------
def _default_state():
    return {
        "last_processed_ts": None,   # ISO string or None
        "last_run": None,            # ISO string of last pipeline run
        "runs": 0
    }

def read_cdc_state(state_file: str):
    """Read checkpoint state from JSON; create if missing."""
    if not os.path.exists(state_file):
        print("ℹ️ No CDC state found; creating a fresh one.")
        return _default_state()
    try:
        with open(state_file, "r") as f:
            state = json.load(f)
        return state
    except Exception as e:
        print(f"⚠️ Could not read CDC state: {e}")
        return _default_state()

def write_cdc_state(state_file: str, state: dict):
    """Persist CDC state atomically."""
    tmp_file = state_file + ".tmp"
    with open(tmp_file, "w") as f:
        json.dump(state, f)
    os.replace(tmp_file, state_file)
    print("💾 CDC state updated:", state)

# -------------------------------
# 3) Load raw transactions (explicitly cast date)
# -------------------------------
transactions_df = spark.read.parquet(RAW_TRANSACTIONS_PATH)

# Ensure transaction_date is TimestampType (adapt if your parquet already stores timestamp)
if transactions_df.schema["transaction_date"].dataType != TimestampType():
    transactions_df = transactions_df.withColumn(
        "transaction_date",
        col("transaction_date").cast(TimestampType())
    )

print(f"💳 Raw transactions rows: {transactions_df.count():,}")
transactions_df.printSchema()

# -------------------------------
# 4) Determine CDC window
# -------------------------------
state = read_cdc_state(CDC_STATE_FILE)
last_ts_str = state.get("last_processed_ts")
print("🧭 Last processed timestamp in state:", last_ts_str)

if last_ts_str is None:
    # First run strategy:
    # Option A) full load; Option B) load last N days.
    # We'll do **full load** to keep it simple and deterministic for the class.
    incremental_df = transactions_df
    print("🔁 First run detected: performing full load of transactions.")
else:
    last_ts = datetime.fromisoformat(last_ts_str)
    incremental_df = transactions_df.filter(col("transaction_date") > lit(last_ts_str))
    print("🔁 Incremental run: filtering where transaction_date >", last_ts_str)

# Safety: If no rows, short-circuit gracefully
if incremental_df.rdd.isEmpty():
    print("✅ No new transactions to process. Exiting CDC step.")
    # Still bump the run counters/time for audit
    state["runs"] = (state.get("runs") or 0) + 1
    state["last_run"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    write_cdc_state(CDC_STATE_FILE, state)
    spark.stop()
    raise SystemExit(0)

# -------------------------------
# 5) ETL metadata + partition columns
# -------------------------------
etl_batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
processed_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

with_meta_df = (
    incremental_df
    .withColumn("etl_batch_id", lit(etl_batch_id))
    .withColumn("etl_processed_at", lit(processed_at))
    .withColumn("source_system", lit("core_banking"))   # adjust as needed
    # Convenience partitions: yyyy, mm, dd for quick pruning
    .withColumn("yyyymmdd", date_format(col("transaction_date"), "yyyyMMdd"))
    .withColumn("yyyy", date_format(col("transaction_date"), "yyyy"))
    .withColumn("mm", date_format(col("transaction_date"), "MM"))
    .withColumn("dd", date_format(col("transaction_date"), "dd"))
)

print("🧱 Sample rows with ETL metadata:")
with_meta_df.show(5, truncate=False)

# -------------------------------
# 6) Write to processed zone (Parquet, partitioned by y/m/d)
# -------------------------------
(
    with_meta_df
    .repartition("yyyy", "mm", "dd")   # good default for partition-balanced writes
    .write
    .mode("append")
    .partitionBy("yyyy", "mm", "dd")
    .parquet(PROCESSED_TRANSACTIONS_PATH)
)
print(f"📦 Written to: {PROCESSED_TRANSACTIONS_PATH}")

# -------------------------------
# 7) Advance the CDC state (use max transaction_date seen this run)
# -------------------------------
max_ts_row = with_meta_df.select(spark_max(col("transaction_date")).alias("max_ts")).collect()[0]
max_ts_val = max_ts_row["max_ts"]
if max_ts_val is None:
    # Shouldn't happen if we had rows, but guard anyway
    max_ts_iso = last_ts_str
else:
    # to Python datetime iso
    max_ts_iso = max_ts_val.strftime("%Y-%m-%dT%H:%M:%S")

state["last_processed_ts"] = max_ts_iso
state["runs"] = (state.get("runs") or 0) + 1
state["last_run"] = processed_at
write_cdc_state(CDC_STATE_FILE, state)

# -------------------------------
# 8) Done
# -------------------------------
print("✅ CDC completed. Last processed ts:", state["last_processed_ts"])
spark.stop()
print("🛑 Spark session stopped.")
```

------

### 🧪 How to test CDC quickly (in class)

1. **Run the script** once — it will do a **full load** (first run).
2. **Simulate new data**: append a few rows into your `transactions.parquet` with `transaction_date` > last processed timestamp (or temporarily lower the state to an older date).
3. Run the script again — it should pick **only the new rows**, write them into the processed zone, and **advance** the checkpoint timestamp.

> Tip: For teaching, show the state file’s content changing after each run.

------

### 🧰 Notes and variations

- **First-run policy**: you can switch from “full load” to “last 7 days” by filtering `transactions_df` with `transaction_date >= current_date() - 7`.
- **S3 mode**: reuse your S3 session and change the three paths to `s3a://your-bucket/...`. The logic remains identical.
- **Batch IDs**: we use a UTC timestamp; feel free to incorporate a job name or an Airflow run ID if you orchestrate it later.

------

### ✅ What you’ve achieved

- A **reliable CDC loop** that:
  - remembers where it left off,
  - processes only **new** transactions,
  - writes partitioned Parquet with **ETL metadata**,
  - advances a **checkpoint** safely.

------

## 5. Data Quality — Guardrails

### 🧰 Why data quality matters

In banking, a single bad row can break reports or trigger compliance issues. Our ETL needs **guardrails** that:

- **Detect** problems early (before writes),
- **Quantify** issues (how many nulls? how many broken FKs?),
- **Decide** whether to **warn** or **fail** the run,
- **Log** results for audit.

We’ll build a small **Data Quality Validator** with common checks:

- Completeness (null thresholds)
- Uniqueness (primary keys)
- Referential Integrity (foreign keys)
- Value ranges & categorical validity

You can extend this later (timeliness, consistency rules, outlier checks, etc.).

------

### ✅ Data Quality Validator

> Save this as `dq_guardrails.py` and run with `python dq_guardrails.py`.
>  It will: start Spark → load the same datasets → run quality checks → print a report → fail or pass based on thresholds.

```python
# dq_guardrails.py
import os, json
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit, lower, trim, avg, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# -------------------------------
# 0) Spark session (local)
# -------------------------------
def create_spark():
    spark = (
        SparkSession.builder
        .appName("Banking_ETL_DataQuality")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    print("✅ Spark started:", spark.version)
    return spark

# -------------------------------
# 1) Paths (same as earlier sections)
# -------------------------------
def get_dataset_paths_local():
    base_path = "banking_dataset/datasets"
    return {
        "customers": f"{base_path}/customers.csv",
        "accounts": f"{base_path}/accounts.csv",
        "transactions": f"{base_path}/transactions.parquet",
        "loans": f"{base_path}/loans.csv",
        "branches": f"{base_path}/branches.json",
        "products": f"{base_path}/products.csv",
        "processed_transactions": f"{base_path}/processed/transactions/"
    }

# -------------------------------
# 2) Explicit schemas (align with Section 3)
# -------------------------------
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("created_at", TimestampType(), True),
])

account_schema = StructType([
    StructField("account_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("created_at", TimestampType(), True),
])

transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("account_id", IntegerType(), False),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("status", StringType(), True),
])

loan_schema = StructType([
    StructField("loan_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("loan_date", TimestampType(), True),
    StructField("status", StringType(), True),
])

# -------------------------------
# 3) Load data with explicit schemas
# -------------------------------
def load_raw(spark: SparkSession, paths: Dict[str, str]) -> Dict[str, DataFrame]:
    customers = (spark.read.option("header", "true").schema(customer_schema).csv(paths["customers"]))
    accounts = (spark.read.option("header", "true").schema(account_schema).csv(paths["accounts"]))
    # transactions are Parquet (local) with schema already embedded; ensure cast for safety
    transactions = spark.read.parquet(paths["transactions"]).select(
        col("transaction_id").cast("int"),
        col("account_id").cast("int"),
        col("amount").cast("double"),
        col("transaction_type").cast("string"),
        col("transaction_date").cast("timestamp"),
        col("status").cast("string"),
    )
    loans = (spark.read.option("header", "true").schema(loan_schema).csv(paths["loans"]))

    print(f"👥 customers: {customers.count():,}")
    print(f"💰 accounts: {accounts.count():,}")
    print(f"💳 transactions: {transactions.count():,}")
    print(f"📄 loans: {loans.count():,}")

    return {
        "customers": customers,
        "accounts": accounts,
        "transactions": transactions,
        "loans": loans,
    }

# -------------------------------
# 4) DQ result types
# -------------------------------
@dataclass
class DQCheckResult:
    name: str
    status: str         # "PASS" | "WARN" | "FAIL"
    details: Dict[str, Any]

@dataclass
class DQSuiteResult:
    dataset: str
    checks: List[DQCheckResult]

    @property
    def overall(self) -> str:
        severities = [c.status for c in self.checks]
        if "FAIL" in severities:
            return "FAIL"
        if "WARN" in severities:
            return "WARN"
        return "PASS"

# -------------------------------
# 5) The Validator
# -------------------------------
class DataQualityValidator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    # Completeness: % of non-null >= threshold for given columns
    def completeness(self, df: DataFrame, columns: List[str], threshold: float, severity_if_below="FAIL") -> DQCheckResult:
        total = df.count() or 1
        null_counts = df.select([count(when(col(c).isNull() | (trim(col(c)) == ""), c)).alias(c) for c in columns]).collect()[0].asDict()
        completeness_pct = {c: round(100.0 * (1 - (null_counts.get(c, 0) / total)), 2) for c in columns}
        below = {c: p for c, p in completeness_pct.items() if p < threshold * 100}
        status = "PASS" if not below else severity_if_below
        return DQCheckResult(
            name=f"completeness_{'_'.join(columns)}",
            status=status,
            details={"total_rows": total, "null_counts": null_counts, "pct_non_null": completeness_pct, "threshold_pct": threshold*100}
        )

    # Uniqueness: composite key has no duplicates
    def uniqueness(self, df: DataFrame, keys: List[str], severity_if_dup="FAIL") -> DQCheckResult:
        dup_count = df.groupBy([col(k) for k in keys]).count().filter(col("count") > 1).count()
        status = "PASS" if dup_count == 0 else severity_if_dup
        return DQCheckResult(
            name=f"uniqueness_{'_'.join(keys)}",
            status=status,
            details={"duplicate_groups": dup_count, "key": keys}
        )

    # Referential integrity: all child keys exist in parent keys
    def referential_integrity(self, child: DataFrame, parent: DataFrame, child_key: str, parent_key: str, severity="FAIL") -> DQCheckResult:
        missing = (
            child.select(col(child_key).alias("k"))
                 .join(parent.select(col(parent_key).alias("k")), on="k", how="left_anti")
                 .count()
        )
        status = "PASS" if missing == 0 else severity
        return DQCheckResult(
            name=f"fk_{child_key}_in_{parent_key}",
            status=status,
            details={"missing_fk_rows": missing, "child_key": child_key, "parent_key": parent_key}
        )

    # Value range numeric
    def value_range(self, df: DataFrame, column: str, min_val: Optional[float], max_val: Optional[float], allow_null=True, severity="WARN") -> DQCheckResult:
        cond = None
        if min_val is not None and max_val is not None:
            cond = (col(column) < lit(min_val)) | (col(column) > lit(max_val))
        elif min_val is not None:
            cond = (col(column) < lit(min_val))
        elif max_val is not None:
            cond = (col(column) > lit(max_val))

        if cond is None:
            bad = 0
        else:
            if allow_null:
                bad = df.filter(cond & col(column).isNotNull()).count()
            else:
                bad = df.filter(cond | col(column).isNull()).count()

        status = "PASS" if bad == 0 else severity
        stats = df.agg(spark_min(col(column)).alias("min"), spark_max(col(column)).alias("max"), avg(col(column)).alias("avg")).collect()[0].asDict()
        return DQCheckResult(
            name=f"value_range_{column}",
            status=status,
            details={"violations": bad, "range": [min_val, max_val], "stats": stats}
        )

    # Categorical validity
    def categorical_validity(self, df: DataFrame, column: str, allowed_values: List[str], case_insensitive=True, severity="WARN") -> DQCheckResult:
        c = lower(col(column)) if case_insensitive else col(column)
        allowed = [v.lower() for v in allowed_values] if case_insensitive else allowed_values
        violations = df.filter(c.isNotNull() & (~c.isin(allowed))).count()
        status = "PASS" if violations == 0 else severity
        return DQCheckResult(
            name=f"categorical_{column}",
            status=status,
            details={"violations": violations, "allowed_values": allowed_values, "case_insensitive": case_insensitive}
        )

# -------------------------------
# 6) Run a DQ suite on our datasets
# -------------------------------
def run_dq_suite(dfs: Dict[str, DataFrame]) -> List[DQSuiteResult]:
    v = DataQualityValidator(dfs["customers"].sparkSession)
    suites: List[DQSuiteResult] = []

    # Customers
    cust_checks = [
        v.completeness(dfs["customers"], ["customer_id"], 1.0, severity_if_below="FAIL"),
        v.uniqueness(dfs["customers"], ["customer_id"], severity_if_dup="FAIL"),
        v.completeness(dfs["customers"], ["first_name", "last_name", "email"], 0.95, severity_if_below="WARN"),
    ]
    suites.append(DQSuiteResult(dataset="customers", checks=cust_checks))

    # Accounts
    acct_checks = [
        v.completeness(dfs["accounts"], ["account_id", "customer_id"], 1.0, severity_if_below="FAIL"),
        v.uniqueness(dfs["accounts"], ["account_id"], severity_if_dup="FAIL"),
        v.referential_integrity(dfs["accounts"], dfs["customers"], "customer_id", "customer_id", severity="FAIL"),
        v.categorical_validity(dfs["accounts"], "account_type", ["savings","current","checking","loan","credit"], severity="WARN"),
        v.value_range(dfs["accounts"], "balance", min_val=0.0, max_val=None, allow_null=True, severity="WARN"),
    ]
    suites.append(DQSuiteResult(dataset="accounts", checks=acct_checks))

    # Transactions
    txn_checks = [
        v.completeness(dfs["transactions"], ["transaction_id","account_id","transaction_date"], 1.0, severity_if_below="FAIL"),
        v.uniqueness(dfs["transactions"], ["transaction_id"], severity_if_dup="FAIL"),
        v.referential_integrity(dfs["transactions"], dfs["accounts"], "account_id", "account_id", severity="FAIL"),
        v.value_range(dfs["transactions"], "amount", min_val=0.0, max_val=None, allow_null=False, severity="FAIL"),
        v.categorical_validity(dfs["transactions"], "transaction_type",
                               ["deposit","withdrawal","transfer","payment","fee","refund"], severity="WARN"),
    ]
    suites.append(DQSuiteResult(dataset="transactions", checks=txn_checks))

    # Loans
    loan_checks = [
        v.completeness(dfs["loans"], ["loan_id","customer_id","loan_amount"], 1.0, severity_if_below="FAIL"),
        v.uniqueness(dfs["loans"], ["loan_id"], severity_if_dup="FAIL"),
        v.referential_integrity(dfs["loans"], dfs["customers"], "customer_id", "customer_id", severity="FAIL"),
        # interest_rate expected between 0 and 100 (percentage). Adjust if your dataset uses 0..1.
        v.value_range(dfs["loans"], "interest_rate", min_val=0.0, max_val=100.0, allow_null=True, severity="WARN"),
    ]
    suites.append(DQSuiteResult(dataset="loans", checks=loan_checks))

    return suites

# -------------------------------
# 7) Pretty-print + fail/exit status
# -------------------------------
def print_report_and_decide(suites: List[DQSuiteResult]) -> None:
    overall_fail = False
    print("\n==================== DATA QUALITY REPORT ====================")
    for suite in suites:
        print(f"\nDataset: {suite.dataset} | Overall: {suite.overall}")
        for check in suite.checks:
            print(f"  - {check.name:<35} [{check.status}]")
            # brief details
            if "violations" in check.details:
                print(f"      violations: {check.details['violations']}")
            if "duplicate_groups" in check.details:
                print(f"      duplicate_groups: {check.details['duplicate_groups']}")
            if "missing_fk_rows" in check.details:
                print(f"      missing_fk_rows: {check.details['missing_fk_rows']}")
            if "pct_non_null" in check.details:
                print(f"      pct_non_null: {check.details['pct_non_null']}")
        if suite.overall == "FAIL":
            overall_fail = True

    print("\n=============================================================")
    if overall_fail:
        raise SystemExit("❌ Data Quality FAILED — stopping pipeline.")
    else:
        print("✅ Data Quality PASSED — proceeding.")

# -------------------------------
# 8) Main
# -------------------------------
if __name__ == "__main__":
    spark = create_spark()
    paths = get_dataset_paths_local()
    dfs = load_raw(spark, paths)

    suites = run_dq_suite(dfs)
    print_report_and_decide(suites)

    spark.stop()
    print("🛑 Spark session stopped.")
```

------

### 🧪 What this runs & why it’s useful

- **Completeness**
  - Hard-fail if core IDs are missing (e.g., `customer_id`, `transaction_id`).
  - Warn if optional descriptive columns (names, emails) drop below **95%** completeness.
- **Uniqueness**
  - Hard-fail if primary keys are duplicated (`customer_id`, `account_id`, `transaction_id`, `loan_id`).
- **Referential integrity**
  - Hard-fail if there are **orphan rows** (e.g., an `account.customer_id` not found in `customers`).
- **Value ranges & categorical validity**
  - Fail if `transactions.amount` is negative or null.
  - Warn if `account_type` or `transaction_type` contains unexpected categories (teaches how to handle taxonomy drift).
  - Warn if `interest_rate` is outside 0–100 (adjust if your data uses 0–1).

> If you expect different category labels (e.g., uppercase), change the `allowed_values` lists or switch `case_insensitive=False`.

------

### 📌 Teaching tips

- Demo how **a single duplicate** `transaction_id` triggers a **FAIL**.
- Show how to **tune thresholds** (e.g., set email completeness threshold to 0.90 and re-run).
- Emphasize that **WARN** keeps the pipeline moving but **FAIL** **stops** it to protect downstream outputs.

------

### ✅ What you’ve achieved

You now have **bank-grade guardrails** that:

- quantify the health of your data,
- **stop the pipeline** on critical issues,
- keep a consistent structure you can later **emit to logs/metrics** (e.g., push to a table or monitoring system).

------

## 6. Orchestration & Error Handling

### 🔎 Why orchestration matters

So far, you’ve built pieces:

- Load raw → Schema enforcement → CDC → Data Quality.
   But in a real bank, pipelines must:
- Run on a **schedule** (hourly, nightly, monthly),
- Recover if something fails,
- Log progress for monitoring and audit,
- Be **modular** so each step (Extract / Transform / Load / Validate) is reusable.

That’s where orchestration and error handling come in.

------

### 🧩 Designing modular ETL components

We’ll define a **base ETL component class** with:

- **start/end timers** (for performance tracking),
- **structured logging** (for monitoring),
- **try/except error handling** (to fail fast with useful messages).

Then we’ll implement a simple **Extractor** for S3/CSV/Parquet and show how to chain steps together.

------

### ✅ ETL Component Framework

Save this as `etl_orchestration.py`:

```python
import time, traceback
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame

# -------------------------------
# 0) Spark session
# -------------------------------
spark = (
    SparkSession.builder
    .appName("Banking_ETL_Orchestration")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
print("✅ Spark started:", spark.version)

# -------------------------------
# 1) Base ETL Component
# -------------------------------
class ETLComponent:
    def __init__(self, name: str):
        self.name = name
        self.metrics: Dict[str, Any] = {}
        self.success = False

    def run(self, *args, **kwargs) -> Any:
        """To be implemented by child classes"""
        raise NotImplementedError

    def execute(self, *args, **kwargs) -> Any:
        print(f"▶️ Starting {self.name} ...")
        start_time = time.time()
        try:
            result = self.run(*args, **kwargs)
            self.success = True
            duration = round(time.time() - start_time, 2)
            self.metrics["duration_sec"] = duration
            print(f"✅ {self.name} completed in {duration}s")
            return result
        except Exception as e:
            duration = round(time.time() - start_time, 2)
            print(f"❌ {self.name} failed after {duration}s")
            print("Error:", str(e))
            traceback.print_exc()
            self.metrics["error"] = str(e)
            self.success = False
            raise

# -------------------------------
# 2) Example Extractor Component
# -------------------------------
class Extractor(ETLComponent):
    def __init__(self, name: str, spark: SparkSession, path: str, fmt: str = "csv"):
        super().__init__(name)
        self.spark = spark
        self.path = path
        self.fmt = fmt

    def run(self) -> DataFrame:
        if self.fmt == "csv":
            return (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(self.path)
            )
        elif self.fmt == "parquet":
            return self.spark.read.parquet(self.path)
        elif self.fmt == "json":
            return self.spark.read.json(self.path)
        else:
            raise ValueError(f"Unsupported format: {self.fmt}")

# -------------------------------
# 3) Example pipeline orchestration
# -------------------------------
def run_pipeline():
    # Define dataset paths (local example)
    base_path = "banking_dataset/datasets"
    customers_path = f"{base_path}/customers.csv"
    accounts_path = f"{base_path}/accounts.csv"

    # Step 1: Extract customers
    customers_extractor = Extractor("Extract_Customers", spark, customers_path, fmt="csv")
    customers_df = customers_extractor.execute()

    # Step 2: Extract accounts
    accounts_extractor = Extractor("Extract_Accounts", spark, accounts_path, fmt="csv")
    accounts_df = accounts_extractor.execute()

    # (You can add schema enforcement, CDC, DQ as new components here)
    print("👥 Customers sample:")
    customers_df.show(5)
    print("💰 Accounts sample:")
    accounts_df.show(5)

    # Summarize metrics
    print("\n📊 Pipeline Metrics:")
    for comp in [customers_extractor, accounts_extractor]:
        print(f"- {comp.name}: {comp.metrics}")

# -------------------------------
# 4) Run the demo pipeline
# -------------------------------
if __name__ == "__main__":
    run_pipeline()
    spark.stop()
    print("🛑 Spark session stopped")
```

------

### ⚡ What happens when you run this

1. Each step (`Extractor`) is wrapped in **ETLComponent**:
   - Logs start/finish,
   - Times execution,
   - Catches errors with a stack trace,
   - Stores metrics.
2. If a step fails, the pipeline **stops immediately** (fail fast).
3. If it succeeds, you see a clean **PASS report** with timings.

------

### 🧭 Where this leads

- You can define **Transformer** and **Loader** classes the same way.
- You can orchestrate the whole flow (Extract → Transform → Load → Validate).
- In production, these steps would be triggered by **Airflow**, **cron jobs**, or **Spark job servers**.

------

### ✅ What you’ve achieved

- Modularized ETL logic into **reusable components**.
- Added **structured error handling** (fail fast with details).
- Captured **metrics** (durations, errors) for monitoring.

------

## 7. Monitoring, Performance & Common Pitfalls

### 🔎 Why Monitoring Matters

In banks, ETL pipelines often run **unattended** overnight or every hour. If a job silently fails or slows down:

- Reports may be late (regulatory risk).
- Fraud alerts may miss real-time signals.
- Downstream systems may consume incomplete data.

That’s why we need **monitoring hooks, performance tuning, and a checklist of common pitfalls**.

------

### 📡 Monitoring ETL Pipelines

We’ve already added **metrics** in our ETL components (durations, errors). Let’s extend that:

1. **Row counts** per stage (input/output).
2. **Null statistics** on critical fields.
3. **Timing metrics** (elapsed seconds).
4. **Audit logs** (who ran it, when, batch ID).

👉 Example: Add a simple monitoring utility

```python
from pyspark.sql.functions import count, when, col
from datetime import datetime

def monitor_dataframe(df, name: str, critical_columns=None):
    rows = df.count()
    print(f"📊 {name}: {rows:,} rows")

    if critical_columns:
        nulls = df.select([count(when(col(c).isNull(), c)).alias(c) for c in critical_columns])
        print(f"🔍 Null analysis for {name}:")
        nulls.show()

    return {
        "dataset": name,
        "rows": rows,
        "checked_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"
    }

# Example usage inside pipeline
txn_metrics = monitor_dataframe(raw_data["transactions"], "Transactions", critical_columns=["transaction_id", "account_id"])
print("Metrics:", txn_metrics)
```

In production, you’d push these metrics into:

- **Databases** (for trend analysis),
- **Monitoring tools** (Prometheus, Grafana, CloudWatch),
- **Alerting systems** (Slack, PagerDuty).

------

### ⚡ Performance Tuning in Spark ETL

Some best practices to keep pipelines **efficient**:

1. **Partitioning wisely**
   - Use `.repartition()` to spread work across executors.
   - Use `.coalesce()` to reduce small files at the end.
   - Partition writes by natural keys (e.g., `yyyy/mm/dd` for transactions).
2. **Caching hot DataFrames**
   - Use `.cache()` if a DataFrame is reused multiple times.
3. **Predicate pushdown**
   - Keep filters (`.filter()`) as close as possible to the read step.
4. **Explain your plan**
   - Run `df.explain(True)` to see Spark’s physical plan.
   - Teach participants to spot **full scans**, **shuffles**, and **skew joins**.

👉 Example:

```python
df = raw_data["transactions"].filter(col("amount") > 1000)
df.explain(True)  # Inspect Spark’s query plan
```

------

### 🚨 Common Pitfalls (and Fixes)

1. **Not validating data** → Garbage-in, garbage-out.
    ✅ Fix: Add data quality checks (Section 5).
2. **Over-repartitioning** → Too many small tasks/files.
    ✅ Fix: Use `.coalesce()` or set `spark.sql.files.maxPartitionBytes`.
3. **Schema evolution breaking pipelines** → New columns cause failures.
    ✅ Fix: Use schema evolution handler (Section 3).
4. **Silent errors** → Job runs but writes partial data.
    ✅ Fix: Add row counts + validation at each stage.
5. **Hardcoding credentials** → Security & compliance violation.
    ✅ Fix: Use IAM roles, secrets managers, or config files.
6. **No monitoring/alerting** → Failures go unnoticed.
    ✅ Fix: Push metrics & alerts into monitoring stack.

------

### ✅ What You’ve Achieved

- Learned to monitor ETL with **row counts, null checks, and execution timings**.
- Understood Spark **performance knobs**: partitions, caching, explain plans.
- Learned the **top pitfalls** in banking ETL and how to avoid them.



Great — let’s wrap up with **Section 8: Advanced Directions**.
 This section broadens participants’ view: beyond batch ETL, what’s next in the real world of banking data engineering?

------

## 8. Advanced Directions

### 🚀 Moving Beyond Batch

So far, you’ve built **batch ETL pipelines**: extract → transform → load once per day/hour.
 Banks, however, are increasingly moving toward **real-time** and **AI-powered** data platforms.

Here are five advanced directions you can explore next.

------

### 1️⃣ Real-Time ETL (Streaming Pipelines)

- **Why:** Fraud detection, instant balance updates, regulatory alerts.
- **How:** Use **Apache Kafka + Spark Structured Streaming**.
- **Pattern:**
  - Kafka → Spark → Write to Parquet/Delta Lake → Real-time dashboards.
- **Example snippet:**

```python
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .load()
)

parsed_df = stream_df.selectExpr("CAST(value AS STRING)")
query = (
    parsed_df.writeStream
    .format("parquet")
    .option("path", "banking_dataset/stream_output")
    .option("checkpointLocation", "banking_dataset/stream_checkpoints")
    .start()
)

query.awaitTermination()
```

------

### 2️⃣ Machine Learning Inside ETL

- **Why:** Many banking processes embed predictive models (e.g., credit scoring, churn prediction).
- **How:**
  - Train with Spark MLlib or scikit-learn.
  - Save model (MLflow, joblib).
  - Load model inside ETL pipeline for scoring customers or transactions.
- **Use case:** Flagging risky transactions in near real-time.

------

### 3️⃣ Multi-Cloud & Kubernetes Deployments

- **Why:** Banks often operate across multiple regions/providers (AWS, Azure, GCP).
- **How:**
  - Use **Kubernetes + Helm** to deploy Spark jobs.
  - Store data in **object storage** (S3, GCS, Azure Blob).
  - Orchestrate pipelines with **Airflow** or **Prefect**.

------

### 4️⃣ Compliance, Lineage & Governance

- **Why:** Regulations (GDPR, Basel III, PCI DSS) require full traceability.
- **How:**
  - Use **Apache Atlas** or a Data Catalog to track lineage.
  - Log all schema changes, transformations, and data movements.
  - Generate audit reports automatically.

------

### 5️⃣ Advanced Cost & Performance Optimization

- **Why:** Data pipelines can become expensive at scale.
- **How:**
  - Partition + Z-order files (if using Delta Lake).
  - Use **spot instances** for non-critical jobs.
  - Monitor **Spark UI** for bottlenecks (shuffle, skew, spilling).
  - Implement **data lakehouse** architecture (Delta Lake/Iceberg/Hudi).

------

### ✅ What You’ve Achieved

By the end of this section, you now know the **future directions** for banking ETL:

- Real-time pipelines with **Kafka + Spark Streaming**.
- Embedding **machine learning** into ETL for predictive insights.
- Deploying pipelines on **Kubernetes & multi-cloud environments**.
- Building **compliance & lineage** into pipelines for auditability.
- Optimizing for **performance and cost** at enterprise scale.

------

