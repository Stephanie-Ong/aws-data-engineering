# Module 7 — PySpark UDFs & Pandas UDFs

**Learning Time:** 60–90 minutes
 **Prerequisites:** Modules 1–4 (transactions/customers datasets)
 **Learning Objectives:**

- Know when **not** to use UDFs (prefer built-ins)
- Create **regular UDFs** (`@udf`) and **register** them for SQL
- Use **Pandas UDFs** (vectorized) for scalar & grouped aggregations
- Add **robust error handling** and lightweight **logging** in UDFs

------

## 📋 Functions & APIs Reference

| Area                       | API                                                          | Purpose                     | Example                                                |
| -------------------------- | ------------------------------------------------------------ | --------------------------- | ------------------------------------------------------ |
| Prefer built-ins           | `when`, `regexp_replace`, `initcap`, `coalesce`, `date_format`, `percentile_approx` | Faster than UDFs            | `F.when(F.col("amount")<0,"W").otherwise("D")`         |
| Regular UDF                | `@udf(returnType)`, `spark.udf.register()`                   | Row-wise Python UDF         | `@udf("string")`                                       |
| Pandas UDF (scalar)        | `@pandas_udf("double")`                                      | Vectorized row-wise         | `@pandas_udf("double") def z(x: pd.Series)->pd.Series` |
| Pandas UDF (grouped agg)   | `@pandas_udf("double", PandasUDFType.GROUPED_AGG)`           | Vectorized reduce per group | `groupBy("customer_id").agg(my_mean(F.col("amount")))` |
| Apply in Pandas (optional) | `applyInPandas(func, schema)`                                | Grouped map                 | `df.groupBy("k").applyInPandas(fn, schema)`            |
| Arrow toggle               | `spark.conf.set("spark.sql.execution.arrow.pyspark.enabled","true")` | Enable Arrow                | —                                                      |
| Logging                    | `logging.getLogger(__name__)`                                | Executor logs               | `logger.info("...")`                                   |

------

## 0) Quick setup

```python
# script_22.py
pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import udf  # optional explicit import
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
import pandas as pd
import logging

# If not already created in earlier modules:
spark = SparkSession.builder.appName("Module7_UDFs").getOrCreate()

# Arrow for Pandas UDFs
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Reuse the same bucket layout you used in Module 4
base_path = "banking_dataset/datasets"

dataset_paths = {
    "customers_raw": f"{base_path}/customers.csv",
    "transactions_raw": f"{base_path}/transactions.csv"
}

# Read sample tables (schema inference is fine for the module demos)
transactions = (spark.read
    .option("header","true").option("inferSchema","true")
    .option("timestampFormat","yyyy-MM-dd HH:mm:ss.SSSSSS")
    .csv(dataset_paths["transactions_raw"])
    # helpful derived columns used later
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("tx_date", F.to_date("transaction_date"))
    .withColumn("year_month", F.date_format("tx_date", "yyyy-MM"))
)
customers = (spark.read
    .option("header","true").option("inferSchema","true")
    .csv(dataset_paths["customers_raw"]))

print("Datasets loaded:")
print(" - customers:", customers.count())
print(" - transactions:", transactions.count())
#-----------------------------
```

------

## 1) UDFs vs. Built-ins — always prefer built-ins

UDFs run Python on the executors and add (de)serialization overhead. If Spark has a built-in, use it.

**Example — built-ins (✅)**

```python
tx_enriched_builtin = (transactions
  .withColumn("tx_kind", F.when(F.col("amount") < 0, F.lit("WITHDRAW"))
                           .otherwise(F.lit("DEPOSIT")))
  .withColumn("merchant_clean",
              F.initcap(F.regexp_replace(F.col("merchant"), r"[^A-Za-z0-9 ]", "")))
  .withColumn("is_large", F.abs(F.col("amount")) > F.lit(5000))
)
```

**Only reach for a UDF if:**

- The logic doesn’t exist as a native function
- It’s too challenging to express with SQL/F built-ins

------

## 2) Regular UDFs — `@udf` and SQL registration

### 2.1 `@udf` decorator

```python
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def risk_band_from_amount(a: float) -> str:
    if a is None:
        return "Unknown"
    aa = abs(a)
    if aa >= 10000: return "Very High"
    if aa >= 5000:  return "High"
    if aa >= 1000:  return "Medium"
    return "Low"

tx_with_band = transactions.withColumn("risk_band", risk_band_from_amount(F.col("amount")))
```

### 2.2 Register UDF for Spark SQL

```python
spark.udf.register("risk_band_from_amount", risk_band_from_amount)

transactions.createOrReplaceTempView("transactions")
spark.sql("""
  SELECT customer_id,
         risk_band_from_amount(amount) AS risk_band,
         COUNT(*) AS transaction_count -- transaction frequency count per customer per risk category
  FROM transactions
  GROUP BY customer_id, risk_band
  ORDER BY customer_id, risk_band
""").show()
```

------

## 3) Pandas UDFs (Vectorized with Arrow)

**When to use:** heavy row-wise math or per-group reductions where vectorization helps.
 **Caveats:** Arrow batch size/memory; avoid for tiny groups/very small data.

### 3.1 Scalar Pandas UDF (row-wise, vectorized)

Z-score of transaction **within the whole dataset** (demo):

```python
from pyspark.sql.functions import pandas_udf

# Precompute global stats via built-ins (fast)
stats = transactions.select(
    F.avg("amount").alias("mu"),
    F.stddev_pop("amount").alias("sigma")
).collect()[0]
mu, sigma = float(stats["mu"] or 0.0), float(stats["sigma"] or 1.0)

@pandas_udf("double")
def zscore(amount: pd.Series) -> pd.Series:
    # Vectorized: operates on pandas Series
    return (amount - mu) / (sigma if sigma != 0 else 1.0)

tx_z = transactions.withColumn("amount_z", zscore(F.col("amount")))
```

### 3.2 Grouped Aggregation Pandas UDF (per-group reduction)

Compute **customer-level median** (Spark lacks an exact median agg; this is a good fit):

```python
from pyspark.sql.functions import pandas_udf

@pandas_udf(returnType="double")
def median_pd(v: pd.Series) -> float:
    # robust to NaNs
    s = v.dropna()
    if len(s) == 0:
        return None
    return float(s.median())

# First get regular aggregations
cust_basic_stats = (transactions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("tx_count"),
        F.avg("amount").alias("amount_mean")
    )
)

# Then get pandas UDF aggregation separately
cust_medians_only = (transactions
    .groupBy("customer_id")
    .agg(
        median_pd(F.col("amount")).alias("amount_median")
    )
)

# Join the results together
cust_medians = cust_basic_stats.join(cust_medians_only, "customer_id")
cust_medians.show(5, truncate=False)
```

> Tip: If you need a **per-group** z-score, compute group stats with built-ins, join back, then a **scalar** Pandas UDF for the vectorized transform. That’s faster than calling a grouped map on every partition.

------

## 4) Try/Except & Logging inside UDFs

- Use **small, pure functions**; catch exceptions and return a neutral value (e.g., `None` or sentinel) so the job doesn’t fail an entire task.
- For debugging, write to **executor logs** (visible in your cluster UI/Glue/EMR logs). Don’t spam logs in hot paths.

```python
import re
logger = logging.getLogger(__name__)

@udf(returnType=StringType())
def email_domain_safe(email: str) -> str:
    try:
        if email is None: 
            return None
        m = re.search(r"@([A-Za-z0-9.-]+\.[A-Za-z]{2,})$", email.strip())
        return m.group(1).lower() if m else None
    except Exception as e:
        # One-line breadcrumb; excessive logging can be noisy at scale
        logger.warning(f"email_domain_safe error: {str(e)[:80]}")
        return None

tx_domains = transactions.withColumn("email_domain", email_domain_safe(F.col("merchant")))
tx_domains.select("merchant", "email_domain").show(10, truncate=False)
```

**Pattern: fail-fast vs. fail-soft**

- **Fail-fast** (raise): great during unit tests/notebooks; surfaces bugs early.
- **Fail-soft** (catch & default): good in production pipelines, paired with **quality gates** that flag high null/error rates.

------

## 5) Performance & correctness checklist

1. **Prefer built-ins** (regex, conditionals, date math, window calcs).
2. **Cache** only if reused; avoid calling `.count()` repeatedly.
3. **Pandas UDFs** → ensure Arrow is enabled; watch peak memory per executor.
4. **Skew**: if a few customers have huge volume, consider salting or two-phase agg.
5. **Test UDFs locally** with small pandas Series before running at cluster scale.
6. **Document** return types precisely; wrong schemas cause hard-to-debug errors.

------

## 6) Hands-On Lab 

**Goal:** add value with UDFs only where built-ins aren’t enough.

1. **Scalar Pandas UDF (per-customer normalization)**
   - Compute per-customer `amount_median` and `amount_iqr` using built-ins:
      `median_pd` (above) for median; IQR via `percentile_approx(amount, array(0.25,0.75))`.
   - Join these stats back to `transactions`.
   - Implement `@pandas_udf("double") def robust_score(amount, med, iqr)` that returns `(amount - med) / max(iqr, 1.0)`.
   - Add column `robust_z`.
2. **Grouped-Agg Pandas UDF**
   - By `customer_id` & `year_month`, compute `median_pd(amount)` and share of **deposits**:
      `share_deposit = mean( amount > 0 )` implemented as a grouped-agg Pandas UDF returning double.
3. **Register a UDF for SQL**
   - Register `risk_band_from_amount` as `risk_band_from_amount` and run a SQL that returns top 5 customers by count of “Very High” transactions.
4. **Error handling**
   - Create `@udf("string")` to safely standardize merchant strings (strip symbols, title-case).
   - Intentionally feed a few bad inputs; verify it returns `None` instead of failing and logs a single warning line.

**Stretch:** Write final outputs to a local or S3 partition based on `year_month`. If stored in S3, you'll be able to query them with Athena.

------

## 7) Quick Reference Card

| Task                                            | Best Tool                                     |
| ----------------------------------------------- | --------------------------------------------- |
| Conditional labels, regex cleanups              | **Built-ins** (`when`, `regexp_*`, `initcap`) |
| Row-wise heavy math (vectorizable)              | **Scalar Pandas UDF**                         |
| Per-group custom reduce (e.g., true median)     | **Grouped-Agg Pandas UDF**                    |
| Weird bespoke transformation (no vectorization) | **Regular UDF** (last resort)                 |
| SQL use of Python logic                         | `spark.udf.register`                          |

------

### Congratulations!