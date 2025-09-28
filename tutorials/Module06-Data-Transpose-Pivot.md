# Module 6: Data Transpose, Pivot/Unpivot & Row Differences

**Duration:** 2.5 hours 

**Prerequisites:** Basic Python knowledge and completed Modules 1-5 

**What You'll Build:** Advanced data reshaping and time-series analysis for banking

------

## What You'll Learn Step by Step

1. **Lesson 1:** Load banking data and understand when data needs reshaping
2. **Lesson 2:** Transpose and Pivot - turning rows into columns the easy way
3. **Lesson 3:** Unpivot - turning columns into rows when you need it
4. **Lesson 4:** Advanced reshaping with real banking scenarios
5. **Lesson 5:** Row differences - tracking changes over time
6. **Lesson 6:** Building reports with reshaped banking data

------

## Lesson 1: Loading Banking Data and Understanding Data Shapes 

### What is Data Reshaping and Why Do We Need It?

Data reshaping is like rearranging furniture in your room. Sometimes you need your table wide to spread out papers, and sometimes you need it narrow to save space. Banking data works the same way - sometimes we need it arranged differently to answer different questions.

Think of a customer's monthly balances. We can organize this information in two main ways:

**Wide format:** One row per customer with separate columns for each month (Jan_Balance, Feb_Balance, Mar_Balance) 

**Long format:** Multiple rows per customer with one column for month and one for balance

### Wide format (one row per customer; months as columns)

| customer_id | customer_name | Jan_2024_Balance | Feb_2024_Balance | Mar_2024_Balance |
| ----------- | ------------- | ---------------- | ---------------- | ---------------- |
| CUST001     | Ali Rahman    | 15,000           | 18,500           | 17,200           |
| CUST002     | Sara Lim      | 8,200            | —                | 9,100            |
| CUST003     | John Tan      | 22,300           | 22,900           | 23,500           |

### Long format (multiple rows per customer; one “month” column)

| customer_id | customer_name | month   | balance |
| ----------- | ------------- | ------- | ------- |
| CUST001     | Ali Rahman    | 2024-01 | 15,000  |
| CUST001     | Ali Rahman    | 2024-02 | 18,500  |
| CUST001     | Ali Rahman    | 2024-03 | 17,200  |
| CUST002     | Sara Lim      | 2024-01 | 8,200   |
| CUST002     | Sara Lim      | 2024-03 | 9,100   |
| CUST003     | John Tan      | 2024-01 | 22,300  |
| CUST003     | John Tan      | 2024-02 | 22,900  |
| CUST003     | John Tan      | 2024-03 | 23,500  |

- In **wide**, it’s easy to compare months side-by-side for a customer.
- In **long**, it’s easy to group by month, chart trends, or run time-series ops.
   (“—” just means no data recorded for that month.)

Neither format is "better" - they just serve different purposes. Wide format is great for comparing months side-by-side. Long format is better for trends and analysis over time.

### Step 1: Set Up and Load Real Banking Data

```python
# script_21.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

def start_banking_spark():
    spark = SparkSession.builder \
            .appName("Banking_ETL_Pipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

    return spark

spark = start_banking_spark()

def load_banking_datasets(spark):
    """
    Load all 6 banking datasets from S3 storage
    """
    base_path = "banking_dataset/datasets"
    # These are the locations of our banking data files
    data_locations = {
        "customers": f"{base_path}/customers.csv",
        "accounts": f"{base_path}/accounts.csv", 
        "transactions": f"{base_path}/transactions.parquet",
        "branches": f"{base_path}/branches.json",
        "products": f"{base_path}/products.csv",
        "loans": f"{base_path}/loans.csv",
        "transactions_processed": f"{base_path}/processed/transactions/"
    }
    
    datasets = {}
    
    for table_name, file_location in data_locations.items():
        print(f"📖 Loading {table_name} data...")
        
        try:
            # Handle different file formats appropriately
            if table_name == "transactions" and file_location.endswith(".parquet"):
                # Read parquet file directly
                df = spark.read.parquet(file_location)
            elif table_name == "branches" and file_location.endswith(".json"):
                # Read JSON file
                df = spark.read \
                    .option("multiline", "true") \
                    .json(file_location)
            elif table_name == "transactions_processed":
                # Skip processed directory for now or handle differently
                print(f"⏭️  Skipping {table_name} (directory)")
                continue
            else:
                # Read CSV files
                df = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                    .csv(file_location)
            
            # Cache important tables in memory for faster access
            if table_name in ['transactions', 'customers', 'accounts']:
                df.cache()
            
            datasets[table_name] = df
            print(f"✅ Loaded {df.count():,} {table_name} records")
            
        except Exception as e:
            print(f"❌ Error loading {table_name}: {str(e)}")
            print(f"   File location: {file_location}")
            # Continue with other datasets
            continue
    
    return datasets

# Load all our banking data
print(" Loading banking datasets...")
banking_data = load_banking_datasets(spark)

# Extract each dataset into separate variables for easy use (with error handling)
customers_df = banking_data.get('customers')
accounts_df = banking_data.get('accounts')
transactions_df = banking_data.get('transactions')
loans_df = banking_data.get('loans')
products_df = banking_data.get('products')
branches_df = banking_data.get('branches')

print(f"\n Successfully loaded {len(banking_data)} banking datasets!")
```

### Step 2: Understanding Long vs Wide Format

Let's look at our transaction data to understand different formats:

```python
# Show sample transaction data (long format)
transactions_df.select("customer_id", "transaction_date", "amount", "transaction_type").show(10)

# This is long format - multiple rows per customer
sample_long = transactions_df \
    .withColumn("month", date_format(col("transaction_date"), "yyyy-MM")) \
    .filter(col("month").between("2023-09", "2023-10")) \
    .groupBy("customer_id", "month") \
    .agg(sum("amount").alias("monthly_total")) \
    .orderBy("customer_id", "month")

sample_long.show(15)
```

In long format, each customer appears in multiple rows - one for each month. This format is excellent for time series analysis, grouping by time periods, creating trend charts, and machine learning algorithms.

Wide format would show the same information differently - each customer in one row with separate columns for each month. Wide format excels at executive reports and dashboards, side-by-side period comparisons, Excel-style summary tables, and calculating period-over-period changes.

------

## Lesson 2: Pivot Operations - Turning Rows into Columns

### Understanding Pivot in Simple Terms

Pivot transforms data from tall-and-skinny to short-and-wide. Imagine you have a notebook where you write one entry per page. Pivoting is like taking those entries and arranging them in columns across a single page instead.

In banking, we often need to pivot when creating executive reports or comparing performance across time periods. For example, showing each customer's balance for January, February, and March in separate columns.

### Step 1: Basic Pivot Operation

```python
# Create monthly transaction summaries from real data
monthly_transactions = transactions_df \
    .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM")) \
    .filter(col("year_month").between("2023-09", "2023-12")) \
    .groupBy("customer_id", "year_month") \
    .agg(
        sum("amount").alias("monthly_total"),
        count("transaction_id").alias("transaction_count")
    )

# Show the long format first
monthly_transactions.orderBy("customer_id", "year_month").show(20)

# Now pivot to wide format
pivoted_monthly = monthly_transactions \
    .groupBy("customer_id") \
    .pivot("year_month") \
    .agg(sum("monthly_total"))

# Clean up missing values
pivoted_clean = pivoted_monthly.fillna(0)
pivoted_clean.show()
```

Let's break down what happened:

1. **groupBy()** specifies which information stays together (customer_id)
2. **pivot()** takes the year_month values and converts them into column names
3. **agg()** determines what calculation to perform for each cell (sum of monthly_total)
4. **fillna()** replaces missing values with zeros

### Step 2: Business Application - Account Analysis by Branch

Now let's use pivot for a real banking scenario using our account data:

```python
# Analyze account distribution by branch and account type
account_branch_analysis = accounts_df \
    .join(branches_df.select("branch_id", "branch_name"), "branch_id") \
    .filter(col("account_status") == "Active") \
    .groupBy("branch_name") \
    .pivot("account_type") \
    .agg(
        count("account_id").alias("count"),
        sum("balance").alias("total_balance")
    )

account_branch_clean = account_branch_analysis.fillna(0)
account_branch_clean.show(truncate=False)
```

This pivot creates a powerful business view showing how each branch performs across different account types. Branch managers can quickly see where they have strengths or gaps in their product portfolio.

------

## Unpivot Operations - Turning Columns into Rows (25 minutes)

### Understanding Unpivot

Unpivot does the reverse of pivot - it takes data that's spread across multiple columns and stacks it into rows. Think of it as taking information from several columns in a spreadsheet and listing it vertically instead.

This transformation is common in banking when you receive reports in spreadsheet format but need to analyze them in a database or create visualizations.

### Step 1: Basic Unpivot Using Stack Function

```python
# First create some pivoted data from our banking data
# Join accounts -> customers -> branches through city relationship
branch_metrics = accounts_df \
    .join(customers_df.select("customer_id", "city"), "customer_id") \
    .join(branches_df.select("city", "branch_name"), "city") \
    .groupBy("branch_name") \
    .agg(
        sum(when(col("account_type") == "Checking", col("balance"))).alias("Checking_Balance"),
        sum(when(col("account_type") == "Savings", col("balance"))).alias("Savings_Balance"),
        sum(when(col("account_type") == "Credit Card", col("balance"))).alias("CreditCard_Balance")
    ).fillna(0)

branch_metrics.show()

# Now unpivot this wide data back to long format
unpivoted_branches = branch_metrics.select(
    "branch_name",
    expr("stack(3, 'Checking', Checking_Balance, 'Savings', Savings_Balance, 'Credit Card', CreditCard_Balance) as (account_type, balance)")
)

unpivoted_branches.show()
```

The stack function works by:

1. Specifying how many column pairs you have (3 in this case)
2. Listing pairs of (column_name, column_value)
3. Creating two new columns with the names you specify

### Step 2: Real Banking Application - Product Performance Analysis

```python
# Create product performance metrics from data
# Join accounts with products using account_type = product_type relationship
product_performance = accounts_df \
    .join(products_df.select("product_type", "product_name"), 
          accounts_df.account_type == products_df.product_type) \
    .groupBy("product_name") \
    .agg(
        count("account_id").alias("total_accounts"),
        sum("balance").alias("total_balance"),
        avg("balance").alias("average_balance")
    )

# Convert to long format for visualization
product_metrics_long = product_performance.select(
    "product_name",
    expr("stack(3, 'Account Count', cast(total_accounts as double), 'Total Balance', cast(total_balance as double), 'Average Balance', cast(average_balance as double)) as (metric_type, metric_value)")
)

product_metrics_long.show()
```

------

## Lesson 4: Advanced Reshaping Techniques

### Multi-Level Pivoting with Customer Segments

Sometimes you need to pivot on multiple dimensions simultaneously. This is common in banking when analyzing performance across different segments and time periods.

```python
# Create transaction analysis by customer segment and transaction type
segment_transaction_analysis = transactions_df \
    .join(customers_df.select("customer_id", "customer_segment"), "customer_id") \
    .filter(col("transaction_date") >= "2023-01-01") \
    .groupBy("customer_segment", "transaction_type") \
    .agg(
        count("transaction_id").alias("transaction_count"),
        sum("amount").alias("total_amount")
    )

# Pivot by transaction type
segment_pivot = segment_transaction_analysis \
    .groupBy("customer_segment") \
    .pivot("transaction_type") \
    .agg(sum("total_amount"))

segment_pivot.fillna(0).show()
```

### Dynamic Pivoting with Branch Analysis

```python
# Get unique account types dynamically from S3 data
unique_account_types = accounts_df.select("account_type").distinct().rdd.flatMap(lambda x: x).collect()

# Create dynamic pivot for branch performance
# Join accounts -> customers -> branches through city relationship
branch_dynamic_pivot = accounts_df \
    .join(customers_df.select("customer_id", "city"), "customer_id") \
    .join(branches_df.select("city", "region"), "city") \
    .groupBy("region") \
    .pivot("account_type", unique_account_types) \
    .agg(sum("balance"))

branch_dynamic_pivot.show()
```

------

## Lesson 5: Row Difference Calculations for Time Series Analysis 

### Understanding Row Differences

Row differences help us track changes over time. Instead of just knowing a customer's balance is $50,000, we can know it increased by $5,000 from last month. This temporal analysis is crucial for banking risk management and performance tracking.

### Step 1: Basic Lag and Lead Functions with Banking Data

```python
# Create monthly customer balance trends from S3 transaction data
customer_monthly_trends = transactions_df \
    .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM")) \
    .filter(col("year_month").between("2023-01", "2023-12")) \
    .groupBy("customer_id", "year_month") \
    .agg(sum("amount").alias("net_flow")) \
    .join(customers_df.select("customer_id", "first_name", "last_name", "customer_segment"), "customer_id")

# Define window specification for time series analysis  
time_window = Window.partitionBy("customer_id").orderBy("year_month")

# Calculate previous month values and changes
balance_trends = customer_monthly_trends \
    .withColumn("prev_month_flow", lag("net_flow", 1).over(time_window)) \
    .withColumn("month_over_month_change", 
                col("net_flow") - lag("net_flow", 1).over(time_window)) \
    .withColumn("percent_change",
                round(((col("net_flow") - lag("net_flow", 1).over(time_window)) / 
                       abs(lag("net_flow", 1).over(time_window))) * 100, 2))

balance_trends.show()
```

### Step 2: Credit Risk Monitoring with Loan Data

```python
# Track loan payment patterns using S3 loan data
loan_payment_analysis = loans_df \
    .select("loan_id", "customer_id", "current_balance", "payment_history_score", "loan_status") \
    .join(customers_df.select("customer_id", "customer_segment"), "customer_id")

# Create time-based analysis (simulating monthly snapshots)
loan_window = Window.partitionBy("loan_id").orderBy("customer_id")

payment_trends = loan_payment_analysis \
    .withColumn("balance_rank", row_number().over(loan_window)) \
    .withColumn("risk_category",
        when(col("payment_history_score") < 60, "High Risk")
        .when(col("payment_history_score") < 80, "Medium Risk")
        .otherwise("Low Risk")
    )

payment_trends.show()
```

------

## Lesson 6: Building Banking Reports with Reshaped Data 

### Executive Dashboard Creation

Executive dashboards require data in specific formats. Let's create a comprehensive performance report using our S3 banking data:

```python
# Create comprehensive branch performance dashboard
# Join accounts -> customers -> branches through city relationship
branch_dashboard = accounts_df \
    .join(customers_df.select("customer_id", "customer_segment", "city"), "customer_id") \
    .join(branches_df.select("city", "branch_name", "region"), "city") \
    .groupBy("branch_name", "region") \
    .agg(
        count("account_id").alias("total_accounts"),
        sum("balance").alias("total_deposits"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("balance").alias("avg_account_balance")
    ) \
    .withColumn("deposits_millions", round(col("total_deposits") / 1000000, 2)) \
    .withColumn("performance_tier",
        when(col("total_deposits") > 50000000, "Top Performer")
        .when(col("total_deposits") > 25000000, "Strong Performer")
        .otherwise("Developing")
    )

branch_dashboard.show()
```

### Customer Profitability Analysis

```python
# Create customer profitability report using reshaped transaction data
customer_profitability = transactions_df \
    .join(customers_df.select("customer_id", "customer_segment", "annual_income"), "customer_id") \
    .groupBy("customer_id", "customer_segment", "annual_income") \
    .agg(
        sum("amount").alias("total_transaction_value"),
        count("transaction_id").alias("transaction_frequency")
    ) \
    .withColumn("profitability_score",
        (col("total_transaction_value") * 0.001 + col("transaction_frequency") * 0.1)
    ) \
    .withColumn("customer_tier",
        when(col("profitability_score") > 100, "High Value")
        .when(col("profitability_score") > 50, "Medium Value")
        .otherwise("Standard Value")
    )

customer_profitability.show()
```

------

## Practice Exercises

### Exercise 1: Monthly Branch Performance Analysis

```python
# Sample data from S3 accounts table - your task is to complete the analysis
branch_sample = accounts_df \
    .join(branches_df.select("branch_id", "branch_name"), "branch_id") \
    .select("branch_name", "account_type", "balance", "account_status") \
    .filter(col("account_status") == "Active")

# Your solution here:
# 1. Create a pivot showing account types as columns and branches as rows
# 2. Calculate total balance for each account type per branch
# 3. Identify which branch has the highest credit card balances

# Solution approach:

branch_pivot_clean.show()

# Find branch with highest credit card balances

    
print(f"Branch with highest credit card balances: {highest_cc_branch}")
```

### Exercise 2: Customer Transaction Patterns (15 minutes)

```python
# Sample data from S3 transactions table - your task is to complete the analysis  
customer_transactions = transactions_df \
    .withColumn("month", date_format(col("transaction_date"), "yyyy-MM")) \
    .filter(col("month").between("2023-01", "2023-06")) \
    .groupBy("customer_id", "month", "transaction_type") \
    .agg(count("transaction_id").alias("tx_count"))

# Your solution here:
# 1. Pivot to show transaction types as columns
# 2. Calculate month-over-month changes in transaction patterns
# 3. Identify customers with declining transaction activity

# Solution approach:


tx_pivot_clean = tx_pivot.fillna(0)
tx_pivot_clean.show()

# Calculate month-over-month changes

declining_customers.select("customer_id", "month", "transaction_trend").show()
```

### Exercise 3: Product Performance Dashboard 

```python
# Sample data combining accounts and products - your task is to complete the analysis
product_analysis = accounts_df \
    .join(products_df.select("product_id", "product_name", "product_type"), "product_id") \
    .join(customers_df.select("customer_id", "customer_segment"), "customer_id") \
    .filter(col("account_status") == "Active")

# Your solution here:
# 1. Create an unpivot operation showing product metrics in long format
# 2. Calculate penetration rates by customer segment
# 3. Build a comprehensive product performance report

# Solution approach:
# First create summary metrics


product_summary.show()

# Unpivot for visualization

product_metrics_long.show()

# Calculate penetration rates


penetration_rates.select("product_name", "customer_segment", "penetration_rate").show()
```

### Exercise 4: Risk Trend Analysis (20 minutes)

```python
# Sample data using loans and customers - your task is to complete the analysis
risk_analysis = loans_df \
    .join(customers_df.select("customer_id", "customer_segment", "risk_score"), "customer_id") \
    .select("loan_id", "customer_id", "customer_segment", "risk_score", 
            "current_balance", "payment_history_score", "loan_status")

# Your solution here:
# 1. Use row difference calculations to identify deteriorating loan performance
# 2. Create risk categories and track changes over time
# 3. Build early warning indicators for loan defaults

# Solution approach:
# Create risk categories


# Simulate time-based changes using window functions


# Show high-risk customers
warning_customers = risk_trends.filter(col("early_warning_flag") == "WARNING")
warning_customers.select("customer_id", "customer_segment", "risk_score", 
                        "payment_history_score", "current_balance").show()
```

------

## Key Takeaways

Data reshaping is a fundamental skill for banking analytics. You now understand:

1. **When to use pivot vs unpivot** operations based on your analysis goals
2. **How to transform data shapes** using real S3 banking datasets
3. **Row difference calculations** for time-series and trend analysis
4. **Performance considerations** when working with large banking datasets
5. **Real-world applications** in regulatory reporting and executive dashboards

These techniques form the foundation for advanced banking analytics, enabling you to transform raw banking data into actionable business insights using production-scale datasets from S3 storage.