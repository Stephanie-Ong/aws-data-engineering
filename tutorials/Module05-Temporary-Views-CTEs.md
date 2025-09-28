# Module 5: Temporary Views & CTEs (Spark SQL)

**Duration:** 2.5 hours 

**Prerequisites:** Basic Python knowledge and completed Modules 1-4 

**What You'll Build:** SQL-powered banking analytics system with real data

------

## What You'll Learn Step by Step

1. **Lesson 1:** Set up Spark and load banking data from S3
2. **Lesson 2:** Create your first SQL views and write basic queries
3. **Lesson 3:** Understand Single CTEs with customer analysis
4. **Lesson 4:** Master Multiple CTEs for complex banking reports
5. **Lesson 5:** Build advanced risk assessment systems
6. **Lesson 6:** Create executive dashboards and reports

------

## Lesson 1: Getting Started with Banking Data

### What is SQL with PySpark?

Think of PySpark as a powerful calculator for big data, and SQL as the language we use to ask questions. Instead of writing complex Python code, we can ask simple questions like "Show me all customers with high credit scores" using familiar SQL commands.

### Step 1: Set Up Your Environment

```python
# script_20.py
pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create our Spark session - think of this as starting up our data processing engine
def start_banking_spark():
    spark = SparkSession.builder \
        .appName("Banking_SQL_Tutorial") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("✅ Spark is ready for banking analytics!")
    return spark

spark = start_banking_spark()
```

**What just happened?** We created a Spark session that can read data from Amazon S3 (cloud storage) and process it using SQL commands.

### Step 2: Load Banking Data from S3

```python
def load_banking_datasets():
    """
    Load all 6 banking datasets from S3 storage
    """
    base_path = "banking_dataset/datasets"
    # These are the locations of our banking data files
    data_locations = {
        "customers": f"{base_path}/customers.csv",
        "accounts": f"{base_path}/accounts.csv", 
        "transactions": f"{base_path}/transactions.parquet",  # Your transactions are in parquet
        "branches": f"{base_path}/branches.json",  # Your branches are in JSON
        "products": f"{base_path}/products.csv",  # Assuming you have this
      	"loans": f"{base_path}/loans.csv",
      	"transactions_processed": f"{base_path}/processed/transactions/"
    }
    
    datasets = {}
    
    for table_name, file_location in data_locations.items():
        print(f"📖 Loading {table_name} data...")
        
        # Handle different file formats appropriately
        if table_name == "transactions" and file_location.endswith(".parquet"):
            # Read parquet file directly
            df = spark.read.parquet(file_location)
        elif table_name == "branches" and file_location.endswith(".json"):
            # Read JSON file
            df = spark.read \
                .option("multiline", "true") \
                .json(file_location)
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
    
    return datasets
  
# Load all our banking data
print("📊 Loading banking datasets...")
banking_data = load_banking_datasets()

# Extract each dataset into separate variables for easy use
customers_df = banking_data['customers']
accounts_df = banking_data['accounts']
transactions_df = banking_data['transactions']
loans_df = banking_data['loans']
products_df = banking_data['products']
branches_df = banking_data['branches']

print("\n🏦 All banking data loaded successfully!")
```

### Step 3: Understand Your Data

Let's look at what data we have:

```python
# Look at the structure of our customer data
print("👥 CUSTOMER DATA STRUCTURE:")
customers_df.printSchema()
customers_df.show(3)

print("\n💰 ACCOUNT DATA STRUCTURE:")
accounts_df.printSchema()
accounts_df.show(3)

print("\n💳 TRANSACTION DATA STRUCTURE:")
transactions_df.printSchema()
transactions_df.show(3)
```

**What you're seeing:**

- **Customers:** Personal info, income, credit scores, segments (Premium, Standard, Basic)
- **Accounts:** Bank accounts linked to customers with balances and types
- **Transactions:** All the money movements (deposits, withdrawals, purchases)
- **Loans:** Credit products like mortgages and personal loans
- **Products:** The bank's product catalog (checking, savings, credit cards)
- **Branches:** Physical bank locations with performance data

------

## Lesson 2: Create SQL Views and Basic Queries

### What are SQL Views?

Think of SQL views as giving nicknames to your data tables. Instead of referring to complex DataFrame objects, you can use simple names like "customers" or "accounts" in SQL queries.

### Step 1: Create SQL Views

```python
def create_banking_sql_tables():
    """
    Convert our DataFrames into SQL tables we can query
    """
    # Create temporary views (they only exist while Spark is running)
    customers_df.createOrReplaceTempView("customers")
    accounts_df.createOrReplaceTempView("accounts")
    transactions_df.createOrReplaceTempView("transactions")
    loans_df.createOrReplaceTempView("loans")
    products_df.createOrReplaceTempView("products")
    branches_df.createOrReplaceTempView("branches")
    
    print("✅ Created SQL views for all banking tables")
    
    # Show which tables are available
    spark.sql("SHOW TABLES").show()

create_banking_sql_tables()
```

### Step 2: Your First SQL Queries

Let's start with simple questions:

```python
# How many customers do we have?
print("📊 Total Customers:")
spark.sql("SELECT COUNT(*) as total_customers FROM customers").show()

# What customer segments do we serve?
print("📊 Customer Segments:")
spark.sql("""
    SELECT 
        customer_segment,
        COUNT(*) as customer_count
    FROM customers
    GROUP BY customer_segment
    ORDER BY customer_count DESC
""").show()
```

**Understanding the SQL:**

- `SELECT` chooses which columns to show
- `COUNT(*)` counts all rows
- `GROUP BY` groups similar data together
- `ORDER BY` sorts the results

### Step 3: Customer Analysis

```python
# Average income by customer segment
print("💰 Income Analysis by Segment:")
spark.sql("""
    SELECT 
        customer_segment,
        COUNT(*) as customers,
        ROUND(AVG(annual_income), 0) as avg_income,
        ROUND(AVG(risk_score), 0) as avg_credit_score
    FROM customers
    GROUP BY customer_segment
    ORDER BY avg_income DESC
""").show()

# Find high-value customers
print("🌟 High-Value Customers:")
spark.sql("""
    SELECT 
        CONCAT(first_name, ' ', last_name) as full_name,
        annual_income,
        risk_score,
        customer_segment
    FROM customers
    WHERE annual_income > 100000 AND risk_score > 700
    ORDER BY annual_income DESC
    LIMIT 10
""").show()
```

**New SQL concepts:**

- `ROUND()` removes decimal places for cleaner numbers
- `CONCAT()` combines text columns
- `WHERE` filters data based on conditions
- `AND` combines multiple conditions
- `LIMIT` controls how many results to show

### Step 4: Account Portfolio Analysis

```python
# What types of accounts do we have?
print("🏦 Account Types Overview:")
spark.sql("""
    SELECT 
        account_type,
        COUNT(*) as total_accounts,
        COUNT(CASE WHEN account_status = 'Active' THEN 1 END) as active_accounts,
        ROUND(AVG(balance), 2) as avg_balance
    FROM accounts
    GROUP BY account_type
    ORDER BY total_accounts DESC
""").show()

# Which accounts have the most money?
print("💎 Highest Value Accounts:")
spark.sql("""
    SELECT 
        account_type,
        ROUND(SUM(balance), 2) as total_balance,
        COUNT(*) as account_count
    FROM accounts
    WHERE account_status = 'Active' AND balance > 0
    GROUP BY account_type
    ORDER BY total_balance DESC
""").show()
```

**New SQL concepts:**

- `CASE WHEN` for conditional counting
- `SUM()` adds up numbers
- Multiple conditions in `WHERE` using `AND`

------

## Lesson 3: Single CTEs - Focused Analysis

### What is a CTE?

CTE stands for "Common Table Expression." Think of it as creating a temporary helper table that makes complex queries easier to read and write.

**Before CTE (confusing):**

```sql
SELECT customer_id, (lots of complex calculations mixed together)
```

**With CTE (clear):**

```sql
WITH helper_table AS (
    SELECT customer_id, (do calculations step by step)
)
SELECT customer_id, (use the clean results)
```

### Step 1: Customer Financial Health Assessment

```python
print("💊 Customer Financial Health Assessment")
customer_health = spark.sql("""
    WITH customer_financial_summary AS (
        -- Step 1: Gather all financial info for each customer
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.customer_segment,
            c.annual_income,
            c.risk_score,
            COUNT(a.account_id) as total_accounts,
            SUM(CASE WHEN a.balance > 0 THEN a.balance ELSE 0 END) as total_deposits,
            SUM(CASE WHEN a.balance < 0 THEN ABS(a.balance) ELSE 0 END) as total_debt,
            SUM(a.balance) as net_worth
        FROM customers c
        LEFT JOIN accounts a ON c.customer_id = a.customer_id
        WHERE a.account_status = 'Active'
        GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment, 
                 c.annual_income, c.risk_score
    )
    
    -- Step 2: Use the summary to create health ratings
    SELECT 
        customer_id,
        CONCAT(first_name, ' ', last_name) as full_name,
        customer_segment,
        annual_income,
        risk_score,
        total_accounts,
        ROUND(total_deposits, 2) as deposits,
        ROUND(total_debt, 2) as debt,
        ROUND(net_worth, 2) as net_worth,
        CASE 
            WHEN net_worth > 100000 AND risk_score > 750 THEN 'Excellent'
            WHEN net_worth > 50000 AND risk_score > 700 THEN 'Very Good'
            WHEN net_worth > 10000 AND risk_score > 650 THEN 'Good'
            WHEN net_worth > 0 AND risk_score > 600 THEN 'Fair'
            ELSE 'Needs Attention'
        END as financial_health_rating
    FROM customer_financial_summary
    ORDER BY net_worth DESC
    LIMIT 20
""")

customer_health.show(truncate=False)
```

**What happened here:**

1. The CTE (`customer_financial_summary`) calculated each customer's financial totals
2. The main query used those results to assign health ratings
3. We broke a complex calculation into manageable steps

**New concepts:**

- `WITH` creates a temporary table
- `LEFT JOIN` connects customers to their accounts (even if they have no accounts)
- `ABS()` makes negative numbers positive
- Complex `CASE WHEN` for business logic

### Step 2: Product Performance Analysis

```python
print("📊 Product Performance Analysis")
product_performance = spark.sql("""
    WITH product_metrics AS (
        -- Calculate metrics for each product
        SELECT 
            p.product_id,
            p.product_name,
            p.product_type,
            p.monthly_fee,
            COUNT(a.account_id) as active_accounts,
            ROUND(AVG(a.balance), 2) as avg_balance,
            ROUND(SUM(a.balance), 2) as total_balance,
            -- Estimate monthly revenue from fees and balances
            ROUND((COUNT(a.account_id) * p.monthly_fee), 2) as monthly_fee_revenue
        FROM products p
        LEFT JOIN accounts a ON p.product_type = a.account_type
            AND a.account_status = 'Active'
        GROUP BY p.product_id, p.product_name, p.product_type, p.monthly_fee
    )
    
    SELECT 
        product_name,
        product_type,
        active_accounts,
        avg_balance,
        total_balance,
        monthly_fee_revenue,
        CASE 
            WHEN active_accounts > 2000 THEN 'High Volume'
            WHEN active_accounts > 1000 THEN 'Medium Volume'
            ELSE 'Low Volume'
        END as volume_category,
        ROUND(monthly_fee_revenue / NULLIF(active_accounts, 0), 2) as revenue_per_account
    FROM product_metrics
    ORDER BY monthly_fee_revenue DESC
""")

product_performance.show(truncate=False)
```

**New concepts:**

- `NULLIF()` prevents division by zero errors
- Revenue calculations using business logic
- Product categorization based on volume

------

## Lesson 4: Multiple CTEs - Complex Banking Analytics

### Why Multiple CTEs?

Real banking analysis often requires combining different types of data. Multiple CTEs let us build complex reports step by step, like cooking a meal where each CTE is a different ingredient preparation.

### Step 1: Comprehensive Customer Risk Assessment

```python
print("⚠️ Comprehensive Customer Risk Assessment")
risk_assessment = spark.sql("""
    -- CTE 1: Customer basic info
    WITH customer_demographics AS (
        SELECT 
            customer_id,
            CONCAT(first_name, ' ', last_name) as full_name,
            customer_segment,
            annual_income,
            risk_score,
            employment_status
        FROM customers
    ),
    
    -- CTE 2: Account portfolio health
    account_health AS (
        SELECT 
            customer_id,
            COUNT(*) as total_accounts,
            SUM(CASE WHEN balance > 0 THEN balance ELSE 0 END) as liquid_assets,
            SUM(CASE WHEN balance < 0 THEN ABS(balance) ELSE 0 END) as account_debt,
            AVG(balance) as avg_account_balance
        FROM accounts
        WHERE account_status = 'Active'
        GROUP BY customer_id
    ),
    
    -- CTE 3: Loan portfolio risk
    loan_risk AS (
        SELECT 
            customer_id,
            COUNT(*) as total_loans,
            SUM(current_balance) as total_loan_debt,
            AVG(payment_history_score) as avg_payment_score,
            COUNT(CASE WHEN loan_status IN ('Late', 'Default') THEN 1 END) as problem_loans
        FROM loans
        GROUP BY customer_id
    ),
    
    -- CTE 4: Recent transaction behavior (last 30 days)
    transaction_behavior AS (
        SELECT 
            customer_id,
            COUNT(*) as recent_transactions,
            COUNT(CASE WHEN is_fraud = true THEN 1 END) as fraud_transactions,
            SUM(amount) as total_transaction_volume
        FROM transactions
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY customer_id
    )
    
    -- Final analysis combining all CTEs
    SELECT 
        cd.full_name,
        cd.customer_segment,
        cd.annual_income,
        cd.risk_score,
        COALESCE(ah.liquid_assets, 0) as assets,
        COALESCE(lr.total_loan_debt, 0) as loan_debt,
        COALESCE(lr.problem_loans, 0) as problem_loans,
        COALESCE(tb.fraud_transactions, 0) as fraud_incidents,
        COALESCE(tb.recent_transactions, 0) as activity_level,
        -- Calculate overall risk score
        CASE 
            WHEN cd.risk_score >= 750 AND COALESCE(lr.problem_loans, 0) = 0 
                 AND COALESCE(tb.fraud_transactions, 0) = 0 THEN 'Low Risk'
            WHEN cd.risk_score >= 650 AND COALESCE(lr.problem_loans, 0) <= 1 THEN 'Medium Risk'
            ELSE 'High Risk'
        END as overall_risk_category
    FROM customer_demographics cd
    LEFT JOIN account_health ah ON cd.customer_id = ah.customer_id
    LEFT JOIN loan_risk lr ON cd.customer_id = lr.customer_id
    LEFT JOIN transaction_behavior tb ON cd.customer_id = tb.customer_id
    WHERE COALESCE(ah.liquid_assets, 0) > 0 OR COALESCE(lr.total_loan_debt, 0) > 0
    ORDER BY cd.risk_score DESC, assets DESC
    LIMIT 25
""")

risk_assessment.show(truncate=False)
```

**What happened:**

1. **CTE 1:** Got basic customer info
2. **CTE 2:** Calculated account portfolio metrics
3. **CTE 3:** Analyzed loan risk factors
4. **CTE 4:** Checked recent transaction behavior
5. **Final query:** Combined everything with business rules

**New concepts:**

- `COALESCE()` handles missing data (replaces NULL with 0)
- `DATE_SUB()` and `CURRENT_DATE()` for date calculations
- Multiple `LEFT JOIN` operations
- Complex risk scoring logic

### Step 2: Branch Performance Analysis

```python
print("🏢 Branch Network Performance Analysis")
branch_performance = spark.sql("""
    -- CTE 1: Customer counts per city (matching with branch cities)
    WITH city_customers AS (
        SELECT 
            city,
            COUNT(DISTINCT customer_id) as total_customers,
            AVG(annual_income) as avg_customer_income,
            COUNT(CASE WHEN customer_segment = 'Premium' THEN 1 END) as premium_customers
        FROM customers
        GROUP BY city
    ),
    
    -- CTE 2: Account portfolio per city
    city_accounts AS (
        SELECT 
            c.city,
            COUNT(*) as total_accounts,
            SUM(a.balance) as total_deposits,
            AVG(a.balance) as avg_account_balance
        FROM accounts a
        JOIN customers c ON a.customer_id = c.customer_id
        WHERE a.account_status = 'Active'
        GROUP BY c.city
    ),
    
    -- CTE 3: Loan portfolio per city
    city_loans AS (
        SELECT 
            c.city,
            COUNT(*) as total_loans,
            SUM(l.current_balance) as total_loan_balance,
            COUNT(CASE WHEN l.loan_status IN ('Late', 'Default') THEN 1 END) as problem_loans
        FROM loans l
        JOIN customers c ON l.customer_id = c.customer_id
        GROUP BY c.city
    )
    
    -- Combine all metrics
    SELECT 
        b.branch_name,
        b.city,
        b.region,
        COALESCE(cc.total_customers, 0) as total_customers,
        COALESCE(cc.premium_customers, 0) as premium_customers,
        COALESCE(ca.total_accounts, 0) as total_accounts,
        ROUND(COALESCE(ca.total_deposits, 0), 2) as deposits,
        ROUND(COALESCE(cl.total_loan_balance, 0), 2) as loans,
        COALESCE(cl.problem_loans, 0) as problem_loans,
        b.employee_count,
        -- Performance metrics
        ROUND(COALESCE(cc.total_customers, 0) / NULLIF(b.employee_count, 0), 1) as customers_per_employee,
        ROUND(COALESCE(ca.total_deposits, 0) / NULLIF(COALESCE(cc.total_customers, 1), 0), 2) as deposits_per_customer,
        CASE 
            WHEN COALESCE(ca.total_deposits, 0) > 30000000 THEN 'Top Performer'
            WHEN COALESCE(ca.total_deposits, 0) > 15000000 THEN 'Strong Performer'
            ELSE 'Developing'
        END as performance_tier
    FROM branches b
    LEFT JOIN city_customers cc ON b.city = cc.city
    LEFT JOIN city_accounts ca ON b.city = ca.city
    LEFT JOIN city_loans cl ON b.city = cl.city
    ORDER BY deposits DESC
    LIMIT 20
""")

branch_performance.show(truncate=False)
```

------

## Lesson 5: Advanced Analytics Patterns 

### Customer Lifecycle Analysis

Understanding how customers evolve over time helps with retention and growth strategies:

```python
print("🛤️ Customer Journey Analysis")
customer_lifecycle = spark.sql("""
    -- CTE 1: Customer timeline info
    WITH customer_timeline AS (
        SELECT 
            customer_id,
            CONCAT(first_name, ' ', last_name) as full_name,
            customer_segment,
            annual_income,
            risk_score,
            account_open_date,
            DATEDIFF(CURRENT_DATE(), account_open_date) as days_as_customer,
            FLOOR(DATEDIFF(CURRENT_DATE(), account_open_date) / 365.25) as years_as_customer
        FROM customers
    ),
    
    -- CTE 2: Account growth over time
    account_evolution AS (
        SELECT 
            customer_id,
            COUNT(*) as total_accounts_opened,
            MIN(open_date) as first_account_date,
            MAX(open_date) as latest_account_date,
            SUM(CASE WHEN account_status = 'Active' THEN 1 ELSE 0 END) as active_accounts,
            SUM(balance) as current_net_worth
        FROM accounts
        GROUP BY customer_id
    ),
    
    -- CTE 3: Transaction patterns
    transaction_patterns AS (
        SELECT 
            customer_id,
            COUNT(*) as lifetime_transactions,
            COUNT(DISTINCT channel) as channels_used,
            SUM(amount) as lifetime_volume
        FROM transactions
        GROUP BY customer_id
    )
    
    -- Final analysis
    SELECT 
        ct.full_name,
        ct.customer_segment,
        ct.years_as_customer,
        ae.total_accounts_opened,
        ae.active_accounts,
        ROUND(ae.current_net_worth, 2) as net_worth,
        tp.channels_used as digital_engagement,
        -- Lifecycle stage
        CASE 
            WHEN ct.years_as_customer < 0.5 THEN 'New Customer'
            WHEN ct.years_as_customer < 2 THEN 'Growing Relationship'
            WHEN ct.years_as_customer < 5 THEN 'Mature Customer'
            ELSE 'Long-term Loyalty'
        END as lifecycle_stage,
        -- Value tier
        ROW_NUMBER() OVER (
            PARTITION BY ct.customer_segment 
            ORDER BY ae.current_net_worth DESC
        ) as value_rank_in_segment
    FROM customer_timeline ct
    LEFT JOIN account_evolution ae ON ct.customer_id = ae.customer_id
    LEFT JOIN transaction_patterns tp ON ct.customer_id = tp.customer_id
    WHERE ae.current_net_worth IS NOT NULL
    ORDER BY ae.current_net_worth DESC
    LIMIT 30
""")

customer_lifecycle.show(truncate=False)
```

**Advanced concepts:**

- `DATEDIFF()` for calculating time differences
- `FLOOR()` for rounding down years
- `ROW_NUMBER() OVER()` for ranking within groups
- `PARTITION BY` for group-based calculations

------

## Lesson 6: Executive Dashboard Creation 

### Monthly Banking KPI Dashboard

```python
print("📈 Executive Dashboard - Monthly Banking KPIs")
executive_dashboard = spark.sql("""
    -- CTE 1: Portfolio overview
    WITH portfolio_metrics AS (
        SELECT 
            COUNT(DISTINCT c.customer_id) as total_customers,
            COUNT(DISTINCT a.account_id) as total_accounts,
            ROUND(SUM(CASE WHEN a.balance > 0 THEN a.balance ELSE 0 END), 2) as total_deposits,
            ROUND(SUM(l.current_balance), 2) as total_loans
        FROM customers c
        LEFT JOIN accounts a ON c.customer_id = a.customer_id AND a.account_status = 'Active'
        LEFT JOIN loans l ON c.customer_id = l.customer_id AND l.loan_status = 'Active'
    ),
    
    -- CTE 2: Transaction activity (last 30 days)
    transaction_metrics AS (
        SELECT 
            COUNT(*) as monthly_transactions,
            ROUND(SUM(amount), 2) as monthly_volume,
            COUNT(CASE WHEN is_fraud = true THEN 1 END) as fraud_transactions,
            ROUND(COUNT(CASE WHEN is_fraud = true THEN 1 END) * 100.0 / COUNT(*), 2) as fraud_rate
        FROM transactions
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    ),
    
    -- CTE 3: Growth metrics
    growth_metrics AS (
        SELECT 
            COUNT(CASE WHEN account_open_date >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) as new_customers_30d,
            COUNT(CASE WHEN customer_segment IN ('Premium', 'VIP') THEN 1 END) as high_value_customers,
            ROUND(AVG(annual_income), 0) as avg_customer_income
        FROM customers
    ),
    
    -- CTE 4: Risk metrics
    risk_metrics AS (
        SELECT 
            COUNT(CASE WHEN loan_status IN ('Late', 'Default') THEN 1 END) as problem_loans,
            COUNT(*) as total_loans,
            ROUND(COUNT(CASE WHEN loan_status IN ('Late', 'Default') THEN 1 END) * 100.0 / COUNT(*), 2) as problem_loan_rate
        FROM loans
    )
    
    -- Dashboard summary
    SELECT 
        'Customer Portfolio' as metric_category,
        CAST(pm.total_customers AS STRING) as total_customers,
        CAST(pm.total_accounts AS STRING) as total_accounts,
        CAST(pm.total_deposits AS STRING) as total_deposits,
        CAST(pm.total_loans AS STRING) as total_loans
    FROM portfolio_metrics pm
    
    UNION ALL
    
    SELECT 
        'Monthly Activity' as metric_category,
        CAST(tm.monthly_transactions AS STRING) as monthly_transactions,
        CAST(tm.monthly_volume AS STRING) as monthly_volume,
        CAST(tm.fraud_transactions AS STRING) as fraud_incidents,
        CONCAT(tm.fraud_rate, '%') as fraud_rate
    FROM transaction_metrics tm
    
    UNION ALL
    
    SELECT 
        'Growth & Quality' as metric_category,
        CAST(gm.new_customers_30d AS STRING) as new_customers,
        CAST(gm.high_value_customers AS STRING) as premium_customers,
        CAST(gm.avg_customer_income AS STRING) as avg_income,
        'N/A' as placeholder
    FROM growth_metrics gm
    
    UNION ALL
    
    SELECT 
        'Risk Management' as metric_category,
        CAST(rm.problem_loans AS STRING) as problem_loans,
        CAST(rm.total_loans AS STRING) as total_portfolio,
        CONCAT(rm.problem_loan_rate, '%') as problem_rate,
        'N/A' as placeholder
    FROM risk_metrics rm
""")

executive_dashboard.show(truncate=False)
```

------

## Practice Exercises and Challenges

### Exercise 1: Customer Segmentation Analysis 

**Challenge:** Create a CTE that identifies customers who should be upgraded to Premium segment based on their financial behavior.

```python
# Your solution here
upgrade_candidates = spark.sql("""
    
""")

upgrade_candidates.show()
```

### Exercise 2: Product Cross-Selling Opportunities 

**Challenge:** Find customers who have checking accounts but no savings accounts - they might be good targets for savings product marketing.

```python
# Your solution here
cross_sell_opportunities = spark.sql("""
    
""")

cross_sell_opportunities.show()
```

------

## Data Quality Validation with CTEs

### Comprehensive Data Quality Check

```python
print("🔍 Banking Data Quality Assessment")
data_quality_report = spark.sql("""
    -- Check customers data quality
    WITH customer_quality AS (
        SELECT 
            'customers' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN first_name IS NULL OR TRIM(first_name) = '' THEN 1 END) as missing_names,
            COUNT(CASE WHEN email NOT LIKE '%@%.%' THEN 1 END) as invalid_emails,
            COUNT(CASE WHEN annual_income <= 0 THEN 1 END) as invalid_income,
            COUNT(CASE WHEN risk_score < 300 OR risk_score > 850 THEN 1 END) as invalid_credit_scores
        FROM customers
    ),
    
    -- Check accounts data quality
    account_quality AS (
        SELECT 
            'accounts' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN customer_id NOT IN (SELECT customer_id FROM customers) THEN 1 END) as orphaned_accounts,
            COUNT(CASE WHEN account_status = 'Closed' AND balance != 0 THEN 1 END) as closed_with_balance,
            COUNT(CASE WHEN balance IS NULL THEN 1 END) as missing_balances,
            COUNT(CASE WHEN open_date > CURRENT_DATE() THEN 1 END) as future_dates
        FROM accounts
    ),
    
    -- Check transaction data quality
    transaction_quality AS (
        SELECT 
            'transactions' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN amount = 0 THEN 1 END) as zero_amount_transactions,
            COUNT(CASE WHEN customer_id NOT IN (SELECT customer_id FROM customers) THEN 1 END) as orphaned_transactions,
            COUNT(CASE WHEN transaction_date > CURRENT_DATE() THEN 1 END) as future_transactions,
            COUNT(CASE WHEN amount > 1000000 THEN 1 END) as suspicious_large_amounts
        FROM transactions
    )
    
    -- Combine all quality checks
    SELECT table_name, 'Total Records' as quality_check, CAST(total_records AS STRING) as issue_count
    FROM customer_quality
    UNION ALL
    SELECT table_name, 'Missing Names' as quality_check, CAST(missing_names AS STRING) as issue_count
    FROM customer_quality
    UNION ALL
    SELECT table_name, 'Invalid Emails' as quality_check, CAST(invalid_emails AS STRING) as issue_count
    FROM customer_quality
    UNION ALL
    SELECT table_name, 'Invalid Income' as quality_check, CAST(invalid_income AS STRING) as issue_count
    FROM customer_quality
    UNION ALL
    SELECT table_name, 'Orphaned Accounts' as quality_check, CAST(orphaned_accounts AS STRING) as issue_count
    FROM account_quality
    UNION ALL
    SELECT table_name, 'Zero Amount Transactions' as quality_check, CAST(zero_amount_transactions AS STRING) as issue_count
    FROM transaction_quality
    ORDER BY table_name, quality_check
""")

data_quality_report.show(20, truncate=False)
```

------

## Performance Tips and Best Practices

### 1. Optimizing CTE Performance

```python
# Cache frequently used CTEs for better performance
def optimize_banking_queries():
    """
    Performance optimization tips for banking CTEs
    """
    
    # Cache large datasets that are used multiple times
    customers_df.cache()
    transactions_df.cache()
    
    # Use proper partitioning for large queries
    optimized_customer_analysis = spark.sql("""
        WITH customer_summary AS (
            SELECT /*+ BROADCAST(p) */
                c.customer_id,
                c.customer_segment,
                p.product_type,
                COUNT(a.account_id) as accounts
            FROM customers c
            JOIN accounts a ON c.customer_id = a.customer_id
            JOIN products p ON a.product_id = p.product_id
            GROUP BY c.customer_id, c.customer_segment, p.product_type
        )
        SELECT * FROM customer_summary
        ORDER BY accounts DESC
    """)
    
    return optimized_customer_analysis

# Performance monitoring
def monitor_query_performance(query_name, sql_query):
    """
    Monitor how long queries take to run
    """
    import time
    
    print(f"⏱️ Running: {query_name}")
    start_time = time.time()
    
    result = spark.sql(sql_query)
    row_count = result.count()
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"✅ {query_name}: {row_count:,} rows in {execution_time:.2f} seconds")
    return result
```

### 2. Memory Management

```python
def cleanup_banking_views():
    """
    Clean up temporary views to free memory
    """
    view_names = ["customers", "accounts", "transactions", "loans", "products", "branches"]
    
    for view_name in view_names:
        try:
            spark.catalog.dropTempView(view_name)
            print(f"✅ Cleaned up {view_name} view")
        except:
            print(f"⚠️ View {view_name} was already cleaned")
    
    # Clear cache
    spark.catalog.clearCache()
    print("🧹 All temporary views and cache cleared")

# Call this when you're done with your analysis
# cleanup_banking_views()
```

------

## Real-World Banking Use Cases

### 1. Regulatory Compliance Report

```python
print("📋 Anti-Money Laundering (AML) Compliance Report")
aml_report = spark.sql("""
    -- Identify customers requiring enhanced monitoring
    WITH high_risk_patterns AS (
        SELECT 
            c.customer_id,
            CONCAT(c.first_name, ' ', c.last_name) as customer_name,
            c.annual_income,
            SUM(a.balance) as total_deposits,
            COUNT(t.transaction_id) as transaction_count_30d,
            SUM(CASE WHEN t.amount > 10000 THEN t.amount ELSE 0 END) as large_transaction_total,
            COUNT(CASE WHEN t.is_international = true THEN 1 END) as international_transactions
        FROM customers c
        LEFT JOIN accounts a ON c.customer_id = a.customer_id AND a.account_status = 'Active'
        LEFT JOIN transactions t ON c.customer_id = t.customer_id 
            AND t.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY c.customer_id, c.first_name, c.last_name, c.annual_income
    ),
    
    risk_assessment AS (
        SELECT 
            *,
            CASE 
                WHEN total_deposits > annual_income * 2 THEN 'Income Mismatch'
                WHEN large_transaction_total > 50000 THEN 'Large Transaction Pattern'
                WHEN international_transactions > 10 THEN 'High International Activity'
                ELSE 'Standard Risk'
            END as risk_flag
        FROM high_risk_patterns
        WHERE total_deposits > 0
    )
    
    SELECT 
        risk_flag,
        COUNT(*) as customer_count,
        ROUND(AVG(total_deposits), 2) as avg_deposits,
        ROUND(SUM(large_transaction_total), 2) as total_large_transactions
    FROM risk_assessment
    GROUP BY risk_flag
    ORDER BY customer_count DESC
""")

aml_report.show()
```

### 2. Monthly Executive Summary

```python
print("📊 Monthly Executive Summary")
executive_summary = spark.sql("""
    WITH monthly_kpis AS (
        -- Customer metrics
        SELECT 
            'Customer Growth' as metric_type,
            COUNT(CASE WHEN account_open_date >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) as current_month,
            COUNT(CASE WHEN account_open_date >= DATE_SUB(CURRENT_DATE(), 60) 
                       AND account_open_date < DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) as previous_month
        FROM customers
        
        UNION ALL
        
        -- Transaction metrics
        SELECT 
            'Transaction Volume' as metric_type,
            COUNT(CASE WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) as current_month,
            COUNT(CASE WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), 60) 
                       AND transaction_date < DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) as previous_month
        FROM transactions
        
        UNION ALL
        
        -- Fraud metrics
        SELECT 
            'Fraud Incidents' as metric_type,
            COUNT(CASE WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), 30) AND is_fraud = true THEN 1 END) as current_month,
            COUNT(CASE WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), 60) 
                       AND transaction_date < DATE_SUB(CURRENT_DATE(), 30) AND is_fraud = true THEN 1 END) as previous_month
        FROM transactions
    )
    
    SELECT 
        metric_type,
        current_month,
        previous_month,
        ROUND(((current_month - previous_month) * 100.0 / NULLIF(previous_month, 0)), 2) as month_over_month_change_pct,
        CASE 
            WHEN current_month > previous_month THEN '📈 Increasing'
            WHEN current_month < previous_month THEN '📉 Decreasing'
            ELSE '➡️ Stable'
        END as trend
    FROM monthly_kpis
""")

executive_summary.show(truncate=False)
```

------

## Summary and Key Takeaways

### What You've Accomplished

By completing this tutorial, you now know how to:

1. **Load banking data from S3** and create SQL views for analysis
2. **Write basic SQL queries** to answer business questions
3. **Use single CTEs** to break complex calculations into manageable steps
4. **Combine multiple CTEs** to create comprehensive analytical reports
5. **Build real-world banking applications** like risk assessment and compliance reporting
6. **Optimize performance** and manage memory for production systems

### Key SQL Concepts Mastered

- **CTEs (Common Table Expressions)** - Breaking complex queries into readable steps
- **JOINs** - Connecting related data across multiple tables
- **Window Functions** - Advanced ranking and analytical calculations
- **Conditional Logic** - Using CASE WHEN for business rules
- **Date Functions** - Time-based analysis and calculations
- **Data Quality** - Identifying and handling data issues

### Real-World Applications

The skills you've learned apply directly to:

- **Risk Management** - Customer risk scoring and portfolio analysis
- **Regulatory Compliance** - AML reporting and audit trail creation
- **Business Intelligence** - Executive dashboards and KPI monitoring
- **Customer Analytics** - Lifecycle analysis and product recommendations
- **Operational Reporting** - Branch performance and efficiency metrics

### Next Steps

1. **Practice with your own data** - Apply these patterns to other datasets
2. **Learn advanced Spark features** - Streaming, machine learning integration
3. **Explore visualization tools** - Connect your SQL results to Tableau, Power BI
4. **Study data engineering** - ETL pipelines, data governance, and architecture
5. **Understand banking domain** - Deepen knowledge of financial services

### Performance Reminders

- Always **cache frequently used data** in memory
- Use **broadcast joins** for small lookup tables
- **Partition large datasets** by commonly filtered columns
- **Monitor query performance** and optimize as needed
- **Clean up resources** when analysis is complete

This tutorial provided hands-on experience with real banking scenarios using production-scale data patterns. The CTEs and SQL techniques you've learned form the foundation for advanced data analytics in financial services and other data-intensive industries.