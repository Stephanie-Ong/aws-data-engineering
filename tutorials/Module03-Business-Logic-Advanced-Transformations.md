# Module 3 - Business Logic & Advanced Transformations

**Learning Time:** 90-120 minutes
**Prerequisites:** Module 1 & 2 completion (DataFrames and Data Cleaning)
**Learning Objectives:** Master complex business logic and advanced analytical transformations for banking

------

## 📋 Module 3 Functions Reference Table

| **Section**              | **Function**             | **Purpose**               | **Syntax Example**                                           |
| ------------------------ | ------------------------ | ------------------------- | ------------------------------------------------------------ |
| **Complex Conditionals** | `when()`                 | Multi-condition logic     | `when(condition1, value1).when(condition2, value2).otherwise(default)` |
| **Complex Conditionals** | `expr()`                 | SQL-style CASE statements | `expr("CASE WHEN score > 700 THEN 'Good' ELSE 'Poor' END")`  |
| **Complex Conditionals** | Nested `when()`          | Hierarchical conditions   | `when(cond1, when(cond2, val).otherwise(val2)).otherwise(val3)` |
| **Aggregations**         | `groupBy().agg()`        | Multiple metrics          | `groupBy("segment").agg(sum("balance"), avg("income"))`      |
| **Aggregations**         | `collect_list()`         | Collect values            | `collect_list("transaction_type")`                           |
| **Aggregations**         | `collect_set()`          | Unique values             | `collect_set("product_type")`                                |
| **Aggregations**         | `approx_quantile()`      | Percentiles               | `df.approxQuantile("balance", [0.25, 0.5, 0.75], 0.01)`      |
| **Window Functions**     | `Window.partitionBy()`   | Define partitions         | `Window.partitionBy("customer_id").orderBy("date")`          |
| **Window Functions**     | `row_number()`           | Sequential numbering      | `row_number().over(window_spec)`                             |
| **Window Functions**     | `rank()`, `dense_rank()` | Ranking with ties         | `rank().over(Window.orderBy(desc("amount")))`                |
| **Window Functions**     | `lag()`, `lead()`        | Previous/next values      | `lag("balance", 1).over(window_spec)`                        |
| **Window Functions**     | `sum().over()`           | Running totals            | `sum("amount").over(window_spec)`                            |
| **Calculated Fields**    | Mathematical ops         | Financial calculations    | `col("principal") * col("rate") / 12`                        |
| **Calculated Fields**    | `months_between()`       | Date calculations         | `months_between(current_date(), col("open_date"))`           |
| **Calculated Fields**    | `datediff()`             | Day differences           | `datediff(current_date(), col("last_login"))`                |
| **Decision Trees**       | Nested `when()`          | Complex decision logic    | Multi-level conditional structures                           |
| **UDFs**                 | `udf()`                  | Custom functions          | `@udf(returnType=StringType())`                              |

------

## 🎯 Module Overview

In banking, raw data becomes actionable intelligence through **sophisticated business logic**. This module teaches you to implement the complex decision-making processes that banks use daily:

- **Credit Risk Assessment:** Multi-factor scoring models
- **Customer Segmentation:** Dynamic classification based on behavior
- **Portfolio Analysis:** Time-based performance tracking
- **Product Recommendations:** Automated decision engines
- **Regulatory Compliance:** Rule-based monitoring and reporting

**Real Banking Applications:**

- Loan approval algorithms with multiple criteria
- Customer lifetime value calculations
- Risk-adjusted pricing models
- Cross-selling recommendation engines
- Fraud detection scoring systems

**Why Advanced Transformations Matter:**

- **Automation:** Replace manual decision-making with consistent rules
- **Scalability:** Process millions of customers with uniform logic
- **Compliance:** Ensure consistent application of regulatory requirements
- **Efficiency:** Reduce processing time from hours to minutes

------

## 1. Complex Conditional Logic - Multi-Layered Business Rules

### 1.1 Advanced `when()` Structures for Banking Decisions

**What are Complex Conditional Logic?**

Banking decisions rarely depend on single factors. Credit approvals, pricing decisions, and risk assessments require evaluating multiple conditions simultaneously. Complex conditional logic allows you to implement sophisticated business rules that mirror real-world banking processes.

**Real Banking Scenarios Requiring Complex Logic:**

- **Credit Approval:** Income, credit score, debt-to-income ratio, employment history
- **Interest Rate Pricing:** Risk profile, relationship depth, market conditions, competition
- **Account Fees:** Balance tiers, transaction volume, customer segment, tenure
- **Product Eligibility:** Age, income, credit history, existing relationships

```python
# script_14.py
# Create comprehensive customer dataset for complex logic examples
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, greatest, expr
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StringCleaningDemo") \
    .getOrCreate()

print("✅ Spark session initialized")
complex_customer_data = [
    ("CUST001", 125000, 750, 3, "Employed", "Premium", 85000, 25, "Active", 4.2),
    ("CUST002", 75000, 680, 1, "Self-Employed", "Standard", 45000, 15, "Active", 3.8),
    ("CUST003", 200000, 800, 8, "Employed", "VIP", 150000, 45, "Active", 4.9),
    ("CUST004", 45000, 620, 0, "Unemployed", "Basic", 5000, 8, "Frozen", 2.1),
    ("CUST005", 95000, 720, 2, "Employed", "Standard", 65000, 18, "Active", 4.1),
    ("CUST006", 250000, 780, 12, "Executive", "VIP", 300000, 85, "Active", 4.8)
]

complex_df = spark.createDataFrame(
    complex_customer_data,
    ["customer_id", "annual_income", "credit_score", "years_with_bank", 
     "employment_status", "current_segment", "total_balance", "monthly_transactions", 
     "account_status", "relationship_rating"]
)

complex_df.show()
```

**Multi-Level Credit Risk Assessment:**

```python
# Complex credit risk assessment with multiple factors
credit_risk_assessment = complex_df.withColumn("credit_risk_tier",
    # Tier 1: Excellent (Multiple positive factors required)
    when(
        (col("credit_score") >= 750) & 
        (col("annual_income") >= 100000) & 
        (col("years_with_bank") >= 3) &
        (col("account_status") == "Active"),
        "Tier 1 - Excellent"
    )
    # Tier 2: Good (Good credit + stable income OR long relationship)
    .when(
        ((col("credit_score") >= 700) & (col("annual_income") >= 75000)) |
        ((col("credit_score") >= 680) & (col("years_with_bank") >= 5)),
        "Tier 2 - Good" 
    )
    # Tier 3: Fair (Moderate risk factors)
    .when(
        (col("credit_score") >= 650) & 
        (col("annual_income") >= 50000) &
        (col("employment_status").isin(["Employed", "Self-Employed"])),
        "Tier 3 - Fair"
    )
    # Tier 4: Poor (High risk indicators)
    .when(
        (col("credit_score") < 650) |
        (col("employment_status") == "Unemployed") |
        (col("account_status") != "Active"),
        "Tier 4 - Poor"
    )
    # Default: Requires manual review
    .otherwise("Manual Review Required")
)

print("Multi-Factor Credit Risk Assessment:")
credit_risk_assessment.select("customer_id", "credit_score", "annual_income", 
                             "years_with_bank", "employment_status", "credit_risk_tier").show()
```

**Dynamic Interest Rate Pricing Logic:**

```python
# Sophisticated interest rate calculation based on multiple factors
interest_rate_pricing = credit_risk_assessment.withColumn("base_rate", lit(5.5)) \
    .withColumn("risk_adjustment",
        # Risk-based pricing adjustments
        when(col("credit_risk_tier") == "Tier 1 - Excellent", -1.5)
        .when(col("credit_risk_tier") == "Tier 2 - Good", -0.75)
        .when(col("credit_risk_tier") == "Tier 3 - Fair", 0.0)
        .when(col("credit_risk_tier") == "Tier 4 - Poor", 2.0)
        .otherwise(3.0)  # Manual review cases get highest rate
    ) \
    .withColumn("relationship_discount",
        # Relationship-based discounts
        when(col("years_with_bank") >= 10, -0.5)
        .when(col("years_with_bank") >= 5, -0.25)
        .otherwise(0.0)
    ) \
    .withColumn("segment_adjustment", 
        # Segment-based pricing
        when(col("current_segment") == "VIP", -0.75)
        .when(col("current_segment") == "Premium", -0.25)
        .otherwise(0.0)
    ) \
    .withColumn("final_interest_rate",
        greatest(
            col("base_rate") + col("risk_adjustment") + 
            col("relationship_discount") + col("segment_adjustment"),
            lit(3.0)  # Floor rate of 3%
        )
    )

print("Dynamic Interest Rate Pricing:")
interest_rate_pricing.select("customer_id", "credit_risk_tier", "years_with_bank", 
                            "current_segment", "base_rate", "risk_adjustment", 
                            "relationship_discount", "final_interest_rate").show()
```

### 1.2 Nested Conditional Logic for Complex Business Rules

**When to Use Nested Conditions:**

Some business rules require hierarchical decision-making where the result of one condition determines which additional conditions to evaluate. This is common in banking for product eligibility, fee structures, and compliance requirements.

```python
# Complex product eligibility with nested conditions
product_eligibility = interest_rate_pricing.withColumn("mortgage_eligibility",
    # Primary eligibility check
    when(col("credit_score") >= 620,
        # If credit score qualifies, check additional factors
        when(
            (col("annual_income") >= 50000) & 
            (col("employment_status").isin(["Employed", "Self-Employed"])),
            # If income and employment qualify, determine loan amount
            when(col("annual_income") >= 150000, "Qualified - Jumbo Loan")
            .when(col("annual_income") >= 100000, "Qualified - High Loan Amount") 
            .when(col("annual_income") >= 75000, "Qualified - Standard Loan Amount")
            .otherwise("Qualified - Conservative Loan Amount")
        )
        .otherwise("Not Qualified - Income/Employment")
    )
    .otherwise("Not Qualified - Credit Score")
) \
.withColumn("credit_card_eligibility",
    # Credit card eligibility with different logic
    when(col("account_status") == "Active",
        when(col("credit_score") >= 750,
            when(col("annual_income") >= 100000, "Premium Card Eligible")
            .otherwise("Rewards Card Eligible")
        )
        .when(col("credit_score") >= 650, "Standard Card Eligible")
        .when(col("credit_score") >= 550, "Secured Card Eligible")
        .otherwise("Not Eligible - Credit Score")
    )
    .otherwise("Not Eligible - Account Status")
)

print("Product Eligibility Assessment:")
product_eligibility.select("customer_id", "credit_score", "annual_income", 
                          "employment_status", "mortgage_eligibility", 
                          "credit_card_eligibility").show(truncate=False)
```



### 1.3 SQL-Style CASE Statements for Complex Logic

**Using `expr()` for Complex `CASE` Logic:**

Sometimes complex business logic is easier to express using SQL-style CASE statements, especially when you have many conditions or when the logic maps closely to existing SQL business rules.

```python
# Customer segment optimization using SQL-style CASE
segment_optimization = product_eligibility.withColumn("optimized_segment",
    expr("""
        CASE 
            WHEN annual_income >= 250000 AND total_balance >= 200000 THEN 'Ultra High Net Worth'
            WHEN annual_income >= 150000 AND total_balance >= 100000 THEN 'High Net Worth'
            WHEN annual_income >= 100000 OR total_balance >= 75000 THEN 'Mass Affluent'
            WHEN annual_income >= 75000 OR total_balance >= 25000 THEN 'Mass Market'
            WHEN annual_income >= 50000 THEN 'Emerging'
            ELSE 'Basic'
        END
    """)
) \
.withColumn("fee_waiver_eligible",
    expr("""
        CASE
            WHEN optimized_segment IN ('Ultra High Net Worth', 'High Net Worth') THEN 'All Fees Waived'
            WHEN optimized_segment = 'Mass Affluent' AND monthly_transactions >= 20 THEN 'Most Fees Waived'
            WHEN optimized_segment = 'Mass Market' AND total_balance >= 10000 THEN 'Some Fees Waived'
            WHEN monthly_transactions >= 30 THEN 'Transaction Fee Waived'
            ELSE 'Standard Fees Apply'
        END
    """)
)

print("Segment Optimization and Fee Structure:")
segment_optimization.select("customer_id", "annual_income", "total_balance", 
                           "current_segment", "optimized_segment", "fee_waiver_eligible").show()
```

------

## 2. Sophisticated Aggregations - Customer Portfolio Analysis

### 2.1 Multi-Dimensional Customer Analysis

**What are Sophisticated Aggregations?**

Basic aggregations answer simple questions (How many customers?), but sophisticated aggregations answer complex business questions (What's the risk-adjusted profitability by customer segment and geography?). These aggregations combine multiple metrics, apply business logic, and provide actionable insights.

```python
# script_15.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, abs, col, when, count, countDistinct, collect_set, avg, sum, stddev, approx_count_distinct, desc, expr, variance


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StringCleaningDemo") \
    .getOrCreate()

complex_customer_data = [
    ("CUST001", 125000, 750, 3, "Employed", "Premium", 85000, 25, "Active", 4.2),
    ("CUST002", 75000, 680, 1, "Self-Employed", "Standard", 45000, 15, "Active", 3.8),
    ("CUST003", 200000, 800, 8, "Employed", "VIP", 150000, 45, "Active", 4.9),
    ("CUST004", 45000, 620, 0, "Unemployed", "Basic", 5000, 8, "Frozen", 2.1),
    ("CUST005", 95000, 720, 2, "Employed", "Standard", 65000, 18, "Active", 4.1),
    ("CUST006", 250000, 780, 12, "Executive", "VIP", 300000, 85, "Active", 4.8)
]

complex_df = spark.createDataFrame(
    complex_customer_data,
    ["customer_id", "annual_income", "credit_score", "years_with_bank", 
     "employment_status", "current_segment", "total_balance", "monthly_transactions", 
     "account_status", "relationship_rating"]
)

# Create transaction data for portfolio analysis
transaction_data = [
    ("TXN001", "CUST001", "2024-01-15", 2500.0, "Deposit", "Checking", "Branch"),
    ("TXN002", "CUST001", "2024-01-20", -850.0, "Withdrawal", "Checking", "ATM"),
    ("TXN003", "CUST001", "2024-01-25", 5000.0, "Transfer", "Savings", "Online"),
    ("TXN004", "CUST002", "2024-01-16", -1200.0, "Purchase", "Credit", "Card"),
    ("TXN005", "CUST002", "2024-01-18", 3200.0, "Deposit", "Checking", "Wire"),
    ("TXN006", "CUST003", "2024-01-17", 15000.0, "Investment", "Investment", "Online"),
    ("TXN007", "CUST003", "2024-01-22", -500.0, "Fee", "Checking", "System"),
    ("TXN008", "CUST004", "2024-01-19", -45.0, "Purchase", "Checking", "Card"),
    ("TXN009", "CUST005", "2024-01-21", 1500.0, "Deposit", "Savings", "Branch"),
    ("TXN010", "CUST006", "2024-01-23", 25000.0, "Investment", "Investment", "Online")
]

transaction_df = spark.createDataFrame(
    transaction_data,
    ["transaction_id", "customer_id", "transaction_date", "amount", 
     "transaction_type", "account_type", "channel"]
)

# Comprehensive customer portfolio analysis
portfolio_analysis = complex_df.join(
    transaction_df.groupBy("customer_id").agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_transaction_volume"),
        sum(when(col("amount") > 0, col("amount")).otherwise(0)).alias("total_deposits"),
        sum(when(col("amount") < 0, col("amount")).otherwise(0)).alias("total_withdrawals"),
        countDistinct("transaction_type").alias("transaction_variety"),
        countDistinct("channel").alias("channel_usage"),
        collect_set("account_type").alias("account_types_used"),
        avg("amount").alias("avg_transaction_amount")
    ), "customer_id", "left"
).fillna(0, ["total_transactions", "total_transaction_volume", "total_deposits", "total_withdrawals"])

print("Comprehensive Customer Portfolio Analysis:")
portfolio_analysis.select("customer_id", "current_segment", "total_balance", 
                         "total_transactions", "total_deposits", "transaction_variety").show()

# Stop Spark session
spark.stop()
```

### 2.2 Advanced Aggregation Patterns

**Customer Profitability Analysis:**

```python
# Calculate customer profitability metrics
profitability_analysis = portfolio_analysis.withColumn("estimated_monthly_revenue",
    # Revenue from deposits (interest spread)
    (col("total_balance") * 0.025 / 12) +
    # Revenue from transactions (fee income)
    (col("total_transactions") * 2.5) +
    # Revenue from credit products (interest income)
    when(col("total_balance") < 0, abs(col("total_balance")) * 0.18 / 12).otherwise(0)
) \
.withColumn("estimated_monthly_cost",
    # Cost of funds
    (when(col("total_balance") > 0, col("total_balance") * 0.01 / 12).otherwise(0)) +
    # Operational costs
    (col("total_transactions") * 0.5) +
    # Risk costs
    when(col("credit_score") < 650, 50).otherwise(10)
) \
.withColumn("estimated_monthly_profit",
    col("estimated_monthly_revenue") - col("estimated_monthly_cost")
) \
.withColumn("customer_lifetime_value",
    # Simple CLV calculation: monthly profit * expected tenure
    col("estimated_monthly_profit") * 
    when(col("current_segment") == "VIP", 120)  # 10 years
    .when(col("current_segment") == "Premium", 84)  # 7 years
    .otherwise(60)  # 5 years
)

print("Customer Profitability Analysis:")
profitability_analysis.select("customer_id", "current_segment", "estimated_monthly_revenue",
                              "estimated_monthly_cost", "estimated_monthly_profit", 
                              "customer_lifetime_value").show()
```

**Segment Performance Comparison:**

```python
# Advanced segment performance analysis
segment_performance = profitability_analysis.groupBy("current_segment").agg(
    # Customer metrics
    count("customer_id").alias("customer_count"),
    avg("annual_income").alias("avg_income"),
    avg("credit_score").alias("avg_credit_score"),
    
    # Portfolio metrics
    sum("total_balance").alias("total_segment_balance"),
    avg("total_balance").alias("avg_customer_balance"),
    
    # Activity metrics
    sum("total_transactions").alias("total_segment_transactions"),
    avg("transaction_variety").alias("avg_product_usage"),
    
    # Profitability metrics
    sum("estimated_monthly_profit").alias("total_segment_profit"),
    avg("estimated_monthly_profit").alias("avg_customer_profit"),
    avg("customer_lifetime_value").alias("avg_clv"),
    
    # Risk metrics
    count(when(col("credit_score") < 650, 1)).alias("high_risk_customers"),
    
    # Advanced metrics
    stddev("annual_income").alias("income_volatility"),
    approx_count_distinct("customer_id").alias("approx_customers")
) \
.withColumn("profit_per_customer", col("total_segment_profit") / col("customer_count")) \
.withColumn("risk_percentage", col("high_risk_customers") / col("customer_count") * 100) \
.withColumn("segment_roi", col("total_segment_profit") / col("total_segment_balance") * 100)

print("Advanced Segment Performance Analysis:")
segment_performance.orderBy(desc("total_segment_profit")).show()
```

### 2.3 Statistical Aggregations for Risk Analysis

**Portfolio Risk Assessment:**

```python
# Statistical analysis for portfolio risk
portfolio_statistics = profitability_analysis.agg(
    # Central tendency
    avg("annual_income").alias("mean_income"),
    expr("percentile_approx(annual_income, 0.5)").alias("median_income"),
    
    # Variability measures
    stddev("annual_income").alias("income_stddev"),
    variance("annual_income").alias("income_variance"),
    
    # Risk measures
    min("credit_score").alias("min_credit_score"),
    max("credit_score").alias("max_credit_score"),
    expr("percentile_approx(credit_score, 0.05)").alias("credit_5th_percentile"),
    expr("percentile_approx(credit_score, 0.95)").alias("credit_95th_percentile"),
    
    # Portfolio concentration
    count("customer_id").alias("total_customers"),
    countDistinct("current_segment").alias("segments_count"),
    sum("total_balance").alias("total_portfolio_value"),
    
    # Profitability distribution
    expr("percentile_approx(estimated_monthly_profit, 0.25)").alias("profit_q1"),
    expr("percentile_approx(estimated_monthly_profit, 0.75)").alias("profit_q3")
)

print("Portfolio Statistical Analysis:")
portfolio_statistics.show()

# Risk concentration by segment
risk_concentration = profitability_analysis.groupBy("current_segment").agg(
    sum("total_balance").alias("segment_balance"),
    count("customer_id").alias("segment_customers")
) \
.withColumn("portfolio_concentration", 
    col("segment_balance") / portfolio_statistics.select("total_portfolio_value").first()[0] * 100
) \
.withColumn("customer_concentration",
    col("segment_customers") / portfolio_statistics.select("total_customers").first()[0] * 100
)

print("Portfolio Concentration Risk:")
risk_concentration.orderBy(desc("portfolio_concentration")).show()

```

------



## 3. Window Functions - Time-Based and Ranking Analytics

### 3.1 Understanding Window Functions in Banking Context

**What are Window Functions?**

Window functions perform calculations across a set of rows related to the current row, without collapsing the result set like GROUP BY. In banking, they're essential for time-series analysis, ranking customers, calculating running totals, and comparing current values to historical values.

**Common Banking Use Cases:**

- **Customer Rankings:** Rank customers by portfolio value within each segment
- **Time Series Analysis:** Track account balance changes over time
- **Performance Tracking:** Compare current month to previous months
- **Trend Analysis:** Calculate moving averages and growth rates

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, abs, col, when, count, countDistinct, collect_set, avg, sum, stddev, approx_count_distinct, desc, expr, variance
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, row_number, percent_rank, ntile
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StringCleaningDemo") \
    .getOrCreate()
# Create time-series customer data for window function examples
monthly_customer_data = [
    ("CUST001", "2024-01", 75000, 25, 4500, "Premium"),
    ("CUST001", "2024-02", 78000, 28, 4800, "Premium"),
    ("CUST001", "2024-03", 82000, 30, 5200, "Premium"),
    ("CUST002", "2024-01", 45000, 15, 2800, "Standard"),
    ("CUST002", "2024-02", 47000, 18, 3000, "Standard"),
    ("CUST002", "2024-03", 44000, 12, 2500, "Standard"),
    ("CUST003", "2024-01", 150000, 45, 8500, "VIP"),
    ("CUST003", "2024-02", 155000, 48, 9200, "VIP"),
    ("CUST003", "2024-03", 160000, 52, 9800, "VIP")
]

monthly_df = spark.createDataFrame(
    monthly_customer_data,
    ["customer_id", "month", "total_balance", "transactions", "fees_paid", "segment"]
)

monthly_df.show()
```

### 3.2 Ranking and Row Number Functions

**Customer Performance Rankings:**

```python
# Customer ranking within segments
segment_ranking_window = Window.partitionBy("segment", "month").orderBy(desc("total_balance"))

customer_rankings = monthly_df.withColumn("balance_rank_in_segment", 
    rank().over(segment_ranking_window)
) \
.withColumn("balance_row_number_in_segment",
    row_number().over(segment_ranking_window)
) \
.withColumn("balance_dense_rank_in_segment",
    dense_rank().over(segment_ranking_window)
)

print("Customer Rankings Within Segments:")
customer_rankings.select("customer_id", "month", "segment", "total_balance", 
                         "balance_rank_in_segment", "balance_row_number_in_segment").show()

# Overall customer rankings across all segments
overall_ranking_window = Window.partitionBy("month").orderBy(desc("total_balance"))

overall_rankings = customer_rankings.withColumn("overall_balance_rank",
    rank().over(overall_ranking_window)
) \
.withColumn("percentile_rank",
    percent_rank().over(overall_ranking_window)
) \
.withColumn("ntile_quartile",
    ntile(4).over(overall_ranking_window)
)

print("Overall Customer Rankings:")
overall_rankings.select("customer_id", "month", "total_balance", "overall_balance_rank", 
                       "percentile_rank", "ntile_quartile").show()
```

### 3.3 Time-Series Analysis with `lag()` and `lead()`

**Month-over-Month Analysis:**

```python
# Time-based window for analyzing trends
time_window = Window.partitionBy("customer_id").orderBy("month")

time_analysis = monthly_df.withColumn("previous_month_balance",
    lag("total_balance", 1).over(time_window)
) \
.withColumn("next_month_balance", 
    lead("total_balance", 1).over(time_window)
) \
.withColumn("balance_change_mom",
    col("total_balance") - col("previous_month_balance")
) \
.withColumn("balance_growth_rate",
    when(col("previous_month_balance") > 0,
         (col("balance_change_mom") / col("previous_month_balance") * 100))
    .otherwise(0)
) \
.withColumn("three_month_avg_balance",
    avg("total_balance").over(
        time_window.rowsBetween(-1, 1)  # Current row, 1 before, 1 after
    )
)

print("Month-over-Month Analysis:")
time_analysis.select("customer_id", "month", "total_balance", "previous_month_balance",
                    "balance_change_mom", "balance_growth_rate", 
                    "three_month_avg_balance").show()
```

### 3.4 Running Totals and Cumulative Analysis

**Customer Engagement Tracking:**

```python
# Running totals and cumulative analysis
cumulative_window = Window.partitionBy("customer_id").orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cumulative_analysis = time_analysis.withColumn("cumulative_fees_paid",
    sum("fees_paid").over(cumulative_window)
) \
.withColumn("cumulative_transactions",
    sum("transactions").over(cumulative_window)
) \
.withColumn("running_avg_balance",
    avg("total_balance").over(cumulative_window)
) \
.withColumn("max_balance_to_date",
    max("total_balance").over(cumulative_window)
) \
.withColumn("months_as_customer",
    row_number().over(Window.partitionBy("customer_id").orderBy("month"))
)

print("Cumulative Customer Analysis:")
cumulative_analysis.select("customer_id", "month", "total_balance", "cumulative_fees_paid",
                          "cumulative_transactions", "running_avg_balance", 
                          "months_as_customer").show()
```

### 3.5 Advanced Window Functions for Business Intelligence

**Customer Lifecycle Analysis:**

```python
# Advanced customer lifecycle metrics
lifecycle_window = Window.partitionBy("customer_id").orderBy("month")

lifecycle_analysis = cumulative_analysis.withColumn("balance_trend",
    when(col("balance_growth_rate") > 5, "Growing")
    .when(col("balance_growth_rate") < -5, "Declining")
    .otherwise("Stable")
) \
.withColumn("customer_momentum",
    # Calculate momentum based on last 2 months
    when(
        (lag("balance_growth_rate", 1).over(lifecycle_window) > 0) & 
        (col("balance_growth_rate") > 0), "Accelerating"
    )
    .when(
        (lag("balance_growth_rate", 1).over(lifecycle_window) < 0) & 
        (col("balance_growth_rate") < 0), "Decelerating"
    )
    .otherwise("Mixed")
) \
.withColumn("customer_value_tier",
    ntile(5).over(Window.partitionBy("month").orderBy("total_balance"))
) \
.withColumn("value_tier_movement",
    col("customer_value_tier") - lag("customer_value_tier", 1).over(lifecycle_window)
)

print("Customer Lifecycle Analysis:")
lifecycle_analysis.select("customer_id", "month", "balance_trend", "customer_momentum",
                         "customer_value_tier", "value_tier_movement").show()
```

------

## 4. Calculated Fields - Financial Metrics and KPIs

### 4.1 Banking-Specific Financial Calculations

**What are Calculated Fields in Banking?**

Calculated fields derive new insights from existing data using mathematical operations, date functions, and business formulas. In banking, these fields power decision-making systems, regulatory reporting, and customer analytics.

**Essential Banking Calculations:**

- **Interest Calculations:** Simple and compound interest
- **Risk Metrics:** Debt-to-income ratios, credit utilization
- **Profitability Measures:** Net interest margin, return on assets
- **Customer Metrics:** Account tenure, relationship depth

```python
# script_17.py
# Create comprehensive account data for financial calculations
account_financial_data = [
    ("ACC001", "CUST001", "Checking", 25000.0, "2023-01-15", 0.01, None, "Active"),
    ("ACC002", "CUST001", "Savings", 75000.0, "2023-02-20", 0.025, None, "Active"),
    ("ACC003", "CUST001", "Credit Card", -2500.0, "2023-03-10", 0.0, 10000.0, "Active"),
    ("ACC004", "CUST002", "Checking", 15000.0, "2022-06-15", 0.01, None, "Active"),
    ("ACC005", "CUST002", "Mortgage", -250000.0, "2022-08-01", 0.065, 300000.0, "Active"),
    ("ACC006", "CUST003", "Investment", 150000.0, "2021-03-20", 0.04, None, "Active")
]

financial_df = spark.createDataFrame(
    account_financial_data,
    ["account_id", "customer_id", "account_type", "current_balance", "open_date", 
     "interest_rate", "credit_limit", "status"]
)

# Calculate financial metrics
financial_metrics = financial_df.withColumn("account_age_months",
    months_between(current_date(), col("open_date"))
) \
.withColumn("account_age_years",
    col("account_age_months") / 12
) \
.withColumn("monthly_interest_earned",
    when(col("current_balance") > 0, 
         col("current_balance") * col("interest_rate") / 12)
    .otherwise(0)
) \
.withColumn("monthly_interest_paid",
    when(col("current_balance") < 0,
         abs(col("current_balance")) * col("interest_rate") / 12)
    .otherwise(0)
) \
.withColumn("credit_utilization_ratio",
    when((col("account_type") == "Credit Card") & (col("credit_limit").isNotNull()),
         abs(col("current_balance")) / col("credit_limit") * 100)
    .otherwise(0)
) \
.withColumn("available_credit",
    when(col("credit_limit").isNotNull(),
         col("credit_limit") - abs(col("current_balance")))
    .otherwise(0)
)

print("Banking Financial Metrics:")
financial_metrics.select("account_id", "account_type", "current_balance", "account_age_years",
                         "monthly_interest_earned", "monthly_interest_paid", 
                         "credit_utilization_ratio").show()
```



### 4.2 Customer-Level Financial KPIs

**Customer Portfolio Metrics:**

```python
# Aggregate financial metrics at customer level
customer_kpis = financial_metrics.groupBy("customer_id").agg(
    # Portfolio composition
    count("account_id").alias("total_accounts"),
    countDistinct("account_type").alias("product_diversity"),
    
    # Balance metrics
    sum("current_balance").alias("net_worth"),
    sum(when(col("current_balance") > 0, col("current_balance")).otherwise(0)).alias("total_assets"),
    sum(when(col("current_balance") < 0, col("current_balance")).otherwise(0)).alias("total_liabilities"),
    
    # Interest and profitability
    sum("monthly_interest_earned").alias("monthly_interest_income"),
    sum("monthly_interest_paid").alias("monthly_interest_expense"),
    
    # Credit metrics
    sum("credit_limit").alias("total_credit_limit"),
    avg("credit_utilization_ratio").alias("avg_credit_utilization"),
    
    # Relationship metrics
    min("account_age_years").alias("newest_account_age"),
    max("account_age_years").alias("oldest_account_age"),
    avg("account_age_years").alias("avg_account_age")
) \
.withColumn("net_monthly_interest",
    col("monthly_interest_income") - col("monthly_interest_expense")
) \
.withColumn("debt_to_asset_ratio",
    when(col("total_assets") > 0,
         abs(col("total_liabilities")) / col("total_assets") * 100)
    .otherwise(0)
) \
.withColumn("relationship_depth_score",
    (col("product_diversity") * 2) + col("avg_account_age")
) \
.withColumn("customer_profitability_tier",
    when(col("net_monthly_interest") >= 100, "High Profit")
    .when(col("net_monthly_interest") >= 25, "Medium Profit")
    .when(col("net_monthly_interest") >= 0, "Low Profit")
    .otherwise("Loss Making")
)

print("Customer-Level Financial KPIs:")
customer_kpis.select("customer_id", "net_worth", "total_assets", "total_liabilities",
                    "net_monthly_interest", "debt_to_asset_ratio", 
                    "relationship_depth_score", "customer_profitability_tier").show()
```

### 4.3 Risk-Adjusted Performance Metrics

**Advanced Risk Calculations:**

```python
# Join with customer demographic data for risk-adjusted metrics
risk_adjusted_metrics = customer_kpis.join(complex_df.select("customer_id", "annual_income", "credit_score", "current_segment"), "customer_id") \
.withColumn("income_to_debt_ratio",
    when(col("total_liabilities") < 0,
         col("annual_income") / abs(col("total_liabilities")))
    .otherwise(999)  # No debt = very high ratio
) \
.withColumn("credit_capacity_utilization",
    when(col("total_credit_limit") > 0,
         abs(col("total_liabilities")) / col("total_credit_limit") * 100)
    .otherwise(0)
) \
.withColumn("risk_adjusted_return",
    # Simple risk adjustment: higher credit scores get full return
    col("net_monthly_interest") * (col("credit_score") / 850)
) \
.withColumn("customer_risk_score",
    # Multi-factor risk score (0-100, lower is better)
    when(col("debt_to_asset_ratio") > 80, 40).otherwise(0) +
    when(col("credit_score") < 650, 30).otherwise(0) +
    when(col("income_to_debt_ratio") < 2, 20).otherwise(0) +
    when(col("credit_capacity_utilization") > 80, 10).otherwise(0)
) \
.withColumn("risk_category",
    when(col("customer_risk_score") >= 70, "High Risk")
    .when(col("customer_risk_score") >= 40, "Medium Risk")
    .when(col("customer_risk_score") >= 20, "Low Risk")
    .otherwise("Very Low Risk")
)

print("Risk-Adjusted Performance Metrics:")
risk_adjusted_metrics.select("customer_id", "annual_income", "debt_to_asset_ratio",
                            "income_to_debt_ratio", "credit_capacity_utilization",
                            "customer_risk_score", "risk_category").show()
```

### 4.4 Time-Based Financial Calculations

**Date-Driven Financial Metrics:**

```python
# Advanced date calculations for financial analysis
from pyspark.sql.functions import datediff, add_months, months_between

date_driven_metrics = financial_metrics.withColumn("days_since_opening",
    datediff(current_date(), col("open_date"))
) \
.withColumn("account_maturity_category",
    when(col("account_age_years") < 1, "New Account")
    .when(col("account_age_years") < 3, "Developing Account")
    .when(col("account_age_years") < 7, "Mature Account")
    .otherwise("Legacy Account")
) \
.withColumn("projected_annual_interest",
    col("monthly_interest_earned") * 12
) \
.withColumn("account_lifetime_value",
    # Simple LTV: Current annual value * remaining expected years
    col("projected_annual_interest") * 
    when(col("account_type") == "Checking", 10)
    .when(col("account_type") == "Savings", 8)
    .when(col("account_type") == "Credit Card", 5)
    .when(col("account_type") == "Mortgage", 20)
    .otherwise(7)
) \
.withColumn("next_review_date",
    when(col("account_type") == "Credit Card", add_months(current_date(), 6))
    .when(col("credit_utilization_ratio") > 80, add_months(current_date(), 3))
    .otherwise(add_months(current_date(), 12))
)

print("Time-Based Financial Calculations:")
date_driven_metrics.select("account_id", "account_type", "account_age_years",
                          "account_maturity_category", "projected_annual_interest",
                          "account_lifetime_value").show()
```

------



## 5. Decision Trees - Automated Business Processes

### 5.1 Complex Decision Tree Implementation

**What are Decision Trees in Banking?**

Decision trees are systematic approaches to complex business decisions that involve multiple criteria and outcomes. In banking, they automate processes like loan approvals, product recommendations, and risk assessments by codifying expert knowledge into algorithmic rules.

**Banking Decision Tree Applications:**

- **Credit Approval Process:** Multi-stage evaluation with different criteria
- **Product Recommendation Engine:** Match customers to appropriate products
- **Risk Assessment Framework:** Systematic risk evaluation and mitigation
- **Pricing Decision Engine:** Dynamic pricing based on multiple factors

```python
#script_18.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import least, lit, concat, col, when
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StringCleaningDemo") \
    .getOrCreate()
# Create comprehensive customer profile for decision tree examples
decision_tree_data = [
    ("CUST001", 125000, 750, 85000, 2.5, "Employed", 28, "Premium", "Own", 3, True),
    ("CUST002", 75000, 680, 45000, 1.8, "Self-Employed", 35, "Standard", "Rent", 1, False),
    ("CUST003", 200000, 800, 150000, 1.2, "Executive", 45, "VIP", "Own", 8, True),
    ("CUST004", 45000, 620, 5000, 4.2, "Unemployed", 25, "Basic", "Rent", 0, False),
    ("CUST005", 95000, 720, 65000, 2.1, "Employed", 32, "Standard", "Own", 2, True),
    ("CUST006", 180000, 780, 120000, 1.5, "Professional", 40, "Premium", "Own", 5, True)
]

decision_df = spark.createDataFrame(
    decision_tree_data,
    ["customer_id", "annual_income", "credit_score", "total_balance", "debt_to_income_ratio",
     "employment_status", "age", "current_segment", "housing_status", "years_with_bank", "has_direct_deposit"]
)

decision_df.show()
```

### 5.2 Credit Approval Decision Tree

**Multi-Stage Credit Approval Process:**

```python
# Comprehensive credit approval decision tree
credit_approval_tree = decision_df.withColumn("credit_decision",
    # Stage 1: Basic eligibility screening
    when(
        (col("credit_score") < 550) | 
        (col("employment_status") == "Unemployed") |
        (col("annual_income") < 25000),
        "DECLINED - Basic Eligibility"
    )
    # Stage 2: High-confidence approvals
    .when(
        (col("credit_score") >= 750) & 
        (col("annual_income") >= 100000) & 
        (col("debt_to_income_ratio") <= 2.0) &
        (col("years_with_bank") >= 2),
        "APPROVED - Excellent Profile"
    )
    # Stage 3: Standard approval criteria
    .when(
        (col("credit_score") >= 680) &
        (col("annual_income") >= 50000) &
        (col("debt_to_income_ratio") <= 3.0),
        # Sub-decision for standard approvals
        when(
            (col("housing_status") == "Own") | 
            (col("has_direct_deposit") == True) |
            (col("years_with_bank") >= 3),
            "APPROVED - Standard"
        )
        .otherwise("CONDITIONAL APPROVAL - Additional Documentation Required")
    )
    # Stage 4: Marginal cases requiring enhanced review
    .when(
        (col("credit_score") >= 620) &
        (col("annual_income") >= 40000) &
        (col("debt_to_income_ratio") <= 4.0),
        "MANUAL REVIEW REQUIRED"
    )
    # Stage 5: All other cases
    .otherwise("DECLINED - Credit Policy")
) \
.withColumn("credit_limit_recommendation",
    when(col("credit_decision").contains("APPROVED - Excellent"), 
         least(col("annual_income") * 0.3, lit(50000)))
    .when(col("credit_decision").contains("APPROVED - Standard"),
         least(col("annual_income") * 0.2, lit(25000)))
    .when(col("credit_decision").contains("CONDITIONAL"),
         least(col("annual_income") * 0.15, lit(15000)))
    .otherwise(0)
) \
.withColumn("interest_rate_recommendation",
    when(col("credit_decision").contains("Excellent"), 12.99)
    .when(col("credit_decision").contains("Standard"), 16.99)
    .when(col("credit_decision").contains("CONDITIONAL"), 21.99)
    .otherwise(0)
)

print("Credit Approval Decision Tree Results:")
credit_approval_tree.select("customer_id", "credit_score", "annual_income", "debt_to_income_ratio",
                           "credit_decision", "credit_limit_recommendation", 
                           "interest_rate_recommendation").show(truncate=False)
```

### 5.3 Product Recommendation Decision Engine

**Intelligent Product Matching:**

```python
# Comprehensive product recommendation engine
product_recommendation_engine = credit_approval_tree.withColumn("primary_product_recommendation",
    # High-value customer recommendations
    when(
        (col("annual_income") >= 150000) & 
        (col("total_balance") >= 100000),
        when(col("age") >= 40, "Private Banking Package")
        .when(col("current_segment") == "VIP", "Investment Advisory Services")
        .otherwise("Premium Banking Bundle")
    )
    # High-income, moderate assets
    .when(
        (col("annual_income") >= 100000) &
        (col("total_balance") >= 50000),
        when(col("housing_status") == "Rent", "Mortgage Pre-qualification")
        .when(col("years_with_bank") < 2, "Premium Checking Upgrade")
        .otherwise("Investment Account")
    )
    # Moderate-income customers
    .when(
        (col("annual_income") >= 75000) &
        (col("credit_score") >= 700),
        when(col("debt_to_income_ratio") <= 2.0, "Premium Credit Card")
        .when(col("housing_status") == "Own", "Home Equity Line of Credit")
        .otherwise("Rewards Credit Card")
    )
    # Standard customers
    .when(
        (col("annual_income") >= 50000) &
        (col("credit_score") >= 650),
        when(col("has_direct_deposit") == False, "Direct Deposit Setup")
        .when(col("total_balance") < 10000, "Savings Account")
        .otherwise("Standard Credit Card")
    )
    # Basic banking needs
    .when(col("annual_income") >= 25000,
        when(col("total_balance") < 1000, "Basic Checking Account")
        .otherwise("Secured Credit Card")
    )
    .otherwise("Financial Counseling Services")
) \
.withColumn("secondary_product_recommendation",
    # Cross-selling opportunities based on primary recommendation
    when(col("primary_product_recommendation").contains("Credit Card"), "Identity Protection Service")
    .when(col("primary_product_recommendation").contains("Mortgage"), "Home Insurance")
    .when(col("primary_product_recommendation").contains("Investment"), "Tax Advisory Service")
    .when(col("primary_product_recommendation").contains("Private Banking"), "Estate Planning")
    .when(col("primary_product_recommendation").contains("Checking"), "Mobile Banking App")
    .otherwise("Financial Education Resources")
) \
.withColumn("contact_priority",
    when(col("primary_product_recommendation").contains("Private Banking"), "High - 24 hours")
    .when(col("annual_income") >= 100000, "Medium - 3 days")
    .when(col("primary_product_recommendation").contains("Credit Card"), "Standard - 1 week")
    .otherwise("Low - Monthly Newsletter")
)

print("Product Recommendation Engine Results:")
product_recommendation_engine.select("customer_id", "annual_income", "current_segment",
                                    "primary_product_recommendation", 
                                    "secondary_product_recommendation", 
                                    "contact_priority").show(truncate=False)
```

### 5.4 Risk Assessment and Monitoring Framework

**Dynamic Risk Assessment Decision Tree:**

```python
# Comprehensive risk assessment framework
risk_assessment_framework = product_recommendation_engine.withColumn("risk_assessment_score",
    # Credit risk factors (40% weight)
    (when(col("credit_score") >= 750, 10)
     .when(col("credit_score") >= 700, 8)
     .when(col("credit_score") >= 650, 6)
     .when(col("credit_score") >= 600, 4)
     .otherwise(0)) * 0.4 +
    
    # Income stability factors (25% weight)
    (when(col("employment_status") == "Executive", 10)
     .when(col("employment_status") == "Professional", 9)
     .when(col("employment_status") == "Employed", 8)
     .when(col("employment_status") == "Self-Employed", 6)
     .otherwise(0)) * 0.25 +
    
    # Relationship factors (20% weight)
    (when(col("years_with_bank") >= 5, 10)
     .when(col("years_with_bank") >= 3, 8)
     .when(col("years_with_bank") >= 1, 6)
     .otherwise(4)) * 0.2 +
    
    # Financial position factors (15% weight)
    (when(col("debt_to_income_ratio") <= 1.5, 10)
     .when(col("debt_to_income_ratio") <= 2.5, 8)
     .when(col("debt_to_income_ratio") <= 3.5, 6)
     .when(col("debt_to_income_ratio") <= 4.5, 4)
     .otherwise(0)) * 0.15
) \
.withColumn("risk_category",
    when(col("risk_assessment_score") >= 8.5, "Very Low Risk")
    .when(col("risk_assessment_score") >= 7.0, "Low Risk")
    .when(col("risk_assessment_score") >= 5.5, "Medium Risk")
    .when(col("risk_assessment_score") >= 4.0, "High Risk")
    .otherwise("Very High Risk")
) \
.withColumn("monitoring_frequency",
    when(col("risk_category") == "Very High Risk", "Weekly Review")
    .when(col("risk_category") == "High Risk", "Monthly Review")
    .when(col("risk_category") == "Medium Risk", "Quarterly Review")
    .otherwise("Annual Review")
) \
.withColumn("required_actions",
    when(col("risk_category") == "Very High Risk", "Immediate Account Review + Credit Limit Reduction")
    .when(col("risk_category") == "High Risk", "Enhanced Monitoring + Documentation Update")
    .when(col("risk_category") == "Medium Risk", "Standard Monitoring")
    .when(col("risk_category") == "Low Risk", "Annual Risk Assessment")
    .otherwise("Portfolio Review Only")
)

print("Risk Assessment Framework Results:")
risk_assessment_framework.select("customer_id", "credit_score", "debt_to_income_ratio",
                                "risk_assessment_score", "risk_category", 
                                "monitoring_frequency", "required_actions").show(truncate=False)
```

### 5.5 Automated Business Process Integration

**Comprehensive Decision Integration:**

```python
# Integrate all decision trees into comprehensive business process
comprehensive_business_process = risk_assessment_framework.withColumn("next_business_action",
    # Priority 1: Address high-risk situations
    when(col("risk_category").isin(["Very High Risk", "High Risk"]),
        concat(lit("PRIORITY: "), col("required_actions"))
    )
    # Priority 2: High-value opportunities
    .when(
        (col("annual_income") >= 150000) & 
        (col("primary_product_recommendation").contains("Private Banking")),
        "Schedule Private Banking Consultation Within 48 Hours"
    )
    # Priority 3: Credit opportunities
    .when(
        col("credit_decision").contains("APPROVED"),
        concat(lit("Send Credit Offer: "), col("credit_decision"))
    )
    # Priority 4: Standard product recommendations
    .when(
        col("contact_priority").contains("High"),
        concat(lit("High-Priority Contact: "), col("primary_product_recommendation"))
    )
    # Priority 5: Standard follow-up
    .otherwise(
        concat(lit("Standard Follow-up: "), col("primary_product_recommendation"))
    )
) \
.withColumn("automation_flag",
    when(col("next_business_action").contains("PRIORITY"), "Manual Intervention Required")
    .when(col("next_business_action").contains("High-Priority"), "Automated High-Priority Queue")
    .when(col("next_business_action").contains("Credit Offer"), "Automated Credit Processing")
    .otherwise("Standard Automated Processing")
) \
.withColumn("estimated_revenue_opportunity",
    when(col("primary_product_recommendation").contains("Private Banking"), 5000)
    .when(col("primary_product_recommendation").contains("Investment"), 2500)
    .when(col("primary_product_recommendation").contains("Mortgage"), 1500)
    .when(col("primary_product_recommendation").contains("Credit Card"), 300)
    .otherwise(100)
)

print("Comprehensive Business Process Results:")
comprehensive_business_process.select("customer_id", "risk_category", "credit_decision",
                                     "next_business_action", "automation_flag", 
                                     "estimated_revenue_opportunity").show(truncate=False)
```

------

## 6. Common Pitfalls and Best Practices

### 6.1 Performance Optimization for Complex Logic

**Complex Logic Performance Best Practices:**

```python
# ✅ DO: Cache DataFrames with complex calculations
complex_df_cached = complex_df.cache()

# ✅ DO: Break complex logic into steps for readability and debugging
step1_risk = complex_df_cached.withColumn("credit_risk_component", 
    when(col("credit_score") >= 750, 1).otherwise(0))

step2_income = step1_risk.withColumn("income_risk_component",
    when(col("annual_income") >= 100000, 1).otherwise(0))

final_score = step2_income.withColumn("combined_score", 
    col("credit_risk_component") + col("income_risk_component"))

# ✅ DO: Use native functions instead of UDFs when possible
# Native functions are optimized and faster

# ❌ AVOID: Overly complex nested when() statements in single column
# Instead, break into multiple steps for maintainability
```

### 6.2 Business Logic Validation

**Ensuring Accuracy in Business Rules:**

```python
def validate_business_logic(df, logic_column, validation_rules):
    """
    Validate business logic results against known rules
    """
    validation_results = []
    
    for rule_name, condition, expected_result in validation_rules:
        actual_count = df.filter(condition & (col(logic_column) == expected_result)).count()
        total_applicable = df.filter(condition).count()
        
        if total_applicable > 0:
            accuracy = actual_count / total_applicable * 100
            validation_results.append((rule_name, accuracy, actual_count, total_applicable))
            
    return validation_results

# Example validation rules for credit decisions
validation_rules = [
    ("High Credit Score Rule", col("credit_score") >= 750, "APPROVED - Excellent Profile"),
    ("Low Credit Score Rule", col("credit_score") < 550, "DECLINED - Basic Eligibility"),
    ("Unemployed Rule", col("employment_status") == "Unemployed", "DECLINED - Basic Eligibility")
]

# Run validation
validation_results = validate_business_logic(credit_approval_tree, "credit_decision", validation_rules)
for rule, accuracy, actual, total in validation_results:
    print(f"{rule}: {accuracy:.1f}% accuracy ({actual}/{total})")
```

### 6.3 Testing Complex Decision Trees

**Decision Tree Testing Framework:**

```python
def test_decision_tree_edge_cases():
    """
    Test edge cases in decision tree logic
    """
    # Create edge case test data
    edge_cases = [
        ("EDGE001", 0, 300, 0, 10.0, "Unemployed", 18, "Basic", "Rent", 0, False),  # Extreme low case
        ("EDGE002", 1000000, 850, 500000, 0.1, "Executive", 65, "VIP", "Own", 20, True),  # Extreme high case
        ("EDGE003", 50000, 680, 25000, 3.0, "Employed", 30, "Standard", "Own", 2, True),  # Boundary case
    ]
    
    edge_df = spark.createDataFrame(edge_cases, decision_df.columns)
    
    # Apply decision tree logic
    edge_results = solve_comprehensive_decision_tree(edge_df)
    
    print("Edge Case Testing Results:")
    edge_results.select("customer_id", "annual_income", "credit_score", 
                       "credit_decision", "primary_product_recommendation").show(truncate=False)
    
    return edge_results

# Test edge cases
edge_test_results = test_decision_tree_edge_cases()
```



------

## Module Summary and Next Steps

### What You've Mastered ✅

**Technical Skills:**

1. **Complex Conditional Logic** - Multi-layered when() statements and nested business rules
2. **Advanced Aggregations** - Sophisticated portfolio analysis and statistical measures
3. **Window Functions** - Time-series analysis, ranking, and cumulative calculations
4. **Financial Calculations** - Banking-specific metrics, ratios, and KPIs
5. **Decision Trees** - Automated business process implementation
6. **Performance Optimization** - Efficient complex logic execution

**Business Applications:**

- **Credit Risk Assessment** - Multi-factor scoring and approval systems
- **Customer Segmentation** - Dynamic classification and value optimization
- **Product Recommendation** - Intelligent matching and cross-selling
- **Portfolio Analysis** - Comprehensive financial health monitoring
- **Automated Decision Making** - Systematic business process automation

### Critical Success Factors 🎯

**For Production Business Logic:**

1. **Test Thoroughly** - Validate logic with edge cases and boundary conditions
2. **Document Decisions** - Record business rules and assumptions clearly
3. **Monitor Performance** - Track execution time and optimize complex calculations
4. **Validate Results** - Cross-check automated decisions with business experts
5. **Plan for Changes** - Design flexible logic that can adapt to new requirements

### Module 3 Quick Reference Card 📚

| **Pattern**          | **Function**                | **Example**                                                  |
| -------------------- | --------------------------- | ------------------------------------------------------------ |
| **Multi-Condition**  | `when().when().otherwise()` | `when(cond1, val1).when(cond2, val2).otherwise(default)`     |
| **Customer Ranking** | `rank().over(Window)`       | `rank().over(Window.partitionBy("segment").orderBy(desc("balance")))` |
| **Time Analysis**    | `lag()`, `lead()`           | `lag("balance", 1).over(time_window)`                        |
| **Running Totals**   | `sum().over(Window)`        | `sum("amount").over(cumulative_window)`                      |
| **Financial Ratios** | Mathematical operations     | `col("debt") / col("income") * 100`                          |
| **Decision Trees**   | Nested `when()`             | Complex hierarchical business logic                          |
| **Risk Scoring**     | Weighted calculations       | `(factor1 * 0.4) + (factor2 * 0.3) + (factor3 * 0.3)`        |

### Advanced Business Logic Principles 🏆

**Remember these core principles:**

- **Business First** - Understand the business requirement before implementing logic
- **Test Incrementally** - Build and test logic in stages, not all at once
- **Handle Edge Cases** - Plan for unusual data scenarios and boundary conditions
- **Document Assumptions** - Record business rules and decision criteria clearly
- **Monitor Outcomes** - Track the results of automated decisions for accuracy

### Performance and Maintainability Guidelines 🚀

**Complex Logic Best Practices:**

- **Break Down Complexity** - Split complex logic into manageable steps
- **Cache Intermediate Results** - Store frequently used calculations
- **Use Native Functions** - Prefer built-in functions over custom UDFs
- **Validate Early** - Check data quality before applying complex logic
- **Profile Performance** - Monitor execution time and optimize bottlenecks

### Real-World Implementation Considerations 💼

**Production Readiness Checklist:**

- ✅ Business logic validated by subject matter experts
- ✅ Edge cases identified and handled appropriately
- ✅ Performance tested with realistic data volumes
- ✅ Error handling implemented for invalid data scenarios
- ✅ Monitoring and alerting configured for decision outcomes
- ✅ Documentation created for maintenance and updates



You now have the advanced skills to implement sophisticated business logic that powers modern banking systems. You can create complex decision trees, perform advanced analytics, and build automated processes that scale to millions of customers while maintaining accuracy and performance.

**The Power You Now Possess:**

- Build credit approval systems that process applications automatically
- Create customer segmentation engines that adapt to changing behaviors
- Implement risk assessment frameworks that comply with regulatory requirements
- Design recommendation engines that drive revenue through targeted offers
- Develop financial calculations that support strategic business decisions

**Next:** Module 4 will teach you to integrate all these capabilities into production-ready data pipelines that can process banking data at enterprise scale, ensuring your sophisticated business logic reaches its full potential in real-world banking operations.