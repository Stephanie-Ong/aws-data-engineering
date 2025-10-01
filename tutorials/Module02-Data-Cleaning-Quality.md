# Module 2 - Data Cleaning & Quality

**Learning Time:** 60-90 minutes
 **Prerequisites:** Module 1 completion (DataFrame fundamentals)
 **Learning Objectives:** Master data cleaning techniques for production-ready banking analytics

------

## 📋 Module 2 Functions Reference Table

| **Section**          | **Function**              | **Purpose**                  | **Syntax Example**                                           |
| -------------------- | ------------------------- | ---------------------------- | ------------------------------------------------------------ |
| **String Cleaning**  | `trim()`                  | Remove whitespace            | `trim(col("name"))`                                          |
| **String Cleaning**  | `ltrim()`, `rtrim()`      | Remove left/right spaces     | `ltrim(col("address"))`                                      |
| **String Cleaning**  | `initcap()`               | Title case formatting        | `initcap(col("first_name"))`                                 |
| **String Cleaning**  | `upper()`, `lower()`      | Case conversion              | `upper(col("state"))`                                        |
| **String Replace**   | `regexp_replace()`        | Pattern-based replacement    | `regexp_replace(col("phone"), "[^0-9]", "")`                 |
| **String Replace**   | `translate()`             | Character replacement        | `translate(col("ssn"), ".-", "")`                            |
| **String Concat**    | `concat()`                | Join strings                 | `concat(col("first"), lit(" "), col("last"))`                |
| **String Concat**    | `concat_ws()`             | Join with separator          | `concat_ws(" ", col("first"), col("last"))`                  |
| **String Functions** | `substring()`             | Extract part of string       | `substring(col("phone"), 1, 3)`                              |
| **String Functions** | `split()`                 | Split into array             | `split(col("full_name"), " ")`                               |
| **String Functions** | `length()`                | String length                | `length(col("email"))`                                       |
| **String Functions** | `lpad()`, `rpad()`        | Pad strings                  | `lpad(col("id"), 10, "0")`                                   |
| **Null Handling**    | `isNull()`, `isNotNull()` | Check for nulls              | `col("email").isNotNull()`                                   |
| **Null Handling**    | `fillna()`                | Replace nulls                | `df.fillna({"income": 0})`                                   |
| **Null Handling**    | `dropna()`                | Remove null rows             | `df.dropna(subset=["customer_id"])`                          |
| **Null Handling**    | `coalesce()`              | First non-null value         | `coalesce(col("phone"), lit("No Phone"))`                    |
| **Duplicates**       | `distinct()`              | Remove exact duplicates      | `df.distinct()`                                              |
| **Duplicates**       | `dropDuplicates()`        | Remove duplicates by columns | `df.dropDuplicates(["customer_id"])`                         |
| **Validation**       | `when()`                  | Conditional validation       | `when(col("email").contains("@"), "Valid").otherwise("Invalid")` |

------

## 🎯 Module Overview

Real-world banking data is **messy, inconsistent, and incomplete**. Data arrives from multiple sources:

- **Legacy systems** with inconsistent formats
- **Manual data entry** with typos and variations
- **External data providers** with different standards
- **Mobile apps** with incomplete information

**The Reality of Banking Data:**

- Customer names like "JOHN SMITH", "john smith", " John Smith "
- Phone numbers as "(555) 123-4567", "555.123.4567", "5551234567"
- Email addresses with validation issues
- Missing values that need business-appropriate defaults

**Why Data Cleaning is Critical in Banking:**

- **Regulatory Compliance:** Clean data for accurate reporting
- **Customer Experience:** Consistent information across channels
- **Risk Management:** Accurate data for credit decisions
- **Analytics Accuracy:** Insights depend on data quality

------

## 1. String Functions and Text Standardization

### 1.1 Trimming Whitespace - The Foundation of Clean Data

**What is Whitespace and Why Does it Matter?**

Whitespace includes spaces, tabs, and other invisible characters that can break joins between tables, cause duplicate records, fail validation rules, and create reporting errors. In banking systems, customer data often exports with inconsistent spacing due to legacy system limitations.

**Business Impact Examples:**

- Marketing campaigns fail because " john.smith@email.com  " doesn't match database records
- Customer service sees "John" and "  John  " as different customers
- JOIN operations fail between systems with different spacing conventions
- Regulatory reports show incorrect customer counts due to apparent duplicates

```python
# script_13
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, lower, initcap, regexp_replace, count, when, coalesce, concat, lit, collect_set

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StringCleaningDemo") \
    .getOrCreate()

print("✅ Spark session initialized")

# Create sample data showing spacing issues from banking systems
messy_customer_data = [
    ("CUST001", "  John  ", "Smith   ", " john.smith@email.com  "),
    ("CUST002", " Sarah", "Johnson  ", "sarah.johnson@bank.com"),
    ("CUST003", "Mike   ", " Wilson", "   mike.wilson@company.org  "),
    ("CUST004", "  Lisa  ", "  Brown  ", "lisa.brown@service.net   ")
]

messy_df = spark.createDataFrame(
    messy_customer_data, 
    ["customer_id", "first_name", "last_name", "email"]
)

messy_df.show(truncate=False)
```

**The trim() Function Family:**

PySpark provides three trimming functions for different scenarios:

- **`trim()`** - Removes spaces from both beginning and end (most common)
- **`ltrim()`** - Removes only leading (left) spaces
- **`rtrim()`** - Removes only trailing (right) spaces

```python
# Basic trimming solution
cleaned_df = messy_df.withColumn("first_name", trim(col("first_name"))) \
                     .withColumn("last_name", trim(col("last_name"))) \
                     .withColumn("email", trim(col("email")))

cleaned_df.show(truncate=False)
```

**When to Use Each Trimming Function:**

- **trim()**: General cleanup for names, emails, addresses (most common use case)
- **ltrim()**: Remove leading spaces from account numbers while preserving trailing formatting
- **rtrim()**: Clean data exports that add trailing spaces for fixed-width formats

### 1.2 Case Standardization - Consistency Across Systems

**Why Case Standardization Matters:**

Different banking systems store data in different cases, creating integration challenges. Core banking systems often use UPPERCASE for legacy compatibility, CRM systems use Title Case for user interfaces, and web applications use lowercase for technical standards.

**Business Problems from Mixed Cases:**

- Customer "JOHN SMITH" in one system doesn't match "John Smith" in another
- Reports show duplicate customers in legacy banking systems due to case differences
- Automated workflows break when case doesn't match expected formats

**Banking Industry Case Standards:**

Different data types require different case formatting based on business rules:

- **Customer Names**: Title Case (John Smith) for professional appearance in communications
- **Email Addresses**: Lowercase (john.smith@email.com) for technical compatibility
- **Business Codes**: Uppercase (PREMIUM, STANDARD) for database consistency
- **Geographic Names**: Title Case (New York) for standard formatting

```python
# Sample data with mixed case issues
mixed_case_data = [
    ("CUST001", "JOHN SMITH", "JOHN.SMITH@EMAIL.COM", "PREMIUM", "NEW YORK"),
    ("CUST002", "sarah johnson", "Sarah.Johnson@Email.Com", "standard", "los angeles"),
    ("CUST003", "Mike WILSON", "mike.WILSON@service.NET", "Premium", "CHICAGO")
]

mixed_case_df = spark.createDataFrame(
    mixed_case_data,
    ["customer_id", "full_name", "email", "segment", "city"]
)

# Strategic case standardization
standardized_df = mixed_case_df.withColumn("full_name", initcap(col("full_name"))) \
                               .withColumn("email", lower(col("email"))) \
                               .withColumn("segment", upper(col("segment"))) \
                               .withColumn("city", initcap(col("city")))

standardized_df.show(truncate=False)
```

**Key Functions for Case Conversion:**

- **`initcap()`** - Converts to Title Case (first letter of each word capitalized)
- **`upper()`** - Converts to UPPERCASE
- **`lower()`** - Converts to lowercase

### 1.3 String Replacement and Pattern Cleaning

**Why String Replacement is Essential:**

Banking data contains formatting artifacts from different systems. Phone numbers arrive in various formats, Social Security Numbers have different separators, and account numbers use inconsistent prefixes.

**Common Banking Data Challenges:**

- Phone numbers: "(555) 123-4567" vs "555.123.4567" vs "555 123 4567"
- SSNs: "123-45-6789" vs "123.45.6789" vs "123456789"
- Account IDs: "ACC-001" vs "ACC001" vs "A001"

**Understanding Regular Expressions for Banking:**

Regular expressions (regex) are patterns that match and replace text. Here's how they work for banking data:

**Phone Number Pattern Breakdown:**

```python
regexp_replace(col("phone"), "[^0-9]", "")
```

- `[^0-9]`: Match any character that is NOT a digit
- `""`: Replace with empty string (removes the character)
- Result: Only digits remain

**Phone Number Formatting Pattern:**

```python
regexp_replace(col("phone_digits"), "(\\d{3})(\\d{3})(\\d{4})", "($1) $2-$3")
```

- `(\\d{3})`: Capture group 1 - exactly 3 digits
- `(\\d{3})`: Capture group 2 - exactly 3 digits
- `(\\d{4})`: Capture group 3 - exactly 4 digits
- `"($1) $2-$3"`: Format as (group1) group2-group3

```python
# Phone number standardization example
contact_df = spark.createDataFrame([
    ("CUST001", "(555) 123-4567"),
    ("CUST002", "555.987.6543"), 
    ("CUST003", "555 456 7890")
], ["customer_id", "phone"])

# Clean and standardize phone numbers
cleaned_phones = contact_df.withColumn("phone_digits", 
    regexp_replace(col("phone"), "[^0-9]", "")
).withColumn("phone_standard",
    regexp_replace(col("phone_digits"), "(\\d{3})(\\d{3})(\\d{4})", "($1) $2-$3")
)

cleaned_phones.show(truncate=False)
```

**Key String Replacement Functions:**

- **`regexp_replace()`** - Pattern-based replacement using regular expressions
- **`translate()`** - Simple character-by-character replacement
- **`substring()`** - Extract specific parts of strings
- **`lpad()`/`rpad()`** - Pad strings to consistent lengths

------



## 2. Handling Null Values and Missing Data

### 2.1 Understanding Null Values in Banking Context

**What are Null Values?**

Null values represent missing or unknown information. In banking, nulls occur for legitimate business reasons:

- Customer privacy (customers may not provide optional information)
- System integration issues (legacy systems may not have all fields)
- Data collection timing (information collected at different lifecycle stages)
- Regulatory requirements (some data only collected when required)

**Types of Missing Data in Banking:**

- **Missing at Random (MAR)**: Income not provided by privacy-conscious customers
- **Missing Completely at Random (MCAR)**: Technical errors during data entry
- **Missing Not at Random (MNAR)**: Customers with poor credit don't provide income data

```python
# Sample banking data with realistic null patterns
customer_data_nulls = [
    ("CUST001", "John", "Smith", "john@email.com", 125000, 750),
    ("CUST002", "Sarah", "Johnson", None, None, 720),  # Privacy-conscious customer
    ("CUST003", "Mike", "Wilson", "mike@email.com", 95000, None),  # New customer
    ("CUST004", "Lisa", "Brown", "lisa@email.com", 85000, 680),
    ("CUST005", None, "Davis", "davis@email.com", 75000, 700)  # Data entry error
]

null_df = spark.createDataFrame(
    customer_data_nulls,
    ["customer_id", "first_name", "last_name", "email", "annual_income", "credit_score"]
)

null_df.show()
```

### 2.2 Detecting and Analyzing Missing Data

**Null Detection Functions:**

PySpark provides several functions to work with null values:

- **`isNull()`** - Returns true if value is null
- **`isNotNull()`** - Returns true if value is not null
- **`count()`** - Can be combined with conditions to count nulls

```python
# Analyze missing data patterns
null_analysis = null_df.select([
    count(when(col(c).isNull(), c)).alias(f"{c}_nulls") 
    for c in null_df.columns
])

null_analysis.show()

# Calculate completeness percentages
total_records = null_df.count()
completeness = null_df.select([
    (count(col(c)) / total_records * 100).alias(f"{c}_complete_pct")
    for c in null_df.columns
])

completeness.show()
```

**Business Impact Analysis:**

Understanding why data is missing helps determine the best handling strategy:

- **High-value customers with missing data**: Priority for manual data collection
- **Systematic missing patterns**: May indicate system integration issues
- **Random missing data**: Can often be handled with statistical methods

### 2.3 Null Value Handling Strategies

**Strategy 1: Removing Null Values**

Use `dropna()` when complete data is essential for analysis:

```python
# Remove rows with any null values
complete_data = null_df.dropna()

# Remove rows with nulls in specific critical columns
essential_data = null_df.dropna(subset=["customer_id", "email"])

# Remove rows with nulls in multiple specified columns
contact_data = null_df.dropna(subset=["email", "income"])
```

**When to Use dropna():**

- Credit risk analysis requiring complete financial data
- Email marketing campaigns needing valid contact information
- Regulatory reporting with strict completeness requirements

**Strategy 2: Filling Null Values**

Use `fillna()` to replace nulls with business-appropriate defaults:

```python
# Fill nulls with specific values by column
filled_df = null_df.fillna({
    "first_name": "Unknown",
    "annual_income": 0,
    "credit_score": 500  # Conservative default for risk assessment
})

# Fill all string columns with same value
string_filled = null_df.fillna("Not Provided", subset=["first_name", "last_name", "email"])
```

**Business Rules for Default Values:**

- **Names**: "Unknown" or "Not Provided" for system processing
- **Income**: 0 or median income by segment for conservative estimates
- **Credit Score**: 500 (neutral score) or segment average
- **Contact Info**: "Not Available" to flag for manual collection

**Strategy 3: Advanced Null Handling with Business Logic**

Use `coalesce()` and conditional logic for sophisticated null handling:

```python
# Use coalesce to try multiple sources
enhanced_df = null_df.withColumn("display_name",
    coalesce(
        concat(col("first_name"), lit(" "), col("last_name")),
        col("email"),
        lit("Unknown Customer")
    )
)

# Conditional filling based on other column values
smart_filled = null_df.withColumn("estimated_income",
    when(col("annual_income").isNotNull(), col("annual_income"))
    .when(col("credit_score") >= 750, 100000)  # High credit suggests high income
    .when(col("credit_score") >= 650, 60000)   # Good credit suggests medium income
    .otherwise(40000)  # Conservative estimate for others
)
```

**Key Null Handling Functions:**

- **`dropna()`** - Remove rows with null values
- **`fillna()`** - Replace nulls with specified values
- **`coalesce()`** - Return first non-null value from multiple columns
- **`when().otherwise()`** - Conditional null replacement

------

## 3. Duplicate Detection and Removal

### 3.1 Understanding Duplicates in Banking Data

**Types of Duplicates in Banking:**

Duplicates occur in banking systems for various reasons:

- **Exact duplicates**: Identical records from system errors or double-processing
- **Near duplicates**: Same customer with slight variations (name spelling, formatting)
- **Business duplicates**: Same customer from different data sources or time periods

**Why Duplicates are Problematic:**

- Inflate customer counts in business reports
- Create multiple credit applications for same person
- Cause compliance issues with know-your-customer (KYC) regulations
- Lead to multiple marketing contacts to same customer

```python
# Sample data with different types of duplicates
duplicate_data = [
    ("CUST001", "John", "Smith", "john@email.com", "Premium"),
    ("CUST001", "John", "Smith", "john@email.com", "Premium"),  # Exact duplicate
    ("CUST002", "Sarah", "Johnson", "sarah@email.com", "Standard"),
    ("CUST003", "SARAH", "JOHNSON", "sarah@email.com", "Standard"),  # Case variation
    ("CUST004", "Mike", "Wilson", "mike@email.com", "Basic"),
    ("CUST005", "Michael", "Wilson", "mike@email.com", "Basic")  # Name variation
]

dup_df = spark.createDataFrame(
    duplicate_data,
    ["customer_id", "first_name", "last_name", "email", "segment"]
)

dup_df.show()
```

### 3.2 Duplicate Detection Strategies

**Strategy 1: Detecting Exact Duplicates**

Use `groupBy()` and `count()` to find exact duplicates:

```python
# Find exact duplicate records
exact_duplicates = dup_df.groupBy(*dup_df.columns).count().filter(col("count") > 1)
exact_duplicates.show()

# Count duplicate customer IDs
id_duplicates = dup_df.groupBy("customer_id").count().filter(col("count") > 1)
id_duplicates.show()
```

**Strategy 2: Detecting Business Logic Duplicates**

Identify duplicates based on business rules rather than exact matches:

```python
# Find customers with same email (potential duplicates)
email_duplicates = dup_df.groupBy("email").agg(
    count("customer_id").alias("customer_count"),
    collect_set("customer_id").alias("customer_ids"),
    collect_set("first_name").alias("first_names")
).filter(col("customer_count") > 1)

email_duplicates.show(truncate=False)
```

### 3.3 Duplicate Removal Strategies

**Strategy 1: Remove Exact Duplicates**

```python
# Remove exact duplicate rows
unique_df = dup_df.distinct()

# Remove duplicates based on specific columns
unique_customers = dup_df.dropDuplicates(["customer_id"])

# Keep most recent record for each customer (requires date column)
# latest_df = dup_df.dropDuplicates(["customer_id"], keep="last")
```

**Strategy 2: Advanced Duplicate Resolution with Business Rules**

Use window functions to implement sophisticated duplicate resolution:

```python
from pyspark.sql.window import Window

# Keep record with highest segment priority
segment_priority = {"Premium": 1, "Standard": 2, "Basic": 3}

# Create priority column
# Map segment to a numeric priority (smaller is better)
priority_df = dup_df.withColumn("segment_priority",
    when(col("segment") == "Premium", 1)
    .when(col("segment") == "Standard", 2)
    .otherwise(3)
)

# Use window function to rank by priority
# Define groups of rows that share the same email, and rank rows inside each email by segment_priority then customer_id
window_spec = Window.partitionBy("email").orderBy("segment_priority", "customer_id")

# Keep the first ranked row in each email group:
deduplicated = priority_df.withColumn("rank", row_number().over(window_spec)) \
                         .filter(col("rank") == 1) \
                         .drop("segment_priority", "rank")

deduplicated.show()
```

**Key Duplicate Handling Functions:**

- **`distinct()`** - Remove exact duplicate rows
- **`dropDuplicates()`** - Remove duplicates based on specified columns
- **`groupBy().count()`** - Identify duplicate patterns
- **Window functions** - Advanced duplicate resolution with business rules

------

## 4. Data Validation and Quality Scoring

### 4.1 Implementing Data Validation Rules

**What is Data Validation?**

Data validation ensures data meets business rules and quality standards. In banking, validation rules prevent processing of incorrect data that could lead to compliance violations or financial losses.

**Common Banking Validation Rules:**

- Email addresses must contain "@" and "."
- Credit scores must be between 300-850
- Phone numbers must have 10 digits
- Customer IDs must follow specific format patterns
- Income values must be positive

```python
# Sample data with validation issues
validation_data = [
    ("CUST001", "John", "Smith", "john@email.com", "555-123-4567", 75000, 750),
    ("CUST002", "Sarah", "Johnson", "invalid-email", "555-234-5678", -50000, 720),  # Invalid email, negative income
    ("CUST003", "Mike", "Wilson", "mike@email.com", "123", 95000, 950),  # Invalid phone, invalid credit score
    ("CUST004", "Lisa", "Brown", "lisa@email.com", "555-456-7890", 85000, 680)
]

val_df = spark.createDataFrame(
    validation_data,
    ["customer_id", "first_name", "last_name", "email", "phone", "annual_income", "credit_score"]
)
```

**Implementing Validation Rules:**

```python
# Email validation
email_valid = val_df.withColumn("email_valid",
    col("email").contains("@") & col("email").contains(".")
)

# Phone validation (10 digits)
phone_valid = email_valid.withColumn("phone_valid",
    length(regexp_replace(col("phone"), "[^0-9]", "")) == 10
)

# Credit score validation (300-850 range)
credit_valid = phone_valid.withColumn("credit_score_valid",
    col("credit_score").between(300, 850)
)

# Income validation (positive values)
income_valid = credit_valid.withColumn("income_valid",
    col("annual_income") > 0
)

# Show validation results
validation_summary = income_valid.select(
    "customer_id", "email_valid", "phone_valid", "credit_score_valid", "income_valid"
)
validation_summary.show()
```

### 4.2 Creating Data Quality Scores

**Why Quality Scoring Matters:**

Data quality scores help prioritize data cleanup efforts and identify customers requiring manual review. Higher quality scores indicate more reliable data for business decisions.

```python
# Create comprehensive quality score
quality_scored = income_valid.withColumn("quality_score",
    # Add points for each valid field
    when(col("first_name").isNotNull() & (col("first_name") != ""), 10).otherwise(0) +
    when(col("last_name").isNotNull() & (col("last_name") != ""), 10).otherwise(0) +
    when(col("email_valid"), 20).otherwise(0) +
    when(col("phone_valid"), 15).otherwise(0) +
    when(col("income_valid"), 25).otherwise(0) +
    when(col("credit_score_valid"), 20).otherwise(0)
).withColumn("quality_grade",
    when(col("quality_score") >= 90, "A")
    .when(col("quality_score") >= 80, "B")
    .when(col("quality_score") >= 70, "C")
    .when(col("quality_score") >= 60, "D")
    .otherwise("F")
)

quality_scored.select("customer_id", "quality_score", "quality_grade").show()
```

**Quality Score Components:**

- **Name completeness**: 20 points (10 each for first/last name)
- **Email validity**: 20 points for valid format
- **Phone validity**: 15 points for valid format
- **Income validity**: 25 points for positive value
- **Credit score validity**: 20 points for valid range
- **Total possible**: 100 points

------

## 5. Comprehensive Data Cleaning Pipeline

### 5.1 Building a Production-Ready Cleaning Function

**Why Create a Cleaning Pipeline?**

A comprehensive cleaning pipeline ensures consistent data processing across all banking applications. It combines all cleaning techniques into a reusable function that can be applied to any customer dataset.

```python
def clean_banking_customer_data(df):
    """
    Comprehensive customer data cleaning pipeline for banking applications
    
    Applies the following cleaning steps:
    1. Trim whitespace from all string columns
    2. Standardize case formatting
    3. Clean and format phone numbers
    4. Validate and clean email addresses
    5. Handle null values with business rules
    6. Create data quality scores
    7. Flag records requiring manual review
    """
    
    # Step 1: Basic string cleaning
    cleaned = df.withColumn("first_name", initcap(trim(col("first_name")))) \
                .withColumn("last_name", initcap(trim(col("last_name")))) \
                .withColumn("email", lower(trim(col("email"))))
    
    # Step 2: Phone number standardization
    phone_cleaned = cleaned.withColumn("phone_digits", 
        regexp_replace(col("phone"), "[^0-9]", "")
    ).withColumn("phone_standardized",
        when(length(col("phone_digits")) == 10,
             regexp_replace(col("phone_digits"), "(\\d{3})(\\d{3})(\\d{4})", "($1) $2-$3"))
        .otherwise("INVALID")
    )
    
    # Step 3: Null value handling
    null_handled = phone_cleaned.fillna({
        "first_name": "Unknown",
        "last_name": "Unknown", 
        "email": "noemail@unknown.com",
        "annual_income": 0
    })
    
    # Step 4: Data validation
    validated = null_handled.withColumn("email_valid",
        col("email").contains("@") & col("email").contains(".")
    ).withColumn("phone_valid",
        col("phone_standardized") != "INVALID"
    ).withColumn("income_valid",
        col("annual_income") >= 0
    ).withColumn("credit_score_valid",
        col("credit_score").between(300, 850)
    )
    
    # Step 5: Quality scoring
    final_df = validated.withColumn("data_quality_score",
        when(col("first_name") != "Unknown", 15).otherwise(0) +
        when(col("last_name") != "Unknown", 15).otherwise(0) +
        when(col("email_valid"), 25).otherwise(0) +
        when(col("phone_valid"), 20).otherwise(0) +
        when(col("income_valid"), 15).otherwise(0) +
        when(col("credit_score_valid"), 10).otherwise(0)
    ).withColumn("requires_review",
        col("data_quality_score") < 70
    )
    
    return final_df

# Apply the cleaning pipeline
cleaned_customer_data = clean_banking_customer_data(val_df)
cleaned_customer_data.select("customer_id", "first_name", "email", "phone_standardized", 
                            "data_quality_score", "requires_review").show()
```

### 5.2 Data Quality Monitoring and Reporting

**Creating Quality Reports:**

```python
# Generate data quality summary report
quality_report = cleaned_customer_data.select(
    count("*").alias("total_records"),
    count(when(col("email_valid"), 1)).alias("valid_emails"),
    count(when(col("phone_valid"), 1)).alias("valid_phones"),
    count(when(col("requires_review"), 1)).alias("needs_review"),
    avg("data_quality_score").alias("avg_quality_score")
)

quality_report.show()

# Quality grade distribution
grade_distribution = cleaned_customer_data.withColumn("quality_grade",
    when(col("data_quality_score") >= 90, "A")
    .when(col("data_quality_score") >= 80, "B")
    .when(col("data_quality_score") >= 70, "C")
    .otherwise("D")
).groupBy("quality_grade").count().orderBy("quality_grade")

grade_distribution.show()
```

------

## 6. Common Pitfalls and Best Practices

### 6.1 Data Cleaning Performance Optimization

**Performance Best Practices:**

```python
# ✅ DO: Cache DataFrames that will be used multiple times
frequently_used_df = messy_df.cache()

# ✅ DO: Chain operations efficiently
efficient_cleaning = messy_df \
    .withColumn("name_clean", trim(col("name"))) \
    .withColumn("email_clean", lower(col("email"))) \
    .withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", ""))

# ✅ DO: Use native functions instead of UDFs when possible
# Native functions are optimized and faster

# ❌ AVOID: Multiple separate operations that could be chained
# less_efficient = messy_df.withColumn("name_clean", trim(col("name")))
# less_efficient = less_efficient.withColumn("email_clean", lower(col("email")))
```

### 6.2 Data Quality Validation Strategies

**Validation Best Practices:**

```python
# ✅ DO: Validate data at multiple stages
def validate_at_each_step(df, step_name):
    """Validate data quality at each cleaning step"""
    null_count = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    print(f"Validation after {step_name}:")
    null_count.show()
    return df

# ✅ DO: Create business-specific validation rules
def validate_banking_rules(df):
    """Validate banking-specific business rules"""
    invalid_records = df.filter(
        (col("credit_score") < 300) | 
        (col("credit_score") > 850) |
        (col("annual_income") < 0)
    )
    
    if invalid_records.count() > 0:
        print("⚠️ Invalid records found:")
        invalid_records.show()
    
    return df
```

### 6.3 Error Handling and Data Recovery

**Robust Error Handling:**

```python
def safe_data_cleaning(df):
    """Data cleaning with error handling and recovery"""
    try:
        # Attempt primary cleaning strategy
        cleaned_df = clean_banking_customer_data(df)
        return cleaned_df
        
    except Exception as e:
        print(f"Primary cleaning failed: {e}")
        print("Applying fallback cleaning strategy...")
        
        # Fallback: Basic cleaning only
        fallback_df = df.withColumn("first_name", trim(col("first_name"))) \
                        .withColumn("email", lower(col("email"))) \
                        .fillna("UNKNOWN")
        
        return fallback_df

# Use safe cleaning function
safely_cleaned = safe_data_cleaning(messy_df)
```

------

## 7. Hands-On Practice Exercise

### 7.1 Banking Data Quality Challenge

**Challenge Overview:**

You've been given a dataset of new customer applications with significant data quality issues. Your task is to clean the data and prepare it for the loan approval system.

```python
# Challenge dataset - messy new customer applications
challenge_data = [
    ("APP001", "  JOHN MICHAEL  ", "  SMITH  ", " john.smith@EMAIL.com  ", "(555) 123-4567", " 125000 ", "750"),
    ("APP002", "sarah", "johnson", "SARAH@email.COM", "555.234.5678", "invalid", "720"),
    ("APP003", "Mike", None, "mike@email.com", "5553456789", "95000", "680"),
    ("APP004", "", "Brown", "lisa.brown@", "555-456-7890", "-50000", "850"),
    ("APP005", "David", "Davis", None, "555 567 8901", "75000", "300")
]

challenge_df = spark.createDataFrame(
    challenge_data,
    ["application_id", "first_name", "last_name", "email", "phone", "annual_income_str", "credit_score_str"]
)

challenge_df.show(truncate=False)
```



**Your Tasks:**

1. **Clean Names**: Remove whitespace, standardize case
2. **Fix Emails**: Standardize format, validate structure
3. **Standardize Phones**: Convert to (XXX) XXX-XXXX format
4. **Handle Income**: Convert to numeric, handle invalid values
5. **Process Credit Scores**: Convert to integer, validate range
6. **Handle Missing Data**: Apply appropriate business rules
7. **Create Quality Score**: Assess overall data quality
8. **Generate Report**: Summarize cleaning results

**Solution Framework:**

```python
# Write your code here
```



------

## Module Summary and Next Steps

### What You've Mastered ✅

**Technical Skills:**

1. **String Standardization** - Trimming, case conversion, and format consistency
2. **Pattern-Based Cleaning** - Regular expressions for phone numbers, emails, and IDs
3. **Null Value Management** - Detection, analysis, and business-appropriate handling
4. **Duplicate Resolution** - Identification and removal strategies
5. **Data Validation** - Business rule implementation and quality scoring
6. **Pipeline Development** - Comprehensive, reusable cleaning functions

**Business Applications:**

- **Customer Data Integration** - Merging data from multiple banking systems
- **Regulatory Compliance** - Ensuring data meets reporting standards
- **Risk Management** - Clean data for accurate credit decisions
- **Operational Efficiency** - Automated data quality processes

### Key Data Cleaning Patterns You Can Now Implement 📈

```python
# PATTERN 1: Complete Customer Data Cleaning
cleaned_customers = raw_customers \
    .withColumn("name", initcap(trim(col("full_name")))) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("phone", standardize_phone(col("phone"))) \
    .fillna(default_values) \
    .withColumn("quality_score", calculate_quality_score())

# PATTERN 2: Multi-Source Data Integration
integrated_data = source1_df.union(source2_df) \
    .dropDuplicates(["customer_id"]) \
    .transform(clean_banking_customer_data) \
    .filter(col("quality_score") >= minimum_threshold)

# PATTERN 3: Incremental Data Quality Improvement
quality_enhanced = existing_data \
    .join(new_data_cleaned, "customer_id", "left") \
    .select(coalesce(col("new_email"), col("old_email")).alias("email")) \
    .withColumn("last_updated", current_timestamp())
```

### Critical Success Factors 🎯

**For Production Data Cleaning:**

1. **Understand Business Context** - Know why data is missing or inconsistent
2. **Implement Gradually** - Start with critical fields, expand systematically
3. **Monitor Quality Continuously** - Track cleaning effectiveness over time
4. **Document Decisions** - Record business rules and assumptions
5. **Plan for Exceptions** - Handle edge cases gracefully

