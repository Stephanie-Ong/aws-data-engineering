# Module 1 - DataFrame Essentials

**Learning Time:** 60-90 minutes
 **Prerequisites:** Basic Python knowledge, familiarity with SQL concepts
 **Learning Objectives:** Master PySpark DataFrame fundamentals for banking data analysis

------

## Introduction to Apache Spark

Apache Spark is a unified analytics engine for large-scale data processing that has revolutionized how we handle big data. Unlike traditional batch processing systems, Spark provides fast, in-memory data processing capabilities that can be 100x faster than Hadoop MapReduce for certain workloads.

**Why Spark Matters in Modern Data Engineering:**

- **Speed**: In-memory computing eliminates the need to write intermediate results to disk
- **Versatility**: Handles batch processing, real-time streaming, machine learning, and graph processing
- **Ease of Use**: High-level APIs in Python, Scala, Java, and R
- **Fault Tolerance**: Automatically recovers from node failures
- **Scalability**: Runs on everything from laptops to thousands of nodes

## Spark Architecture Deep Dive

### Core Components Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        SPARK ECOSYSTEM                          │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │
│  │   Spark     │  │   Spark     │  │    MLlib    │  │ GraphX  │ │
│  │    SQL      │  │ Streaming   │  │ (ML Library)│  │(Graphs) │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘ │
├─────────────────────────────────────────────────────────────────┤
│                       SPARK CORE                                │
│              (RDDs, Tasks, Caching, Scheduling)                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │
│  │ Standalone  │  │  Hadoop     │  │ Kubernetes  │  │  Mesos  │ │
│  │  Scheduler  │  │    YARN     │  │             │  │         │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

> **Apache Mesos** is a cluster manager that abstracts CPU, memory, and storage across machines, making your datacenter look like one big OS.
>  It lets multiple frameworks (e.g., Spark, Marathon) share resources safely via fine-grained scheduling and container/cgroups isolation.
>
> **Apache Hadoop YARN** (Yet Another Resource Negotiator) is Hadoop’s cluster resource manager that allocates CPU and memory across nodes for applications.
>  It lets multiple engines (MapReduce, Spark, Tez, etc.) share a Hadoop cluster via ResourceManager + NodeManager, container-based execution, and pluggable schedulers (Capacity/Fair).
>
> **Kubernetes** is an open-source container orchestration platform that automates deploying, scaling, and managing applications.
>  It uses a declarative API and a control plane (API server, scheduler, controller manager, etcd) to run Pods on Nodes via kubelet, with self-healing, rolling updates, Services/Ingress, and autoscaling.



### Spark Application Architecture

In the real-world Kubernetes environment, when you run a Spark application through Airflow, here's what happens:

```
┌─────────────────────────────────────────────────────────────────┐
│                     YOUR KUBERNETES CLUSTER                     │
│                                                                 │
│  ┌─────────────────┐                                            │
│  │  Airflow Pod    │                                            │
│  │  ┌───────────┐  │    Creates and Manages                     │
│  │  │   DAG     │  │          ↓                                 │
│  │  │ Task      │  │    ┌─────────────────┐                     │
│  │  └───────────┘  │    │   Spark Driver  │                     │
│  └─────────────────┘    │      Pod        │                     │
│                         │  ┌───────────┐  │                     │
│                         │  │  Driver   │  │                     │
│                         │  │ Program   │  │                     │
│                         │  └───────────┘  │                     │
│                         │  ┌───────────┐  │                     │
│                         │  │  Spark    │  │                     │
│                         │  │ Context   │  │                     │
│                         │  └───────────┘  │                     │
│                         └─────────────────┘                     │
│                                   │                             │
│                         Creates and Coordinates                 │
│                                   ↓                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌────────────┐   │
│  │ Executor Pod 1  │    │ Executor Pod 2  │    │Executor... │   │
│  │ ┌─────────────┐ │    │ ┌─────────────┐ │    │            │   │
│  │ │   Tasks     │ │    │ │   Tasks     │ │    │   Tasks    │   │
│  │ │             │ │    │ │             │ │    │            │   │
│  │ └─────────────┘ │    │ └─────────────┘ │    │            │   │
│  │ ┌─────────────┐ │    │ ┌─────────────┐ │    │            │   │
│  │ │   Cache     │ │    │ │   Cache     │ │    │   Cache    │   │
│  │ └─────────────┘ │    │ └─────────────┘ │    │            │   │
│  └─────────────────┘    └─────────────────┘    └────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

**1. Driver Program (Driver Pod)**

- **Purpose**: The main control program that runs your application's main() function
- **Responsibilities**:
  - Creates SparkContext and coordinates the overall execution
  - Analyzes your code and creates a logical plan
  - Converts operations into tasks
  - Schedules tasks across executors
  - Collects results from executors
- **In Your Infrastructure**: Runs as a separate Kubernetes pod managed by your Airflow task

**2. Spark Context**

- **Purpose**: The entry point to all Spark functionality
- **Responsibilities**:
  - Connects to the cluster manager (Kubernetes in your case)
  - Acquires executors on worker nodes
  - Sends application code to executors
  - Sends tasks to executors for execution

**3. Cluster Manager (Kubernetes)**

- **Purpose**: Manages resources across the cluster
- **Responsibilities**:
  - Allocates pods for driver and executors
  - Manages resource allocation (CPU, memory)
  - Handles pod failures and restarts
  - Scales executor pods based on demand

**4. Executors (Executor Pods)**

- **Purpose**: Distributed agents responsible for executing tasks
- **Responsibilities**:
  - Run individual tasks sent by the driver
  - Store data for caching and intermediate results
  - Report task status and results back to driver
  - Provide in-memory storage for RDDs and DataFrames

**5. Tasks**

- **Purpose**: Individual units of work
- **Characteristics**:
  - Smallest unit of execution in Spark
  - Applied to a single data partition
  - Run in parallel across multiple executors

## Understanding RDDs (Resilient Distributed Datasets)

### What Makes RDDs Special

RDDs are the fundamental data abstraction in Spark. Think of them as distributed collections that can be processed in parallel across your cluster.

**Key Characteristics:**

1. **Resilient (Fault-Tolerant)**:

   ```
   Original RDD → Transformation → Transformed RDD
        ↓              ↓                ↓
   Lineage Graph: filter() → map() → reduceByKey()
   
   If Executor Pod 2 fails:
   ┌─────────────────────────────────────────────┐
   │ Spark automatically rebuilds lost data      │
   │ using the lineage graph (DAG of operations) │
   └─────────────────────────────────────────────┘
   ```

2. **Distributed**:

   ```
   Large Dataset
        ↓
   ┌─────────────────────────────────────────────┐
   │         Automatic Partitioning              │
   └─────────────────────────────────────────────┘
        ↓
   ┌─────────┐  ┌─────────┐  ┌─────────┐
   │Partition│  │Partition│  │Partition│
   │    1    │  │    2    │  │    3    │
   │ Pod 1   │  │ Pod 2   │  │ Pod 3   │
   └─────────┘  └─────────┘  └─────────┘
   ```

3. **Immutable**:

   ```mermaid
   graph TD
       A[RDD1: Original Data<br/>1,2,3,4,5,6,7,8,9,10] --> B[filter x > 5]
       A --> C[map x * 2]
       
       B --> D[RDD2: Filtered Data<br/>6,7,8,9,10]
       C --> E[RDD3: Transformed Data<br/>2,4,6,8,10,12,14,16,18,20]
       
       D --> F[map x * 10]
       E --> G[filter x < 15]
       
       F --> H[RDD4: Final Result<br/>60,70,80,90,100]
       G --> I[RDD5: Final Result<br/>2,4,6,8,10,12,14]
       
       classDef original fill:#FFE6CC,stroke:#D79B00,stroke-width:3px
       classDef transformation fill:#E1F5FE,stroke:#0277BD,stroke-width:2px
       classDef result fill:#E8F5E8,stroke:#2E7D32,stroke-width:2px
       classDef lineage fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px
       classDef recovery fill:#FFEBEE,stroke:#C62828,stroke-width:2px
       class A original
       class B,C,F,G transformation
       class D,E,H,I result
       class J,K lineage
       class L,M recovery
   ```

   The diagram above shows how **RDD immutability** works in Apache Spark. Every transformation creates a new RDD while preserving the original data structure.

   ### Key Principles

   #### 1. **Transformation Chain**

   ```python
   # Spark 01
   from pyspark import SparkContext
   
   # Create a SparkContext object, which is the entry point for using Spark functionality.
   # "local" means Spark will run locally on your machine (not on a cluster).
   # The second argument is the application name shown in Spark UI.
   sc = SparkContext("local", "RDD Example")
   
   # sc.parallelize() distributes a Python collection (here, numbers 1 to 10) as an RDD (Resilient Distributed Dataset).
   # This allows Spark to process the data in parallel across multiple nodes or cores.
   rdd1 = sc.parallelize(range(1, 11))  # Create an RDD with numbers 1 to 10
   
   # rdd1.filter() returns a new RDD containing only elements where the lambda returns True.
   # Here, it keeps numbers greater than 5.
   rdd2 = rdd1.filter(lambda x: x > 5)  # Filter: keep values > 5
   
   # rdd1.map() returns a new RDD by applying the lambda to each element.
   # Here, it multiplies each number by 2.
   rdd3 = rdd1.map(lambda x: x * 2)     # Map: multiply each value by 2
   
   # .collect() brings the RDD data back to the driver as a Python list (useful for small datasets only).
   print(f"rdd1: {rdd1.collect()}")    # Output: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
   print(f"rdd2: {rdd2.collect()}")    # Output: [6, 7, 8, 9, 10]
   print(f"rdd3: {rdd3.collect()}")    # Output: [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
   
   # Keep the Spark application running to allow inspection in the Spark UI at http://localhost:4040
   # Press Enter in the console to stop Spark and exit the program.
   input("Press Enter to stop Spark and exit...")
   # Always stop the SparkContext when your job is done to free resources.
   sc.stop()
   ```

   Notice that `rdd2` and `rdd3` both derive from `rdd1`, but `rdd1` remains unchanged.

   

   #### 2. **Lineage Graph (DAG)**

   Spark maintains a **Directed Acyclic Graph (DAG)** showing:

   - **Dependencies**: Which RDDs depend on which others

   - **Transformations**: The operations applied at each step

   - **Recovery Path**: How to recreate any lost RDD

     

   #### 3.**Lazy Evaluation**

   ```python
   rdd2 = rdd1.filter(lambda x: x > 5)    # Nothing executed yet!
   rdd4 = rdd2.map(lambda x: x * 10)      # Still nothing executed!
   rdd4.collect()                         # NOW everything executes
   ```

   Transformations are **lazy** - they only execute when an **action** (like `collect()`) is called.

   ```mermaid
   graph TD
       
       subgraph Lineage["Lineage Tracking"]
           J[RDD1 → filter → RDD2 → map → RDD4]
           K[RDD1 → map → RDD3 → filter → RDD5]
       end
       
       subgraph Recovery["Fault Recovery"]
           L[If RDD4 fails:<br/>Replay RDD1 → filter → map]
           M[If RDD5 fails:<br/>Replay RDD1 → map → filter]
       end
   		classDef original fill:#FFE6CC,stroke:#D79B00,stroke-width:3px
       classDef transformation fill:#E1F5FE,stroke:#0277BD,stroke-width:2px
       classDef result fill:#E8F5E8,stroke:#2E7D32,stroke-width:2px
       classDef lineage fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px
       classDef recovery fill:#FFEBEE,stroke:#C62828,stroke-width:2px
       
       class A original
       class B,C,F,G transformation
       class D,E,H,I result
       class J,K lineage
       class L,M recovery
   ```

   ### Benefits of Immutability

   #### **Fault Tolerance**

   If a worker node crashes and loses RDD4:

   - Spark checks the lineage: `RDD1 → filter → map → RDD4`
   - It replays only the lost computations
   - No need to recompute the entire job

   #### **Parallel Safety**

   Multiple threads can safely read from the same RDD simultaneously because it cannot be modified.

   #### **Caching Strategy**

   ```python
   rdd1 = sc.textFile("huge-file.txt")
   rdd2 = rdd1.filter(lambda line: "ERROR" in line)
   rdd2.cache()  # Store RDD2 in memory for reuse
   errorCount = rdd2.count()
   errorSample = rdd2.take(10)  # Reuses cached RDD2
   ```

   #### **Debugging & Optimization**

   - **Checkpoint**: Save intermediate RDDs to disk for faster recovery
   - **Persist**: Keep frequently-used RDDs in memory
   - **Lineage Pruning**: Spark can optimize the execution plan

   ### Memory Management

   **Question**: Don't all these RDDs consume huge amounts of memory?

   **Answer**: No! Spark is smart about this:

   - **Lazy evaluation** means intermediate RDDs exist only as execution plans
   - **Garbage collection** removes unused RDDs automatically
   - **Caching** is explicit - you choose what to keep in memory
   - **Spill to disk** when memory is full

   ### Real-World Example

   ```scala
   # Processing web server logs
   logs = sc.textFile("access.log")                    # RDD1
   errors = logs.filter(lambda line: "ERROR" in line)  # RDD2  
   errorIPs = errors.map(lambda line: line.split(" ")[0])  # RDD3
   uniqueIPs = errorIPs.distinct()                     # RDD4
   ipCount = uniqueIPs.count()                         # Action - triggers execution
   # If any step fails, Spark can replay from the lineage which means Spark can recreate lost data by re-executing the transformation steps.:
   # logs → filter → map → distinct → count
   ```

   This immutable approach makes Spark both **reliable** and **performant** for large-scale distributed computing!

### RDD Operations Deep Dive

**Transformations (Lazy Evaluation)**:
These operations define what you want to do but don't execute immediately:

```python
# Example: Processing sales data
sales_rdd = spark_context.textFile("/data/sales.csv")

# These are all transformations - nothing executes yet!
header_removed = sales_rdd.filter(lambda line: not line.startswith("transaction_id"))
sales_parsed = header_removed.map(lambda line: line.split(","))
high_value = sales_parsed.filter(lambda sale: float(sale[5]) > 1000)  # Price > $1000

# Still no execution! Spark builds a computation graph
```

**Actions (Trigger Execution)**:
These operations actually execute the computation:

```python
# NOW Spark executes the entire pipeline!
expensive_sales = high_value.collect()  # Brings all data to driver
count = high_value.count()              # Counts records
first_sale = high_value.first()         # Gets first record
```

**Why This Matters in Your Environment:**

- **Resource Efficiency**: Kubernetes pods are only used when actions are called
- **Optimization**: Spark can optimize the entire pipeline before execution
- **Fault Recovery**: If a pod fails during execution, only affected partitions are recomputed



## DataFrames: The Modern Spark Interface

### DataFrames vs RDDs

While RDDs give you low-level control, DataFrames provide a higher-level, more intuitive interface:

```
┌─────────────────────────────────────────────────────────────────┐
│                           RDD LEVEL                             │
│  - Low-level API                                                │
│  - Manual optimization                                          │
│  - No schema enforcement                                        │
│  - Direct partition control                                     │
└─────────────────────────────────────────────────────────────────┘
                                ↑
                         Abstraction Layer
                                ↓
┌─────────────────────────────────────────────────────────────────┐
│                       DATAFRAME LEVEL                           │
│  - High-level API (SQL-like)                                    │
│  - Automatic optimization (Catalyst)                            │
│  - Schema enforcement and validation                            │
│  - Query planning and optimization                              │
└─────────────────────────────────────────────────────────────────┘
```

### Catalyst Optimizer in Action

When you write DataFrame operations, Spark's Catalyst optimizer works behind the scenes:

Your DataFrame Code:

```python
df.filter(col("price") > 1000).select("customer_id", "price").groupBy("customer_id").sum("price")
```

```text
↓ Catalyst Optimizer Analysis ↓

Logical Plan:

1. Read data
2. Filter by price > 1000
3. Select only needed columns
4. Group by customer_id
5. Sum prices

↓ Optimization ↓

Optimized Physical Plan:

1. Read data (only customer_id and price columns - Column Pruning)
2. Filter by price > 1000 (Predicate Pushdown)
3. Group and sum in parallel across partitions
4. Combine results

↓ Execution in Your Kubernetes Cluster ↓

Multiple executor pods process different partitions simultaneously
```



## 📋 Module 1 Functions Reference Table

| **Section**        | **Function**            | **Purpose**                      | **Syntax Example**                                           |
| ------------------ | ----------------------- | -------------------------------- | ------------------------------------------------------------ |
| **Setup**          | `SparkSession.builder`  | Create Spark engine              | `spark = SparkSession.builder.appName("name").getOrCreate()` |
| **Creation**       | `createDataFrame()`     | Convert Python data to DataFrame | `spark.createDataFrame(data, columns)`                       |
| **File I/O**       | `.read.csv()`           | Load CSV files                   | `spark.read.option("header", "true").csv("file.csv")`        |
| **File I/O**       | `.read.json()`          | Load JSON files                  | `spark.read.json("file.json")`                               |
| **File I/O**       | `.read.parquet()`       | Load Parquet files               | `spark.read.parquet("file.parquet")`                         |
| **Schema**         | `StructType()`          | Define data structure            | `StructType([StructField("name", StringType(), True)])`      |
| **Inspection**     | `.show()`               | Display data                     | `df.show(5)` - shows 5 rows                                  |
| **Inspection**     | `.printSchema()`        | Show structure                   | `df.printSchema()`                                           |
| **Inspection**     | `.count()`              | Count rows                       | `df.count()`                                                 |
| **Selection**      | `.select()`             | Choose columns                   | `df.select("col1", "col2")`                                  |
| **Selection**      | `col()`                 | Reference column                 | `col("column_name")`                                         |
| **Selection**      | `.alias()`              | Rename column                    | `col("old_name").alias("new_name")`                          |
| **Filtering**      | `.filter()`             | Filter rows                      | `df.filter(col("amount") > 1000)`                            |
| **Sorting**        | `.orderBy()`            | Sort data                        | `df.orderBy("column")` or `df.orderBy(desc("column"))`       |
| **Sorting**        | `desc()`, `asc()`       | Sort direction                   | `desc("column")` for descending                              |
| **Transformation** | `.withColumn()`         | Add/modify column                | `df.withColumn("new_col", expression)`                       |
| **Conditional**    | `when()`, `otherwise()` | If-then logic                    | `when(condition, value).otherwise(default)`                  |
| **String**         | `concat()`, `lit()`     | Combine text                     | `concat(col("first"), lit(" "), col("last"))`                |
| **Joins**          | `.join()`               | Combine DataFrames               | `df1.join(df2, "key", "inner")`                              |
| **Aggregation**    | `.groupBy()`            | Group data                       | `df.groupBy("column").agg(...)`                              |
| **Aggregation**    | `.agg()`                | Calculate metrics                | `.agg(count("*"), avg("amount"))`                            |
| **Memory**         | `.cache()`              | Store in memory                  | `df.cache()`                                                 |
| **Memory**         | `.fillna()`             | Replace nulls                    | `df.fillna(0)`                                               |

------

## 🎯 Module Overview

In banking, we constantly work with different types of data spread across multiple systems:

- **Customer information** (names, addresses, demographics)
- **Account details** (balances, types, status)
- **Transaction records** (payments, deposits, transfers)
- **Product information** (loans, credit cards, investments)

Think of DataFrames as **smart spreadsheets** that can:

- Handle millions of rows without crashing
- Automatically distribute work across multiple computers
- Provide built-in functions for complex calculations
- Connect to various data sources (files, databases, APIs)

**Real-world analogy:** If Excel is like a calculator, PySpark DataFrames are like having a team of accountants working simultaneously on different parts of your data.

------

## 1. Setting Up Your Banking Analytics Environment

### 1.1 The Spark Session - Your Data Processing Engine

**What is a Spark Session?** A Spark Session is like the "engine" of your data processing car. Just as you need to start your car's engine before driving, you need to create a Spark Session before working with data. This engine coordinates all the data processing work across your computer (or multiple computers in production).

**Why do we need it?**

- It's the **entry point** to all Spark functionality
- It **manages resources** (memory, CPU cores)
- It **coordinates** data processing across multiple machines
- It provides the **interface** to create and manipulate DataFrames

**When do we create it?**

- At the **beginning** of every PySpark program
- **Once per application** - don't create multiple sessions
- **Before** any data operations

```python
# Script 02
# WHAT: Start your PySpark data processing engine
# WHY: Every PySpark program needs this to work with data
# WHEN: First thing in your script, before any data operations

from pyspark.sql import SparkSession

# Create your data processing engine
spark = SparkSession.builder \
    .appName("BankingAnalytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print(f"✅ Spark session started - Version: {spark.version}")

# Let's break down each part:
# .builder = Start building a Spark session
# .appName() = Give your application a name (shows up in Spark UI)
# .config() = Set performance optimizations. It enables Adaptive Query Execution (AQE) in Spark SQL, letting Spark re-optimize a query at runtime using actual stats.
# .getOrCreate() = Create new session OR use existing one

# BUSINESS CONTEXT: 
# Like opening Excel, but this "Excel" can handle billions of rows
# and automatically use all your computer's processing power

# BEST PRACTICE: 
# Always give your app a descriptive name for monitoring and debugging
```

**Real-world Banking Example:** Imagine you're a bank analyst who needs to process daily transaction files. Your *Spark Session* is like having a smart assistant that:

- Automatically uses all available computer cores
- Manages memory efficiently
- Can scale to process files of any size
- Provides detailed progress reports

### 1.2 Understanding Shared Utilities

**What are Utility Functions?** Utility functions are **reusable pieces of code** that perform common tasks. For example, instead of writing the same data creation code in every example, we create functions once and reuse them everywhere.

**Why do we need them?** In some projects, you'll often need the same sample data for:

- **Testing** your analysis code
- **Learning** new concepts with familiar data
- **Demonstrating** results to stakeholders
- **Debugging** when something goes wrong

**The Problem Without Utilities:**

```python
# ❌ BAD: Repeating the same data creation in every script
# Script 1
customers_data = [("CUST001", "John", "Smith", ...)]  # 20 lines of data
# Script 2  
customers_data = [("CUST001", "John", "Smith", ...)]  # Same 20 lines again!
# Script 3
customers_data = [("CUST001", "John", "Smith", ...)]  # Repeated again!
```

**The Solution With Utilities:**

```python
# ✅ GOOD: Write once, use everywhere
# utils/banking_data.py - Our reusable data factory

def create_customers_data(spark):
    """
    Create sample customer dataset for learning and testing
    
    WHY THIS FUNCTION EXISTS:
    - Provides consistent test data across all examples
    - Saves time by avoiding repetitive data creation
    - Makes examples focus on concepts, not data setup
    - Easy to modify data in one place if needed
    
    REAL-WORLD USAGE:
    - Testing data pipelines before using real customer data
    - Training new team members with safe, anonymized data
    - Debugging data processing logic
    - Creating reproducible examples for documentation
    """
    
    # Sample customer data representing different banking segments
    data = [
        # (ID, First, Last, Email, Segment, Income, Credit Score)
        ("CUST001", "John", "Smith", "john@email.com", "Premium", 125000, 750),
        ("CUST002", "Sarah", "Johnson", "sarah@email.com", "Standard", 75000, 720),
        ("CUST003", "Mike", "Wilson", "mike@email.com", "VIP", 200000, 800),
        ("CUST004", "Lisa", "Brown", "lisa@email.com", "Basic", 45000, 650),
        ("CUST005", "David", "Davis", "david@email.com", "Premium", 95000, 740)
    ]
    
    # Column names - these become the DataFrame column headers
    columns = ["customer_id", "first_name", "last_name", "email", 
               "segment", "annual_income", "credit_score"]
    
    # Convert Python list to Spark DataFrame
    return spark.createDataFrame(data, columns)

def create_accounts_data(spark):
    """
    Create sample account dataset showing different account types and balances
    
    BUSINESS CONTEXT:
    - Each customer can have multiple accounts (checking, savings, credit cards)
    - Balances can be positive (assets) or negative (liabilities like credit cards)
    - Account status tracks if the account is active, frozen, or closed
    """
    
    data = [
        # (Account ID, Customer ID, Type, Balance, Status)
        ("ACC001", "CUST001", "Checking", 25000.00, "Active"),
        ("ACC002", "CUST001", "Savings", 75000.00, "Active"),      # Same customer, different account
        ("ACC003", "CUST002", "Checking", 15000.00, "Active"),
        ("ACC004", "CUST002", "Credit Card", -2500.00, "Active"),  # Negative balance = debt
        ("ACC005", "CUST003", "Investment", 150000.00, "Active"),
        ("ACC006", "CUST004", "Checking", 5000.00, "Active"),
        ("ACC007", "CUST005", "Savings", 45000.00, "Active")
    ]
    
    columns = ["account_id", "customer_id", "account_type", "balance", "status"]
    return spark.createDataFrame(data, columns)

# HOW TO USE THESE FUNCTIONS:
# 1. Save this code in a file called 'banking_data.py'
# 2. Import the functions in your main script
# 3. Call them whenever you need sample data

# Example usage in your main script:
# from banking_data import create_customers_data, create_accounts_data
# customers_df = create_customers_data(spark)
# accounts_df = create_accounts_data(spark)
```



**File Organization Best Practices:**

```
your_project/
├── script_03.py              # Your main analysis script
└── utils/
    ├── __init__.py           # Makes utils a Python package
    └── banking_data.py       # Our utility functions

```



**Why This Organization Matters:**

- **Maintainability:** Change test data in one place, affects all scripts
- **Consistency:** Everyone uses the same test data
- **Clarity:** Main scripts focus on analysis logic, not data setup
- **Reusability:** Functions can be shared across team members
- **Testing:** Easy to create variations for different test scenarios

------

## 2. Creating DataFrames - Your Data Foundation

### 2.1 From Python Data (Learning & Testing Scenarios)

**What is a DataFrame?** A DataFrame is like a **smart table** that:

- Has **columns** (like Excel columns) with names and data types
- Has **rows** (like Excel rows) containing the actual data
- Can perform **operations** like filtering, sorting, and calculations
- Can be **distributed** across multiple computers for big data

**When do we create DataFrames from Python data?**

- **Learning:** When practicing new concepts with known data
- **Testing:** When verifying your code works correctly
- **Prototyping:** When designing new analysis before using real data
- **Small datasets:** When working with reference data or lookup tables

```python
# Script 03
from utils.banking_data import create_customers_data, create_accounts_data
from pyspark.sql import SparkSession
from tabulate import tabulate

# Create your data processing engine
spark = SparkSession.builder \
    .appName("BankingAnalytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

customers_df = create_customers_data(spark)
accounts_df = create_accounts_data(spark)

# Function to pretty print a Spark DataFrame using tabulate
def show_df_tabulate(df, num_rows=10):
    data = df.limit(num_rows).toPandas()
    print(tabulate(data, headers='keys', tablefmt='outline', showindex=False))


# --- Inspect Customer DataFrame structure and preview data ---
print("\n🏦 Customer Data Structure:")
customers_df.printSchema()  # Shows column names, types, and nullability

print("\n👤 Customer Data Preview:")
show_df_tabulate(customers_df, 5)  # Show first 5 rows for quick inspection

# --- Inspect Account DataFrame structure and preview data ---
print("\n💰 Account Data Structure:")
accounts_df.printSchema()

print("\n💳 Account Data Preview:")
show_df_tabulate(accounts_df, 5)

# --- Data summary ---
print(f"\n📊 Data Summary:")
print(f"Total Customers: {customers_df.count()}")
print(f"Total Accounts: {accounts_df.count()}")

# --- Quick notes on data types and business meaning ---
# STRING columns: IDs, names, emails (for identification and communication)
# NUMERIC columns: income, credit_score, balances (for analytics and risk)
# Correct types enable calculations and business logic (e.g., credit_score > 700)
```



### 2.2 From Files (Real-World Production Scenarios)

**When do we load data from files?** In real environments, data comes from various sources:

- **Core banking systems** export customer data as CSV files
- **Transaction systems** generate JSON logs
- **Data warehouses** store optimized data in Parquet format
- **Regulatory systems** require specific file formats for compliance

**Understanding File Formats:**

| **Format**  | **Best For**              | **Banking Use Case**                | **Pros**                | **Cons**                      |
| ----------- | ------------------------- | ----------------------------------- | ----------------------- | ----------------------------- |
| **CSV**     | Human-readable exports    | Regulatory reports, data exchange   | Easy to read, universal | Large file size, slow         |
| **JSON**    | API responses, logs       | Transaction logs, customer profiles | Flexible structure      | Can be large, variable schema |
| **Parquet** | Analytics, data warehouse | Optimized analytics storage         | Fast, compressed        | Not human-readable            |

```python
# Script 04
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# Create SparkSession for standalone execution
spark = SparkSession.builder \
    .appName("BankingDataLoad") \
    .getOrCreate()

# SCENARIO 1: Loading Customer Data from CSV (Most Common)
# BUSINESS CONTEXT: Your core banking system exports customer data nightly
print("📁 Loading Customer Data from CSV File...")

customers_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# .option("header", "true"):
# WHAT: Tells Spark the first row contains column names
# WHY: Without this, Spark would treat column names as data
# EXAMPLE: First row "customer_id,first_name,last_name" becomes column headers

# .option("inferSchema", "true"):
# WHAT: Spark automatically guesses data types by examining the data
# WHY: Without this, everything would be treated as strings
# HOW: Spark looks at values and determines if they're numbers, dates, etc.

print("✅ CSV data loaded successfully!")
customers_csv.show(3)  # Show first 3 rows to verify

# SCENARIO 2: Loading Transaction Logs from JSON
# BUSINESS CONTEXT: Your mobile banking app logs transactions in JSON format

print("\n📱 Loading Transaction Data from JSON...")

# JSON files are self-describing (column names are in the data)
transactions_json = spark.read.json("banking_dataset/datasets/branches.json")

# WHY JSON IS GOOD:
# - Can handle complex nested data (customer info within transaction)
# - Flexible schema (different transaction types can have different fields)
# - Common format for web APIs and modern applications

print("✅ JSON data loaded successfully!")
print("Schema automatically detected from JSON:")
transactions_json.printSchema()


# SCENARIO 3: Loading Transaction Logs from Parquet
# BUSINESS CONTEXT: Your data warehouse stores historical transaction data in Parquet format for analytics
print("\n🏭 Loading Data Warehouse Data from Parquet...")

transactions_parquet = spark.read.parquet("banking_dataset/datasets/transactions_.parquet")

# WHY PARQUET IS PERFECT FOR ANALYTICS:
# ✅ FAST: Optimized for reading large amounts of data quickly
# ✅ SMALL: Compressed file size saves storage costs
# ✅ TYPED: Data types are preserved exactly (no guessing needed)
# ✅ COLUMNAR: Perfect for analytics queries (sum, average, etc.)

print("✅ Parquet data loaded successfully!")
print("Notice: No options needed - Parquet is self-describing!")
transactions_parquet.show(3)

```



**File Loading Best Practices for Banking:**

```python
# Add this code to script_04.py
# ✅ ALWAYS verify data after loading
print(f"Loaded {customers_csv.count()} customer records")
print(f"Expected columns: {len(customers_csv.columns)}")

# ✅ CHECK for data quality issues immediately
null_check = customers_csv.filter(col("customer_id").isNull()).count()
if null_check > 0:
    print(f"⚠️ WARNING: {null_check} customers missing IDs!")

# ✅ SAMPLE data to verify correctness
print("Sample of loaded data:")
customers_csv.sample(0.1).show(5)  # Show 10% sample

# ✅ DOCUMENT your data sources
print("Data loaded from: Core Banking System Export")
print("Export date: 2024-01-15")
print("File format: CSV with headers")
```



### 2.3 Schema Management (Production-Critical Best Practice)

**What is a Schema?** A schema is like a **blueprint** for your data that defines:

- **Column names** (what each column is called)
- **Data types** (string, number, date, etc.)
- **Nullable rules** (can this column be empty?)

**Why is Schema Management Critical in Banking?** Banking data must be **precise** and **consistent** because:

- **Regulatory compliance** requires exact data formats
- **Financial calculations** must be accurate to the penny
- **Risk assessments** depend on correct data types
- **System integration** needs predictable data structures

**The Problem with Schema Inference:**

```python
# ❌ RISKY: Letting Spark guess data types
risky_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("customer_data.csv")

# WHAT CAN GO WRONG:
# - Customer IDs like "000123" might become integers (loses leading zeros)
# - ZIP codes like "01234" might become integers (breaks East Coast addresses)
# - Large numbers might overflow or lose precision
# - Date formats might be misinterpreted
# - Different environments might infer differently
```

**The Solution: Explicit Schema Definition:**

```python
# script_05
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
spark = SparkSession.builder \
    .appName("BankingDataSchema") \
    .getOrCreate()

# WHAT: Define exactly what our customer data should look like
# WHY: Ensures consistency across environments and prevents data corruption
# WHEN: Always in production systems, recommended for important analysis

customer_schema = StructType([
    # StructField(column_name, data_type, can_be_null)
    StructField("customer_id", StringType(), False),     # Must have customer ID
    StructField("first_name", StringType(), True),       # Name can be missing (privacy)
    StructField("last_name", StringType(), True),        # Name can be missing (privacy)
    StructField("email", StringType(), True),            # Email might not be provided
    StructField("segment", StringType(), True),          # Segment might be unassigned
    StructField("annual_income", DoubleType(), True),    # Income might be unknown
    StructField("credit_score", IntegerType(), True)     # Credit score might be unavailable
])

# LET'S UNDERSTAND EACH DATA TYPE CHOICE:

# StringType() for customer_id:
# WHY: Even if it looks like a number, IDs should be strings
# REASON: Preserves leading zeros, prevents mathematical operations
# EXAMPLE: "000123" stays "000123", not 123

# DoubleType() for annual_income:
# WHY: Can handle large numbers and decimal places
# REASON: Incomes like $125,000.50 need decimal precision
# ALTERNATIVE: DecimalType for exact financial calculations

# IntegerType() for credit_score:
# WHY: Credit scores are whole numbers (300-850)
# REASON: No decimal places needed, saves memory

# Boolean flags for nullable:
# False = This column MUST have a value (required field)
# True = This column CAN be empty (optional field)

print("📋 Our Customer Schema Definition:")
for field in customer_schema.fields:
    nullable = "Optional" if field.nullable else "Required"
    print(f"  {field.name}: {field.dataType} ({nullable})")

# Now load data with our explicit schema
customers_typed = spark.read \
    .schema(customer_schema) \
    .option("header", "true") \
    .csv("banking_dataset/datasets/customers.csv")

print("\n✅ Data loaded with explicit schema:")
customers_typed.printSchema()

# BENEFITS OF EXPLICIT SCHEMAS:
print("\n🎯 Benefits of Explicit Schema:")
print("✅ Consistent data types across all environments")
print("✅ Faster loading (no time spent inferring schema)")
print("✅ Early error detection (fails fast if data doesn't match)")
print("✅ Regulatory compliance (exact data format requirements)")
print("✅ Team collaboration (everyone uses same data structure)")
```

**Advanced Schema Patterns for Banking:**

```python
# PATTERN 1: Account Schema with Financial Precision
from pyspark.sql.types import DecimalType, DateType

account_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), False),
    StructField("balance", DecimalType(15, 2), True),      # 15 digits total, 2 after decimal
    StructField("interest_rate", DecimalType(5, 4), True), # 4 decimal places for precision
    StructField("open_date", DateType(), True),
    StructField("is_active", BooleanType(), False)
])

# WHY DecimalType for money:
# ✅ EXACT: No floating-point rounding errors
# ✅ PRECISE: Perfect for financial calculations
# ✅ COMPLIANT: Meets regulatory requirements for accuracy

# PATTERN 2: Transaction Schema with Complex Types
from pyspark.sql.types import TimestampType, ArrayType

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("amount", DecimalType(15, 2), False),
    StructField("transaction_time", TimestampType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("tags", ArrayType(StringType()), True),    # Multiple tags per transaction
    StructField("is_international", BooleanType(), False)
])

# PATTERN 3: Schema Evolution (Adding New Columns)
# BUSINESS SCENARIO: New regulation requires tracking customer consent

updated_customer_schema = StructType([
    # All existing fields...
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    # ... other existing fields ...
    
    # New compliance fields
    StructField("gdpr_consent", BooleanType(), True),      # New: GDPR compliance
    StructField("consent_date", DateType(), True),         # New: When consent given
    StructField("data_retention_years", IntegerType(), True) # New: How long to keep data
])

# HOW TO HANDLE SCHEMA CHANGES:
# 1. Add new fields as nullable (True) to handle old data
# 2. Use default values for missing fields
# 3. Version your schemas for backward compatibility
```

------



## 3. Basic DataFrame Operations - Your Daily Banking Tools

### 3.1 Selecting Columns (Choosing What You Need)

**What is Column Selection?** Column selection is like **choosing which columns to show in Excel**. Instead of seeing all 50+ columns in a customer database, you pick only the ones relevant for your current analysis.

**Why is this Crucial in Banking?**

- **Performance:** Fewer columns = faster processing and less memory usage
- **Security:** Only access data you need (principle of least privilege)
- **Clarity:** Focus on relevant information for specific business questions
- **Compliance:** Some columns may contain sensitive data requiring special handling

**Real Banking Scenarios Where You Select Columns:**

- **Executive Dashboard:** Show only customer count and total assets
- **Marketing Campaign:** Select only contact info and preferences
- **Risk Assessment:** Focus on income, credit score, and debt ratios
- **Regulatory Report:** Extract only fields required by regulation

```python
# script_06.py
# SCENARIO 1: Executive Summary Dashboard
# BUSINESS NEED: CEO wants high-level customer overview for board meeting
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
spark = SparkSession.builder \
    .appName("BankingDataSchema") \
    .getOrCreate()
customer_schema = StructType([
    # StructField(column_name, data_type, can_be_null)
    StructField("customer_id", StringType(), False),     # Must have customer ID
    StructField("first_name", StringType(), True),       # Name can be missing (privacy)
    StructField("last_name", StringType(), True),        # Name can be missing (privacy)
    StructField("email", StringType(), True),            # Email might not be provided
    StructField("phone", StringType(), True),            # Phone number
    StructField("address", StringType(), True),          # Address
    StructField("city", StringType(), True),             # City
    StructField("state", StringType(), True),            # State
    StructField("zip_code", StringType(), True),         # ZIP code
    StructField("account_open_date", StringType(), True), # Account opening date
    StructField("customer_segment", StringType(), True), # Segment might be unassigned
    StructField("risk_score", IntegerType(), True),      # Risk score
    StructField("annual_income", DoubleType(), True),    # Income might be unknown
    StructField("birth_date", StringType(), True),       # Birth date
    StructField("employment_status", StringType(), True), # Employment status
    StructField("marital_status", StringType(), True)    # Marital status
])
customers_df = spark.read \
    .schema(customer_schema) \
    .option("header", "true") \
    .csv("banking_dataset/datasets/customers.csv")
print("📊 Executive Summary - Customer Overview")

executive_summary = customers_df.select(
    "customer_id",           # Unique identifier
    "customer_segment",      # Customer tier (Premium, Standard, Basic)
    "annual_income",         # Income level for portfolio analysis
    "risk_score"             # Risk indicator
)

print("Key metrics for executive review:")
executive_summary.show(10)

# WHY THESE COLUMNS:
# - customer_id: Count total customers
# - customer_segment: Understand customer mix
# - annual_income: Calculate portfolio value
# - risk_score: Assess portfolio risk

# SCENARIO 2: Marketing Campaign - Contact Information Only
# BUSINESS NEED: Marketing team needs contact details for email campaign

print("\n📧 Marketing Campaign - Contact List")

marketing_contacts = customers_df.select(
    "customer_id",
    "first_name",
    "last_name", 
    "email",
    "customer_segment"      # For targeted messaging
)

print("Contact list for marketing campaign:")
marketing_contacts.show(5)

# PRIVACY NOTE: In real scenarios, marketing team would get anonymized IDs
# and email would be handled through secure marketing platforms

# SCENARIO 3: Advanced Selection with Column Functions
# BUSINESS NEED: Create formatted customer display names and categorize income

from pyspark.sql.functions import col, upper, concat, lit, when

print("\n🎯 Advanced Customer Display Format")

customer_display = customers_df.select(
    col("customer_id"),
    
    # Transform first name to uppercase for formal display
    upper(col("first_name")).alias("first_name_formal"),
    
    # Create full name for personalized communications
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
    
    # Original income for calculations
    col("annual_income"),
    
    # Categorize income level for business rules
    when(col("annual_income") >= 150000, "High Income")
    .when(col("annual_income") >= 75000, "Medium Income")
    .otherwise("Standard Income").alias("income_category"),
    
    col("customer_segment")
)

print("Formatted customer display data:")
customer_display.show(5, truncate=False)  # truncate=False shows full column values

# BUSINESS VALUE:
# - first_name_formal: For formal communications and legal documents
# - full_name: For personalized marketing and customer service
# - income_category: For product recommendations and credit decisions

# SCENARIO 4: Column Selection Best Practices

# ✅ DO: Select only columns you need
needed_columns = customers_df.select("customer_id", "customer_segment", "annual_income")

# ✅ DO: Use meaningful aliases when transforming data
renamed_columns = customers_df.select(
    col("customer_id").alias("cust_id"),
    col("annual_income").alias("yearly_income_usd")
)

# ✅ DO: Chain column operations for efficiency
chained_selection = customers_df.select(
    col("customer_id"),
    upper(col("customer_segment")).alias("segment_code"),
    (col("annual_income") / 12).alias("monthly_income"),
    when(col("risk_score") > 750, "Excellent")
    .when(col("risk_score") > 650, "Good")
    .otherwise("Fair").alias("credit_rating")
)

print("\nChained column operations result:")
chained_selection.show(5)

# ❌ AVOID: Selecting all columns when you don't need them
# bad_practice = customers_df.select("*")  # Gets everything, slow and unnecessary

# ❌ AVOID: Repeated similar operations
# Instead of multiple selects, chain them together
```

**Performance Impact of Column Selection:**

```python
# Add this code at the end of the sc
import time

# Test 1: Select all columns (inefficient)
start_time = time.time()
all_columns_result = customers_df.select("*").count()
all_columns_time = time.time() - start_time

# Test 2: Select only needed columns (efficient)
start_time = time.time()
few_columns_result = customers_df.select("customer_id", "customer_segment").count()
few_columns_time = time.time() - start_time

print(f"\n⚡ Performance Comparison:")
print(f"All columns: {all_columns_time:.3f} seconds")
print(f"Selected columns: {few_columns_time:.3f} seconds")
print(f"Performance improvement: {((all_columns_time - few_columns_time) / all_columns_time * 100):.1f}%")

# WHY SELECTING FEWER COLUMNS IS FASTER:
# ✅ Less data transfer over network
# ✅ Reduced memory usage
# ✅ Faster serialization/deserialization
# ✅ More efficient caching
```



**Business Intelligence Column Selection Patterns:**

```python
# PATTERN 1: Customer 360 View (Comprehensive Analysis)
customer_360 = customers_df.select(
    "customer_id",
    "customer_segment",
    "annual_income", 
    "risk_score",
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name")
).withColumn("customer_value_tier",
    when((col("annual_income") > 100000) & (col("risk_score") > 750), "Platinum")
    .when((col("annual_income") > 75000) & (col("risk_score") > 700), "Gold")
    .when(col("annual_income") > 50000, "Silver")
    .otherwise("Bronze")
)

# PATTERN 2: Risk Assessment View (Focused on Risk Metrics)
risk_assessment = customers_df.select(
    "customer_id",
    "risk_score",
    "annual_income",
    (col("annual_income") / 12).alias("monthly_income"),
    when(col("risk_score") < 600, "High Risk")
    .when(col("risk_score") < 700, "Medium Risk")
    .otherwise("Low Risk").alias("risk_category")
)

# PATTERN 3: Compliance Report View (Regulatory Requirements)
compliance_report = customers_df.select(
    "customer_id",
    "customer_segment",
    when(col("annual_income") > 200000, "High Net Worth").otherwise("Standard").alias("wealth_category"),
    when(col("customer_segment") == "Premium", True).otherwise(False).alias("requires_enhanced_due_diligence")
)

print("Customer 360 View Sample:")
customer_360.show(3)
```

### 3.2 Filtering Rows (Finding What Matters)

**What is Row Filtering?** Row filtering is like **using Excel's filter feature** to show only rows that meet certain conditions. Instead of looking at all 1 million customers, you focus on the 50,000 that match your specific criteria.

**Why is Filtering Essential in Banking?** Banking datasets are enormous, and business questions are usually specific:

- **Risk Management:** "Show me customers with credit scores below 600"
- **Marketing:** "Find high-income customers without premium accounts"
- **Compliance:** "Identify accounts with suspicious transaction patterns"
- **Operations:** "List all frozen accounts requiring review"

**Real Banking Filter Scenarios:**

```python
# SCENARIO 1: Credit Risk Assessment
# BUSINESS NEED: Risk team needs to review high-risk customers for loan defaults

print("🚨 High-Risk Customer Analysis")

high_risk_customers = customers_df.filter(col("risk_score") < 600)

print(f"High-risk customers (risk score < 600): {high_risk_customers.count()}")
high_risk_customers.select("customer_id", "risk_score", "annual_income", "customer_segment").show()

# BUSINESS IMPACT:
# - These customers need manual loan review
# - Higher interest rates may be applied
# - Additional collateral might be required
# - Enhanced monitoring for early warning signs

# SCENARIO 2: Premium Product Marketing
# BUSINESS NEED: Marketing wants to target high-income customers for premium credit cards

print("\n💎 Premium Product Target Customers")

premium_targets = customers_df.filter(col("annual_income") > 100000)

print(f"High-income customers (>$100K): {premium_targets.count()}")
premium_targets.select("customer_id", "annual_income", "customer_segment", "risk_score").show()

# BUSINESS STRATEGY:
# - Offer premium credit cards with higher limits
# - Invite to exclusive banking events
# - Provide dedicated relationship managers
# - Cross-sell investment products

# SCENARIO 3: Complex Multi-Condition Filtering
# BUSINESS NEED: Find ideal customers for mortgage products

print("\n🏠 Mortgage Product Qualified Customers")

# Mortgage qualification criteria:
# - Good risk score (>= 650)
# - Stable income (>= $50,000)
# - Not already in Premium segment (growth opportunity)

mortgage_qualified = customers_df.filter(
    (col("risk_score") >= 650) & 
    (col("annual_income") >= 50000) & 
    (col("customer_segment") != "Premium")
)

print(f"Mortgage-qualified customers: {mortgage_qualified.count()}")
mortgage_qualified.select("customer_id", "risk_score", "annual_income", "customer_segment").show()

# LOGICAL OPERATORS EXPLANATION:
# & = AND (both conditions must be true)
# | = OR (either condition can be true)
# ~ = NOT (opposite of the condition)

# SCENARIO 4: Employment Status Analysis
# BUSINESS NEED: HR insights for targeted products

print("\n💼 Employment Status Analysis")

# Find employed customers for loan products
employed_customers = customers_df.filter(col("employment_status") == "Employed")
print(f"Employed customers: {employed_customers.count()}")
employed_customers.select("customer_id", "employment_status", "annual_income").show(5)

# Find self-employed customers (higher risk assessment)
self_employed = customers_df.filter(col("employment_status") == "Self-Employed")
print(f"Self-employed customers: {self_employed.count()}")
self_employed.select("customer_id", "employment_status", "annual_income").show(5)

# SCENARIO 5: Advanced Filtering Techniques

# String operations in filters
email_gmail = customers_df.filter(col("email").contains("@gmail.com"))
print(f"\nCustomers with Gmail addresses: {email_gmail.count()}")

# Multiple value matching
premium_segments = customers_df.filter(col("customer_segment").isin(["Premium", "Standard", "Basic"]))
print(f"All customer segments: {premium_segments.count()}")

# Range filtering
mid_income = customers_df.filter(
    (col("annual_income") >= 50000) & (col("annual_income") <= 100000)
)
print(f"Middle-income customers ($50K-$100K): {mid_income.count()}")

# Null value filtering
customers_with_email = customers_df.filter(col("email").isNotNull())
print(f"Customers with email addresses: {customers_with_email.count()}")

# SCENARIO 6: Business Rule Implementation
# BUSINESS NEED: Implement complex business rules for customer classification

print("\n🎯 Complex Business Rule Implementation")

# Rule: "Gold Status" customers meet specific criteria
gold_status_customers = customers_df.filter(
    # High risk score OR high income
    ((col("risk_score") >= 750) | (col("annual_income") >= 150000)) &
    # AND not already Premium (to avoid downgrade)
    (col("customer_segment") != "Premium") &
    # AND has valid email for communication
    (col("email").isNotNull() & col("email").contains("@"))
)

print("Gold Status Eligible Customers:")
gold_status_customers.select("customer_id", "risk_score", "annual_income", "customer_segment").show()

# BUSINESS VALUE:
# - Identify upgrade opportunities
# - Improve customer retention
# - Increase fee income through premium services
# - Better risk-adjusted pricing
```

**Filter Performance Best Practices:**

```python
# script_07
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FilteringBestPractices") \
    .getOrCreate()

# Load customer data
customers_df = spark.read \
    .option("header", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# Load accounts data
accounts_df = spark.read \
    .option("header", "true") \
    .csv("banking_dataset/datasets/accounts.csv")

# ✅ BEST PRACTICE 1: Filter early in your data pipeline
# Good: Filter first, then do expensive operations
efficient_pipeline = customers_df \
    .filter(col("annual_income") > 100000) \
    .join(accounts_df, "customer_id") \
    .groupBy("customer_segment").count()

# ❌ Bad: Do expensive operations first, filter later
# inefficient_pipeline = customers_df \
#     .join(accounts_df, "customer_id") \
#     .groupBy("segment").count() \
#     .filter(col("count") > 100)

# ✅ BEST PRACTICE 2: Use indexed columns for filtering when possible
# If risk_score is indexed, this will be fast:
indexed_filter = customers_df.filter(col("risk_score") > 700)

# ✅ BEST PRACTICE 3: Combine related filters
# Good: Single filter with multiple conditions
combined_filter = customers_df.filter(
    (col("annual_income") > 50000) & 
    (col("risk_score") > 650) & 
    (col("customer_segment") == "Premium")
)

# ❌ Less efficient: Multiple separate filters
# separate_filters = customers_df \
#     .filter(col("annual_income") > 50000) \
#     .filter(col("risk_score") > 650) \
#     .filter(col("customer_segment") == "Premium")

print("✅ Efficient filtering pipeline demonstrated")

# Clean up
spark.stop()
```

**Common Banking Filter Patterns:**

```python
# PATTERN 1: Customer Segmentation Filters
high_value = customers_df.filter((col("annual_income") > 100000) & (col("risk_score") > 750))
growth_potential = customers_df.filter((col("annual_income") > 75000) & (col("customer_segment") == "Standard"))
at_risk = customers_df.filter((col("risk_score") < 600) | (col("annual_income") < 30000))

# PATTERN 2: Account Status Filters
active_accounts = accounts_df.filter(col("account_status") == "Active")
problem_accounts = accounts_df.filter((col("balance") < -1000) | (col("account_status") == "Frozen"))

# PATTERN 3: Time-based Filters (when you have date columns)
# recent_customers = customers_df.filter(col("account_open_date") >= "2024-01-01")

# PATTERN 4: Compliance and Risk Filters
aml_review = customers_df.filter(
    (col("annual_income") > 200000) | 
    (col("customer_segment") == "Premium")
)

print("All filter patterns demonstrated successfully! ✅")

```



### 3.3 Sorting Data (Ranking and Ordering)

**What is Data Sorting?** Sorting arranges your data in a specific order, like **organizing a filing cabinet alphabetically** or **ranking customers by account value**. In banking, sorting helps identify top performers, find outliers, and create meaningful reports.

**Why is Sorting Critical in Banking?**

- **Performance Analysis:** Rank customers by profitability
- **Risk Management:** Identify highest-risk accounts first
- **Regulatory Reporting:** Present data in required order
- **Business Intelligence:** Find top/bottom performers quickly

**Real Banking Sorting Scenarios:**

```python
# script_08.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, when

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FilteringBestPractices") \
    .getOrCreate()

# Load customer data
customers_df = spark.read \
    .option("header", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# Load accounts data
accounts_df = spark.read \
    .option("header", "true") \
    .csv("banking_dataset/datasets/accounts.csv")

# SCENARIO 1: Customer Wealth Ranking
# BUSINESS NEED: Relationship managers need to prioritize high-value customers

print("👑 Customer Wealth Ranking")

# Sort customers by income (highest first)
wealth_ranking = customers_df.orderBy(desc("annual_income"))

print("Top customers by annual income:")
wealth_ranking.select("customer_id", "first_name", "last_name", "annual_income", "customer_segment").show(10)

# BUSINESS APPLICATION:
# - Relationship managers call top customers first
# - VIP events invite list prioritization
# - Resource allocation for customer service

# Alternative: Sort by income (lowest first) for different analysis
low_income_analysis = customers_df.orderBy(asc("annual_income"))  # asc() is optional (default)
print("\nCustomers needing financial assistance programs:")
low_income_analysis.select("customer_id", "annual_income", "customer_segment").show(5)

# SCENARIO 2: Credit Risk Assessment Ranking
# BUSINESS NEED: Risk team needs to review customers by credit risk

print("\n⚠️ Credit Risk Ranking")

risk_ranking = customers_df.orderBy("risk_score")  # Lowest scores first (highest risk)

print("Highest risk customers (lowest credit scores):")
risk_ranking.select("customer_id", "risk_score", "annual_income", "customer_segment").show(10)

# BUSINESS IMPACT:
# - Prioritize manual credit reviews
# - Adjust credit limits proactively
# - Identify customers for credit counseling programs

# SCENARIO 3: Multi-Level Sorting (Most Common in Business)
# BUSINESS NEED: Organize customers by segment, then by income within each segment

print("\n🎯 Customer Organization by Segment and Income")

segmented_ranking = customers_df.orderBy(
    col("customer_segment"),                    # First sort by segment (alphabetical)
    desc("annual_income")             # Then by income (highest first within each segment)
)

print("Customers organized by segment, then income:")
segmented_ranking.select("customer_id", "customer_segment", "annual_income", "risk_score").show(15)

# WHY MULTI-LEVEL SORTING MATTERS:
# - VIP customers listed first within their segment
# - Within each segment, highest income customers prioritized
# - Clear organization for relationship management

# SCENARIO 4: Account Balance Analysis
# BUSINESS NEED: Treasury team needs account balance overview

print("\n💰 Account Balance Rankings")

# Highest balances first (asset management focus)
top_accounts = accounts_df.orderBy(desc("balance"))
print("Highest balance accounts:")
top_accounts.show(10)

# Lowest balances first (risk management focus)
low_balance_accounts = accounts_df.orderBy("balance")
print("\nLowest balance accounts (overdraft risk):")
low_balance_accounts.show(10)

# SCENARIO 5: Advanced Sorting with Business Logic
# BUSINESS NEED: Create comprehensive customer priority ranking

print("\n🚀 Comprehensive Customer Priority Ranking")

# Create a calculated priority score and sort by it
customer_priority = customers_df.withColumn("priority_score",
    # Income contributes 60% to priority
    (col("annual_income") / 1000 * 0.6) +
    # Risk score contributes 40% to priority  
    (col("risk_score") * 0.4)
).orderBy(desc("priority_score"))

print("Customer priority ranking (income + credit score):")
customer_priority.select(
    "customer_id", 
    "annual_income", 
    "risk_score", 
    "priority_score", 
    "customer_segment"
).show(10)

# BUSINESS VALUE:
# - Combines multiple factors for holistic ranking
# - Helps allocate customer service resources
# - Identifies best prospects for premium products

# SCENARIO 6: Sorting with Complex Conditions
# BUSINESS NEED: Sort customers for targeted marketing campaign

print("\n📧 Marketing Campaign Priority Sorting")

marketing_priority = customers_df.orderBy(
    # First priority: Premium customers
    when(col("customer_segment") == "Premium", 1)
    .when(col("customer_segment") == "Standard", 2)
    .when(col("customer_segment") == "Basic", 3)
    .otherwise(4).asc(),
    
    # Second priority: Income within each segment
    desc("annual_income"),
    
    # Third priority: Risk score
    desc("risk_score")
)

print("Marketing campaign contact priority:")
marketing_priority.select("customer_id", "customer_segment", "annual_income", "risk_score").show(12)

# Clean up
spark.stop()
```

**Sorting Performance and Best Practices:**

```python
# ✅ BEST PRACTICE 1: Use limit() with orderBy() for top-N queries
# Instead of sorting everything and then taking top 10:
top_10_customers = customers_df.orderBy(desc("annual_income")).limit(10)

print("✅ Efficient top-10 query:")
top_10_customers.show()

# ✅ BEST PRACTICE 2: Sort on indexed columns when possible
# If annual_income is indexed, this will be faster
indexed_sort = customers_df.orderBy(desc("annual_income"))

# ✅ BEST PRACTICE 3: Use coalesce() to handle nulls
from pyspark.sql.functions import coalesce, lit

null_safe_sort = customers_df.orderBy(desc(coalesce(col("annual_income"), lit(0))))

# ✅ BEST PRACTICE 4: Cache datasets you'll sort multiple times
frequently_sorted = customers_df.cache()
sort1 = frequently_sorted.orderBy("annual_income")
sort2 = frequently_sorted.orderBy("risk_score")

print("✅ Performance best practices demonstrated")

# UNDERSTANDING SORT PERFORMANCE:
# - Sorting is expensive for large datasets
# - Use limit() when you only need top/bottom N rows
# - Consider partitioning for very large datasets
# - Cache data if you'll sort it multiple ways
```

**Null Handling in Sorting:**

```python
# SCENARIO: Handling customers with missing income data
print("\n🔍 Null Value Handling in Sorting")

# Import required functions for null handling
from pyspark.sql.functions import coalesce, lit

# Default behavior: nulls typically sort last in ascending order
default_sort = customers_df.orderBy("annual_income")

# Explicit null handling: nulls first
nulls_first = customers_df.orderBy(col("annual_income").asc_nulls_first())

# Explicit null handling: nulls last  
nulls_last = customers_df.orderBy(col("annual_income").asc_nulls_last())

# Replace nulls before sorting (often preferred in business contexts)
income_with_default = customers_df.withColumn("income_for_sort",
    coalesce(col("annual_income"), lit(0))  # Replace null with 0
).orderBy(desc("income_for_sort"))

print("Customers sorted with null income handling:")
income_with_default.select("customer_id", "annual_income", "income_for_sort").show(10)

# BUSINESS DECISION: How to handle missing income data?
# Option 1: Treat as $0 (conservative)
# Option 2: Exclude from analysis (filter out nulls first)
# Option 3: Use estimated value based on other factors
```



### 3.4 Adding and Transforming Columns

**What is Column Transformation?** Column transformation means **creating new columns** or **modifying existing ones** to derive business insights. It's like adding calculated fields in Excel, but more powerful and scalable.

**Why Transform Columns in Banking?** Raw data rarely answers business questions directly. You need to:

- **Calculate metrics:** Monthly income from annual income
- **Categorize customers:** Risk levels from credit scores
- **Create business rules:** Eligibility flags for products
- **Standardize data:** Consistent formats for analysis

**Real Banking Transformation Scenarios:**

```python
# script_09.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, when, concat, lit, substring

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FilteringBestPractices") \
    .getOrCreate()

# Load customer data
customers_df = spark.read \
    .option("header", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# Load accounts data
accounts_df = spark.read \
    .option("header", "true") \
    .csv("banking_dataset/datasets/accounts.csv")

# SCENARIO 1: Customer Risk Categorization
# BUSINESS NEED: Convert numeric credit scores into business-friendly categories

print("⚠️ Customer Risk Categorization")

customers_with_risk = customers_df.withColumn("risk_category",
    when(col("risk_score") >= 750, "Low Risk")      # Excellent credit
    .when(col("risk_score") >= 700, "Medium Risk")  # Good credit  
    .when(col("risk_score") >= 650, "High Risk")    # Fair credit
    .otherwise("Very High Risk")                      # Poor credit
)

print("Customer risk categorization:")
customers_with_risk.select("customer_id", "risk_score", "risk_category").show(10)

# BUSINESS IMPACT:
# - Loan officers can quickly assess risk
# - Automated credit limit decisions
# - Risk-based pricing for products
# - Portfolio risk reporting

# SCENARIO 2: Income-Based Customer Segmentation  
# BUSINESS NEED: Create income tiers for product targeting

print("\n💰 Income-Based Customer Tiers")

customers_with_tiers = customers_df.withColumn("income_tier",
    when(col("annual_income") >= 150000, "High Income")     # Top 20%
    .when(col("annual_income") >= 100000, "Upper Middle")   # Next 20%
    .when(col("annual_income") >= 75000, "Middle Income")   # Next 30%
    .when(col("annual_income") >= 50000, "Lower Middle")    # Next 20%
    .otherwise("Standard Income")                           # Bottom 10%
)

print("Income tier distribution:")
customers_with_tiers.groupBy("income_tier").count().orderBy(desc("count")).show()

# BUSINESS APPLICATIONS:
# - Targeted marketing campaigns
# - Product recommendations
# - Fee structure decisions
# - Resource allocation

# SCENARIO 3: Financial Calculations
# BUSINESS NEED: Calculate derived financial metrics

print("\n📊 Financial Metric Calculations")

customers_with_metrics = customers_df.withColumn("monthly_income",
    col("annual_income") / 12
).withColumn("credit_utilization_capacity",
    # Estimate credit capacity based on income and credit score
    (col("annual_income") * 0.3) * (col("risk_score") / 850)
).withColumn("debt_to_income_threshold",
    # Conservative DTI threshold based on risk
    when(col("risk_score") >= 750, col("annual_income") * 0.43)  # 43% DTI for excellent credit
    .when(col("risk_score") >= 650, col("annual_income") * 0.36)  # 36% DTI for good credit
    .otherwise(col("annual_income") * 0.28)                        # 28% DTI for fair credit
)

print("Financial metrics calculation:")
customers_with_metrics.select(
    "customer_id", 
    "annual_income", 
    "monthly_income", 
    "credit_utilization_capacity",
    "debt_to_income_threshold"
).show(5)

# SCENARIO 4: Text Processing and Standardization
# BUSINESS NEED: Create standardized customer display names

print("\n👤 Customer Name Standardization")

customers_with_names = customers_df.withColumn("full_name",
    concat(col("first_name"), lit(" "), col("last_name"))
).withColumn("display_name",
    # Format: "Smith, John (Premium)"
    concat(
        col("last_name"), 
        lit(", "), 
        col("first_name"),
        lit(" ("),
        col("customer_segment"),
        lit(")")
    )
).withColumn("initials",
    concat(
        substring(col("first_name"), 1, 1),  # First letter of first name
        lit("."),
        substring(col("last_name"), 1, 1),   # First letter of last name  
        lit(".")
    )
)

print("Standardized customer names:")
customers_with_names.select("customer_id", "full_name", "display_name", "initials").show(5, truncate=False)

# BUSINESS USE CASES:
# - Formal communications and documents
# - Customer service screen displays
# - Marketing personalization
# - Legal and compliance documentation

# SCENARIO 5: Boolean Flags for Business Rules
# BUSINESS NEED: Create eligibility flags for various banking products

print("\n🎯 Product Eligibility Flags")

customers_with_eligibility = customers_df.withColumn("mortgage_eligible",
    # Mortgage eligibility: Good credit + sufficient income
    (col("risk_score") >= 650) & (col("annual_income") >= 50000)
).withColumn("premium_card_eligible", 
    # Premium credit card: Excellent credit + high income
    (col("risk_score") >= 750) & (col("annual_income") >= 100000)
).withColumn("investment_eligible",
    # Investment products: High income OR already premium customer
    (col("annual_income") >= 75000) | (col("customer_segment") == "Premium")
).withColumn("requires_manual_review",
    # Manual review needed for high-risk scenarios
    (col("risk_score") < 600) | 
    (col("annual_income") > 500000) |  # Very high income requires extra verification
    (col("customer_segment") == "Premium")          # Premium always gets personal attention
)

print("Product eligibility analysis:")
customers_with_eligibility.select(
    "customer_id", 
    "risk_score",
    "annual_income", 
    "mortgage_eligible",
    "premium_card_eligible", 
    "investment_eligible",
    "requires_manual_review"
).show(10)

# SCENARIO 6: Complex Multi-Step Transformations
# BUSINESS NEED: Create comprehensive customer scoring system

print("\n🚀 Comprehensive Customer Scoring")

customer_scoring = customers_df \
    .withColumn("income_score",
        # Score income on 1-100 scale
        when(col("annual_income") >= 200000, 100)
        .when(col("annual_income") >= 150000, 90)
        .when(col("annual_income") >= 100000, 80)
        .when(col("annual_income") >= 75000, 70)
        .when(col("annual_income") >= 50000, 60)
        .otherwise(50)
    ) \
    .withColumn("credit_score_normalized",
        # Normalize credit score to 1-100 scale
        ((col("risk_score") - 300) / (850 - 300) * 100).cast("integer")
    ) \
    .withColumn("segment_bonus",
        # Bonus points for premium segments
        when(col("customer_segment") == "Premium", 10)
        .when(col("customer_segment") == "Standard", 5)
        .otherwise(0)
    ) \
    .withColumn("total_customer_score",
        # Weighted total score
        (col("income_score") * 0.4) +           # 40% weight on income
        (col("credit_score_normalized") * 0.5) + # 50% weight on credit
        (col("segment_bonus") * 0.1)            # 10% weight on segment
    ) \
    .withColumn("customer_tier",
        when(col("total_customer_score") >= 90, "Platinum")
        .when(col("total_customer_score") >= 80, "Gold") 
        .when(col("total_customer_score") >= 70, "Silver")
        .otherwise("Bronze")
    )

print("Comprehensive customer scoring:")
customer_scoring.select(
    "customer_id",
    "income_score", 
    "credit_score_normalized",
    "segment_bonus",
    "total_customer_score",
    "customer_tier"
).orderBy(desc("total_customer_score")).show(10)

# Clean up
spark.stop()
```

**Performance Optimization for Transformations:**

```python
# ✅ BEST PRACTICE 1: Chain multiple transformations efficiently
efficient_transformations = customers_df \
    .withColumn("monthly_income", col("annual_income") / 12) \
    .withColumn("risk_category", when(col("risk_score") >= 700, "Low").otherwise("High")) \
    .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))

# ✅ BEST PRACTICE 2: Use native Spark functions instead of UDFs when possible
# Good: Native functions
native_calc = customers_df.withColumn("income_category", 
    when(col("annual_income") > 100000, "High").otherwise("Standard"))

# ❌ Avoid: User-defined functions (UDFs) are slower
# def categorize_income(income):
#     return "High" if income > 100000 else "Standard"
# udf_categorize = udf(categorize_income)
# slow_calc = customers_df.withColumn("income_category", udf_categorize(col("annual_income")))

# ✅ BEST PRACTICE 3: Cache DataFrames with expensive transformations
expensive_transformations = customers_df \
    .withColumn("complex_calc", col("annual_income") * col("risk_score") / 1000) \
    .cache()  # Cache because we'll use this multiple times

print("✅ Performance optimization practices demonstrated")
```

------



## 4. Joins - Combining Banking Data Sources

### 4.1 Understanding Join Types and When to Use Them

**What are Joins?** Joins **combine data from multiple tables** based on common columns. In banking, customer information is typically **normalized** across multiple systems:

- **Customer table:** Personal information, demographics
- **Account table:** Account details, balances, types
- **Transaction table:** Individual transactions, payments
- **Product table:** Available banking products

**Why are Joins Essential in Banking?** Banking data is intentionally separated for:

- **Security:** Sensitive data isolated in secure systems
- **Performance:** Smaller tables are faster to query
- **Maintenance:** Changes to account info don't affect customer info
- **Compliance:** Different data retention rules for different data types

**Join Types Explained with Banking Examples:**

| **Join Type**  | **What It Does**            | **Banking Use Case**                    | **Result**                                  |
| -------------- | --------------------------- | --------------------------------------- | ------------------------------------------- |
| **Inner Join** | Only matching records       | Active customers WITH accounts          | Customers who are actively banking          |
| **Left Join**  | All left records + matches  | ALL customers + their accounts (if any) | Complete customer view, including prospects |
| **Right Join** | All right records + matches | All accounts + customer info (if any)   | Account-centric view                        |
| **Full Outer** | Everything from both tables | All customers AND all accounts          | Complete picture with gaps highlighted      |

```python
# script_10
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc, when, concat, lit, substring, countDistinct

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FilteringBestPractices") \
    .getOrCreate()

# Load customer data
customers_df = spark.read \
    .option("header", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# Load accounts data
accounts_df = spark.read \
    .option("header", "true") \
    .csv("banking_dataset/datasets/accounts.csv")

# Let's understand our data first
print("🔍 Understanding Our Data Before Joining")
print(f"Customers: {customers_df.count()} records")
print(f"Accounts: {accounts_df.count()} records")

print("\nCustomer data sample:")
customers_df.show(3)

print("\nAccount data sample:")
accounts_df.show(3)

# Notice: Some customers might have multiple accounts
# Some customers might have no accounts (prospects)
```

### 4.2 Inner Join - Core Customer-Account Relationships

**When to Use Inner Join:**

- You want **only customers who have accounts** (active banking relationships)
- **Performance matters** (inner joins are fastest)
- **Analysis focus** is on existing customers, not prospects

```python
# # script_10
# SCENARIO 1: Active Customer Portfolio Analysis
# BUSINESS NEED: Analyze only customers who are actively banking with us

print("🤝 Inner Join: Active Customer-Account Relationships")

# Method 1: Explicit join condition (more readable)
active_customers = customers_df.join(
    accounts_df,
    customers_df.customer_id == accounts_df.customer_id,  # Join condition
    "inner"                                               # Join type
)

print("Active customers with their accounts:")
active_customers.select(
    customers_df.customer_id, "first_name", "customer_segment", "account_type", "balance"
).show(10)

print(f"Result: {active_customers.count()} customer-account combinations")

# Method 2: Simplified syntax (when column names match exactly)
active_customers_simple = customers_df.join(accounts_df, "customer_id", "inner")

# BUSINESS INSIGHTS FROM INNER JOIN:
print("\n📊 Business Insights from Active Customers:")

# 1. Portfolio summary by customer segment
portfolio_by_segment = active_customers.groupBy("customer_segment").agg(
    countDistinct(customers_df.customer_id).alias("unique_customers"),
    count("account_id").alias("total_accounts"),
    sum("balance").alias("total_balance")
)
portfolio_by_segment.show()

# 2. Account type distribution
account_distribution = active_customers.groupBy("account_type").agg(
    countDistinct(customers_df.customer_id).alias("customers"),
    sum("balance").alias("total_balance")
)
account_distribution.show()

# BUSINESS VALUE:
# ✅ Focus on revenue-generating customers
# ✅ Understand product penetration
# ✅ Calculate portfolio metrics accurately
# ✅ Identify cross-selling opportunities within active base
```

### 4.3 Left Join - Complete Customer View (Including Prospects)

**When to Use Left Join:**

- You want **all customers** regardless of whether they have accounts
- **Prospect identification** (customers without products)
- **Complete customer view** for relationship management
- **Cross-selling analysis** to find gaps

```python
# SCENARIO 2: Complete Customer Relationship Analysis
# BUSINESS NEED: See all customers, identify those without accounts for cross-selling

print("👥 Left Join: Complete Customer View")

customer_complete_view = customers_df.join(accounts_df, "customer_id", "left")

print("Complete customer view (all customers + their accounts if any):")
customer_complete_view.select(
    customers_df.customer_id, "first_name", "customer_segment", "annual_income", "account_type", "balance"
).show(15)

# IDENTIFY CUSTOMERS WITHOUT ACCOUNTS (PROSPECTS)
print("\n🚨 Prospect Identification: Customers Without Accounts")

prospects = customer_complete_view.filter(col("account_id").isNull())

print("Customers without any accounts (prospects):")
prospects.select(customers_df.customer_id, "first_name", "last_name", "customer_segment", "annual_income").show()

print(f"Total prospects identified: {prospects.count()}")

# BUSINESS IMPACT OF PROSPECT IDENTIFICATION:
# - These are customers who registered but never opened accounts
# - High-income prospects are priority for sales outreach
# - Segment-specific marketing campaigns can be designed
# - Convert prospects to revenue-generating customers

# CROSS-SELLING OPPORTUNITY ANALYSIS
print("\n💰 Cross-Selling Analysis")

# Calculate accounts per customer
accounts_per_customer = customer_complete_view.groupBy(
    customers_df.customer_id, "first_name", "customer_segment", "annual_income"
).agg(
    count("account_id").alias("account_count"),
    sum("balance").alias("total_balance")
).fillna(0)  # Replace null balances with 0

# Identify high-value customers with few accounts (cross-sell opportunities)
cross_sell_opportunities = accounts_per_customer.filter(
    (col("annual_income") > 75000) & 
    (col("account_count") <= 1)
)

print("High-income customers with ≤1 account (cross-sell targets):")
cross_sell_opportunities.orderBy(desc("annual_income")).show()

# BUSINESS VALUE OF LEFT JOIN:
print("\n💡 Left Join Business Value:")
print("✅ Complete customer view for relationship management")
print("✅ Prospect identification for sales pipeline")
print("✅ Cross-selling opportunity analysis")
print("✅ Customer lifecycle understanding")
print("✅ Revenue potential assessment")
```



### 4.4 Advanced Join Scenarios and Portfolio Analysis

```python
from pyspark.sql.functions import col, desc, asc, when, concat, lit, substring, countDistinct, count, sum, avg, collect_set

# SCENARIO 3: Customer Portfolio Aggregation
# BUSINESS NEED: Calculate comprehensive portfolio metrics per customer

print("💼 Customer Portfolio Summary Analysis")

customer_portfolio = customers_df.join(accounts_df, "customer_id", "left") \
    .groupBy(customers_df.customer_id, "first_name", "last_name", "customer_segment", "annual_income", "risk_score") \
    .agg(
        # Account counts
        count("account_id").alias("total_accounts"),
        countDistinct("account_type").alias("unique_account_types"),
        
        # Financial metrics
        sum("balance").alias("total_balance"),
        avg("balance").alias("avg_account_balance"),
        
        # Asset/Liability breakdown
        sum(when(col("balance") > 0, col("balance")).otherwise(0)).alias("total_assets"),
        sum(when(col("balance") < 0, col("balance")).otherwise(0)).alias("total_liabilities"),
        
        # Account type diversity
        collect_set("account_type").alias("account_types_held")
    ).fillna(0)  # Replace nulls with 0 for customers without accounts

print("Customer portfolio summary:")
customer_portfolio.select(
    customers_df.customer_id, "first_name", "customer_segment", "total_accounts", 
    "total_balance", "total_assets", "total_liabilities"
).orderBy(desc("total_balance")).show(10)

# SCENARIO 4: Risk-Adjusted Portfolio Analysis
# BUSINESS NEED: Combine customer risk profile with portfolio performance

print("\n⚖️ Risk-Adjusted Portfolio Analysis")

risk_adjusted_portfolio = customer_portfolio.withColumn("net_worth",
    col("total_assets") + col("total_liabilities")  # Note: liabilities are negative
).withColumn("risk_adjusted_value",
    # Adjust portfolio value based on customer risk score
    col("net_worth") * 
    when(col("risk_score") >= 750, 1.0)      # No adjustment for excellent credit
    .when(col("risk_score") >= 700, 0.95)    # 5% risk adjustment
    .when(col("risk_score") >= 650, 0.90)    # 10% risk adjustment
    .otherwise(0.80)                           # 20% risk adjustment for poor credit
).withColumn("portfolio_risk_category",
    when((col("total_liabilities") < -10000) & (col("risk_score") < 650), "High Risk")
    .when(col("total_liabilities") < -5000, "Medium Risk")
    .when(col("net_worth") > 100000, "Low Risk - High Value")
    .otherwise("Standard Risk")
)

print("Risk-adjusted portfolio analysis:")
risk_adjusted_portfolio.select(
    customers_df.customer_id, "risk_score", "net_worth", 
    "risk_adjusted_value", "portfolio_risk_category"
).orderBy(desc("risk_adjusted_value")).show(10)

# SCENARIO 5: Multiple Table Joins (Advanced)
# If we had a products table, we could join all three tables

print("\n🔗 Multi-Table Join Example (Conceptual)")

# Create a sample products table for demonstration
products_data = [
    ("Checking", "Basic Banking", 0.01, 10.00),
    ("Savings", "Savings Account", 0.02, 0.00),
    ("Credit Card", "Revolving Credit", 0.18, 25.00),
    ("Investment", "Investment Account", 0.05, 50.00)
]

products_df = spark.createDataFrame(
    products_data, 
    ["account_type", "product_name", "interest_rate", "monthly_fee"]
)

# Three-way join: Customers + Accounts + Products
comprehensive_view = customers_df \
    .join(accounts_df, "customer_id", "left") \
    .join(products_df, "account_type", "left")

print("Comprehensive customer-account-product view:")
comprehensive_view.select(
    customers_df.customer_id, "first_name", "account_type", 
    "balance", "product_name", "interest_rate", "monthly_fee"
).show(10)

# Calculate monthly revenue per customer
monthly_revenue = comprehensive_view.groupBy(customers_df.customer_id, "first_name", "customer_segment").agg(
    sum("monthly_fee").alias("monthly_fee_revenue"),
    sum(col("balance") * col("interest_rate") / 12).alias("monthly_interest_revenue")
).withColumn("total_monthly_revenue",
    col("monthly_fee_revenue") + col("monthly_interest_revenue")
).filter(col("total_monthly_revenue") > 0)

print("\nMonthly revenue per customer:")
monthly_revenue.orderBy(desc("total_monthly_revenue")).show()
```

### 4.5 Join Performance Best Practices

```python
# PERFORMANCE OPTIMIZATION FOR JOINS

print("⚡ Join Performance Best Practices")

# ✅ BEST PRACTICE 1: Broadcast small tables
# If products table is small (< 200MB), broadcast it
from pyspark.sql.functions import broadcast

# Create sample products table first
products_data = [
    ("Checking", "Basic Banking", 0.01, 10.00),
    ("Savings", "Savings Account", 0.02, 0.00),
    ("Credit Card", "Revolving Credit", 0.18, 25.00),
    ("Investment", "Investment Account", 0.05, 50.00)
]

products_df = spark.createDataFrame(
    products_data, 
    ["account_type", "product_name", "interest_rate", "monthly_fee"]
)

optimized_join = customers_df.join(accounts_df, "customer_id").join(
    broadcast(products_df),  # Broadcast small table to all nodes
    "account_type"
)

# ✅ BEST PRACTICE 2: Filter before joining
# Filter customers first, then join
high_value_customers = customers_df.filter(col("annual_income") > 100000)
high_value_portfolio = high_value_customers.join(accounts_df, "customer_id")

print(f"High-value customer portfolios: {high_value_portfolio.count()}")

# ✅ BEST PRACTICE 3: Use appropriate join types
# Inner join when you only need matching records (fastest)
active_only = customers_df.join(accounts_df, "customer_id", "inner")

# Left join when you need all customers
complete_view = customers_df.join(accounts_df, "customer_id", "left")

# ✅ BEST PRACTICE 4: Select only needed columns after join
efficient_selection = customers_df.join(accounts_df, "customer_id") \
    .select(customers_df.customer_id, "first_name", "customer_segment", "account_type", "balance")

# ❌ AVOID: Cartesian joins (no join condition)
# This would create every customer paired with every account - very expensive!
# cartesian_join = customers_df.crossJoin(accounts_df)  # Don't do this!

print("✅ Join performance best practices demonstrated")

# MONITORING JOIN PERFORMANCE
print("\n📊 Join Performance Monitoring")

# Use explain() to understand join strategy
print("Join execution plan:")
customers_df.join(accounts_df, "customer_id").explain()

# Check for skewed joins (some keys have many more values)
key_distribution = accounts_df.groupBy("customer_id").count()
skewed_keys = key_distribution.filter(col("count") > 10)
print(f"Customers with >10 accounts (potential skew): {skewed_keys.count()}")
```

### 4.6 Real-World Banking Join Patterns

```python
# COMMON BANKING JOIN PATTERNS

print("🏦 Common Banking Join Patterns")

# PATTERN 1: Customer 360 View
customer_360 = customers_df \
    .join(accounts_df, "customer_id", "left") \
    .groupBy(customers_df.customer_id, "first_name", "customer_segment", "annual_income") \
    .agg(
        count("account_id").alias("total_accounts"),
        sum("balance").alias("total_balance"),
        collect_set("account_type").alias("products_held")
    )

# PATTERN 2: Product Penetration Analysis
product_penetration = customers_df \
    .join(accounts_df, "customer_id", "inner") \
    .groupBy("customer_segment", "account_type") \
    .count() \
    .orderBy("customer_segment", desc("count"))

print("Product penetration by segment:")
product_penetration.show()

# PATTERN 3: Risk Assessment Join
risk_assessment = customers_df \
    .join(accounts_df, "customer_id", "left") \
    .withColumn("portfolio_risk",
        when(col("balance") < -5000, "High Risk")
        .when(col("balance") < 0, "Medium Risk")
        .otherwise("Low Risk")
    ) \
    .groupBy(customers_df.customer_id, "risk_score") \
    .agg(
        collect_set("portfolio_risk").alias("risk_factors"),
        sum("balance").alias("net_position")
    )

# PATTERN 4: Cross-Selling Opportunity Matrix
cross_sell_matrix = customers_df \
    .join(accounts_df, "customer_id", "left") \
    .groupBy("customer_segment") \
    .agg(
        countDistinct(customers_df.customer_id).alias("total_customers"),
        countDistinct(when(col("account_type") == "Checking", customers_df.customer_id)).alias("has_checking"),
        countDistinct(when(col("account_type") == "Savings", customers_df.customer_id)).alias("has_savings"),
        countDistinct(when(col("account_type") == "Credit Card", customers_df.customer_id)).alias("has_credit_card"),
        countDistinct(when(col("account_type") == "Investment", customers_df.customer_id)).alias("has_investment")
    )

print("Cross-selling opportunity matrix:")
cross_sell_matrix.show()

print("✅ All join patterns demonstrated successfully!")
```

------



## 5. Essential Operations for Analytics

### 5.1 Statistics and Aggregations

Basic statistics and aggregation techniques give banks a clear view of their customers, portfolios, and overall business health. These methods are the **foundation for dashboards, regulatory reporting, risk assessment, and strategic planning**. They cover two main areas:

- **Counting & Descriptive Statistics**: Total customers, portfolio size, balances, income levels, credit scores, and uniqueness checks (e.g., duplicate emails). These metrics reveal customer base composition, account balances, profitability indicators, and risk distribution.
- **Grouping & Aggregation**: Similar to Excel pivot tables, grouping enables deeper insights by segment, product type, geography, and risk tier. It highlights patterns such as segment performance, account adoption, geographic concentration, cross-sell opportunities, and customer value tiers.

Together, these approaches move from **simple counts and averages** to **multi-dimensional business insights**—showing where growth is happening, which products drive value, how risk is distributed, and where opportunities exist for cross-sell or customer retention.

------

```python
# script 11
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, avg, min, max, stddev, when, round, desc, approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FilteringBestPractices") \
    .getOrCreate()

# Define customer schema
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("account_open_date", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("annual_income", DoubleType(), True),
    StructField("birth_date", StringType(), True),
    StructField("employment_status", StringType(), True),
    StructField("marital_status", StringType(), True)
])

# Load customer data
customers_df = spark.read \
    .schema(customer_schema) \
    .option("header", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# Define accounts schema
accounts_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("credit_limit", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("account_status", StringType(), True),
    StructField("open_date", StringType(), True),
    StructField("last_activity_date", StringType(), True)
])

# Load accounts data
accounts_df = spark.read \
    .schema(accounts_schema) \
    .option("header", "true") \
    .csv("banking_dataset/datasets/accounts.csv")
print("📊 Core Banking Statistics and Aggregations")

# 1. Customer Base Overview
total_customers = customers_df.count()
print(f"Total Customers: {total_customers:,}")

segment_stats = customers_df.groupBy("customer_segment").agg(
    count("customer_id").alias("customer_count"),
    avg("annual_income").alias("avg_income"),
    avg("risk_score").alias("avg_risk_score")
).orderBy(desc("customer_count"))
print("Customer Segment Stats:")
segment_stats.show()

# 2. Portfolio Analysis
portfolio_stats = accounts_df.agg(
    count("account_id").alias("total_accounts"),
    sum("balance").alias("total_balance"),
    avg("balance").alias("avg_balance")
).collect()[0]

print(f"Total Accounts: {portfolio_stats['total_accounts']:,}")
print(f"Total Portfolio Value: ${portfolio_stats['total_balance']:,.2f}")
print(f"Average Balance: ${portfolio_stats['avg_balance']:,.2f}")

account_stats = accounts_df.groupBy("account_type").agg(
    count("account_id").alias("accounts"),
    sum("balance").alias("total_balance"),
    avg("balance").alias("avg_balance")
).orderBy(desc("total_balance"))
print("Accounts by Type:")
account_stats.show()

# 3. Risk and Credit Distribution
risk_tiers = customers_df.withColumn("risk_tier",
    when(col("risk_score") >= 750, "Low Risk")
    .when(col("risk_score") >= 650, "Medium Risk")
    .otherwise("High Risk")
).groupBy("risk_tier").agg(
    count("customer_id").alias("customer_count"),
    avg("annual_income").alias("avg_income")
).orderBy("risk_tier")
print("Risk Tier Distribution:")
risk_tiers.show()

# 4. Geographic Insights
geo_stats = customers_df.groupBy("state").agg(
    count("customer_id").alias("customers"),
    avg("annual_income").alias("avg_income")
).orderBy(desc("customers"))
print("Top Markets by Customers:")
geo_stats.show(10)

# 5. Customer Value (Simple CLV Estimate)
customer_value = customers_df.join(accounts_df, "customer_id", "left").groupBy("customer_id").agg(
    sum("balance").alias("total_balance"),
    countDistinct("account_type").alias("product_variety")
).withColumn("estimated_value",
    (col("total_balance") * 0.02) + (col("product_variety") * 120)
)

value_tiers = customer_value.groupBy(
    when(col("estimated_value") >= 5000, "High Value")
    .when(col("estimated_value") >= 2000, "Medium Value")
    .otherwise("Standard")
).agg(
    count("customer_id").alias("customer_count"),
    avg("estimated_value").alias("avg_value")
)
print("Customer Value Tiers:")
value_tiers.show()

# Clean up
spark.stop()
```



------

### 5.2 Aggregation Performance Best Practices

When working with large banking datasets, performance matters. Even small inefficiencies can multiply at scale. Here are some **practical optimization patterns** to keep your aggregations fast and reliable:

```python
print("⚡ Aggregation Performance Best Practices")

# PATTERN 1: Efficient Aggregation Functions
# - Use count("*") instead of count("col") for speed
# - Use approximate distinct counts when exact precision is not required
efficient_agg = customers_df.groupBy("customer_segment").agg(
    count("*").alias("customer_count"),
    avg("annual_income").alias("avg_income"),
    approx_count_distinct("customer_id", 0.05).alias("approx_unique_customers")
)
efficient_agg.show()

# PATTERN 2: Filter Before Grouping
# - Reduce dataset size before aggregation to save computation
filtered_agg = customers_df.filter(col("annual_income") > 50000) \
    .groupBy("customer_segment").count()
filtered_agg.show()

# PATTERN 3: Cache Intermediate Results
# - Cache reusable joins or pre-aggregated tables if multiple analyses depend on them
analysis_base = customers_df.join(accounts_df, "customer_id", "left").cache()

agg_by_segment = analysis_base.groupBy("customer_segment").count()
agg_by_product = analysis_base.groupBy("account_type").sum("balance")
agg_cross = analysis_base.groupBy("customer_segment", "account_type").avg("balance")

print("✅ Caching used to accelerate repeated aggregations")
analysis_base.unpersist()
```

**Performance Insights:**

- Use **approximate counts** when precision is not business-critical.
- Always **filter early** to minimize the data you process.
- Apply **caching** when multiple queries rely on the same intermediate dataset.

---



## 6. Common Pitfalls and Best Practices

### 6.1 Performance Optimization for Large Banking Datasets

**Why Performance Matters in Banking:** Banking datasets are typically **massive** (millions of customers, billions of transactions). Poor performance leads to:

- **Delayed reporting** to regulators (compliance violations)
- **Slow customer service** (poor customer experience)
- **High infrastructure costs** (wasted compute resources)
- **Failed batch jobs** (operational disruptions)

```python
# script_12
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, count, avg, broadcast, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FilteringBestPractices") \
    .getOrCreate()

# Define customer schema
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("account_open_date", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("annual_income", DoubleType(), True),
    StructField("birth_date", StringType(), True),
    StructField("employment_status", StringType(), True),
    StructField("marital_status", StringType(), True)
])

# Load customer data
customers_df = spark.read \
    .schema(customer_schema) \
    .option("header", "true") \
    .csv("banking_dataset/datasets/customers.csv")

# Define accounts schema
accounts_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("credit_limit", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("account_status", StringType(), True),
    StructField("open_date", StringType(), True),
    StructField("last_activity_date", StringType(), True)
])

# Load accounts data
accounts_df = spark.read \
    .schema(accounts_schema) \
    .option("header", "true") \
    .csv("banking_dataset/datasets/accounts.csv")

# PERFORMANCE OPTIMIZATION STRATEGIES
print("⚡ Performance Optimization for Banking Analytics")

# ❌ COMMON MISTAKE 1: Using collect() on large datasets
print("\n1️⃣ Memory Management - collect() vs show()")

# DON'T DO THIS with large datasets:
# all_customers = customers_df.collect()  # Brings ALL data to driver - memory crash!

# ✅ DO THIS instead:
customers_df.show(20)                     # Shows only 20 rows
customers_count = customers_df.count()    # Returns just the count number

print(f"Safe way to inspect data: {customers_count} total customers")
print("Sample shown above (20 rows only)")

# ❌ COMMON MISTAKE 2: Not caching frequently used DataFrames
print("\n2️⃣ Caching Strategy for Repeated Operations")

# If you'll use a DataFrame multiple times, cache it:
frequently_used = customers_df.filter(col("customer_segment") == "Premium")

# ✅ Cache before multiple operations
frequently_used.cache()

# Now these operations will be faster:
premium_count = frequently_used.count()
premium_income = frequently_used.agg(avg("annual_income")).collect()[0][0]
premium_by_state = frequently_used.groupBy("state").count()

print(f"Premium customers: {premium_count}")
print(f"Average premium income: ${premium_income:,.2f}")

# ✅ Clean up cache when done
frequently_used.unpersist()

# ✅ BEST PRACTICE 3: Select only needed columns early
print("\n3️⃣ Column Selection for Performance")

# Bad: Processing all columns when you only need a few
# all_columns = customers_df.join(accounts_df, "customer_id").groupBy("customer_segment").sum("balance")

# Good: Select only needed columns first
optimized_analysis = customers_df.select("customer_id", "customer_segment") \
    .join(accounts_df.select("customer_id", "balance"), "customer_id") \
    .groupBy("customer_segment").sum("balance")

print("Optimized column selection demonstrated")

# ✅ BEST PRACTICE 4: Filter early in your pipeline
print("\n4️⃣ Early Filtering Strategy")

# Bad: Filtering after expensive operations
# expensive_then_filter = customers_df.join(accounts_df, "customer_id") \
#     .groupBy("customer_id").sum("balance") \
#     .filter(col("sum(balance)") > 100000)

# Good: Filter early to reduce data volume
filter_then_process = customers_df.filter(col("annual_income") > 75000) \
    .join(accounts_df, "customer_id") \
    .groupBy("customer_id").sum("balance")

print("Early filtering strategy applied")

# ✅ BEST PRACTICE 5: Use appropriate join strategies
print("\n5️⃣ Join Optimization")

# If one table is small (< 200MB), broadcast it
small_reference_table = spark.createDataFrame([
    ("Premium", 0.15),
    ("Standard", 0.05),
    ("Basic", 0.02)
], ["customer_segment", "fee_rate"])

# Broadcast the small table
optimized_join = customers_df.join(
    broadcast(small_reference_table),
    "customer_segment"
)
print("Broadcast join optimization applied")
```

### 6.2 Data Quality and Validation

**Why Data Quality is Critical in Banking:**

- **Regulatory compliance** requires accurate data
- **Risk calculations** must be precise
- **Customer trust** depends on correct account information
- **Financial decisions** based on wrong data can cause losses

```python
# DATA QUALITY VALIDATION FRAMEWORK
print("🔍 Comprehensive Data Quality Validation")

# VALIDATION 1: Check for required fields
print("1️⃣ Required Field Validation")
required_field_check = customers_df.select([
    count(when(col(c).isNull(), c)).alias(f"{c}_nulls")
    for c in ["customer_id", "first_name", "last_name"]
])
print("Null counts in required fields:")
required_field_check.show()

# VALIDATION 2: Business rule validation
print("\n2️⃣ Business Rule Validation")

# Check for invalid risk scores (should be 300-850)
invalid_risk_scores = customers_df.filter(
    (col("risk_score") < 300) | (col("risk_score") > 850)
).count()
print(f"Invalid risk scores: {invalid_risk_scores}")

# Check for negative incomes
negative_incomes = customers_df.filter(col("annual_income") < 0).count()
print(f"Customers with negative income: {negative_incomes}")

# Check for unrealistic high incomes (potential data entry errors)
extreme_incomes = customers_df.filter(col("annual_income") > 10_000_000).count()
print(f"Customers with >$10M income (review needed): {extreme_incomes}")

# VALIDATION 3: Referential integrity
print("\n3️⃣ Referential Integrity Validation")

# Accounts without valid customers (anti-join)
orphaned_accounts = accounts_df.join(
    customers_df.select("customer_id"),
    "customer_id",
    "left_anti"  # records in accounts NOT in customers
).count()
print(f"Accounts without valid customer records: {orphaned_accounts}")

# VALIDATION 4: Duplicate detection
print("\n4️⃣ Duplicate Detection")

# Duplicate customer IDs
customer_id_duplicates = customers_df.groupBy("customer_id").count() \
    .filter(col("count") > 1).count()
print(f"Duplicate customer IDs: {customer_id_duplicates}")

# Duplicate email addresses (non-null)
email_duplicates = customers_df.filter(col("email").isNotNull()) \
    .groupBy("email").count() \
    .filter(col("count") > 1).count()
print(f"Duplicate email addresses: {email_duplicates}")

# VALIDATION 5: Data type and format validation
print("\n5️⃣ Data Type and Format Validation")

# Email format validation (basic '@' check)
invalid_emails = customers_df.filter(
    col("email").isNotNull() & (~col("email").contains("@"))
).count()
print(f"Invalid email formats: {invalid_emails}")

# Customer ID format validation (CUST + 6 digits)
invalid_customer_ids = customers_df.filter(
    ~col("customer_id").rlike(r"^CUST\d{6}$")  # e.g., CUST012345
).count()
print(f"Invalid customer ID formats: {invalid_customer_ids}")

# VALIDATION 6: Statistical outlier detection
print("\n6️⃣ Statistical Outlier Detection")

# Income quartiles (IQR method)
q1, q3 = customers_df.approxQuantile("annual_income", [0.25, 0.75], 0.01)
iqr = q3 - q1
lower_bound = q1 - (1.5 * iqr)
upper_bound = q3 + (1.5 * iqr)

income_outliers = customers_df.filter(
    (col("annual_income") < lower_bound) | (col("annual_income") > upper_bound)
).count()
print(f"Income outliers (IQR method): {income_outliers}")
print(f"Normal income range: ${lower_bound:,.2f} - ${upper_bound:,.2f}")

# VALIDATION 7: Cross-field validation
print("\n7️⃣ Cross-Field Business Logic Validation")

# Premium customers should have high income
premium_low_income = customers_df.filter(
    (col("customer_segment") == "Premium") & (col("annual_income") < 100_000)
).count()
print(f"Premium customers with income <$100K (review needed): {premium_low_income}")

# Excellent risk score with low income (unusual)
high_risk_low_income = customers_df.filter(
    (col("risk_score") >= 750) & (col("annual_income") < 50_000)
).count()
print(f"Excellent risk score with low income (unusual pattern): {high_risk_low_income}")

# VALIDATION SUMMARY REPORT
print("\n📋 Data Quality Summary Report")
total_customers = customers_df.count()

validation_summary = [
    ("Total Customers", total_customers),
    ("Invalid Risk Scores", invalid_risk_scores),
    ("Negative Incomes", negative_incomes),
    ("Extreme Incomes", extreme_incomes),
    ("Orphaned Accounts", orphaned_accounts),
    ("Duplicate Customer IDs", customer_id_duplicates),
    ("Duplicate Emails", email_duplicates),
    ("Invalid Emails", invalid_emails),
    ("Invalid Customer ID Format", invalid_customer_ids),
    ("Income Outliers", income_outliers),
    ("Premium-Income Mismatches", premium_low_income),
    ("Risk-Income Anomalies", high_risk_low_income),
]

print("\nValidation Results:")
for metric, value in validation_summary:
    status = "✅ PASS" if value == 0 else f"⚠️ {value} issues"
    print(f"{metric:<28}: {status}")
```



### 6.3 Memory Management and Resource Optimization

```python
# MEMORY MANAGEMENT BEST PRACTICES

print("💾 Memory Management and Resource Optimization")

# ✅ BEST PRACTICE 1: Understand DataFrame operations
print("1️⃣ Lazy vs Eager Evaluation")

# These operations are LAZY (no computation until action)
lazy_operations = customers_df.filter(col("annual_income") > 100000) \
                              .select("customer_id", "customer_segment") \
                              .withColumn("high_value", lit(True))

print("Lazy operations defined (no computation yet)")

# This triggers EAGER evaluation (actual computation)
result_count = lazy_operations.count()
print(f"Action triggered: {result_count} high-value customers")

# ✅ BEST PRACTICE 2: Partition awareness
print("\n2️⃣ Partition Management")

# Check current partitioning
print(f"Current partitions: {customers_df.rdd.getNumPartitions()}")

# Repartition for better parallelism (rule of thumb: 2-4 partitions per core)
optimized_partitions = customers_df.repartition(4)
print(f"Optimized partitions: {optimized_partitions.rdd.getNumPartitions()}")

# ✅ BEST PRACTICE 3: Avoid wide transformations when possible
print("\n3️⃣ Transformation Optimization")

# Wide transformation (expensive - requires shuffle)
wide_transform = customers_df.groupBy("state").count()

# Narrow transformation (cheap - no shuffle)
narrow_transform = customers_df.filter(col("customer_segment") == "Premium")

print("Transformation types demonstrated")

# ✅ BEST PRACTICE 4: Use appropriate data types
print("\n4️⃣ Data Type Optimization")

# Convert string columns to more efficient types when possible
optimized_customers = customers_df.withColumn("risk_score", col("risk_score").cast("integer")) \
                                  .withColumn("annual_income", col("annual_income").cast("double"))

print("Data type optimization applied")

# ✅ BEST PRACTICE 5: Clean up resources
print("\n5️⃣ Resource Cleanup")

# Explicitly unpersist cached DataFrames when done
# cached_df.unpersist()

# Clear the catalog cache
spark.catalog.clearCache()

print("Resources cleaned up")
```

### 6.4 Error Handling and Debugging

```python
# ERROR HANDLING AND DEBUGGING STRATEGIES
print("🐛 Error Handling and Debugging Best Practices")

# STRATEGY 1: Graceful error handling
print("1️⃣ Graceful Error Handling")

try:
    # Operation that might fail
    risky_calculation = customers_df.withColumn("risk_ratio", 
        col("annual_income") / col("risk_score")
    )
    
    result = risky_calculation.count()
    print(f"✅ Calculation successful: {result} records")
    
except Exception as e:
    print(f"❌ Error occurred: {str(e)}")
    print("Applying fallback strategy...")
    
    # Fallback: Handle division by zero
    safe_calculation = customers_df.withColumn("risk_ratio",
        when(col("risk_score") > 0, col("annual_income") / col("risk_score"))
        .otherwise(0)
    )

# STRATEGY 2: Data validation before processing
print("\n2️⃣ Proactive Data Validation")

def validate_dataframe(df, required_columns):
    """Validate DataFrame before processing"""
    
    # Check if DataFrame is not empty
    if df.count() == 0:
        raise ValueError("DataFrame is empty")
    
    # Check if required columns exist
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for null values in critical columns
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in required_columns
    ]).collect()[0].asDict()
    
    critical_nulls = {col: count for col, count in null_counts.items() if count > 0}
    if critical_nulls:
        print(f"⚠️ Warning: Null values found: {critical_nulls}")
    
    return True

# Validate before processing
try:
    validate_dataframe(customers_df, ["customer_id", "customer_segment", "annual_income"])
    print("✅ Data validation passed")
except ValueError as e:
    print(f"❌ Data validation failed: {e}")

# STRATEGY 3: Debugging with explain()
print("\n3️⃣ Performance Debugging")

# Use explain() to understand query execution
complex_query = customers_df.join(accounts_df, "customer_id") \
                           .groupBy("customer_segment") \
                           .agg(sum("balance"))

print("Query execution plan:")
complex_query.explain(True)  # True for extended explanation

# STRATEGY 4: Step-by-step debugging
print("\n4️⃣ Step-by-Step Debugging")

# Break complex operations into steps for easier debugging
step1 = customers_df.filter(col("annual_income") > 50000)
print(f"Step 1 - High income filter: {step1.count()} records")

step2 = step1.join(accounts_df, "customer_id")  
print(f"Step 2 - After join: {step2.count()} records")

step3 = step2.groupBy("customer_segment").agg(avg("balance"))
print("Step 3 - Final aggregation:")
step3.show()

# STRATEGY 5: Logging and monitoring
print("\n5️⃣ Logging and Monitoring")

import time

def timed_operation(operation_name, operation_func):
    """Time operations for performance monitoring"""
    start_time = time.time()
    try:
        result = operation_func()
        end_time = time.time()
        print(f"✅ {operation_name}: {end_time - start_time:.2f} seconds")
        return result
    except Exception as e:
        end_time = time.time()
        print(f"❌ {operation_name} failed after {end_time - start_time:.2f} seconds: {e}")
        return None

# Example usage
result = timed_operation(
    "Customer aggregation",
    lambda: customers_df.groupBy("customer_segment").count().collect()
)

print("✅ Error handling and debugging strategies demonstrated")
```



## 7. Hands-On Practice Exercise 

### Scenario

You’ve joined **NorthStar Bank** Ops Analytics. By end of day, you must deliver a **Customer Contactability & Activation Pack** for Operations—**not** a 360 dashboard or segment summary. Build **new** operational artifacts using only Module 1 skills (schemas, select/filter, withColumn/when, join, groupBy/agg, orderBy, cache/unpersist).

## Data

- `customers_df`: id, names, email, phone, state, customer_segment, annual_income, risk_score, …
- `accounts_df`: account_id, customer_id, account_type, balance, account_status, last_activity_date (string), …

> You may load CSVs with **explicit schemas** or use provided utilities. You’ll also create one tiny reference DataFrame in code.

------

## Deliverables (build these—no solved code provided)

### D0. Spark Setup

- Start a session named `"NorthStarOpsLab"` with AQE enabled.
- Print Spark version.

**Check**: `spark.sparkContext.appName == "NorthStarOpsLab"` and `spark.conf.get("spark.sql.adaptive.enabled") == "true"`.

------

### D1. Intake & Reference Table

- Load `customers_df` and `accounts_df` with **explicit schemas** (no inference).
- Create a **consent reference** in code:
  - `consents_df(customer_id, email_opt_in:boolean, sms_opt_in:boolean)`, at least 6 rows (mix of True/False).

**Check**: no nulls in `customer_id`, `account_id`.

------

### D2. Contactability & Compliance Flags 

Build `contact_flags_df` from `customers_df` (+ join `consents_df`):

- `has_email` (non-null & contains `"@"`)
- `has_phone` (non-null)
- `is_contactable_email` = `has_email & email_opt_in`
- `is_contactable_sms` = `has_phone & sms_opt_in`
- `contact_score` = `is_contactable_email*2 + is_contactable_sms*1` (ints 0–3)
- `id_format_valid` = `customer_id` matches `^CUST\\d{6}$`

**Check**: `contact_score` distribution printed (count per score).

------

### D3. Prospect & Dormancy Map 

Produce `relationship_ops_df`:

- Join **column-pruned** `customers_df` ↔ `accounts_df` (left).
- Per customer, aggregate:
  - `total_accounts`
  - `has_active_account` (any `account_status == "Active"`)
  - `days_since_last_activity` (use `to_date` + datediff from a fixed anchor like `"2025-06-30"`)
  - `is_dormant` = `days_since_last_activity > 90` **and** `total_accounts > 0`
  - `is_prospect` = `total_accounts == 0`

**Check**: counts for `is_prospect` and `is_dormant`.

------

### D4. Product Gaps Matrix 

From the joined base, compute booleans per customer:

- `has_checking`, `has_savings`, `has_credit_card`, `has_investment`
- `gap_code` = concatenation like `"CHK0_SAV1_CC0_INV1"` (0/1 for each presence)

**Check**: print top 10 distinct `gap_code` values with counts.

------

### D5. Target Lists (new operational outputs)

Create three **distinct** DataFrames:

1. **Activation Kit** (`activation_df`)
   - Criteria: `is_prospect` **and** `annual_income >= 70_000` **and** `contact_score >= 2`.
   - Columns: `customer_id, state, annual_income, contact_score`.
2. **Reactivation Kit** (`reactivation_df`)
   - Criteria: `is_dormant` **and** `total_accounts >= 1` **and** `sum(balance) > 0`.
   - Columns: `customer_id, days_since_last_activity, total_accounts, total_balance`.
3. **Safety Net Saver** (`saver_df`)
   - Criteria: `has_checking == 1` **and** `has_savings == 0` **and** `annual_income >= 80_000` **and** `risk_score >= 650`.
   - Columns: `customer_id, annual_income, risk_score, gap_code`.

**Check**: each list has ≥ 1 row (based on sample data); show counts and top 5 rows.

------

### D6. State Ops Brief (new)

Group **by `state`** and compute:

- `prospects` (count where `is_prospect`)
- `dormant_customers` (count where `is_dormant`)
- `contactable_rate` = `% of customers with contact_score ≥ 2`

Sort by `prospects` **desc**; print top 10.

------

### D7. Performance Guardrails

- Build a **single** joined `analysis_base` with only needed columns, `cache()` it, reuse in D3–D6, then `unpersist()`.
- Demonstrate **filter-early** and **select-early** once (comment it).

------

### D8. Ops Readout (concise printout)

Print ≤ 15 lines:

- Total customers, accounts
- Prospects, Dormant counts
- Top 2 states by prospects
- Row counts for each target list (Activation, Reactivation, Saver)

------

## Starter Skeleton (fill the TODOs)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, sum, avg, count, countDistinct, desc, lit,
    to_date, datediff
)

# D0 ─ Spark
spark = SparkSession.builder \
    .appName("NorthStarOpsLab") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
print(f"✅ Spark {spark.version} started with AQE:",
      spark.conf.get("spark.sql.adaptive.enabled"))

# D1 ─ Load data with explicit schemas (TODO) → customers_df, accounts_df
# Also create consents_df in code (TODO)

# D2 ─ Contactability & Compliance Flags
contact_flags_df = (
    customers_df
    .join(consents_df, "customer_id", "left")
    .withColumn("has_email", col("email").isNotNull() & col("email").contains("@"))
    .withColumn("has_phone", col("phone").isNotNull())
    .withColumn("is_contactable_email", col("has_email") & col("email_opt_in"))
    .withColumn("is_contactable_sms", col("has_phone") & col("sms_opt_in"))
    .withColumn("contact_score",
        when(col("is_contactable_email"), lit(2)).otherwise(lit(0)) +
        when(col("is_contactable_sms"), lit(1)).otherwise(lit(0))
    )
    .withColumn("id_format_valid", col("customer_id").rlike(r"^CUST\d{6}$"))
)
# TODO: print distribution of contact_score

# D3 ─ Prospect & Dormancy Map
anchor = lit("2025-06-30")
analysis_base = (
    customers_df.select("customer_id","state","customer_segment","annual_income","risk_score","email","phone")
    .join(accounts_df.select("customer_id","account_id","account_type","balance","account_status","last_activity_date"),
          "customer_id", "left")
    .cache()
)

per_customer = (
    analysis_base
    .withColumn("last_dt", to_date(col("last_activity_date")))
    .groupBy("customer_id","state","customer_segment","annual_income","risk_score")
    .agg(
        count("account_id").alias("total_accounts"),
        sum(when(col("account_status")=="Active", lit(1)).otherwise(lit(0))).alias("active_accts"),
        sum("balance").alias("total_balance"),
        # max last activity date per customer
        # use max(last_dt) when present
        )
    # TODO: compute max last_dt, then datediff(anchor, max_last_dt) as days_since_last_activity
    # TODO: is_dormant, is_prospect
)

# D4 ─ Product Gaps Matrix
# TODO: derive has_checking/savings/credit_card/investment and gap_code

# D5 ─ Target Lists
# TODO: activation_df, reactivation_df, saver_df with required columns

# D6 ─ State Ops Brief
# TODO: group by state to compute prospects, dormant_customers, contactable_rate

# D7 ─ Unpersist after reuse
# analysis_base.unpersist()

# D8 ─ Ops Readout (print concise summary)
# TODO: compute and print summary lines
```

------

## Hints

- Use `max(to_date(last_activity_date))` to get the last activity per customer, then `datediff(anchor, max_date)`.
- For product gaps, `sum(when(col("account_type")=="Checking",1).otherwise(0)) > 0` → `has_checking` (cast to int).
- Build `gap_code` with `concat_ws` or chained `concat` on 0/1 flags.



### 7.2 Self-Assessment Questions

Use these questions to evaluate your comprehension of the key concepts covered in Module 1. Take time to think through each answer before moving to the next module.

#### Question 1: Spark Session Fundamentals

**Question:** Why do we create a SparkSession at the beginning of every PySpark script? What would happen if we tried to create DataFrames without it?

**Think about:**

- The role of SparkSession as the entry point
- Resource management and coordination
- What happens when you skip this step

**Your Answer:** ________________________________

#### Question 2: Join Strategy Selection

**Question:** You're analyzing customer portfolios. When would you use a LEFT JOIN versus an INNER JOIN when combining customer and account data? Give a specific business scenario for each.

**Consider:**

- LEFT JOIN: All customers + their accounts (if any)
- INNER JOIN: Only customers who have accounts
- Business implications of each choice

**Your Answer:** ________________________________

#### Question 3: Performance and Memory Management

**Question:** What's problematic about using `customers_df.collect()` on a large banking dataset? What should you do instead?

**Think about:**

- Where the data goes when you use collect()
- Memory limitations of the driver node
- Alternative methods for data inspection

**Your Answer:** ________________________________

#### Question 4: Data Quality Validation

**Question:** How would you check if the customer_id column contains duplicate values? Write out the logical steps you would take.

**Consider:**

- Grouping and counting techniques
- Filtering for problematic records
- Business impact of duplicates

**Your Answer:** ________________________________

#### Question 5: Business Logic Implementation

**Question:** Write the logical structure (not code) for categorizing customer income into High, Medium, and Low tiers. What income thresholds would you choose and why?

**Think about:**

- Conditional logic structure
- Appropriate income brackets for banking
- Business relevance of the categories

**Your Answer:** ________________________________

#### Question 6: Schema Design

**Question:** Why is explicit schema definition important in banking applications? What could go wrong if you always rely on schema inference?

**Consider:**

- Data type consistency
- Regulatory requirements
- Production reliability

**Your Answer:** ________________________________

#### Question 7: Aggregation Analysis

**Question:** You need to analyze customer portfolio performance by segment. What aggregation functions would you use and what business insights would each provide?

**Think about:**

- Count, sum, average, min, max functions
- Business meaning of each metric
- Decision-making implications

**Your Answer:** ________________________________

#### Question 8: Error Prevention

**Question:** What are three data quality checks you should perform before analyzing customer financial data? Why is each important?

**Consider:**

- Null value detection
- Business rule validation
- Referential integrity

**Your Answer:** ________________________________



------

## Module Summary and Next Steps

### What You've Mastered ✅

**Technical Skills:**

1. **Spark Session Management** - Creating and configuring your data processing engine
2. **DataFrame Creation** - From Python data, CSV, JSON, and Parquet files
3. **Schema Definition** - Explicit data structure for production reliability
4. **Core Operations** - Select, filter, sort, and transform data effectively
5. **Join Operations** - Combining customer, account, and product data
6. **Aggregations** - Grouping and calculating business metrics
7. **Performance Optimization** - Memory management and efficient processing
8. **Data Quality** - Validation, error handling, and debugging

**Business Applications:**

- **Customer 360 View** - Complete customer relationship analysis
- **Portfolio Analysis** - Asset and liability assessment
- **Risk Assessment** - Credit and portfolio risk evaluation
- **Cross-Selling** - Product opportunity identification
- **Segment Performance** - Customer tier comparison and optimization

### Critical Success Factors 🎯

**For Banking Analytics Success:**

1. **Data Quality First** - Always validate before analysis
2. **Performance Awareness** - Design for scale from day one
3. **Business Context** - Understand the "why" behind every metric
4. **Iterative Development** - Start simple, add complexity gradually
5. **Documentation** - Document assumptions and business rules

