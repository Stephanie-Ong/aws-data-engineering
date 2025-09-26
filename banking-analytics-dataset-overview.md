# Banking Analytics Dataset Overview

## S3 Bucket Structure: `banking-analytics-dataset`

### Top-level Folders
- `processed-data/` - Processed parquet files
- `raw-data/` - Raw CSV and JSON data
- `scripts/` - Python processing scripts

## Folder Details

### processed-data/
```
customers_processed/
├── risk_category=High Risk/part-00000-74741f41-71d4-4ed2-be97-cb0dc08cac43.c000.snappy.parquet (184KB)
├── risk_category=Low Risk/part-00000-74741f41-71d4-4ed2-be97-cb0dc08cac43.c000.snappy.parquet (188KB)
└── risk_category=Medium Risk/part-00000-74741f41-71d4-4ed2-be97-cb0dc08cac43.c000.snappy.parquet (137KB)

segment_summary/
└── part-00000-67b808c8-70ab-43c9-ae06-6a55775b584a-c000.snappy.parquet (1KB)

transactions/
├── _SUCCESS
├── part-00000-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
├── part-00001-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
├── part-00002-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
├── part-00003-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
├── part-00004-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
├── part-00005-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
├── part-00006-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
├── part-00007-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
├── part-00008-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (2.8MB)
└── part-00009-3098a5fe-0774-42db-b5e7-732bd1c3c929-c000.snappy.parquet (1.6MB)
```

### raw-data/
```
csv/
├── accounts.csv (769KB)
├── branches.csv (31KB)
├── customers.csv (859KB)
├── loans.csv (553KB)
├── products.csv (442B)
└── transactions.csv (94MB)

json/
└── (empty)
```

### scripts/
```
script_02.py (10KB)
```

## Raw CSV Data Details

### 1. accounts.csv (769KB)
**Schema:**
- account_id, customer_id, account_type, balance, credit_limit, interest_rate, account_status, open_date, last_activity_date

**Sample Data (First 10 rows):**
```
account_id,customer_id,account_type,balance,credit_limit,interest_rate,account_status,open_date,last_activity_date
ACC00000101,CUST000001,Credit Card,-666,9570.0,0.2001,Active,2016-09-06,2025-03-31
ACC00000102,CUST000001,Checking,22600,,0.0,Active,2016-08-29,2024-11-08
ACC00000201,CUST000002,CD,15311,,0.0238,Active,2017-05-20,2024-11-16
ACC00000202,CUST000002,Credit Card,-1812,36541.0,0.2299,Active,2017-05-27,2025-08-08
ACC00000301,CUST000003,CD,3725,,0.0136,Active,2023-03-07,2025-02-25
ACC00000302,CUST000003,Credit Card,-968,30167.0,0.1773,Active,2023-03-17,2025-06-30
ACC00000401,CUST000004,Money Market,56499,,0.0117,Active,2016-11-18,2025-04-23
ACC00000402,CUST000004,CD,76599,,0.0228,Frozen,2016-10-29,2025-01-26
ACC00000501,CUST000005,Credit Card,-809,11337.0,0.2247,Active,2023-04-30,2025-07-19
```

### 2. branches.csv (31KB)
**Schema:**
- branch_id, branch_name, address, city, state, zip_code, region, branch_type, open_date, manager_name, phone, total_deposits, total_loans, employee_count

**Sample Data (First 10 rows):**
```
branch_id,branch_name,address,city,state,zip_code,region,branch_type,open_date,manager_name,phone,total_deposits,total_loans,employee_count
BR001,Josephchester Branch,19524 Chad Square,Garciamouth,NH,28958,Southwest,Limited Service,2020-06-24,Shannon Clark,001-909-613-9460x165,85700045,46033947,10
BR002,Lake Nathanmouth Branch,2358 Murphy Shores,Lake Emma,MO,71750,Northeast,Limited Service,2024-07-16,Cheryl Hopkins,7355324760,37529993,16208579,11
BR003,Jeffreyview Branch,293 Rodgers Causeway Apt. 487,Heatherchester,AR,41898,Midwest,ATM Only,2024-01-23,Maria Simmons,(807)245-6526,77503934,13010142,8
BR004,East Billport Branch,4483 Brian Junction,South Rachelstad,NJ,03811,Northeast,Full Service,2010-09-30,Douglas Duncan,+1-333-552-6823x9906,60253766,23268389,16
BR005,Parkston Branch,9742 John Pines Suite 296,South Loritown,MN,73933,Southeast,Limited Service,2015-12-28,Megan Copeland,001-969-745-3374x82602,55462514,15299326,9
BR006,Hillville Branch,5483 Michael Corner,Moralesfort,NM,37431,Southwest,Limited Service,2012-12-31,Kendra Holland,+1-980-927-1991x5193,95120141,15033179,20
BR007,Port Steven Branch,851 Charles Corners,Powellchester,FM,28543,Southwest,Full Service,2023-06-02,Stephen Smith,+1-594-346-1135,70641729,5724387,4
BR008,East Joshuastad Branch,31631 Garrison Radial,South Melissa,FM,19270,Southeast,Full Service,2018-10-17,Allen Evans,001-971-254-5176x0102,31274250,36664919,11
BR009,Diazborough Branch,332 Shannon Isle,Feliciachester,TX,82700,West,ATM Only,2022-02-23,Dean Austin,001-311-876-3222x4688,32086198,18026260,5
```

### 3. customers.csv (859KB)
**Schema:**
- customer_id, first_name, last_name, email, phone, address, city, state, zip_code, account_open_date, customer_segment, risk_score, annual_income, birth_date, employment_status, marital_status

**Sample Data (First 10 rows):**
```
customer_id,first_name,last_name,email,phone,address,city,state,zip_code,account_open_date,customer_segment,risk_score,annual_income,birth_date,employment_status,marital_status
CUST000001,DANIELLE,Johnson,danielle.johnson@sanchez-taylor.com,+1-581-896-0013x38908,9402 Peterson Drives,Port Matthew,CO,50298,2016-08-19,Standard,724,70741.0,1998-01-21,Employed,Married
CUST000002,CASSANDRA,Roman,cassandra.roman@stevens.com,+1-649-359-3103,64752 Kelly Skyway,Jacquelineland,PA,83728,2017-05-18,Standard,629,71781.0,1990-08-08,Employed,Married
CUST000003,Frank,Gray,frank.gray@watts.com,334-941-1525,4139 Lewis Parks Suite 724,East Julie,ND,34939,2023-02-19,Basic,747,15212.0,1971-08-29,Unemployed,Married
CUST000004,Brandy,Cox,brandy.cox@hicks.com,(981) 212-3504,691 James Mountain,Tashatown,TX,94967,2016-10-23,Premium,850,175072.0,2000-05-28,Employed,Widowed
CUST000005,  karen  ,Mack ,karen.mack@rodriguez-graham.info,5484281489,88095 Smith Lake Suite 303,North Jessicaland,WV,70323,2023-04-29,Standard,614,65108.0,1952-10-03,Self-Employed,Single
CUST000006,Thomas,Pierce,thomas.pierce@rose-obrien.com,652-873-4295,5787 John Circles Suite 098,New Jeffrey,CA,93279,2021-12-23,Standard,614,72837.0,1959-02-02,Employed,Married
CUST000007,  Rebecca  ,Heath ,rebecca.heath@palmer.info,582.399.7376,656 Owens Stream,Lake Deniseville,AR,53273,2022-12-13,Basic,850,22628.0,1995-02-07,Retired,Married
CUST000008,Daniel,Lee,daniel.lee@west.com,(995) 177-8260,80132 Tucker Forest,Barreraburgh,MS,00783,2025-07-09,Standard,765,59260.0,1961-05-17,Retired,Married
CUST000009,Kristen,Gibson,kristen.gibson@horton-cross.net,+1-972-434-3098x05009,121 Emma Freeway,Wilsonshire,IA,76381,2021-08-11,Premium,579,154840.0,1983-09-08,Employed,Single
```

### 4. loans.csv (553KB)
**Schema:**
- loan_id, customer_id, loan_type, original_amount, current_balance, interest_rate, term_months, monthly_payment, origination_date, maturity_date, loan_status, payment_history_score, collateral_value, ltv_ratio, purpose

**Sample Data (First 10 rows):**
```
loan_id,customer_id,loan_type,original_amount,current_balance,interest_rate,term_months,monthly_payment,origination_date,maturity_date,loan_status,payment_history_score,collateral_value,ltv_ratio,purpose
LOAN00208601,CUST002086,Auto,54096,18991.184171769444,0.339,72,1765.81,2020-10-24,2026-09-23,Active,54.8,64915.2,0.8,Refinance
LOAN00208602,CUST002086,Personal,47739,0.0,0.404,24,2931.36,2021-05-06,2023-04-26,Paid Off,54.8,,,Refinance
LOAN00346601,CUST003466,Business,489622,458961.9963316867,0.381,120,15919.6,2022-04-17,2032-02-24,Active,52.8,,,Investment
LOAN00067701,CUST000677,Auto,69104,35836.5050931345,0.322,72,2177.92,2021-06-30,2027-05-30,Active,56.8,82924.8,0.8,Refinance
LOAN00228901,CUST002289,Mortgage,680846,678853.8322740457,0.225,240,12915.44,2024-08-18,2044-05-05,Active,65.9,817015.2,0.8,Investment
LOAN00228902,CUST002289,Credit Line,17951,8062.165172501412,0.34,36,801.9,2023-09-09,2026-08-24,Late,65.9,,,Refinance
LOAN00228903,CUST002289,Auto,76303,47068.367506341645,0.245,36,3013.65,2024-04-07,2027-03-23,Active,65.9,91563.59999999999,0.8,Investment
LOAN00184401,CUST001844,Business,258967,237321.53956336062,0.167,120,4451.68,2023-11-07,2033-09-15,Active,78.0,,,Refinance
LOAN00494101,CUST004941,Auto,34467,1053.6368110340595,0.067,36,1059.52,2022-09-22,2025-09-06,Active,86.8,41360.4,0.8,Purchase
```

### 5. products.csv (442B)
**Schema:**
- product_id, product_name, product_type, monthly_fee, minimum_balance, interest_rate

**Sample Data (All 8 rows):**
```
product_id,product_name,product_type,monthly_fee,minimum_balance,interest_rate
CHK001,Basic Checking,Checking,10.0,100,0.001
CHK002,Premium Checking,Checking,25.0,1000,0.005
SAV001,Basic Savings,Savings,0.0,25,0.01
SAV002,High Yield Savings,Savings,0.0,500,0.025
MM001,Money Market,Money Market,15.0,2500,0.02
CD001,12 Month CD,CD,0.0,1000,0.03
CC001,Basic Credit Card,Credit Card,0.0,0,0.18
CC002,Premium Credit Card,Credit Card,95.0,0,0.15
```

### 6. transactions.csv (94MB)
**Schema:**
- transaction_id, account_id, customer_id, transaction_date, transaction_type, amount, merchant, merchant_category, location, is_international, is_fraud, processing_date, channel, year_month

**Sample Data (First 10 rows):**
```
transaction_id,account_id,customer_id,transaction_date,transaction_type,amount,merchant,merchant_category,location,is_international,is_fraud,processing_date,channel,year_month
TXN00428023,ACC00371901,CUST003719,2023-09-02 16:12:32.672301,Fee,-44.39,,,"East Johnburgh, OR",False,False,2023-09-04 16:12:32.672301,Branch,2023-09
TXN00169065,ACC00147402,CUST001474,2023-09-02 16:13:32.672301,Payment,-1078.45,Harrison Group - Online,Bank,"North Nicholas, SC",False,False,2023-09-02 16:13:32.672301,Card,2023-09
TXN00157076,ACC00137401,CUST001374,2023-09-02 16:15:32.672301,Payment,-1560.47,Lee and Sons - Bank,Gas,"Stevenbury, MO",False,False,2023-09-04 16:15:32.672301,Card,2023-09
TXN00339427,ACC00296201,CUST002962,2023-09-02 16:15:32.672301,Withdrawal,-592.6,"Pope, Cannon and Evans - Restaurant",Gas,"Vincentborough, MO",False,False,2023-09-05 16:15:32.672301,Online,2023-09
TXN00561559,ACC00484402,CUST004844,2023-09-02 16:17:32.672301,Withdrawal,-1282.11,Phillips-Garcia - Bank,ATM,"East Karenside, MH",False,False,2023-09-02 16:17:32.672301,Online,2023-09
TXN00520417,ACC00450803,CUST004508,2023-09-02 16:18:32.672301,Transfer,-122.84,,,"South Frances, MP",False,False,2023-09-03 16:18:32.672301,Mobile,2023-09
TXN00380029,ACC00330401,CUST003304,2023-09-02 16:19:32.672301,Withdrawal,-978.22,Davis-Frazier - Gas,Online,"New Lawrencemouth, MA",False,False,2023-09-03 16:19:32.672301,Branch,2023-09
TXN00339228,ACC00295903,CUST002959,2023-09-02 16:23:32.672301,Withdrawal,-1711.99,Richard Group - Grocery,ATM,"Johnsonshire, AR",False,False,2023-09-02 16:23:32.672301,Mobile,2023-09
TXN00548498,ACC00473302,CUST004733,2023-09-02 16:23:32.672301,Withdrawal,-353.96,Howard LLC - Grocery,Gas,"Lorimouth, AK",False,False,2023-09-04 16:23:32.672301,Card,2023-09
```

## Key Observations

### Data Characteristics
- **Customer Segments:** Basic, Standard, Premium
- **Account Types:** Checking, Credit Card, CD, Money Market, Savings
- **Loan Types:** Auto, Personal, Business, Mortgage, Credit Line
- **Transaction Types:** Fee, Payment, Withdrawal, Transfer
- **Geographic Coverage:** All US states and territories
- **Time Range:** Data spans from 2010 to 2025

### Data Quality Notes
- Some customer names have leading/trailing spaces (CUST000005, CUST000007)
- Credit card accounts show negative balances (represents debt)
- Transaction amounts are negative for outgoing transactions
- Fraud detection flags present (is_fraud column)
- International transaction tracking available

### File Sizes
- **Largest:** transactions.csv (94MB) - main transactional data
- **Medium:** customers.csv (859KB), accounts.csv (769KB), loans.csv (553KB)
- **Small:** branches.csv (31KB), products.csv (442B)

This dataset represents a comprehensive banking analytics environment suitable for various data engineering and analytics use cases including customer segmentation, fraud detection, risk analysis, and operational reporting.