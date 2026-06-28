# Real-World Architecture Patterns

## Pattern 1: Batch ELT Pipeline (Most Common in Finance)

```
Source Systems (Oracle, SQL Server, APIs)
    │
    ▼
Cloud Storage (Raw Zone) ← Ingestion (Datastream CDC / Transfer Service / Custom)
    │
    ▼
BigQuery (Raw Layer) ← External Tables or Load Jobs
    │
    ▼
BigQuery (Curated Layer) ← SQL Transforms (dbt / Scheduled Queries)
    │
    ▼
BigQuery (Serving Layer) ← Aggregated, business-ready tables
    │
    ▼
Looker / Data Studio / APIs ← Consumption
```

### Orchestration: Cloud Composer DAG
```python
ingest_oracle >> load_to_bq_raw >> run_dbt_transforms >> data_quality_checks >> notify_team
```

---

## Pattern 2: Real-Time Streaming Pipeline

```
Event Sources (Payment Gateway, IoT, App Events)
    │
    ▼
Pub/Sub (Ingestion Buffer)
    │
    ▼
Dataflow (Stream Processing) → Windowed Aggregations
    │                          → Enrichment (Side Inputs)
    │                          → Filtering / Routing
    ├────────────────────┐
    ▼                    ▼
BigQuery (Real-time)   Cloud Storage (Archive)
    │
    ▼
Dashboard (Looker) + Alerting (Cloud Monitoring)
```

### Use Case: Fraud Detection
```python
# Dataflow pipeline
(
    messages
    | 'Parse' >> beam.Map(parse_transaction)
    | 'Window' >> beam.WindowInto(SlidingWindows(60, 10))  # 60s window, 10s slide
    | 'KeyByCustomer' >> beam.Map(lambda t: (t['customer_id'], t))
    | 'CountPerWindow' >> beam.combiners.Count.PerKey()
    | 'FlagHigh' >> beam.Filter(lambda kv: kv[1] > 10)  # >10 txns per minute
    | 'Alert' >> beam.Map(send_fraud_alert)
)
```

---

## Pattern 3: Lambda Architecture (Batch + Real-time)

```
┌─────────────────────────────────────────────┐
│              Source Events                    │
└──────────────┬──────────────────┬───────────┘
               │                  │
    ┌──────────▼──────────┐  ┌───▼────────────┐
    │   Batch Layer       │  │ Speed Layer     │
    │   (Dataproc/BQ)     │  │ (Dataflow)      │
    │   Daily aggregation │  │ Real-time view  │
    └──────────┬──────────┘  └───┬────────────┘
               │                  │
    ┌──────────▼──────────────────▼───────────┐
    │        Serving Layer (BigQuery)          │
    │   UNION of batch + real-time views       │
    └─────────────────────────────────────────┘
```

### BigQuery Implementation
```sql
-- Batch view (refreshed daily)
CREATE OR REPLACE TABLE `project.serving.daily_metrics` AS
SELECT date, region, SUM(amount) as daily_total, COUNT(*) as txn_count
FROM `project.curated.transactions`
GROUP BY date, region;

-- Real-time view (streaming inserts)
-- Dataflow writes to: project.serving.realtime_metrics

-- Combined serving view
CREATE VIEW `project.serving.combined_metrics` AS
SELECT * FROM `project.serving.daily_metrics`
WHERE date < CURRENT_DATE()
UNION ALL
SELECT * FROM `project.serving.realtime_metrics`
WHERE date = CURRENT_DATE();
```

---

## Pattern 4: Data Mesh on GCP

```
┌─────────────────────────────────────────────────┐
│                 Data Mesh                         │
├───────────────┬───────────────┬─────────────────┤
│  Domain: Loans│ Domain: Cards │ Domain: Payments│
│  ┌──────────┐ │ ┌──────────┐ │ ┌──────────┐   │
│  │Own BQ DS │ │ │Own BQ DS │ │ │Own BQ DS │   │
│  │Own Pipelines│ │Own Pipelines│ │Own Pipelines│
│  │Own Quality│ │ │Own Quality│ │ │Own Quality│  │
│  └──────────┘ │ └──────────┘ │ └──────────┘   │
├───────────────┴───────────────┴─────────────────┤
│  Platform: Shared infra (Composer, Dataflow,    │
│            IAM, Monitoring, Data Catalog)         │
└─────────────────────────────────────────────────┘
```

### Implementation
- Each domain owns a BigQuery **dataset**
- Cross-domain access via **Authorized Views** or **Analytics Hub**
- Central **Data Catalog** for discovery
- Shared **Cloud Composer** with domain-specific DAG folders

---

## Pattern 5: CDC (Change Data Capture) Pipeline

```
Source DB (Oracle/PostgreSQL)
    │
    ▼ (Datastream - real-time CDC)
Cloud Storage (Avro/JSON change events)
    │
    ▼ (Dataflow or BQ merge)
BigQuery (Target Table)
    - INSERT for new records
    - UPDATE for modifications
    - SOFT DELETE for removals
```

### BigQuery MERGE for CDC
```sql
MERGE `project.curated.customers` AS target
USING `project.raw.customers_cdc` AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source.op_type = 'DELETE' THEN
    UPDATE SET target.is_deleted = TRUE, target.updated_at = source.event_time
WHEN MATCHED AND source.op_type = 'UPDATE' THEN
    UPDATE SET
        target.name = source.name,
        target.email = source.email,
        target.updated_at = source.event_time
WHEN NOT MATCHED AND source.op_type = 'INSERT' THEN
    INSERT (customer_id, name, email, created_at, updated_at, is_deleted)
    VALUES (source.customer_id, source.name, source.email, source.event_time, source.event_time, FALSE);
```

---

## Pattern 6: ML Feature Pipeline

```
Raw Data (BigQuery/GCS)
    │
    ▼
Feature Engineering (Dataflow / BigQuery SQL)
    │
    ├──────────────────────────┐
    ▼                          ▼
Vertex AI Feature Store    BigQuery Feature Table
(Online Serving - low lat)  (Offline Training - batch)
    │                          │
    ▼                          ▼
Model Serving (Vertex AI)  Model Training (Vertex AI)
```

---

## Architecture Decision Framework

| Question | Recommendation |
|----------|---------------|
| Need real-time? | Pub/Sub → Dataflow → BQ |
| Existing Spark code? | Dataproc |
| New pipeline, no Spark? | Dataflow (Apache Beam) |
| Simple transform on schedule? | BigQuery scheduled queries |
| Complex DAG orchestration? | Cloud Composer |
| CDC from RDBMS? | Datastream |
| Cost-sensitive, variable load? | Serverless (Dataflow, BQ) |
| Predictable high load? | Reservations (BQ slots, Dataproc cluster) |

---

## Pattern 7: ETL vs ELT Pipeline — Data Cleaning & Transformation

### Problem: How do you move data from source to warehouse, and where should you clean/transform it?

**Definition:**
- **ETL (Extract → Transform → Load):** Data is extracted from source, transformed OUTSIDE the warehouse (in Dataflow/Spark/Python), then loaded clean.
- **ELT (Extract → Load → Transform):** Data is extracted and loaded RAW into the warehouse first, then transformed INSIDE the warehouse (using SQL/dbt).

### ETL vs ELT — When to Use Each

| Aspect | ETL (Transform first) | ELT (Load first, transform in warehouse) |
|--------|----------------------|------------------------------------------|
| **Transform engine** | Dataflow, Spark, Python scripts | BigQuery SQL, dbt, Scheduled Queries |
| **Best for** | Complex logic, external APIs, ML models | SQL-expressible transforms, large-scale joins |
| **Data volume** | Any (distributed compute) | Large (leverages BQ's massive compute) |
| **Cost model** | Pay for Dataflow/Dataproc VMs | Pay for BQ query bytes scanned |
| **Debugging** | Code-level (Python/Java) | SQL-level (easier for analysts) |
| **When to choose** | Non-SQL logic, streaming, file parsing | Most analytics workloads (80% of cases) |
| **GCP tools** | Dataflow, Dataproc, Cloud Functions | BigQuery, dbt, Scheduled Queries |

**Modern recommendation:** **ELT for most workloads.** BigQuery's compute power makes it cheaper and simpler to transform data in-place rather than building external processing infrastructure.

### Interview Perspective:
> "I default to ELT on GCP — load raw data into BigQuery first, then transform using SQL or dbt. BigQuery's serverless compute handles terabyte-scale transforms without managing infrastructure. I only use ETL (Dataflow/Spark) when the transformation requires non-SQL logic: calling external APIs, ML model inference, complex parsing of semi-structured files, or streaming data that needs sub-second processing."

---

### Complete ETL Pipeline Example (Dataflow/Python)

### Interview Scenario:

> **Interviewer:** "You receive 50,000 loan application CSV files daily from 15 different branch systems. Each branch has slightly different formats — some use `MM/DD/YYYY` dates, others use `YYYY-MM-DD`. Some files have trailing spaces in fields, some encode nulls as `"N/A"` or empty strings. Credit scores occasionally come as `-1` (meaning 'not available'). Duplicate records exist because branch systems retry on timeout. How would you design a pipeline to clean this data and make it analytics-ready?"

### Problem Breakdown:

| Challenge | Real-World Impact if Not Handled |
|-----------|----------------------------------|
| **Mixed date formats** | Joins/filters on date fail silently or return wrong results |
| **Trailing whitespace** | `"APPROVED "` ≠ `"APPROVED"` → broken GROUP BYs, wrong counts |
| **Null indicators** ("N/A", "", -1) | Aggregations include junk values → wrong averages/sums |
| **Duplicate records** | Double-counting loans → inflated portfolio size → regulatory risk |
| **Invalid credit scores** | Risk models trained on garbage data → wrong lending decisions |
| **Special chars in IDs** | Join keys don't match → orphan records, data loss |

### Why ETL (not ELT) for This Scenario:

1. **Complex parsing logic** — Multiple date formats need procedural `try/except` loops (hard in pure SQL)
2. **Dead-letter routing** — Records failing validation must go to a separate output (Beam's `TaggedOutput` is elegant)
3. **Stateful deduplication** — GroupByKey across 50K files in a distributed manner
4. **External enrichment potential** — Could call a fraud-scoring API during transformation
5. **File-level metadata** — Need to track which source file each record came from

### Solution Architecture:

```
50K CSV files (15 branches, different formats)
    │
    ▼
┌─────────────────────────────────────────────────┐
│  Dataflow Pipeline (ETL)                         │
│                                                   │
│  1. EXTRACT: Read all CSVs from GCS              │
│  2. CLEAN: Trim, standardize case, fix dates     │
│  3. VALIDATE: Check business rules               │
│       ├── Valid → continue                        │
│       └── Invalid → Dead Letter table (for ops)  │
│  4. DEDUPLICATE: GroupByKey on loan_id           │
│  5. TRANSFORM: Calculate EMI, risk category      │
│  6. LOAD: Write to BigQuery curated table        │
└─────────────────────────────────────────────────┘
```

### How to Explain Each Step in an Interview:

| Step | What to Say | Why Interviewer Cares |
|------|-------------|----------------------|
| **Clean** | "I standardize formats at the source — trim whitespace, uppercase categoricals, parse dates with fallback formats" | Shows you understand real-world dirty data |
| **Validate** | "I separate validation from cleaning. Records that violate business rules go to a dead-letter table — the pipeline doesn't crash" | Shows fault-tolerance thinking |
| **Deduplicate** | "I GroupByKey on loan_id and keep the latest record. This handles retry duplicates from branch systems" | Shows you think about data integrity |
| **Transform** | "I add derived fields (EMI, risk category) during processing so analysts don't recalculate in every query" | Shows you reduce downstream complexity |
| **Dead Letter** | "Invalid records go to a separate BigQuery table with error reasons and timestamps. Ops team reviews daily" | Shows production mindset (observability) |

### Expected Interview Follow-ups & Answers:

**Q: "What if one branch sends a corrupt file with 0 valid records?"**
> "The pipeline still succeeds — all records route to dead-letter, the curated table just gets no new rows from that branch. We have an Airflow quality check that alerts if any branch has zero records loaded."

**Q: "How do you handle schema evolution — a branch adds a new column?"**
> "I use flexible parsing (parse only known columns, ignore extras). For new required columns, I version the pipeline and deploy via CI/CD with backward compatibility."

**Q: "What happens if this pipeline fails mid-way?"**
> "Dataflow provides exactly-once semantics with checkpointing. If a worker fails, another retries the failed bundle. For the BigQuery write, I use WRITE_APPEND so partial writes don't corrupt existing data. The pipeline is idempotent — re-running for the same date is safe because I deduplicate."

**Q: "How would you test this pipeline?"**
> "Unit tests for each DoFn (Clean, Validate, Transform) with edge cases. Integration test with DirectRunner on a small sample. Canary deployment that processes 1% of data before full run."

---

### Schema Validation — Simple Spark/Dataproc Approach

#### What is it?
Schema validation = **checking that incoming data has the right columns and types** before you process it. Without it, your Spark job crashes mid-way or loads garbage data silently.

#### Common Schema Problems:

| Problem | What Happens | Example |
|---------|-------------|---------|
| Missing column | `AnalysisException: cannot resolve 'interest_rate'` | Source renamed it to `int_rate` |
| Extra column | Ignored (but unexpected schema drift) | Source added `branch_code` |
| Wrong type | Cast fails or silent corruption | `credit_score = "750.0"` instead of `750` |
| Wrong column order | Values land in wrong fields | Amount ends up in credit_score column |

---

#### Simple Schema Validation in PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

spark = SparkSession.builder.appName("loan_pipeline").getOrCreate()

# ═══════════════════════════════════════════════════════════════
# STEP 1: Define expected schema (your "contract" with source team)
# ═══════════════════════════════════════════════════════════════

EXPECTED_SCHEMA = StructType([
    StructField("loan_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("loan_type", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("interest_rate", DoubleType(), nullable=True),
    StructField("term_months", IntegerType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("region", StringType(), nullable=True),
    StructField("application_date", StringType(), nullable=True),
    StructField("credit_score", IntegerType(), nullable=True),
])


# ═══════════════════════════════════════════════════════════════
# STEP 2: Read with schema enforcement (Spark validates on read)
# ═══════════════════════════════════════════════════════════════

# Option A: Enforce schema strictly — Spark throws error if mismatch
df = spark.read \
    .schema(EXPECTED_SCHEMA) \
    .option("header", "true") \
    .option("mode", "FAILFAST") \
    .csv("gs://bucket/raw/loans/2026-06-22/")
# mode="FAILFAST" → crashes immediately if any row doesn't match schema
# mode="PERMISSIVE" → puts bad rows in a _corrupt_record column (default)
# mode="DROPMALFORMED" → silently drops bad rows


# Option B: Read and then validate (more control over error messages)
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv("gs://bucket/raw/loans/2026-06-22/")


# ═══════════════════════════════════════════════════════════════
# STEP 3: Validate column names (simple check)
# ═══════════════════════════════════════════════════════════════

def validate_schema(df, expected_schema):
    """Check that DataFrame has expected columns and types. Fail fast if not."""
    
    expected_cols = set(f.name for f in expected_schema.fields)
    actual_cols = set(df.columns)
    
    # Check missing columns
    missing = expected_cols - actual_cols
    if missing:
        raise ValueError(f"❌ MISSING COLUMNS: {missing}. Source schema changed!")
    
    # Check extra columns (warning, not error)
    extra = actual_cols - expected_cols
    if extra:
        print(f"⚠️ WARNING: New columns detected: {extra} (schema drift)")
    
    # Check column count
    if len(df.columns) != len(expected_schema.fields):
        raise ValueError(
            f"❌ COLUMN COUNT: expected {len(expected_schema.fields)}, got {len(df.columns)}"
        )
    
    print(f"✓ Schema validation passed: {len(df.columns)} columns match")
    return True

# Run it
validate_schema(df_raw, EXPECTED_SCHEMA)


# ═══════════════════════════════════════════════════════════════
# STEP 4: Validate data types (cast and check for failures)
# ═══════════════════════════════════════════════════════════════

from pyspark.sql.functions import col, when, count, lit

def validate_types(df):
    """Try casting columns and count failures. Alert if too many bad rows."""
    
    # Cast with error handling — bad values become NULL
    df_typed = df.select(
        col("loan_id"),
        col("customer_id"),
        col("loan_type"),
        col("amount").cast("double").alias("amount"),         # "abc" → NULL
        col("interest_rate").cast("double").alias("interest_rate"),
        col("term_months").cast("int").alias("term_months"),  # "12.5" → NULL
        col("status"),
        col("region"),
        col("application_date"),
        col("credit_score").cast("int").alias("credit_score"),
    )
    
    # Count how many rows failed to cast (became NULL that weren't already NULL)
    total = df.count()
    cast_failures = df_typed.filter(
        col("amount").isNull() & col("loan_id").isNotNull()  # NULL amount on non-null row = cast fail
    ).count()
    
    failure_rate = cast_failures / total * 100 if total > 0 else 0
    
    if failure_rate > 5:  # More than 5% bad → something is wrong with source
        raise ValueError(f"❌ TYPE FAILURES: {cast_failures}/{total} rows ({failure_rate:.1f}%) failed type cast")
    
    print(f"✓ Type validation passed: {failure_rate:.1f}% failure rate (threshold: 5%)")
    return df_typed

df_validated = validate_types(df_raw)


# ═══════════════════════════════════════════════════════════════
# STEP 5: Validate row count (catch empty/duplicate loads)
# ═══════════════════════════════════════════════════════════════

def validate_row_count(df, min_rows=100, max_rows=1_000_000):
    """Catch empty files or accidentally duplicated loads."""
    row_count = df.count()
    
    if row_count < min_rows:
        raise ValueError(f"❌ TOO FEW ROWS: {row_count} (min: {min_rows}). File truncated?")
    
    if row_count > max_rows:
        raise ValueError(f"❌ TOO MANY ROWS: {row_count} (max: {max_rows}). Duplicate load?")
    
    print(f"✓ Row count: {row_count} (range: {min_rows}-{max_rows})")
    return row_count

validate_row_count(df_validated)
```

---

#### Complete Spark Pipeline with Schema Validation

```python
# loan_spark_pipeline.py
# Simple: Read → Validate Schema → Clean → Transform → Write

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper, round as spark_round, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("loan_etl_pipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# --- SCHEMA CONTRACT ---
EXPECTED_COLUMNS = [
    "loan_id", "customer_id", "loan_type", "amount", "interest_rate",
    "term_months", "status", "region", "application_date", "credit_score"
]

# --- READ ---
df_raw = spark.read.option("header", "true").csv("gs://bucket/raw/loans/2026-06-22/")

# --- VALIDATE SCHEMA (fail fast) ---
missing = set(EXPECTED_COLUMNS) - set(df_raw.columns)
if missing:
    raise ValueError(f"Schema validation FAILED. Missing: {missing}")
print(f"✓ Schema OK: {len(df_raw.columns)} columns")

# --- VALIDATE ROW COUNT ---
row_count = df_raw.count()
if row_count < 100:
    raise ValueError(f"Too few rows: {row_count}. File may be truncated.")

# --- CAST TYPES ---
df_typed = df_raw.select(
    trim(col("loan_id")).alias("loan_id"),
    trim(col("customer_id")).alias("customer_id"),
    upper(trim(col("loan_type"))).alias("loan_type"),
    col("amount").cast("double").alias("amount"),
    col("interest_rate").cast("double").alias("interest_rate"),
    col("term_months").cast("int").alias("term_months"),
    upper(trim(col("status"))).alias("status"),
    upper(trim(col("region"))).alias("region"),
    col("application_date"),
    col("credit_score").cast("int").alias("credit_score"),
)

# --- SEPARATE GOOD vs BAD (records that failed casting) ---
df_good = df_typed.filter(col("amount").isNotNull() & col("loan_id").isNotNull())
df_bad = df_typed.filter(col("amount").isNull() & col("loan_id").isNotNull())

# --- TRANSFORM (add derived fields) ---
df_enriched = df_good.withColumn(
    "risk_category",
    when(col("credit_score") >= 750, "LOW")
    .when(col("credit_score") >= 650, "MEDIUM")
    .when(col("credit_score") >= 550, "HIGH")
    .otherwise("VERY_HIGH")
).withColumn(
    "loan_size_bucket",
    when(col("amount") > 300000, "JUMBO")
    .when(col("amount") > 100000, "LARGE")
    .when(col("amount") > 25000, "MEDIUM")
    .otherwise("SMALL")
).withColumn("etl_timestamp", current_timestamp())

# --- WRITE GOOD RECORDS TO BIGQUERY ---
df_enriched.write \
    .format("bigquery") \
    .option("table", "project.loan_analytics.curated_loans") \
    .option("temporaryGcsBucket", "your-bucket-temp") \
    .mode("append") \
    .save()

# --- WRITE BAD RECORDS TO DEAD-LETTER ---
df_bad.write \
    .format("bigquery") \
    .option("table", "project.loan_analytics.dead_letter") \
    .option("temporaryGcsBucket", "your-bucket-temp") \
    .mode("append") \
    .save()

print(f"✓ Pipeline complete. Good: {df_good.count()}, Bad: {df_bad.count()}")
```

---

#### Interview Perspective — Schema Validation (Simple):

> "Before processing, I validate three things: (1) **Column names** — do we have all expected columns? If source renamed something, we fail immediately with a clear error. (2) **Data types** — I cast columns and check how many rows failed. If more than 5% can't cast, something changed at source. (3) **Row count** — too few means truncated file, too many means duplicate load. All this takes 3 lines of code in Spark and saves hours of debugging corrupt data downstream."

---

### Code Implementation:

**Use case:** Raw loan CSV files arrive in GCS. Clean, validate, enrich, then load to BigQuery.

```
GCS (raw CSV) → Dataflow (clean + transform) → BigQuery (curated table)
```

```python
# etl_loan_pipeline.py
# Purpose: ETL pipeline — transforms data BEFORE loading to warehouse
# Used when: Complex parsing, external lookups, non-SQL transformations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import re


# ═══════════════════════════════════════════════════════════════
# DATA CLEANING FUNCTIONS
# ═══════════════════════════════════════════════════════════════

class CleanLoanRecord(beam.DoFn):
    """
    Handles all data cleaning:
    - Trim whitespace
    - Standardize formats
    - Handle nulls/empty fields
    - Fix data type issues
    """
    def process(self, record):
        cleaned = {}
        
        # --- 1. TRIM WHITESPACE from all string fields ---
        # Problem: Source systems often have trailing spaces " APPROVED " → "APPROVED"
        for key, value in record.items():
            if isinstance(value, str):
                cleaned[key] = value.strip()
            else:
                cleaned[key] = value
        
        # --- 2. STANDARDIZE CASE (uppercase for categorical fields) ---
        # Problem: Source sends "approved", "Approved", "APPROVED" → inconsistent
        if 'status' in cleaned:
            cleaned['status'] = cleaned['status'].upper()
        if 'loan_type' in cleaned:
            cleaned['loan_type'] = cleaned['loan_type'].upper().replace(' ', '_')
        if 'region' in cleaned:
            cleaned['region'] = cleaned['region'].upper().replace('-', '_')
        
        # --- 3. HANDLE NULL/EMPTY strings ---
        # Problem: Source sends "", "NULL", "None", "N/A" for missing values
        null_indicators = {'', 'null', 'none', 'n/a', 'na', 'nil', '-'}
        for key, value in cleaned.items():
            if isinstance(value, str) and value.lower() in null_indicators:
                cleaned[key] = None
        
        # --- 4. STANDARDIZE DATE formats ---
        # Problem: Source sends "2024-01-15", "01/15/2024", "15-Jan-2024" → need one format
        if cleaned.get('application_date'):
            cleaned['application_date'] = self._parse_date(cleaned['application_date'])
        
        # --- 5. FIX NUMERIC precision ---
        # Problem: Floating point: 10000.000001 → should be 10000.00
        if cleaned.get('amount') is not None:
            cleaned['amount'] = round(float(cleaned['amount']), 2)
        if cleaned.get('interest_rate') is not None:
            cleaned['interest_rate'] = round(float(cleaned['interest_rate']), 2)
        
        # --- 6. REMOVE special characters from IDs ---
        # Problem: Source sends "LN-001#" or "CUST 001" → clean to "LN-001", "CUST-001"
        if cleaned.get('loan_id'):
            cleaned['loan_id'] = re.sub(r'[^A-Za-z0-9\-]', '', cleaned['loan_id'])
        if cleaned.get('customer_id'):
            cleaned['customer_id'] = re.sub(r'[^A-Za-z0-9\-]', '', cleaned['customer_id'])
        
        yield cleaned
    
    def _parse_date(self, date_str):
        """Try multiple date formats and return standardized YYYY-MM-DD."""
        formats = ['%Y-%m-%d', '%m/%d/%Y', '%d-%b-%Y', '%d/%m/%Y', '%Y%m%d']
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None  # Unparseable date → null


# ═══════════════════════════════════════════════════════════════
# DATA VALIDATION
# ═══════════════════════════════════════════════════════════════

class ValidateRecord(beam.DoFn):
    """
    Business rule validation — separate good records from bad.
    Bad records go to dead-letter for investigation.
    """
    def process(self, record):
        errors = []
        
        # Rule 1: Required fields cannot be null
        required_fields = ['loan_id', 'customer_id', 'amount', 'application_date']
        for field in required_fields:
            if record.get(field) is None:
                errors.append(f'Missing required field: {field}')
        
        # Rule 2: Amount must be positive and within business limits
        if record.get('amount') is not None:
            if record['amount'] <= 0:
                errors.append(f"Invalid amount: {record['amount']} (must be > 0)")
            if record['amount'] > 10_000_000:
                errors.append(f"Amount exceeds max: {record['amount']} (max $10M)")
        
        # Rule 3: Credit score must be in FICO range
        if record.get('credit_score') is not None:
            if record['credit_score'] < 300 or record['credit_score'] > 850:
                errors.append(f"Invalid credit score: {record['credit_score']} (range: 300-850)")
        
        # Rule 4: Interest rate must be reasonable
        if record.get('interest_rate') is not None:
            if record['interest_rate'] < 0 or record['interest_rate'] > 30:
                errors.append(f"Invalid rate: {record['interest_rate']} (range: 0-30%)")
        
        # Rule 5: Loan type must be in allowed set
        allowed_types = {'PERSONAL', 'AUTO', 'MORTGAGE', 'CREDIT_CARD', 'STUDENT', 'HOME_EQUITY'}
        if record.get('loan_type') and record['loan_type'] not in allowed_types:
            errors.append(f"Unknown loan type: {record['loan_type']}")
        
        # Rule 6: Date must not be in the future
        if record.get('application_date'):
            if record['application_date'] > datetime.now().strftime('%Y-%m-%d'):
                errors.append(f"Future date: {record['application_date']}")
        
        if errors:
            yield beam.pvalue.TaggedOutput('invalid', {
                'record': record,
                'errors': errors,
                'error_timestamp': datetime.now().isoformat()
            })
        else:
            yield record


# ═══════════════════════════════════════════════════════════════
# DATA TRANSFORMATION (Enrichment)
# ═══════════════════════════════════════════════════════════════

class TransformRecord(beam.DoFn):
    """
    Business logic transformations — add derived/calculated fields.
    This is where raw data becomes analytics-ready.
    """
    def process(self, record):
        # --- 1. CALCULATE monthly payment (EMI formula) ---
        r = record.get('interest_rate', 0) / 100 / 12
        n = record.get('term_months', 12)
        p = record.get('amount', 0)
        
        if r > 0 and n > 0:
            monthly_payment = p * (r * (1 + r)**n) / ((1 + r)**n - 1)
        elif n > 0:
            monthly_payment = p / n
        else:
            monthly_payment = 0
        
        record['monthly_payment'] = round(monthly_payment, 2)
        record['total_interest'] = round(monthly_payment * n - p, 2)
        record['total_cost'] = round(monthly_payment * n, 2)
        
        # --- 2. DERIVE risk category from credit score ---
        score = record.get('credit_score', 0)
        if score >= 750:
            record['risk_category'] = 'LOW'
            record['risk_score'] = 1
        elif score >= 700:
            record['risk_category'] = 'MEDIUM_LOW'
            record['risk_score'] = 2
        elif score >= 650:
            record['risk_category'] = 'MEDIUM'
            record['risk_score'] = 3
        elif score >= 550:
            record['risk_category'] = 'HIGH'
            record['risk_score'] = 4
        else:
            record['risk_category'] = 'VERY_HIGH'
            record['risk_score'] = 5
        
        # --- 3. DERIVE debt-to-income ratio bucket ---
        # (Simplified — in reality you'd join with income data)
        amount = record.get('amount', 0)
        if amount > 300000:
            record['loan_size_bucket'] = 'JUMBO'
        elif amount > 100000:
            record['loan_size_bucket'] = 'LARGE'
        elif amount > 25000:
            record['loan_size_bucket'] = 'MEDIUM'
        else:
            record['loan_size_bucket'] = 'SMALL'
        
        # --- 4. ADD processing metadata ---
        record['etl_load_timestamp'] = datetime.utcnow().isoformat() + 'Z'
        record['etl_source_file'] = record.get('_source_file', 'unknown')
        record['etl_pipeline_version'] = '2.1.0'
        
        yield record


# ═══════════════════════════════════════════════════════════════
# DATA DEDUPLICATION
# ═══════════════════════════════════════════════════════════════

class DeduplicateRecords(beam.DoFn):
    """
    Remove duplicate records based on loan_id.
    Keep the record with the latest application_date (most recent version).
    """
    def process(self, element):
        key, records = element  # key = loan_id, records = all records with that loan_id
        records_list = list(records)
        
        if len(records_list) == 1:
            yield records_list[0]
        else:
            # Keep the most recent record (by application_date)
            sorted_records = sorted(
                records_list,
                key=lambda x: x.get('application_date', ''),
                reverse=True
            )
            yield sorted_records[0]  # Keep latest


# ═══════════════════════════════════════════════════════════════
# MAIN ETL PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_etl_pipeline():
    """
    Full ETL flow: Read → Clean → Validate → Deduplicate → Transform → Write
    """
    options = PipelineOptions([
        '--runner=DataflowRunner',
        '--project=your-project',
        '--region=us-central1',
        '--temp_location=gs://your-bucket/temp/',
    ])
    
    schema = (
        'loan_id:STRING,customer_id:STRING,loan_type:STRING,'
        'amount:FLOAT64,interest_rate:FLOAT64,term_months:INT64,'
        'status:STRING,region:STRING,application_date:DATE,credit_score:INT64,'
        'monthly_payment:FLOAT64,total_interest:FLOAT64,total_cost:FLOAT64,'
        'risk_category:STRING,risk_score:INT64,loan_size_bucket:STRING,'
        'etl_load_timestamp:TIMESTAMP,etl_pipeline_version:STRING'
    )
    
    with beam.Pipeline(options=options) as p:
        
        # STEP 1: EXTRACT — Read raw data from GCS
        raw_records = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('gs://bucket/raw/loans/*.csv')
            | 'ParseCSV' >> beam.ParDo(ParseCSVLine())
        )
        
        # STEP 2: CLEAN — Standardize formats, handle nulls, fix types
        cleaned = raw_records | 'Clean' >> beam.ParDo(CleanLoanRecord())
        
        # STEP 3: VALIDATE — Check business rules, route bad records to dead-letter
        validated = (
            cleaned
            | 'Validate' >> beam.ParDo(ValidateRecord())
                .with_outputs('invalid', main='valid')
        )
        
        # STEP 4: DEDUPLICATE — Remove duplicate loan_ids (keep latest)
        deduped = (
            validated['valid']
            | 'KeyByLoanId' >> beam.Map(lambda r: (r['loan_id'], r))
            | 'GroupByKey' >> beam.GroupByKey()
            | 'Deduplicate' >> beam.ParDo(DeduplicateRecords())
        )
        
        # STEP 5: TRANSFORM — Add calculated fields, categorizations
        transformed = deduped | 'Transform' >> beam.ParDo(TransformRecord())
        
        # STEP 6: LOAD — Write to BigQuery curated table
        transformed | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            'project:loan_analytics.curated_loans',
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
        
        # Write rejected records to dead-letter table for investigation
        (
            validated['invalid']
            | 'WriteErrors' >> beam.io.WriteToBigQuery(
                'project:loan_analytics.dead_letter_loans',
                schema='record:STRING,errors:STRING,error_timestamp:TIMESTAMP',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == '__main__':
    run_etl_pipeline()
```

---

### Complete ELT Pipeline Example (BigQuery SQL / dbt)

**Use case:** Same loan data, but load raw first, then transform using SQL inside BigQuery.

```
GCS (raw CSV) → BigQuery (raw table) → SQL transforms → BigQuery (curated table)
```

#### Step 1: Load raw data as-is (no transformation)

```sql
-- Load CSV directly into raw table (no cleaning yet)
-- ELT philosophy: load everything, transform later
LOAD DATA INTO `project.loan_analytics.raw_loans`
FROM FILES (
    format = 'CSV',
    uris = ['gs://bucket/raw/loans/2026-06-22/*.csv'],
    skip_leading_rows = 1
);
```

#### Step 2: Data Cleaning in SQL

```sql
-- Cleaning layer: fix formats, handle nulls, standardize values
-- This replaces the Python CleanLoanRecord class
CREATE OR REPLACE TABLE `project.loan_analytics.cleaned_loans` AS
SELECT
    -- 1. TRIM whitespace from strings
    TRIM(loan_id) AS loan_id,
    TRIM(customer_id) AS customer_id,
    
    -- 2. STANDARDIZE case for categorical fields
    UPPER(TRIM(loan_type)) AS loan_type,
    UPPER(TRIM(status)) AS status,
    UPPER(TRIM(region)) AS region,
    
    -- 3. HANDLE NULL indicators ("", "NULL", "N/A" → actual NULL)
    CASE 
        WHEN LOWER(TRIM(loan_type)) IN ('', 'null', 'none', 'n/a') THEN NULL
        ELSE UPPER(TRIM(loan_type))
    END AS loan_type_clean,
    
    -- 4. FIX NUMERIC precision
    ROUND(CAST(amount AS FLOAT64), 2) AS amount,
    ROUND(CAST(interest_rate AS FLOAT64), 2) AS interest_rate,
    CAST(term_months AS INT64) AS term_months,
    CAST(credit_score AS INT64) AS credit_score,
    
    -- 5. STANDARDIZE DATE format (handle multiple input formats)
    SAFE.PARSE_DATE('%Y-%m-%d', application_date) AS application_date,
    
    -- 6. REMOVE special characters from IDs
    REGEXP_REPLACE(loan_id, r'[^A-Za-z0-9\-]', '') AS loan_id_clean,
    
    -- 7. ADD metadata
    CURRENT_TIMESTAMP() AS cleaned_at,
    _FILE_NAME AS source_file  -- Built-in: which GCS file this row came from

FROM `project.loan_analytics.raw_loans`
-- Filter out completely empty rows
WHERE loan_id IS NOT NULL AND TRIM(loan_id) != '';
```

#### Step 3: Data Validation in SQL

```sql
-- Validation layer: flag records that violate business rules
-- Good records → curated table; Bad records → quarantine table
CREATE OR REPLACE TABLE `project.loan_analytics.validated_loans` AS
SELECT
    *,
    -- Build an error array (empty = valid record)
    ARRAY_CONCAT(
        IF(amount <= 0 OR amount IS NULL, ['Invalid amount'], []),
        IF(amount > 10000000, ['Amount exceeds $10M limit'], []),
        IF(credit_score < 300 OR credit_score > 850, ['Invalid credit score'], []),
        IF(interest_rate < 0 OR interest_rate > 30, ['Invalid interest rate'], []),
        IF(application_date > CURRENT_DATE(), ['Future application date'], []),
        IF(loan_type NOT IN ('PERSONAL','AUTO','MORTGAGE','CREDIT_CARD','STUDENT','HOME_EQUITY'),
           ['Unknown loan type'], []),
        IF(loan_id IS NULL OR customer_id IS NULL, ['Missing required ID'], [])
    ) AS validation_errors
FROM `project.loan_analytics.cleaned_loans`;

-- Quarantine bad records
INSERT INTO `project.loan_analytics.dead_letter_loans`
SELECT *, CURRENT_TIMESTAMP() AS quarantined_at
FROM `project.loan_analytics.validated_loans`
WHERE ARRAY_LENGTH(validation_errors) > 0;
```

#### Step 4: Deduplication in SQL

```sql
-- Deduplication: Keep only the latest record per loan_id
-- Uses ROW_NUMBER() window function to rank duplicates
CREATE OR REPLACE TABLE `project.loan_analytics.deduped_loans` AS
SELECT * EXCEPT(row_num)
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id                    -- Group by unique key
            ORDER BY application_date DESC,         -- Latest date first
                     cleaned_at DESC                -- If same date, latest load
        ) AS row_num
    FROM `project.loan_analytics.validated_loans`
    WHERE ARRAY_LENGTH(validation_errors) = 0      -- Only valid records
)
WHERE row_num = 1;  -- Keep only the first (most recent) per loan_id
```

#### Step 5: Transformation in SQL

```sql
-- Transformation layer: add derived/calculated fields
-- This replaces the Python TransformRecord class
CREATE OR REPLACE TABLE `project.loan_analytics.curated_loans` AS
SELECT
    loan_id,
    customer_id,
    loan_type,
    amount,
    interest_rate,
    term_months,
    status,
    region,
    application_date,
    credit_score,
    
    -- CALCULATED: Monthly payment (EMI formula in SQL)
    ROUND(
        CASE
            WHEN interest_rate > 0 THEN
                amount * (interest_rate/100/12 * POW(1 + interest_rate/100/12, term_months))
                / (POW(1 + interest_rate/100/12, term_months) - 1)
            ELSE amount / NULLIF(term_months, 0)
        END, 2
    ) AS monthly_payment,
    
    -- CALCULATED: Total interest over loan life
    ROUND(
        CASE
            WHEN interest_rate > 0 THEN
                (amount * (interest_rate/100/12 * POW(1 + interest_rate/100/12, term_months))
                / (POW(1 + interest_rate/100/12, term_months) - 1)) * term_months - amount
            ELSE 0
        END, 2
    ) AS total_interest,
    
    -- DERIVED: Risk category from credit score
    CASE
        WHEN credit_score >= 750 THEN 'LOW'
        WHEN credit_score >= 700 THEN 'MEDIUM_LOW'
        WHEN credit_score >= 650 THEN 'MEDIUM'
        WHEN credit_score >= 550 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END AS risk_category,
    
    -- DERIVED: Loan size bucket
    CASE
        WHEN amount > 300000 THEN 'JUMBO'
        WHEN amount > 100000 THEN 'LARGE'
        WHEN amount > 25000 THEN 'MEDIUM'
        ELSE 'SMALL'
    END AS loan_size_bucket,
    
    -- DERIVED: Days since application
    DATE_DIFF(CURRENT_DATE(), application_date, DAY) AS days_since_application,
    
    -- DERIVED: Application year/quarter for easy reporting
    EXTRACT(YEAR FROM application_date) AS application_year,
    EXTRACT(QUARTER FROM application_date) AS application_quarter,
    
    -- METADATA
    CURRENT_TIMESTAMP() AS etl_load_timestamp,
    'v2.1.0' AS etl_pipeline_version

FROM `project.loan_analytics.deduped_loans`;
```

---

### Common Data Cleaning Scenarios — Quick Reference

| Dirty Data Problem | Python (ETL) Fix | SQL (ELT) Fix |
|-------------------|-----------------|---------------|
| Leading/trailing spaces | `value.strip()` | `TRIM(column)` |
| Mixed case | `value.upper()` | `UPPER(column)` |
| Null indicators ("N/A", "") | `if val in null_set: None` | `CASE WHEN ... THEN NULL` |
| Multiple date formats | `datetime.strptime()` loop | `SAFE.PARSE_DATE()` + `COALESCE` |
| Special characters in IDs | `re.sub(r'[^A-Za-z0-9-]', '', val)` | `REGEXP_REPLACE(col, r'[^A-Za-z0-9\-]', '')` |
| Floating point precision | `round(val, 2)` | `ROUND(col, 2)` |
| Duplicates | GroupByKey → keep latest | `ROW_NUMBER() OVER (PARTITION BY ...)` |
| Out-of-range values | if/else validation | `CASE WHEN ... validation_errors` |
| Type mismatches | `int(val)` with try/except | `SAFE_CAST(col AS INT64)` |
| Future dates | `if date > today: error` | `IF(date > CURRENT_DATE(), ...)` |
| Phone/email formatting | regex extraction | `REGEXP_EXTRACT()` |
| JSON nested fields | `json.loads()` → flatten | `JSON_EXTRACT_SCALAR()` |

---

### ETL vs ELT Decision Flowchart

```
Does the transformation require Python/Java logic?
    │
    ├── YES → Is it a streaming use case?
    │          ├── YES → Dataflow Streaming (ETL)
    │          └── NO  → Dataflow Batch or Dataproc Spark (ETL)
    │
    └── NO (can be expressed in SQL) → 
            │
            ├── Is data volume > 10TB?
            │     └── YES → BigQuery SQL (ELT) — native petabyte-scale
            │
            ├── Do you need version control + testing for transforms?
            │     └── YES → dbt + BigQuery (ELT) — best practice
            │
            └── Simple one-off transform?
                  └── YES → BigQuery Scheduled Query (ELT)
```

### Interview Perspective — ETL/ELT:
> "For most analytics workloads on GCP, I prefer ELT — load raw data into BigQuery first, then transform using SQL or dbt. This leverages BigQuery's serverless compute (no infrastructure to manage), makes transforms testable and version-controlled (with dbt), and lets analysts understand and modify the logic. I switch to ETL with Dataflow when I need: streaming processing, external API enrichment, complex file parsing (nested JSON/XML), or ML model inference during transformation."
