# Hands-On Mini Project: End-to-End Loan Analytics Pipeline

## Business Use Case & Purpose

### The Problem

Imagine you work at a **financial company** (bank or lending firm). Every day, thousands of customers apply for loans — personal loans, auto loans, mortgages, credit cards, student loans. The business needs answers to:

- How many loans were approved vs rejected today?
- Which regions have the highest default rates?
- What's the average credit score of applicants by loan type?
- Are we taking on too much risk in a particular segment?

**Without a pipeline**, analysts manually download spreadsheets, clean data in Excel, and paste results — slow, error-prone, and not scalable.

### What We're Building

An **automated, production-grade data pipeline** that runs daily without human intervention:

```
Raw Data (CSV files)
   ↓
Landing Zone (GCS) — safe storage of raw files
   ↓
Processing (Dataflow/Apache Beam) — clean, validate, enrich records
   ↓
Data Warehouse (BigQuery) — structured, queryable, fast analytics
   ↓
Orchestration (Cloud Composer/Airflow) — schedules everything, handles failures
```

### Purpose of Each Step

| Step | What It Does | Why It Matters |
|------|-------------|----------------|
| **Generate Data** | Creates 10,000 fake loan records | Simulates real source systems (core banking, CRM) |
| **GCS Landing Zone** | Stores raw files in organized folders by date | Keeps originals untouched for auditing/replay |
| **Dataflow Pipeline** | Parses CSV → validates → calculates payments → categorizes risk | Transforms messy raw data into clean, enriched, analytics-ready data |
| **BigQuery Tables** | 3 layers: raw → curated → aggregated | Separates concerns — raw for audit, curated for analysis, summary for dashboards |
| **Composer DAG** | Runs daily at 6 AM: process → check quality → build summary | Ensures pipeline runs reliably on schedule with retries and alerts |

### Key Engineering Patterns Demonstrated

1. **Dead-letter pattern** — Bad records (missing fields, invalid values) are routed to a separate error file instead of crashing the pipeline. Analysts can investigate later.

2. **Data quality gates** — After loading, SQL checks verify: "Did we load data? Are amounts valid? Any duplicates?" If checks fail, the pipeline stops before building reports on bad data.

3. **Medallion architecture** (Raw → Curated → Aggregated) — Each layer serves a different audience:
   - Raw = "exactly what we received" (compliance/audit)
   - Curated = "cleaned and enriched" (data scientists, analysts)
   - Aggregated = "pre-computed metrics" (dashboards, executives)

4. **Partitioning & Clustering** — Tables are partitioned by date and clustered by region/loan_type so queries scan less data = faster + cheaper.

### Real-World Analogy

Think of it like a **factory assembly line**:
- Raw materials arrive (CSV files land in GCS)
- Quality inspection rejects defective parts (validation filters bad records)
- Workers add components (enrichment adds monthly payment, risk score)
- Finished products go to warehouse shelves organized by category (BigQuery partitioned tables)
- A manager checks production reports daily (Composer DAG runs quality checks + summaries)
- If something breaks, alarms go off (email alerts on failure)

### What This Proves in an Interview

- End-to-end **data engineering lifecycle** understanding
- How to handle **dirty data** gracefully
- **Cost optimization** (partitioning, clustering)
- **Orchestration** with proper dependency chains and retries
- **Separation of concerns** across storage layers
- Building for **production** (not just ad-hoc scripts)

---

## Project Overview

Build a complete data pipeline that ingests loan application data, processes it, loads to BigQuery, runs quality checks, and serves analytics — mimicking a real finance company workflow.

```
CSV/API (Source) → GCS (Landing) → Dataflow (Process) → BigQuery (Warehouse)
                                                              │
Cloud Composer (Orchestration) ←─────────────────────────────┘
                │
                ├── Data Quality Checks
                ├── Aggregation Jobs
                └── Alerting on Failure
```

---

## Step 1: Project Setup

### Problem: How do you securely set up cloud infrastructure for a data pipeline?

**Definition:** Project setup involves enabling GCP APIs, creating service accounts (identity for your pipeline), assigning least-privilege IAM roles, and provisioning storage buckets.

**Why it matters:**
- Without proper service accounts, your pipeline runs with over-privileged credentials (security risk)
- Without enabled APIs, services simply won't respond
- Without organized buckets, data becomes a mess with no clear lifecycle

**How we resolve it:**
- Create a **dedicated service account** (not your personal account) — so the pipeline has its own identity
- Grant **only the roles it needs** (principle of least privilege) — if compromised, blast radius is limited
- Separate buckets by purpose (landing, processed, temp) — clear data lifecycle

**Interview Perspective:**
> "I always set up a dedicated service account with minimal IAM roles. For this pipeline, it needs `bigquery.dataEditor` to write data, `bigquery.jobUser` to run queries, `storage.objectAdmin` for file access, and `dataflow.worker` to execute jobs. I never use a personal account or over-privileged roles in production."

---

```bash
# Set the active GCP project for all subsequent gcloud commands
export PROJECT_ID="your-gcp-project"
gcloud config set project $PROJECT_ID

# Enable required GCP APIs (must be enabled before using any service)
gcloud services enable \
    bigquery.googleapis.com \
    storage.googleapis.com \
    dataflow.googleapis.com \
    composer.googleapis.com \
    pubsub.googleapis.com

# Create a dedicated service account for the pipeline (principle of least privilege)
gcloud iam service-accounts create loan-pipeline-sa \
    --display-name="Loan Pipeline Service Account"

# Grant only the necessary roles to the service account
# - bigquery.dataEditor: read/write BQ tables
# - bigquery.jobUser: run BQ queries
# - storage.objectAdmin: read/write GCS files
# - dataflow.worker: execute Dataflow jobs
for role in roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectAdmin roles/dataflow.worker; do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:loan-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
        --role="$role"
done

# Create GCS buckets for different pipeline stages
gsutil mb -l US gs://${PROJECT_ID}-loan-landing      # Raw data arrives here
gsutil mb -l US gs://${PROJECT_ID}-loan-processed    # Cleaned/transformed data
gsutil mb -l US gs://${PROJECT_ID}-dataflow-temp     # Dataflow temporary staging
```

---

## Step 2: Create Sample Data

### Problem: How do you test a pipeline without access to production data?

**Definition:** Sample/synthetic data generation creates realistic test data that mimics production patterns (data types, distributions, edge cases) without exposing real customer PII.

**Why it matters:**
- You can't use real customer data in dev/test (compliance: GDPR, PCI-DSS)
- Without representative test data, you'll miss edge cases that crash production
- Data needs to cover all variations: different loan types, regions, credit score ranges, dates

**How we resolve it:**
- Generate **10,000 records** with randomized but realistic values
- Cover all categorical combinations (5 loan types × 5 regions × 5 statuses)
- Include edge cases naturally (very low credit scores, very high amounts)
- Use deterministic IDs for traceability (`LN-0000001`, `CUST-000001`)

**Interview Perspective:**
> "In production, source data comes from core banking systems via CDC or batch exports. For development, I generate synthetic data that mirrors the real schema and value distributions — this lets me test pipeline logic, schema evolution, and error handling without touching PII."

---

```python
# generate_loan_data.py
# Purpose: Generate realistic fake loan application data to simulate source system output
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Define possible values for categorical fields (mimics real domain values)
LOAN_TYPES = ["PERSONAL", "AUTO", "MORTGAGE", "CREDIT_CARD", "STUDENT"]
STATUSES = ["APPROVED", "REJECTED", "PENDING", "DEFAULT", "CLOSED"]
REGIONS = ["US_EAST", "US_WEST", "US_CENTRAL", "APAC", "EMEA"]

def generate_loans(num_records=10000, output_file="loans.csv"):
    """Generate a CSV file with fake loan records for pipeline testing."""
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        # Write header row (column names matching BigQuery schema)
        writer.writerow([
            "loan_id", "customer_id", "loan_type", "amount", "interest_rate",
            "term_months", "status", "region", "application_date", "credit_score"
        ])
        
        for i in range(num_records):
            loan_id = f"LN-{i+1:07d}"                          # Unique loan ID (e.g., LN-0000001)
            customer_id = f"CUST-{random.randint(1, 5000):06d}" # 5000 unique customers (some have multiple loans)
            loan_type = random.choice(LOAN_TYPES)
            amount = round(random.uniform(1000, 500000), 2)     # Loan amount between $1K and $500K
            rate = round(random.uniform(3.0, 18.0), 2)          # Annual interest rate 3%-18%
            term = random.choice([12, 24, 36, 48, 60, 120, 240, 360])  # Common loan terms in months
            status = random.choice(STATUSES)
            region = random.choice(REGIONS)
            # Random date within ~2.5 years starting Jan 2024
            app_date = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 900))).strftime("%Y-%m-%d")
            credit_score = random.randint(300, 850)             # FICO score range
            
            writer.writerow([
                loan_id, customer_id, loan_type, amount, rate,
                term, status, region, app_date, credit_score
            ])
    
    print(f"Generated {num_records} records in {output_file}")

if __name__ == "__main__":
    generate_loans()
```

---

## Step 3: BigQuery Schema

### Problem: How do you design a warehouse schema that balances performance, cost, and usability?

**Definition:** Schema design in BigQuery involves choosing the right data types, partitioning strategy (how data is physically split), clustering (how data is sorted within partitions), and layered architecture (raw → curated → aggregated).

**Why it matters:**
- **Without partitioning**: Every query scans the entire table → slow + expensive (BigQuery charges per bytes scanned)
- **Without clustering**: Queries that filter on common columns still scan large data ranges
- **Without layers**: Raw data has no enrichment, analysts write complex queries, and one bad load corrupts everything

**How we resolve it:**

| Design Decision | What It Does | Cost/Performance Impact |
|----------------|-------------|-------------------------|
| `PARTITION BY application_date` | Splits table into daily chunks | Query for 1 day scans only that partition (not entire table) |
| `CLUSTER BY region, loan_type` | Sorts data within each partition | Filters on region/type skip irrelevant blocks |
| 3-layer model (Raw→Curated→Agg) | Separates concerns | Raw for audit, curated for analysis, agg for dashboards |
| `NUMERIC` for amounts | Exact decimal (no floating point errors) | Critical for financial data — $0.01 rounding matters |

**Data Skew Consideration:**
- If 80% of loans are from `US_EAST` region, that partition/cluster is much larger than others
- This causes **uneven query performance** — queries filtering other regions are fast, US_EAST is slow
- Solution: Add a secondary clustering column (e.g., `loan_type`) to further subdivide large clusters

**Interview Perspective:**
> "I design BigQuery schemas with a medallion architecture: raw layer preserves source data as-is for audit/replay, curated layer adds business logic (calculated fields, risk categories), and aggregation layer pre-computes metrics for dashboards. I partition by date since most queries are time-bounded, and cluster by the most common filter columns to minimize bytes scanned."

---

```sql
-- Create dataset (logical container for all loan-related tables)
CREATE SCHEMA IF NOT EXISTS `project.loan_analytics`
OPTIONS(location='US');

-- LAYER 1: Raw table — stores data exactly as received from source
-- PARTITION BY date = queries filter by date scan less data (cost savings)
-- CLUSTER BY region, loan_type = physically sorts data for faster filtered queries
CREATE TABLE IF NOT EXISTS `project.loan_analytics.raw_loans` (
    loan_id STRING,
    customer_id STRING,
    loan_type STRING,
    amount NUMERIC,
    interest_rate FLOAT64,
    term_months INT64,
    status STRING,
    region STRING,
    application_date DATE,
    credit_score INT64,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()  -- Track when each record was loaded
)
PARTITION BY application_date
CLUSTER BY region, loan_type;

-- LAYER 2: Curated table — cleaned data with calculated/derived fields
-- Adds: monthly_payment, total_interest (computed by Dataflow)
-- Adds: risk_category (derived from credit_score thresholds)
CREATE TABLE IF NOT EXISTS `project.loan_analytics.curated_loans` (
    loan_id STRING,
    customer_id STRING,
    loan_type STRING,
    amount NUMERIC,
    interest_rate FLOAT64,
    term_months INT64,
    monthly_payment NUMERIC,       -- Calculated: EMI formula
    total_interest NUMERIC,        -- Calculated: total_paid - principal
    status STRING,
    risk_category STRING,          -- Derived: LOW/MEDIUM/HIGH/VERY_HIGH based on credit score
    region STRING,
    application_date DATE,
    credit_score INT64,
    load_date DATE
)
PARTITION BY application_date
CLUSTER BY region, risk_category;

-- LAYER 3: Aggregation table — pre-computed daily metrics for dashboards
-- Executives and dashboards query this (fast, small table)
CREATE TABLE IF NOT EXISTS `project.loan_analytics.daily_summary` (
    summary_date DATE,
    region STRING,
    loan_type STRING,
    total_applications INT64,
    total_amount NUMERIC,
    avg_credit_score FLOAT64,
    approval_rate FLOAT64,         -- % of applications approved
    default_rate FLOAT64           -- % of loans that defaulted
)
PARTITION BY summary_date;
```

---

## Step 4: Dataflow Pipeline

### Problem: How do you process millions of records reliably, handling errors without losing data?

**Definition:** Apache Beam is a unified batch+streaming framework. Dataflow is GCP's managed runner — it auto-scales workers, handles retries, and provides exactly-once processing guarantees.

**Why it matters:**
- **Without validation**: Bad data (negative amounts, invalid scores) flows into your warehouse → wrong reports → bad business decisions
- **Without dead-letter handling**: One malformed record crashes the entire pipeline → 9,999 good records don't get processed
- **Without enrichment**: Analysts must recalculate monthly payments in every query → slow, inconsistent, error-prone

**How we resolve it:**

| Problem | Solution in This Pipeline |
|---------|---------------------------|
| Malformed CSV rows | `ParseLoanRecord` catches exceptions → routes to dead-letter side output |
| Invalid business data | `ValidateLoanRecord` checks rules (amount > 0, score 300-850) → routes to 'invalid' |
| Missing derived fields | `EnrichLoanRecord` calculates EMI, total_interest, risk_category |
| Pipeline crashes | Beam's `with_outputs` ensures good records still flow even if some fail |

**4. Data Skew — Detection and Handling:**

**What is Data Skew?**
Data skew occurs when data is unevenly distributed across workers. Example: if 70% of loans are `PERSONAL` type and you group-by `loan_type`, one worker handles 70% of the work while others sit idle.

**How to Detect:**
- Dataflow UI shows one worker taking much longer than others
- "Hot keys" in GroupByKey operations
- Uneven element counts across parallel bundles

**How to Overcome:**
1. **Avoid unnecessary GroupByKey** — this pipeline uses element-wise transforms (ParDo), so skew isn't an issue here
2. **Use CombinePerKey instead of GroupByKey** — Beam can partially combine on each worker before shuffling
3. **Add a random salt to hot keys** — distribute `PERSONAL` into `PERSONAL_1`, `PERSONAL_2`, etc., then re-aggregate
4. **Use Dataflow's autoscaling** — it detects slow workers and redistributes ("dynamic work rebalancing")

**Interview Perspective:**
> "My Dataflow pipeline has three stages: parse, validate, enrich. I use the dead-letter pattern — bad records go to a side output instead of failing the job. This ensures 99.9% of good data still gets processed even when source systems send garbage. For skew, I avoid GroupByKey when possible and use element-wise ParDo transforms. If I must aggregate, I use CombinePerKey which does partial combining to reduce shuffle."

---

```python
# loan_pipeline.py
# Purpose: Apache Beam pipeline that reads CSV → validates → enriches → writes to BigQuery
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime


# --- STEP A: Parse raw CSV lines into structured dictionaries ---
class ParseLoanRecord(beam.DoFn):
    """Converts each CSV line into a Python dict. Bad records go to dead-letter."""
    def process(self, line):
        fields = line.split(',')
        if fields[0] == 'loan_id':  # Skip CSV header row
            return
        
        try:
            # Map positional CSV fields to named keys
            record = {
                'loan_id': fields[0],
                'customer_id': fields[1],
                'loan_type': fields[2],
                'amount': float(fields[3]),
                'interest_rate': float(fields[4]),
                'term_months': int(fields[5]),
                'status': fields[6],
                'region': fields[7],
                'application_date': fields[8],
                'credit_score': int(fields[9]),
            }
            yield record
        except (ValueError, IndexError) as e:
            # Dead-letter pattern: route unparseable records to a side output
            # instead of crashing the entire pipeline
            yield beam.pvalue.TaggedOutput('errors', {'line': line, 'error': str(e)})


class EnrichLoanRecord(beam.DoFn):
    def process(self, record):
        # Calculate monthly payment using the standard amortization formula
        # Convert annual interest rate (e.g., 12%) to monthly decimal (e.g., 0.01)
        r = record['interest_rate'] / 100 / 12  # monthly interest rate
        n = record['term_months']               # total number of monthly payments
        p = record['amount']                    # principal (loan amount)
        
        if r > 0:
            # Standard EMI (Equated Monthly Installment) formula:
            # EMI = P * [r * (1+r)^n] / [(1+r)^n - 1]
            # Where: P = principal, r = monthly rate, n = number of months
            # Example: $10,000 loan at 12% for 12 months → r=0.01, EMI ≈ $888.49
            monthly_payment = p * (r * (1 + r)**n) / ((1 + r)**n - 1)
        else:
            # If interest rate is 0%, simply divide principal by number of months
            monthly_payment = p / n
        
        record['monthly_payment'] = round(monthly_payment, 2)
        # Total interest = (all payments combined) minus the original loan amount
        record['total_interest'] = round(monthly_payment * n - p, 2)
        
        # Risk categorization based on FICO credit score thresholds
        # These buckets help downstream analytics segment loan portfolios
        score = record['credit_score']
        if score >= 750:
            record['risk_category'] = 'LOW'        # Excellent credit
        elif score >= 650:
            record['risk_category'] = 'MEDIUM'     # Good credit
        elif score >= 550:
            record['risk_category'] = 'HIGH'       # Fair credit
        else:
            record['risk_category'] = 'VERY_HIGH'  # Poor credit
        
        # Stamp the processing date for tracking when this record was enriched
        record['load_date'] = datetime.now().strftime('%Y-%m-%d')
        yield record


# --- STEP B: Validate business rules before enrichment ---
class ValidateLoanRecord(beam.DoFn):
    """Checks each record against business rules. Invalid records go to side output."""
    def process(self, record):
        errors = []
        # Business rule checks — catch data that makes no logical sense
        if record['amount'] <= 0:
            errors.append('Invalid amount')          # Can't have zero/negative loan
        if record['credit_score'] < 300 or record['credit_score'] > 850:
            errors.append('Invalid credit score')    # FICO range is 300-850
        if record['term_months'] <= 0:
            errors.append('Invalid term')            # Must have positive duration
        
        if errors:
            # Route invalid records to 'invalid' side output for investigation
            yield beam.pvalue.TaggedOutput('invalid', {
                'record': record, 'errors': errors
            })
        else:
            # Record passes all checks — proceed to enrichment
            yield record


# --- MAIN PIPELINE: Wire all steps together ---
def run(argv=None):
    # Parse command-line arguments (input file, output table, temp location)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='GCS path to input CSV')
    parser.add_argument('--output_table', required=True, help='BQ table: project:dataset.table')
    parser.add_argument('--temp_location', required=True)
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    
    # BigQuery schema definition (must match the enriched record fields)
    schema = (
        'loan_id:STRING,customer_id:STRING,loan_type:STRING,'
        'amount:NUMERIC,interest_rate:FLOAT64,term_months:INT64,'
        'monthly_payment:NUMERIC,total_interest:NUMERIC,'
        'status:STRING,risk_category:STRING,region:STRING,'
        'application_date:DATE,credit_score:INT64,load_date:DATE'
    )
    
    # Build the Beam DAG (Directed Acyclic Graph of transforms)
    with beam.Pipeline(options=options) as p:
        # PHASE 1: Read CSV file and parse each line into a dict
        # with_outputs creates two streams: 'records' (good) and 'errors' (bad)
        parsed = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText(known_args.input)
            | 'Parse' >> beam.ParDo(ParseLoanRecord()).with_outputs('errors', main='records')
        )
        
        # PHASE 2: Validate parsed records against business rules
        # Splits into 'valid' (proceed) and 'invalid' (quarantine)
        validated = (
            parsed['records']
            | 'Validate' >> beam.ParDo(ValidateLoanRecord()).with_outputs('invalid', main='valid')
        )
        
        # PHASE 3: Enrich valid records (add monthly_payment, risk_category)
        enriched = (
            validated['valid']
            | 'Enrich' >> beam.ParDo(EnrichLoanRecord())
        )
        
        # PHASE 4: Write enriched records to BigQuery curated table
        # WRITE_APPEND = add new rows (don't overwrite existing data)
        enriched | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
        
        # PHASE 5: Write parse errors to GCS as JSON for later investigation
        # This is the "dead-letter queue" — bad data doesn't get lost
        (
            parsed['errors']
            | 'FormatErrors' >> beam.Map(json.dumps)
            | 'WriteErrors' >> beam.io.WriteToText(
                f'{known_args.temp_location}/errors/parse_errors'
            )
        )


if __name__ == '__main__':
    run()
```

---

## Step 5: Cloud Composer DAG

### Problem: How do you reliably schedule and coordinate multi-step pipelines with dependency management?

**Definition:** Cloud Composer is GCP's managed Apache Airflow service. A DAG (Directed Acyclic Graph) defines tasks and their dependencies — ensuring steps run in the right order, with retries on failure and alerts to the team.

**Why it matters:**
- **Without orchestration**: You manually run scripts in order → what if you forget a step? What if step 2 fails but step 3 runs anyway on stale data?
- **Without retries**: A transient network error at 6 AM fails the pipeline → analysts have no data for morning reports
- **Without quality gates**: Bad data from step 1 flows into aggregations → executives see wrong numbers on dashboards

**How we resolve it:**

| Problem | Solution |
|---------|----------|
| Running steps out of order | `process_loans >> quality_check >> daily_summary` (explicit dependency chain) |
| Transient failures | `retries=2, retry_delay=5min` (auto-retry before alerting) |
| Bad data propagating | `BigQueryCheckOperator` blocks downstream if quality fails |
| Team not knowing about failures | `email_on_failure=True` sends immediate alert |
| Backfill confusion | `catchup=False` prevents running for all missed past dates |

**Common Orchestration Issues:**

1. **Task stuck in "running"** — Usually a Dataflow job that didn't terminate. Set `execution_timeout` on the task.
2. **Downstream ran on bad data** — Missing quality gate. Always put a check between load and aggregation.
3. **DAG not appearing in Airflow UI** — Syntax error in the Python file. Airflow silently skips broken DAGs.
4. **Idempotency** — If you re-run the DAG for the same date, does it produce duplicates? Use `WRITE_TRUNCATE` for the partition or `MERGE` statements.

**Interview Perspective:**
> "I use Cloud Composer for orchestration with explicit task dependencies. The DAG has a quality gate between data loading and aggregation — if the check fails (no data, duplicates, or invalid amounts), downstream tasks are blocked and the team gets an email. I configure retries with exponential backoff for transient failures, and I design tasks to be idempotent so re-runs are safe."

---

```python
# dags/loan_daily_pipeline.py
# Purpose: Airflow DAG that orchestrates the daily loan pipeline end-to-end
# Runs daily at 6 AM: Dataflow processing → Quality checks → Build summary
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Configuration — change these to match your GCP project
PROJECT = "your-project"
BUCKET = f"{PROJECT}-loan-landing"
BQ_DATASET = "loan_analytics"

# Default settings applied to all tasks in this DAG
default_args = {
    'owner': 'data-team',
    'retries': 2,                        # Retry failed tasks up to 2 times
    'retry_delay': timedelta(minutes=5), # Wait 5 min between retries
    'email_on_failure': True,            # Alert team if task fails after all retries
}

with DAG(
    'loan_daily_pipeline',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # Cron: run daily at 6:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,                  # Don't backfill missed runs
    tags=['loans', 'production'],
) as dag:

    # TASK 1: Trigger Dataflow job to process today's loan CSV file
    # {{ ds }} = Airflow execution date (e.g., 2026-06-22)
    process_loans = DataflowCreatePythonJobOperator(
        task_id='process_loans',
        py_file=f'gs://{BUCKET}/pipelines/loan_pipeline.py',
        job_name='loan-daily-{{ ds_nodash }}',
        options={
            'input': f'gs://{BUCKET}/raw/loans/{{{{ ds }}}}/loans.csv',
            'output_table': f'{PROJECT}:{BQ_DATASET}.curated_loans',
            'temp_location': f'gs://{PROJECT}-dataflow-temp/temp',
        },
        project_id=PROJECT,
        location='us-central1',
    )

    # TASK 2: Data quality gate — blocks downstream if checks fail
    # All 3 conditions must return 1 (true) for the task to pass:
    #   has_data = at least one record was loaded today
    #   valid_amounts = no records with zero/negative amounts
    #   no_dupes = every loan_id is unique (no duplicate loads)
    quality_check = BigQueryCheckOperator(
        task_id='quality_check',
        sql=f"""
            SELECT
                CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END as has_data,
                CASE WHEN COUNTIF(amount <= 0) = 0 THEN 1 ELSE 0 END as valid_amounts,
                CASE WHEN COUNT(*) = COUNT(DISTINCT loan_id) THEN 1 ELSE 0 END as no_dupes
            FROM `{PROJECT}.{BQ_DATASET}.curated_loans`
            WHERE load_date = '{{{{ ds }}}}'
        """,
        use_legacy_sql=False,
    )

    # TASK 3: Build pre-aggregated summary table for dashboards
    # Groups today's loans by region + loan_type and computes KPIs
    # This runs ONLY if quality_check passes (dependency chain below)
    daily_summary = BigQueryInsertJobOperator(
        task_id='build_daily_summary',
        configuration={
            'query': {
                'query': f"""
                    INSERT INTO `{PROJECT}.{BQ_DATASET}.daily_summary`
                    SELECT
                        DATE('{{{{ ds }}}}') as summary_date,
                        region,
                        loan_type,
                        COUNT(*) as total_applications,
                        SUM(amount) as total_amount,
                        AVG(credit_score) as avg_credit_score,
                        COUNTIF(status = 'APPROVED') / COUNT(*) as approval_rate,
                        COUNTIF(status = 'DEFAULT') / COUNT(*) as default_rate
                    FROM `{PROJECT}.{BQ_DATASET}.curated_loans`
                    WHERE load_date = '{{{{ ds }}}}'
                    GROUP BY region, loan_type
                """,
                'useLegacySql': False,
            }
        },
    )

    # Define task execution order: process → check quality → build summary
    # If quality_check fails, daily_summary will NOT run (pipeline stops)
    process_loans >> quality_check >> daily_summary
```

---

## Step 6: Run the Project

### Problem: How do you test locally before spending money on cloud execution?

**Definition:** Apache Beam's runner abstraction lets you run the same pipeline code locally (`DirectRunner`) for testing and on cloud (`DataflowRunner`) for production — zero code changes required.

**Why it matters:**
- **Cloud runs cost money** — a Dataflow job spins up VMs (~$0.05/worker/min). Testing a buggy pipeline 10 times = wasted spend
- **Cloud runs are slow to start** — 3-5 min just to provision workers. Local runs start instantly
- **Debugging is harder in cloud** — logs are scattered across workers. Local runs give you a single stack trace

**How we resolve it:**

| Phase | Runner | When to Use |
|-------|--------|-------------|
| Development | `DirectRunner` | Testing logic, debugging errors, fast iteration |
| Integration test | `DataflowRunner` (small data) | Verify cloud permissions, BQ writes, GCS access |
| Production | `DataflowRunner` (full data) | Daily runs with autoscaling |

**Common Gotchas:**
1. **DirectRunner works but DataflowRunner fails** — Usually a serialization issue (lambdas with local references) or missing pip packages on workers
2. **Permission denied on cloud** — Service account missing a role. Check IAM bindings.
3. **Pipeline succeeds but BQ table empty** — Schema mismatch between your code and the actual table. Check column names/types exactly.

**Interview Perspective:**
> "I always develop and test with DirectRunner locally first — it's instant and free. Once logic is verified, I run a small integration test on DataflowRunner to confirm cloud permissions and BQ connectivity. Only then do I deploy to production with full data. This saves significant cost and debugging time."

---

```bash
# 1. Generate fake loan data (creates loans.csv with 10,000 records)
python generate_loan_data.py

# 2. Upload raw CSV to GCS landing zone (organized by date)
gsutil cp loans.csv gs://${PROJECT_ID}-loan-landing/raw/loans/2026-06-22/

# 3. Test pipeline locally first (DirectRunner = runs on your machine, no cloud cost)
#    Good for debugging before deploying to cloud
python loan_pipeline.py \
    --input=loans.csv \
    --output_table=${PROJECT_ID}:loan_analytics.curated_loans \
    --temp_location=gs://${PROJECT_ID}-dataflow-temp/temp \
    --runner=DirectRunner

# 4. Run on Dataflow in the cloud (DataflowRunner = auto-scales workers)
#    Use this for production — handles large datasets in parallel
python loan_pipeline.py \
    --input=gs://${PROJECT_ID}-loan-landing/raw/loans/2026-06-22/loans.csv \
    --output_table=${PROJECT_ID}:loan_analytics.curated_loans \
    --temp_location=gs://${PROJECT_ID}-dataflow-temp/temp \
    --runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --region=us-central1

# 5. Verify results — query BigQuery to see enriched data grouped by risk
bq query --use_legacy_sql=false \
    "SELECT risk_category, COUNT(*), AVG(amount) FROM \`${PROJECT_ID}.loan_analytics.curated_loans\` GROUP BY 1"
```

---

## What This Project Demonstrates

| Skill | How It's Shown |
|-------|---------------|
| GCS data lake | Landing zone structure |
| Dataflow/Beam | Batch processing with error handling |
| BigQuery | Schema design, partitioning, clustering |
| Data quality | Validation in pipeline + SQL checks |
| Orchestration | Composer DAG with dependencies |
| Data modeling | Raw → Curated → Aggregation layers |
| Error handling | Dead-letter pattern for bad records |
| Cost optimization | Partitioned tables, targeted queries |

---

## Extensions (For Extra Credit)

1. **Add streaming**: Send events to Pub/Sub, run Dataflow streaming pipeline
2. **Add ML**: Train BigQuery ML model to predict loan defaults
3. **Add CI/CD**: Cloud Build to test and deploy pipeline changes
4. **Add monitoring**: Cloud Monitoring alerts on pipeline failures
5. **Add CDC**: Simulate updates and implement MERGE logic
