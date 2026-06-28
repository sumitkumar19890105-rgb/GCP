# dbt & Dataform — Complete Guide for GCP Data Engineers

---

## Table of Contents

1. [What is dbt? (In-Depth Definition)](#1-what-is-dbt-in-depth-definition)
2. [What is Dataform? (GCP-Native Alternative)](#2-what-is-dataform-gcp-native-alternative)
3. [dbt vs Dataform — Detailed Comparison](#3-dbt-vs-dataform--detailed-comparison)
4. [dbt Project Structure](#4-dbt-project-structure)
5. [Core Concepts with Examples](#5-core-concepts-with-examples)
6. [Incremental Models — Deep Dive](#6-incremental-models--deep-dive)
7. [Snapshots (SCD Type 2)](#7-snapshots-scd-type-2)
8. [Testing](#8-testing)
9. [Macros (Reusable SQL)](#9-macros-reusable-sql)
10. [dbt with BigQuery — Production Config](#10-dbt-with-bigquery--production-config)
11. [dbt in Cloud Composer (Airflow)](#11-dbt-in-cloud-composer-airflow)
12. [Interview Questions — dbt & Dataform](#12-interview-questions--dbt--dataform)

---

## 1. What is dbt? (In-Depth Definition)

**dbt (data build tool)** is an open-source SQL transformation framework that lets data engineers and analysts write modular, testable, version-controlled SQL to transform data **inside** the warehouse. It handles the **T in ELT** — you write SELECT statements, and dbt compiles them into CREATE TABLE/VIEW DDL, manages dependencies, runs tests, and generates documentation.

### Core Philosophy

> "dbt treats SQL like software engineering code — with version control, modularity, testing, and documentation."

**Key idea**: You never write `INSERT INTO` or `CREATE TABLE`. You write a SELECT query in a `.sql` file, declare how to materialize it (table/view/incremental), and dbt handles everything else.

### How dbt Works

```
┌─────────────────────────────────────────────────────────────┐
│  Developer writes:                                           │
│    models/fct_transactions.sql (just a SELECT statement)     │
│                                                              │
│  dbt compiles to:                                            │
│    CREATE OR REPLACE TABLE project.analytics.fct_transactions│
│    AS (SELECT ... FROM ... JOIN ...)                         │
│                                                              │
│  dbt also:                                                   │
│    ✓ Resolves {{ ref() }} into actual table names            │
│    ✓ Builds dependency DAG (knows execution order)           │
│    ✓ Runs data quality tests after materialization           │
│    ✓ Generates documentation + lineage graph                 │
└─────────────────────────────────────────────────────────────┘
```

### What Problems dbt Solves

| Problem (Without dbt) | Solution (With dbt) |
|------------------------|---------------------|
| 2000-line SQL scripts with no modularity | Modular models with `ref()` — each model is one concern |
| No dependency management (which query runs first?) | DAG built automatically from `ref()` calls |
| No testing — bad data silently propagates | Built-in tests: `unique`, `not_null`, `accepted_values`, custom SQL |
| No documentation — tribal knowledge | Auto-generated docs site with lineage |
| No version control — changes overwrite production | Git-native: branch, review, merge, rollback |
| Incremental processing is painful to write | `is_incremental()` macro handles it elegantly |
| SCD Type 2 is complex to implement | `dbt snapshot` does it in 10 lines of config |

### Core Features

| Feature | What it does | Example |
|---------|-------------|---------|
| **`ref()`** | Declares dependency between models, builds DAG | `FROM {{ ref('stg_transactions') }}` |
| **Materializations** | Controls how SQL becomes a database object | `view`, `table`, `incremental`, `ephemeral` |
| **Tests** | Validates data quality after each run | `unique`, `not_null`, `relationships`, custom SQL |
| **Sources** | Defines + freshness-checks raw tables | `{{ source('raw', 'transactions') }}` |
| **Macros** | Reusable SQL functions (Jinja) | `{{ cents_to_dollars('amount') }}` |
| **Snapshots** | SCD Type 2 from any source | Automatically tracks historical changes |
| **Seeds** | Static reference data loaded from CSV | Country codes, category mappings |
| **Packages** | Installable community libraries | `dbt-utils`, `dbt-expectations`, `dbt-audit-helper` |

### dbt in the Modern Data Stack
```
Sources → Ingestion (Fivetran/Airbyte) → Warehouse (BigQuery) → dbt (Transform) → BI (Looker)
                                              │
                                              └── dbt runs SQL transforms inside BigQuery
```

### Deployment Options

| Option | Description | Best For |
|--------|-------------|----------|
| **dbt Core (CLI)** | Open-source, run locally or in CI/CD | Engineers who want full control |
| **dbt Cloud** | Managed SaaS with IDE, scheduler, docs hosting | Teams wanting managed experience |
| **dbt in Airflow/Composer** | Orchestrated via `BashOperator` or `dbt-airflow` | Production pipelines with complex DAGs |

---

## 2. What is Dataform? (GCP-Native Alternative)

**Dataform** is Google Cloud's **fully managed SQL workflow tool** for BigQuery — acquired by Google in 2020. It serves the same purpose as dbt (SQL-based transformations, dependency management, testing) but is **natively integrated into the GCP Console** with zero additional infrastructure.

### Core Philosophy

> "Dataform is dbt for BigQuery, managed by Google — zero infrastructure, native IAM, console-integrated."

### How Dataform Works

```
┌─────────────────────────────────────────────────────────────┐
│  Developer writes:                                           │
│    definitions/fct_transactions.sqlx (SQLX = SQL+JS)         │
│                                                              │
│  Dataform compiles to:                                       │
│    CREATE OR REPLACE TABLE project.analytics.fct_transactions│
│    AS (SELECT ... FROM ... JOIN ...)                         │
│                                                              │
│  Dataform also:                                              │
│    ✓ Resolves ${ref()} into actual table names               │
│    ✓ Builds dependency graph (execution order)               │
│    ✓ Runs assertions (data quality checks)                   │
│    ✓ Integrated into GCP Console (no separate tool)          │
└─────────────────────────────────────────────────────────────┘
```

### Dataform Project Structure

```
dataform_project/
├── dataform.json              # Project config (warehouse, schema)
├── definitions/
│   ├── staging/
│   │   ├── stg_transactions.sqlx
│   │   └── stg_customers.sqlx
│   ├── intermediate/
│   │   └── int_loan_enriched.sqlx
│   └── marts/
│       ├── fct_transactions.sqlx
│       └── dim_customers.sqlx
├── includes/                  # Reusable JS functions (like dbt macros)
│   └── constants.js
└── package.json               # Dependencies
```

### Dataform SQLX Example

```sql
-- definitions/marts/fct_transactions.sqlx
config {
    type: "table",
    schema: "analytics",
    description: "Cleaned transaction fact table partitioned by date",
    bigquery: {
        partitionBy: "transaction_date",
        clusterBy: ["region", "merchant_category"]
    },
    assertions: {
        uniqueKey: ["transaction_id"],
        nonNull: ["transaction_id", "amount", "transaction_date"]
    }
}

SELECT
    t.transaction_id,
    t.amount,
    t.transaction_date,
    t.region,
    t.merchant_category,
    c.customer_name,
    c.segment,
    CASE
        WHEN t.amount > 10000 THEN 'HIGH_VALUE'
        WHEN t.amount > 1000 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END AS value_tier
FROM ${ref("stg_transactions")} t
LEFT JOIN ${ref("dim_customers")} c ON t.customer_id = c.customer_id
WHERE t.amount > 0
```

### Key Dataform Features

| Feature | Syntax | Equivalent in dbt |
|---------|--------|-------------------|
| Dependencies | `${ref("model")}` | `{{ ref('model') }}` |
| Materializations | `config { type: "table" }` | `{{ config(materialized='table') }}` |
| Assertions (tests) | `assertions: { uniqueKey: [...] }` | `tests:` in YAML |
| Incremental | `config { type: "incremental" }` | `{{ config(materialized='incremental') }}` |
| Reusable functions | JavaScript in `includes/` | Jinja macros |
| Pre/post operations | `pre_operations`, `post_operations` | `pre-hook`, `post-hook` |

### Dataform Incremental Example

```sql
-- definitions/marts/fct_events_incremental.sqlx
config {
    type: "incremental",
    schema: "analytics",
    uniqueKey: ["event_id"],
    bigquery: {
        partitionBy: "event_date"
    }
}

SELECT
    event_id,
    user_id,
    event_type,
    event_date,
    CURRENT_TIMESTAMP() AS loaded_at
FROM ${ref("stg_events")}
${when(incremental(), `WHERE event_date > (SELECT MAX(event_date) FROM ${self()})`)}
```

---

## 3. dbt vs Dataform — Detailed Comparison

### Feature-by-Feature

| Aspect | dbt (dbt-core / dbt Cloud) | Dataform (GCP-native) |
|--------|---------------------------|----------------------|
| **Owned by** | dbt Labs (open source + cloud) | Google Cloud |
| **Language** | SQL + Jinja (Python templating) | SQLX (SQL + JavaScript) |
| **Supported warehouses** | BigQuery, Snowflake, Redshift, Databricks, etc. | **BigQuery only** |
| **Hosting** | Self-managed (CLI) or dbt Cloud (SaaS) | Fully integrated in GCP Console |
| **Dependency syntax** | `{{ ref('model') }}` | `${ref("model")}` |
| **Testing** | YAML-defined tests + custom SQL tests | Assertions in config block |
| **Scheduling** | dbt Cloud, Airflow, Composer | Cloud Scheduler / Composer / Console |
| **Version control** | Git (any provider) | Git (built-in GitHub/GitLab integration) |
| **Cost** | Free (core) / $100+/mo (Cloud) | **Free** (included with BigQuery) |
| **IAM integration** | Service account key or Workload Identity | Native GCP IAM |
| **Community** | Very large, 1000+ packages | Smaller but growing |
| **Learning curve** | Jinja templating can be complex | JavaScript is more familiar to many |
| **Multi-cloud** | ✅ Yes | ❌ No (BigQuery only) |
| **Documentation** | Excellent (auto-generated docs site) | Good (integrated in Console) |
| **Incremental** | Mature, many strategies | Supported, fewer options |
| **Snapshots (SCD)** | Built-in `dbt snapshot` | Manual implementation |

### When to Choose Which

| Scenario | Recommendation | Why |
|----------|---------------|-----|
| BigQuery-only, want simplest setup | **Dataform** | Zero extra infra, free, native GCP |
| Multi-cloud / multi-warehouse | **dbt** | Only option that supports Snowflake, Redshift, etc. |
| Large team with existing dbt expertise | **dbt** | Bigger community, more packages, established workflows |
| GCP-native, tight IAM/console integration | **Dataform** | Managed in GCP console, native IAM |
| Need complex macros/packages | **dbt** | 1000+ community packages (dbt-utils, etc.) |
| Need Airflow orchestration | **dbt** | More mature Airflow integration |
| Small team, new project on GCP | **Dataform** | Fastest time-to-value |
| Need SCD Type 2 snapshots | **dbt** | Built-in snapshot feature |
| Want managed scheduling without Composer | **Dataform** | Built-in schedule in Console |
| Enterprise with compliance requirements | **Either** | Both support Git, both auditable |

### Code Comparison — Same Logic

**dbt version:**
```sql
-- models/marts/fct_daily_revenue.sql
{{ config(
    materialized='incremental',
    unique_key='date_region_key',
    partition_by={'field': 'revenue_date', 'data_type': 'date'}
) }}

SELECT
    CONCAT(CAST(transaction_date AS STRING), '_', region) AS date_region_key,
    transaction_date AS revenue_date,
    region,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_transaction
FROM {{ ref('stg_transactions') }}
{% if is_incremental() %}
WHERE transaction_date > (SELECT MAX(revenue_date) FROM {{ this }})
{% endif %}
GROUP BY 1, 2, 3
```

**Dataform version:**
```sql
-- definitions/marts/fct_daily_revenue.sqlx
config {
    type: "incremental",
    uniqueKey: ["date_region_key"],
    bigquery: {
        partitionBy: "revenue_date"
    }
}

SELECT
    CONCAT(CAST(transaction_date AS STRING), '_', region) AS date_region_key,
    transaction_date AS revenue_date,
    region,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_transaction
FROM ${ref("stg_transactions")}
${when(incremental(), `WHERE transaction_date > (SELECT MAX(revenue_date) FROM ${self()})`)}
GROUP BY 1, 2, 3
```

### Interview Takeaway

> "Both dbt and Dataform solve the same problem: modular, testable, version-controlled SQL transformations in the warehouse (ELT's T layer). I choose **Dataform** for pure BigQuery projects because it's free, native to GCP, and requires no additional infrastructure. I choose **dbt** when working across multiple warehouses, or when the team needs the mature ecosystem (1000+ packages, large community). In either case, the core pattern is identical: write SELECT queries, declare dependencies with `ref()`, add data quality tests, and let the tool handle materialization and execution order."

---

## 4. dbt Project Structure

```
dbt_project/
├── dbt_project.yml          # Project config
├── profiles.yml             # Connection config (BigQuery)
├── models/
│   ├── staging/             # 1:1 with sources (clean, rename, type-cast)
│   │   ├── stg_loans.sql
│   │   ├── stg_customers.sql
│   │   └── _staging.yml     # Schema tests
│   ├── intermediate/        # Business logic, joins
│   │   ├── int_loan_enriched.sql
│   │   └── int_customer_360.sql
│   ├── marts/               # Final business tables (star schema)
│   │   ├── fct_transactions.sql
│   │   ├── dim_customers.sql
│   │   └── _marts.yml
│   └── sources.yml          # Source definitions
├── tests/                   # Custom data tests
│   └── assert_no_orphan_loans.sql
├── macros/                  # Reusable SQL snippets
│   └── generate_schema_name.sql
├── snapshots/               # SCD Type 2
│   └── snap_customers.sql
└── seeds/                   # Static reference data (CSV)
    └── country_codes.csv
```

---

## 5. Core Concepts with Examples

### Sources (Define raw tables)
```yaml
# models/sources.yml
version: 2

sources:
  - name: raw
    database: my_project
    schema: raw_layer
    tables:
      - name: loans
        description: "Raw loan applications from source system"
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
        columns:
          - name: loan_id
            tests:
              - not_null
              - unique
      - name: customers
        description: "Raw customer records"
```

### Staging Models (Clean + Rename)
```sql
-- models/staging/stg_loans.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'loans') }}
),

renamed AS (
    SELECT
        loan_id,
        cust_id AS customer_id,
        CAST(loan_amt AS NUMERIC) AS loan_amount,
        CAST(int_rate AS FLOAT64) AS interest_rate,
        CAST(term_mths AS INT64) AS term_months,
        UPPER(TRIM(loan_status)) AS status,
        UPPER(TRIM(region_cd)) AS region,
        PARSE_DATE('%Y%m%d', app_date_str) AS application_date,
        credit_score,
        _loaded_at AS loaded_at
    FROM source
    WHERE loan_id IS NOT NULL  -- Remove junk records
)

SELECT * FROM renamed
```

### Intermediate Models (Business Logic)
```sql
-- models/intermediate/int_loan_enriched.sql
WITH loans AS (
    SELECT * FROM {{ ref('stg_loans') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

enriched AS (
    SELECT
        l.loan_id,
        l.customer_id,
        c.customer_name,
        c.segment,
        l.loan_amount,
        l.interest_rate,
        l.term_months,
        l.status,
        l.region,
        l.application_date,
        l.credit_score,
        -- Derived fields
        ROUND(l.loan_amount * (l.interest_rate / 100 / 12) * 
            POWER(1 + l.interest_rate / 100 / 12, l.term_months) /
            (POWER(1 + l.interest_rate / 100 / 12, l.term_months) - 1), 2
        ) AS monthly_payment,
        CASE
            WHEN l.credit_score >= 750 THEN 'LOW_RISK'
            WHEN l.credit_score >= 650 THEN 'MEDIUM_RISK'
            WHEN l.credit_score >= 550 THEN 'HIGH_RISK'
            ELSE 'VERY_HIGH_RISK'
        END AS risk_category,
        CASE
            WHEN l.loan_amount > 100000 THEN 'LARGE'
            WHEN l.loan_amount > 25000 THEN 'MEDIUM'
            ELSE 'SMALL'
        END AS loan_size_bucket
    FROM loans l
    LEFT JOIN customers c ON l.customer_id = c.customer_id
)

SELECT * FROM enriched
```

### Mart Models (Final Facts and Dimensions)
```sql
-- models/marts/fct_loan_applications.sql
{{ config(
    materialized='incremental',
    partition_by={
        "field": "application_date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["region", "risk_category"]
) }}

WITH enriched AS (
    SELECT * FROM {{ ref('int_loan_enriched') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    loan_id,
    customer_id,
    customer_name,
    segment,
    loan_amount,
    interest_rate,
    term_months,
    monthly_payment,
    status,
    risk_category,
    loan_size_bucket,
    region,
    application_date,
    credit_score,
    loaded_at
FROM enriched
```

---

## 6. Incremental Models — Deep Dive

### Strategy Comparison
| Strategy | How It Works | Use When |
|----------|-------------|----------|
| `append` | INSERT new rows only | Event logs, immutable data |
| `merge` | MERGE on unique key | Mutable records (updates + inserts) |
| `delete+insert` | DELETE matching, then INSERT | Partitioned data with known boundaries |
| `insert_overwrite` | Overwrite entire partitions | Partition-level idempotency |

### Advanced Incremental Example
```sql
-- models/marts/fct_daily_balances.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['account_id', 'balance_date'],
    partition_by={"field": "balance_date", "data_type": "date"},
    on_schema_change='append_new_columns'
) }}

WITH source AS (
    SELECT * FROM {{ ref('int_account_balances') }}
    {% if is_incremental() %}
    -- Only process last 3 days (handles late-arriving data)
    WHERE balance_date >= DATE_SUB(
        (SELECT MAX(balance_date) FROM {{ this }}), 
        INTERVAL 3 DAY
    )
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, balance_date 
            ORDER BY updated_at DESC
        ) AS rn
    FROM source
)

SELECT * EXCEPT(rn) FROM deduplicated WHERE rn = 1
```

---

## 7. Snapshots (SCD Type 2)

```sql
-- snapshots/snap_customers.sql
{% snapshot snap_customers %}

{{ config(
    target_database='my_project',
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['name', 'segment', 'region', 'credit_limit'],
    invalidate_hard_deletes=True
) }}

SELECT
    customer_id,
    name,
    segment,
    region,
    credit_limit,
    email,
    updated_at
FROM {{ source('raw', 'customers') }}

{% endsnapshot %}

-- Output table automatically gets:
--   dbt_scd_id (surrogate key)
--   dbt_updated_at
--   dbt_valid_from (effective_start)
--   dbt_valid_to (effective_end, NULL = current)
```

### Query the Snapshot
```sql
-- Current state
SELECT * FROM {{ ref('snap_customers') }}
WHERE dbt_valid_to IS NULL;

-- State as of a specific date
SELECT * FROM {{ ref('snap_customers') }}
WHERE '2026-03-15' BETWEEN dbt_valid_from AND COALESCE(dbt_valid_to, '9999-12-31');
```

---

## 8. Testing

### Schema Tests (YAML)
```yaml
# models/marts/_marts.yml
version: 2

models:
  - name: fct_loan_applications
    description: "Fact table for loan applications"
    columns:
      - name: loan_id
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: loan_amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 10000000
      - name: risk_category
        tests:
          - accepted_values:
              values: ['LOW_RISK', 'MEDIUM_RISK', 'HIGH_RISK', 'VERY_HIGH_RISK']
      - name: application_date
        tests:
          - not_null
          - dbt_utils.not_accepted_values:
              values: ['9999-12-31']
```

### Custom Data Tests
```sql
-- tests/assert_loan_amounts_balance.sql
-- This test FAILS if it returns any rows

SELECT
    application_date,
    SUM(loan_amount) as total_amount,
    (SELECT SUM(loan_amount) FROM {{ ref('stg_loans') }} WHERE application_date = t.application_date) as source_amount
FROM {{ ref('fct_loan_applications') }} t
GROUP BY application_date
HAVING ABS(SUM(loan_amount) - (
    SELECT SUM(loan_amount) FROM {{ ref('stg_loans') }} WHERE application_date = t.application_date
)) > 0.01
```

```sql
-- tests/assert_no_future_dates.sql
SELECT *
FROM {{ ref('fct_loan_applications') }}
WHERE application_date > CURRENT_DATE()
```

---

## 9. Macros (Reusable SQL)

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, precision=2) %}
    ROUND(CAST({{ column_name }} AS NUMERIC) / 100, {{ precision }})
{% endmacro %}

-- Usage in a model:
SELECT
    transaction_id,
    {{ cents_to_dollars('amount_cents') }} AS amount_dollars
FROM {{ ref('stg_transactions') }}
```

```sql
-- macros/generate_surrogate_key.sql
{% macro surrogate_key(field_list) %}
    FARM_FINGERPRINT(CONCAT(
        {% for field in field_list %}
            COALESCE(CAST({{ field }} AS STRING), '_null_')
            {% if not loop.last %}, '|', {% endif %}
        {% endfor %}
    ))
{% endmacro %}

-- Usage:
SELECT
    {{ surrogate_key(['customer_id', 'effective_date']) }} AS customer_key,
    *
FROM {{ ref('int_customer_versions') }}
```

---

## 10. dbt with BigQuery — Production Config

### profiles.yml
```yaml
my_project:
  target: prod
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: my-project-dev
      dataset: dbt_dev_{{ env_var('USER') }}
      location: US
      threads: 4
      timeout_seconds: 300

    prod:
      type: bigquery
      method: service-account
      project: my-project-prod
      dataset: analytics
      location: US
      keyfile: /secrets/dbt-sa-key.json
      threads: 8
      timeout_seconds: 600
      retries: 3
```

### dbt_project.yml
```yaml
name: 'loan_analytics'
version: '1.0.0'
profile: 'my_project'

model-paths: ["models"]
test-paths: ["tests"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]
seed-paths: ["seeds"]

models:
  loan_analytics:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: ephemeral  # CTEs, not persisted
    marts:
      +materialized: table
      +schema: analytics
      +tags: ["daily"]
```

### Run Commands
```bash
# Full run
dbt run

# Run specific model + all downstream
dbt run --select int_loan_enriched+

# Run only models with tag
dbt run --select tag:daily

# Run tests
dbt test

# Run specific test
dbt test --select fct_loan_applications

# Generate docs
dbt docs generate
dbt docs serve

# Snapshot
dbt snapshot

# Full pipeline
dbt seed && dbt snapshot && dbt run && dbt test
```

---

## 11. dbt in Cloud Composer (Airflow)

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'dbt_daily',
    schedule_interval='0 7 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /dbt/loan_analytics && dbt run --target prod --select tag:daily',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /dbt/loan_analytics && dbt test --target prod --select tag:daily',
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /dbt/loan_analytics && dbt snapshot --target prod',
    )

    dbt_snapshot >> dbt_run >> dbt_test
```

---

## 12. Interview Questions — dbt & Dataform

---

### dbt Questions

**Q1: What is dbt and what problem does it solve?**
> dbt is a SQL transformation framework that brings software engineering practices (version control, modularity, testing, documentation) to analytics SQL. It solves the problem of having thousands of lines of unmanaged SQL scripts with no dependency tracking, no testing, and no documentation.
>
> **Key insight**: dbt doesn't extract or load data — it only transforms data that's already in the warehouse. It's the "T" in ELT.

---

**Q2: When would you use dbt vs Dataflow for transformations?**
> **dbt:** SQL-based transforms inside BigQuery. Best for ELT pattern where data is already in the warehouse. Great for business logic, aggregations, dimensional modeling. No infra to manage.
>
> **Dataflow:** For complex processing that can't be expressed in SQL. Streaming pipelines. Data that needs heavy Python/Java processing before loading. Transformations BEFORE data reaches the warehouse.
>
> **In practice:** Use both. Dataflow for ingestion + initial cleaning → BigQuery raw layer → dbt for all business transforms.
>
> **Decision rule**: "Can this logic be expressed in SQL? → dbt. Does it need Python, streaming, or external API calls? → Dataflow."

---

**Q3: Explain dbt materializations. When would you use each?**
> | Materialization | What it creates | When to use |
> |----------------|----------------|-------------|
> | **view** | `CREATE VIEW` | Staging models (1:1 with source), lightweight, always fresh |
> | **table** | `CREATE TABLE` | Final marts, heavy aggregations, models queried by BI tools |
> | **incremental** | `INSERT/MERGE` into existing table | Large fact tables where full rebuild is too expensive |
> | **ephemeral** | Nothing (becomes a CTE in downstream model) | Intermediate logic used only once, avoid table creation overhead |
>
> **Tricky interview point**: An ephemeral model creates NO database object — it's inlined as a CTE wherever it's referenced. This means it can't be queried directly or tested independently.

---

**Q4: How do you handle a model that takes too long to run?**
> 1. Switch from `table` to `incremental` (only process new data)
> 2. Add partition pruning in the incremental filter
> 3. Use `cluster_by` for columns used in WHERE/JOIN
> 4. Break into smaller intermediate models (parallelize independent branches)
> 5. Use `ephemeral` for models only used once (become CTEs, no table creation)
> 6. Check BigQuery execution plan for full scans
>
> **Example**: A `fct_transactions` model rebuilt daily was scanning 2TB. Switching to incremental with `WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})` reduced daily scan to ~50GB.

---

**Q5: How does `ref()` work and why is it important?**
> `ref()` does two things:
> 1. **Resolves table names** — compiles `{{ ref('stg_transactions') }}` into the actual schema-qualified table name (handles dev/prod environments automatically)
> 2. **Builds the DAG** — tells dbt that this model depends on `stg_transactions`, so it must run first
>
> **Without `ref()`**: You'd hardcode table names (breaks across environments) and manually manage execution order (error-prone).
>
> **Tricky point**: `ref()` can cross projects in dbt (multi-project DAGs), and you can ref a model in another dbt package.

---

**Q6: How do you manage dbt across environments (dev/staging/prod)?**
> 1. **profiles.yml** defines different targets (dev writes to personal dataset, prod to shared)
> 2. **generate_schema_name** macro controls target schema per model
> 3. Dev: `dbt run --target dev` → writes to `dbt_dev_username`
> 4. PR: CI runs `dbt run --target ci` + `dbt test` against isolated dataset
> 5. Merge to main: CD deploys with `dbt run --target prod`
> 6. Use `{{ target.name }}` in models for environment-specific logic
>
> **Custom schema macro** (most common interview question):
> ```sql
> -- macros/generate_schema_name.sql
> {% macro generate_schema_name(custom_schema_name, node) %}
>     {% if target.name == 'prod' %}
>         {{ custom_schema_name | trim }}
>     {% else %}
>         {{ target.schema }}_{{ custom_schema_name | trim }}
>     {% endif %}
> {% endmacro %}
> ```

---

**Q7: What is an incremental model? How do you handle late-arriving data?**
> An incremental model only processes new/changed rows on subsequent runs (first run does a full load).
>
> **Late-arriving data problem**: If yesterday's data arrives today, `WHERE date > MAX(date)` misses it.
>
> **Solutions**:
> 1. **Lookback window**: `WHERE date > MAX(date) - INTERVAL 3 DAY` (reprocess last 3 days)
> 2. **merge_update_columns**: Use `unique_key` + MERGE to upsert late arrivals
> 3. **full_refresh on schedule**: Weekly full rebuild to catch anything missed
>
> ```sql
> {{ config(
>     materialized='incremental',
>     unique_key='transaction_id',
>     incremental_strategy='merge'
> ) }}
> SELECT * FROM {{ ref('stg_transactions') }}
> {% if is_incremental() %}
>     WHERE transaction_date >= (SELECT MAX(transaction_date) - INTERVAL 3 DAY FROM {{ this }})
> {% endif %}
> ```

---

**Q8: How do you test data quality in dbt?**
> **Three levels of testing:**
>
> 1. **Schema tests** (YAML — declarative):
> ```yaml
> columns:
>   - name: transaction_id
>     tests: [unique, not_null]
>   - name: amount
>     tests:
>       - dbt_utils.accepted_range:
>           min_value: 0
>           max_value: 10000000
> ```
>
> 2. **Custom SQL tests** (singular tests):
> ```sql
> -- tests/assert_no_orphan_transactions.sql
> SELECT * FROM {{ ref('fct_transactions') }} t
> LEFT JOIN {{ ref('dim_customers') }} c ON t.customer_id = c.customer_id
> WHERE c.customer_id IS NULL
> -- If this returns rows, test FAILS
> ```
>
> 3. **Source freshness tests**:
> ```yaml
> sources:
>   - name: raw
>     tables:
>       - name: transactions
>         loaded_at_field: _loaded_at
>         freshness:
>           warn_after: {count: 2, period: hour}
>           error_after: {count: 6, period: hour}
> ```

---

**Q9: What are dbt snapshots? How do they implement SCD Type 2?**
> Snapshots track changes to source data over time — implementing SCD Type 2 automatically.
>
> **How it works**:
> - First run: Loads all rows with `dbt_valid_from = now`, `dbt_valid_to = NULL`
> - Subsequent runs: Detects changes (via `check_cols` or `updated_at`), closes old records (`dbt_valid_to = now`), inserts new version
>
> **Two strategies**:
> - `strategy='timestamp'` — uses an `updated_at` column to detect changes (faster, requires source to have this column)
> - `strategy='check'` — compares specified column values (works on any source, but slower)
>
> **Interview trap**: "How do you track deletes?" → Use `invalidate_hard_deletes=True` — closes records that disappear from source.

---

**Q10: Explain the dbt DAG. What happens if you have a circular dependency?**
> The DAG (Directed Acyclic Graph) is built automatically from `ref()` and `source()` calls. dbt:
> 1. Parses all models to find dependencies
> 2. Builds a directed graph
> 3. Executes in topological order (parents before children)
> 4. Parallelizes independent branches (controlled by `--threads`)
>
> **Circular dependency**: dbt will throw a compilation error — `Cycle detected in model dependencies`. This is impossible to execute (A needs B, B needs A).
>
> **Fix**: Refactor into a shared parent model, or use `ephemeral` for the common logic.

---

### Dataform Questions

**Q11: What is Dataform and how does it differ from dbt?**
> Dataform is Google's native SQL transformation tool for BigQuery. Key differences:
> - **Language**: SQLX (SQL + JavaScript) vs dbt's SQL + Jinja
> - **Scope**: BigQuery only vs dbt's multi-warehouse support
> - **Cost**: Free (included with BigQuery) vs dbt Cloud (paid)
> - **Infrastructure**: Zero — managed in GCP Console
> - **IAM**: Native GCP IAM vs dbt's service account key management
>
> **Same core pattern**: Write SELECT, declare deps with `ref()`, add tests, build DAG.

---

**Q12: When would you choose Dataform over dbt on GCP?**
> **Choose Dataform when:**
> - Pure BigQuery environment (no other warehouses)
> - Want zero additional infrastructure or billing
> - Need tight GCP IAM integration (Workload Identity, org policies)
> - Small-to-medium team that doesn't need the dbt ecosystem
> - Want scheduling without Cloud Composer
>
> **Choose dbt when:**
> - Multi-warehouse (also have Snowflake, Redshift)
> - Team already knows dbt, has existing models/macros
> - Need advanced features: snapshots (SCD2), 1000+ community packages
> - Complex macro logic (Jinja is more powerful than JS for SQL templating)
> - Mature CI/CD with dbt Cloud or established Airflow patterns

---

**Q13: How do you implement data quality assertions in Dataform?**
> Dataform uses `assertions` in the config block:
> ```sql
> config {
>     type: "table",
>     assertions: {
>         uniqueKey: ["transaction_id"],
>         nonNull: ["transaction_id", "amount", "customer_id"],
>         rowConditions: [
>             "amount > 0",
>             "amount < 10000000",
>             "transaction_date <= CURRENT_DATE()"
>         ]
>     }
> }
> ```
>
> **Key difference from dbt**: In Dataform, assertions are part of the model config (co-located). In dbt, tests are separate YAML files or SQL files.
>
> **Custom assertions**: Create a separate `.sqlx` file with `type: "assertion"`:
> ```sql
> config { type: "assertion" }
> SELECT * FROM ${ref("fct_transactions")} t
> LEFT JOIN ${ref("dim_customers")} c ON t.customer_id = c.customer_id
> WHERE c.customer_id IS NULL
> ```

---

### Tricky / Advanced Questions

**Q14: You have 500 dbt models. A run takes 45 minutes. How do you optimize?**
> 1. **Identify bottlenecks**: `dbt run --select +slow_model` to see its upstream chain
> 2. **Convert heavy tables to incremental**: Biggest cost savings
> 3. **Increase threads**: `dbt run --threads 16` (parallelize independent models)
> 4. **Use ephemeral for lightweight intermediates**: Skip table creation
> 5. **Slim CI**: On PRs, only run modified models + downstream: `dbt run --select state:modified+`
> 6. **Split into multiple dbt projects**: Separate domains (loans, cards, payments)
> 7. **Materialize strategically**: Staging as views (instant), marts as tables
>
> **Production pattern**: Tag models by schedule — `tag:hourly`, `tag:daily`, `tag:weekly`. Run only what's needed.

---

**Q15: How do you handle breaking changes in dbt? (e.g., renaming a column that downstream models depend on)**
> 1. **Never rename directly** — add new column, keep old as alias
> 2. **Deprecation pattern**:
>    - Add new column in the model
>    - Update downstream models to use new column (over multiple PRs)
>    - After all consumers migrated, remove old column
> 3. **Use CI to catch breakages**: `dbt build --select state:modified+` runs all downstream models — they'll fail if a column disappears
> 4. **Contract enforcement** (dbt 1.5+): Define model contracts that prevent accidental breaking changes
>
> ```yaml
> models:
>   - name: fct_transactions
>     config:
>       contract:
>         enforced: true
>     columns:
>       - name: transaction_id
>         data_type: string
>       - name: amount
>         data_type: numeric
> ```

---

**Q16: What is `dbt build` vs `dbt run` + `dbt test`?**
> - `dbt run` → materializes all models (no tests)
> - `dbt test` → runs all tests (after models exist)
> - `dbt build` → **interleaves** run + test: runs model A → tests model A → runs model B → tests model B
>
> **Why `build` is better**: If model A has bad data, `dbt build` catches it immediately before model B (which depends on A) runs. With `run` + `test`, model B already ran on bad data before tests flag the issue.
>
> **Production best practice**: Always use `dbt build` in production pipelines.

---

**Q17: How do you implement a full CI/CD pipeline for dbt?**
> ```
> Developer pushes PR
>     │
>     ▼
> CI Pipeline (GitHub Actions / Cloud Build):
>     1. dbt deps (install packages)
>     2. dbt compile (check for syntax errors)
>     3. dbt build --target ci --select state:modified+ (run only changed + downstream)
>     4. dbt source freshness (check sources are up to date)
>     5. If all pass → PR is green
>     │
>     ▼
> Merge to main
>     │
>     ▼
> CD Pipeline:
>     1. dbt build --target prod --full-refresh (if schema change) or regular run
>     2. dbt docs generate → deploy to hosting
>     3. Notify team via Slack
> ```
>
> **Key pattern**: `state:modified+` uses dbt's manifest comparison to only run models that changed since last production run — avoids rebuilding 500 models for a 1-model change.

---

**Q18: What are dbt packages? Name 3 you'd use in production.**
> Packages are installable libraries of macros, tests, and models from the community.
>
> | Package | What it provides | Example use |
> |---------|-----------------|-------------|
> | **dbt-utils** | Generic tests, SQL helpers, cross-DB macros | `surrogate_key`, `pivot`, `star`, `date_spine` |
> | **dbt-expectations** | Great Expectations-style tests in dbt | `expect_column_values_to_be_between`, `expect_table_row_count_to_be_between` |
> | **dbt-audit-helper** | Compare two relations/queries for differences | Validate migration: old model vs new model have same output |
>
> **Install**: Add to `packages.yml`, run `dbt deps`:
> ```yaml
> packages:
>   - package: dbt-labs/dbt_utils
>     version: ">=1.0.0"
>   - package: calogica/dbt_expectations
>     version: ">=0.8.0"
> ```
