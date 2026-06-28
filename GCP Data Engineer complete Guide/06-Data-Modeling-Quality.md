# Data Modeling, Schema Evolution, and Data Quality

---

## Table of Contents

1. [What is Data Modeling? (In-Depth Definition)](#1-what-is-data-modeling-in-depth-definition)
2. [Data Modeling on BigQuery](#2-data-modeling-on-bigquery)
3. [Schema Evolution](#3-schema-evolution)
4. [Data Quality](#4-data-quality)
5. [Interview Questions — Data Modeling & Quality](#5-interview-questions--data-modeling--quality)

---

## 1. What is Data Modeling? (In-Depth Definition)

**Data Modeling** is the process of creating a visual/logical representation of how data is structured, stored, related, and accessed within a system. It defines the entities (tables), their attributes (columns), relationships (keys/joins), and constraints (rules) that govern the data.

### Uses of Data Modeling

| Use Case | Description |
|----------|-------------|
| **Query Performance** | Proper modeling reduces scan size, join cost, and latency |
| **Data Integrity** | Enforces relationships and business rules at the structural level |
| **Communication** | Acts as a shared blueprint between engineers, analysts, and stakeholders |
| **Scalability** | A good model handles growing data volumes without redesign |
| **Cost Optimization** | In BigQuery, model choices directly impact bytes scanned = dollars spent |

### Three Levels of Data Modeling

#### Conceptual Model (Business-level)
- **What it captures**: High-level entities and relationships (e.g., "Customer places Order")
- **Audience**: Business stakeholders, architects
- **No technical detail** — just the "what"

#### Logical Model (Design-level)
- **What it captures**: Entities, attributes, primary/foreign keys, data types, normalization
- **Audience**: Data architects, engineers
- **Database-agnostic** — defines structure independent of platform

#### Physical Model (Implementation-level)
- **What it captures**: Actual table DDL, partitioning, clustering, indexes, storage format
- **Audience**: Engineers implementing on a specific platform (BigQuery, Snowflake, etc.)
- **Platform-specific** — tuned for the engine

### Types of Data Models

#### A. Normalized Models (3NF — Third Normal Form)

**Definition**: Data is split into many tables to eliminate redundancy. Each fact is stored once.

```
Customer → Order → Order_Item → Product
```

- **Pros**: No data duplication, easy updates, strong integrity
- **Cons**: Many JOINs at query time = slow for analytics
- **Best for**: OLTP systems (transactional databases like PostgreSQL, MySQL)

#### B. Star Schema

**Definition**: One central **Fact table** (measurable events) surrounded by **Dimension tables** (descriptive context).

```
           dim_date
              |
dim_customer — fact_transactions — dim_merchant
              |
           dim_product
```

- **Fact table**: Contains numeric measures (amount, quantity) + foreign keys
- **Dimension tables**: Contain descriptive attributes (customer name, region, category)
- **Pros**: Simple to understand, fast aggregations, fewer joins than 3NF
- **Cons**: Some redundancy in dimensions
- **Best for**: Data warehouses, BI/reporting

#### C. Snowflake Schema

**Definition**: A star schema where dimensions are further normalized into sub-dimensions.

```
fact_sales → dim_product → dim_category → dim_department
```

- **Pros**: Less storage, reduced redundancy in dimensions
- **Cons**: More joins, more complex queries
- **Best for**: When dimension tables are very large and rarely queried in full

#### D. Denormalized / Wide Table (BigQuery-Optimized)

**Definition**: Everything flattened into one massive table with all fields pre-joined.

- **Pros**: Zero joins at query time, fastest reads, simplest SQL
- **Cons**: Data duplication, harder updates, larger storage
- **Best for**: BigQuery, Snowflake — columnar engines that scan only needed columns

#### E. Data Vault

**Definition**: A highly flexible model with three entity types:
- **Hubs**: Business keys (e.g., customer_id)
- **Links**: Relationships between hubs
- **Satellites**: Descriptive attributes with full history

- **Pros**: Full auditability, handles source changes gracefully, parallel loading
- **Cons**: Complex to query, requires transformation layer for consumption
- **Best for**: Enterprise data lakes where sources change frequently

#### F. Nested/Repeated (BigQuery-Native)

**Definition**: Uses `STRUCT` and `ARRAY` types to store hierarchical/related data inside a single row — avoiding joins entirely.

```sql
-- One row holds the order + all line items
-- order_id | customer STRUCT<id, name> | items ARRAY<STRUCT<product, qty, price>>
```

- **Pros**: Eliminates joins for parent-child data, preserves natural hierarchy
- **Cons**: Harder to update individual nested elements
- **Best for**: Event data, logs, JSON-origin data in BigQuery

### Choosing the Right Model

| Scenario | Recommended Model |
|----------|-------------------|
| Transactional app (OLTP) | 3NF Normalized |
| Enterprise DWH with BI tools | Star Schema |
| BigQuery analytics with known queries | Denormalized Wide Table |
| Audit-heavy, multi-source enterprise | Data Vault |
| Hierarchical/JSON data in BigQuery | Nested/Repeated (STRUCT + ARRAY) |
| Rapidly changing sources | Data Vault or Schema-on-Read |

### Key Takeaway for Interviews

> "In BigQuery, I favor **denormalized wide tables** partitioned by date and clustered by high-cardinality filter columns. For cases needing dimensional flexibility (ad-hoc slicing across many attributes), I use a **star schema**. For parent-child relationships, I use **nested/repeated fields** to avoid expensive JOINs — since BigQuery charges by bytes scanned, fewer joins = lower cost + better performance."

---

## 2. Data Modeling on BigQuery

### Star Schema (Most Common in Analytics)
```sql
-- Fact Table: Transactions
CREATE TABLE `project.analytics.fact_transactions` (
    transaction_id STRING,
    customer_key INT64,        -- FK to dim_customers
    merchant_key INT64,        -- FK to dim_merchants
    date_key INT64,            -- FK to dim_date (YYYYMMDD)
    amount NUMERIC,
    fee NUMERIC,
    currency_code STRING,
    transaction_type STRING,
    is_fraud BOOL
)
PARTITION BY RANGE_BUCKET(date_key, GENERATE_ARRAY(20200101, 20301231, 1));

-- Dimension: Customers (SCD Type 2)
CREATE TABLE `project.analytics.dim_customers` (
    customer_key INT64,         -- Surrogate key
    customer_id STRING,         -- Natural key
    name STRING,
    segment STRING,
    region STRING,
    effective_from DATE,
    effective_to DATE,
    is_current BOOL
);

-- Dimension: Date
CREATE TABLE `project.analytics.dim_date` (
    date_key INT64,             -- YYYYMMDD
    full_date DATE,
    year INT64,
    quarter INT64,
    month INT64,
    week INT64,
    day_of_week STRING,
    is_weekend BOOL,
    is_holiday BOOL,
    fiscal_year INT64,
    fiscal_quarter INT64
);
```

### Denormalized / Wide Tables (BigQuery-Optimized)
```sql
-- BigQuery favors wide, denormalized tables over joins
CREATE TABLE `project.analytics.transactions_wide` (
    transaction_id STRING,
    transaction_date DATE,
    amount NUMERIC,
    -- Customer fields (denormalized)
    customer_id STRING,
    customer_name STRING,
    customer_segment STRING,
    customer_region STRING,
    -- Merchant fields (denormalized)
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    -- Derived fields
    is_high_value BOOL,
    risk_score FLOAT64
)
PARTITION BY transaction_date
CLUSTER BY customer_region, merchant_category;
```

### Nested and Repeated Fields (BigQuery-Native)
```sql
-- Use STRUCT and ARRAY for hierarchical data
CREATE TABLE `project.analytics.orders` (
    order_id STRING,
    customer STRUCT<
        id STRING,
        name STRING,
        email STRING
    >,
    items ARRAY<STRUCT<
        product_id STRING,
        product_name STRING,
        quantity INT64,
        unit_price NUMERIC
    >>,
    order_date DATE,
    total_amount NUMERIC
);

-- Query nested data
SELECT
    order_id,
    customer.name as customer_name,
    item.product_name,
    item.quantity * item.unit_price as line_total
FROM `project.analytics.orders`,
UNNEST(items) as item
WHERE order_date = '2026-06-01';
```

---

## 3. Schema Evolution

### BigQuery Schema Changes
```sql
-- Adding columns (always safe)
ALTER TABLE `project.dataset.transactions`
ADD COLUMN IF NOT EXISTS risk_category STRING;

-- Changing column type (limited)
-- Can widen: INT64 → FLOAT64, but not narrow
-- Best practice: create new column + backfill + drop old

-- Adding nested fields
ALTER TABLE `project.dataset.orders`
ADD COLUMN IF NOT EXISTS metadata STRUCT<source STRING, version STRING>;
```

### Schema Evolution Strategies

| Strategy | Pros | Cons |
|----------|------|------|
| **Additive only** | No breaking changes | Table grows wide over time |
| **Versioned tables** | Clear history | Complex pipeline logic |
| **Views for abstraction** | Consumers unaffected | View maintenance |
| **Schema registry** | Enforced contracts | Additional infrastructure |

### Handling Schema Changes in Pipelines
```python
# Python pattern: Schema-aware loading
from google.cloud import bigquery

client = bigquery.Client()

def safe_load_with_schema_update(table_id, dataframe):
    """Load data allowing new columns to be auto-detected."""
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
        autodetect=True,
    )
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()
    return job
```

---

## 4. Data Quality

### Data Quality Dimensions
| Dimension | Check | Example |
|-----------|-------|---------|
| Completeness | NULL counts, missing rows | `COUNT(*) WHERE col IS NULL` |
| Accuracy | Value ranges, format | `amount > 0 AND amount < 1000000` |
| Consistency | Cross-table matches | Source count = Target count |
| Timeliness | Data freshness | `MAX(load_time) > TIMESTAMP_SUB(NOW(), INTERVAL 2 HOUR)` |
| Uniqueness | Duplicate detection | `COUNT(*) = COUNT(DISTINCT id)` |
| Validity | Business rules | `status IN ('ACTIVE', 'CLOSED', 'PENDING')` |

### BigQuery Data Quality Checks
```sql
-- Completeness check
SELECT
    'transactions' as table_name,
    COUNT(*) as total_rows,
    COUNTIF(customer_id IS NULL) as null_customer,
    COUNTIF(amount IS NULL) as null_amount,
    COUNTIF(transaction_date IS NULL) as null_date,
    ROUND(COUNTIF(customer_id IS NULL) / COUNT(*) * 100, 2) as null_pct
FROM `project.dataset.transactions`
WHERE transaction_date = CURRENT_DATE() - 1;

-- Uniqueness check
SELECT
    COUNT(*) as total,
    COUNT(DISTINCT transaction_id) as distinct_ids,
    COUNT(*) - COUNT(DISTINCT transaction_id) as duplicates
FROM `project.dataset.transactions`
WHERE transaction_date = CURRENT_DATE() - 1;

-- Freshness check
SELECT
    MAX(load_timestamp) as last_load,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(load_timestamp), MINUTE) as minutes_since_load,
    CASE
        WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(load_timestamp), MINUTE) > 120
        THEN 'STALE'
        ELSE 'FRESH'
    END as freshness_status
FROM `project.dataset.transactions`;

-- Reconciliation check (source vs target)
SELECT
    s.source_count,
    t.target_count,
    s.source_count - t.target_count as difference,
    CASE WHEN s.source_count = t.target_count THEN 'PASS' ELSE 'FAIL' END as status
FROM
    (SELECT COUNT(*) as source_count FROM `project.raw.transactions` WHERE date = '2026-06-21') s,
    (SELECT COUNT(*) as target_count FROM `project.curated.transactions` WHERE date = '2026-06-21') t;
```

### Great Expectations Pattern (Python)
```python
import great_expectations as gx

context = gx.get_context()

# Define expectations
validator = context.sources.pandas_default.read_dataframe(df)
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_between("amount", min_value=0, max_value=1000000)
validator.expect_column_values_to_be_in_set("status", ["ACTIVE", "CLOSED", "PENDING"])
validator.expect_column_values_to_be_unique("transaction_id")
validator.expect_table_row_count_to_be_between(min_value=1000, max_value=10000000)

results = validator.validate()
if not results.success:
    raise ValueError(f"Data quality check failed: {results}")
```

### Airflow Data Quality Task
```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

quality_check = BigQueryCheckOperator(
    task_id='data_quality_check',
    sql="""
        SELECT
            CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END as has_data,
            CASE WHEN COUNTIF(amount < 0) = 0 THEN 1 ELSE 0 END as no_negatives,
            CASE WHEN COUNT(*) = COUNT(DISTINCT txn_id) THEN 1 ELSE 0 END as no_dupes
        FROM `project.curated.transactions`
        WHERE date = '{{ ds }}'
    """,
    use_legacy_sql=False,
)
```

---

## 5. Interview Questions — Data Modeling & Quality

---

### Data Modeling Questions

**Q1: What is data modeling and why is it important?**
> Data modeling is the process of defining how data is structured, stored, and related within a system. It's important because:
> - **Performance**: A poorly modeled table can cause full table scans costing thousands of dollars in BigQuery
> - **Maintainability**: Clear relationships make pipelines easier to debug and extend
> - **Business alignment**: Models translate business requirements into technical structures
> - **Cost control**: In cloud DWHs, model design directly impacts storage and compute costs
>
> *Example*: At my previous role, restructuring a normalized model into a denormalized wide table reduced query costs by 60% and improved dashboard load times from 12s to 2s.

---

**Q2: When would you denormalize vs use star schema in BigQuery?**
> **Denormalize (wide table) when:**
> - Queries are repetitive and well-known (dashboards, reports)
> - Dimension data rarely changes
> - You want minimal JOIN overhead and fastest reads
> - Table is < 10TB and duplication is acceptable
>
> **Use star schema when:**
> - Ad-hoc queries span many different dimension combinations
> - Dimensions change frequently (SCD Type 2 — need history tracking)
> - Multiple fact tables share the same dimensions
> - Storage cost is a concern (dimensions stored once, referenced by key)
>
> **Real-world decision**: "For our daily transaction dashboard, I denormalized customer + merchant into the fact table because queries always needed those fields. But for our fraud investigation team doing ad-hoc analysis across 15+ dimensions, I kept a star schema for flexibility."

---

**Q3: Explain the difference between Star Schema and Snowflake Schema. When would you pick one over the other?**
> | Aspect | Star Schema | Snowflake Schema |
> |--------|-------------|-----------------|
> | Structure | Fact + flat dimensions | Fact + normalized dimensions |
> | Joins | 1 level (fact → dim) | Multiple levels (fact → dim → sub-dim) |
> | Query speed | Faster (fewer joins) | Slower (more joins) |
> | Storage | More (dimension redundancy) | Less (no redundancy) |
> | Complexity | Simple | Complex |
>
> **Pick Star**: When query performance matters more than storage (most BigQuery/DWH use cases)
>
> **Pick Snowflake**: When dimensions are extremely large (e.g., a product dimension with 50M rows having deeply hierarchical categories) and storage cost is significant

---

**Q4: What are Slowly Changing Dimensions (SCD)? Explain Type 1, 2, and 3.**
> SCDs handle how dimension attributes change over time.
>
> **Type 1 — Overwrite**: Simply update the value. No history kept.
> ```sql
> -- Customer moves from "NY" to "CA" → just UPDATE
> UPDATE dim_customers SET region = 'CA' WHERE customer_id = 'C001';
> ```
> - Use when: History doesn't matter (e.g., fixing a typo)
>
> **Type 2 — Add New Row**: Insert new row with versioning columns. Full history preserved.
> ```sql
> -- Close old record
> UPDATE dim_customers SET effective_to = '2026-06-27', is_current = FALSE
> WHERE customer_id = 'C001' AND is_current = TRUE;
> -- Insert new record
> INSERT INTO dim_customers VALUES (new_key, 'C001', 'John', 'Premium', 'CA', '2026-06-28', '9999-12-31', TRUE);
> ```
> - Use when: You need to analyze historical state (e.g., "what segment was this customer in when they made that purchase?")
>
> **Type 3 — Add New Column**: Store previous + current value in same row.
> ```sql
> ALTER TABLE dim_customers ADD COLUMN previous_region STRING;
> UPDATE dim_customers SET previous_region = region, region = 'CA' WHERE customer_id = 'C001';
> ```
> - Use when: You only need one level of history (rare in practice)
>
> **In interviews, always mention Type 2 as the most common in data warehousing.**

---

**Q5: How do you design a data model for a new project? Walk me through your process.**
> 1. **Understand business requirements**: What questions will the data answer? Who are the consumers (BI tools, ML models, APIs)?
> 2. **Identify entities and relationships**: Map out the key business objects (customers, transactions, products) and how they relate
> 3. **Choose granularity**: What is the lowest level of detail needed? (e.g., per-transaction vs daily aggregates)
> 4. **Select model type**: Based on query patterns, choose star schema, wide table, or nested
> 5. **Define partitioning & clustering**: Based on most common WHERE/GROUP BY clauses
> 6. **Plan for change**: Build in schema evolution strategy (additive columns, view abstraction)
> 7. **Validate with stakeholders**: Ensure the model supports their actual query patterns
>
> *Example*: "For a fraud detection pipeline, I chose a wide table partitioned by `transaction_date` and clustered by `merchant_category, customer_region` because 90% of queries filtered on date range + merchant type."

---

**Q6: What are nested and repeated fields in BigQuery? When would you use them?**
> **STRUCT (nested)**: Groups related fields into a single column — like an embedded object.
> **ARRAY (repeated)**: Stores multiple values/structs in one row — like an embedded list.
>
> **When to use:**
> - Parent-child relationships (order → line items)
> - Avoiding expensive JOINs on 1-to-many relationships
> - Data that arrives as JSON/events with natural hierarchy
> - When you always query parent + children together
>
> **When NOT to use:**
> - When you need to UPDATE individual nested elements (BigQuery makes this hard)
> - When child data is queried independently of the parent
> - When arrays can grow unboundedly (performance degrades)
>
> **Performance benefit**: Querying nested data avoids shuffle/join operations entirely — data is co-located in the same row, so BigQuery reads it in a single pass.

---

**Q7: What is Data Vault modeling? When would you choose it over Star Schema?**
> Data Vault uses three components:
> - **Hubs**: Contain business keys + load metadata (e.g., `hub_customer` with `customer_id`)
> - **Links**: Represent relationships between hubs (e.g., `link_customer_order`)
> - **Satellites**: Store descriptive attributes with full history + timestamps
>
> **Choose Data Vault when:**
> - Multiple source systems feed into one warehouse
> - Sources change schemas frequently
> - Full audit trail is required (regulatory/compliance)
> - Teams need to load data in parallel without dependencies
>
> **Choose Star Schema when:**
> - Direct BI consumption is the priority
> - Sources are stable and well-understood
> - Simpler is better (smaller team, fewer sources)
>
> **In practice**: Many enterprises use Data Vault as the **raw/staging layer** and then transform into star schema for the **consumption/presentation layer**.

---

### Schema Evolution Questions

**Q8: How do you handle schema changes in production without downtime?**
> **Strategy (4-step safe evolution):**
> 1. **Always make additive changes** — add columns, never remove or rename directly
> 2. **Use ALLOW_FIELD_ADDITION** in BigQuery load configs so new fields are auto-added
> 3. **Abstract with views** — consumers query a view; underlying table can evolve freely
> 4. **For breaking changes**: Create v2 table → dual-write → migrate consumers → deprecate v1
>
> **Example pipeline config:**
> ```python
> schema_update_options=[
>     bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
>     bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,  # REQUIRED → NULLABLE
> ]
> ```
>
> **What you CANNOT do in BigQuery:**
> - Rename a column (must create new + backfill + drop old)
> - Narrow a type (FLOAT64 → INT64)
> - Remove a column from a table with data (must recreate)

---

**Q9: How would you implement a schema registry in a streaming pipeline?**
> A schema registry (e.g., Confluent Schema Registry or GCP Schema Registry for Pub/Sub) enforces contracts between producers and consumers:
>
> 1. **Producer registers schema** before publishing (Avro/Protobuf/JSON Schema)
> 2. **Registry validates compatibility** — rejects breaking changes based on rules (BACKWARD, FORWARD, FULL)
> 3. **Consumer fetches schema** to deserialize messages correctly
>
> **Compatibility modes:**
> - **BACKWARD**: New schema can read old data (safe to add optional fields, remove fields)
> - **FORWARD**: Old schema can read new data (safe to remove optional fields, add fields)
> - **FULL**: Both backward and forward compatible
>
> **In GCP**: Pub/Sub supports schema validation natively — messages that don't match the registered schema are rejected at publish time.

---

### Data Quality Questions

**Q10: How do you implement data quality in a pipeline?**
> Implement checks at **three stages** (defense in depth):
>
> | Stage | Checks | Tools |
> |-------|--------|-------|
> | **Ingestion** | Schema validation, row count, format, null % | Pub/Sub schema validation, Dataflow assertions |
> | **Transformation** | Business rules, referential integrity, reconciliation | dbt tests, Great Expectations, BigQueryCheckOperator |
> | **Serving** | Freshness, anomaly detection, SLA monitoring | Cloud Monitoring, custom alerting, Dataplex |
>
> **Key principle**: Fail fast. Catch issues at ingestion before they propagate downstream.
>
> **Example Airflow pattern:**
> ```python
> ingest >> quality_gate >> transform >> reconciliation_check >> publish
> # If quality_gate fails → alert + stop pipeline (no bad data downstream)
> ```

---

**Q11: What are the 6 dimensions of data quality? Give a real-world example for each.**
> | Dimension | Definition | Real-World Example |
> |-----------|-----------|-------------------|
> | **Completeness** | No missing data | 5% of transactions missing `merchant_id` → broken joins in reporting |
> | **Accuracy** | Values are correct | Amount shows $1,000,000 for a coffee purchase → data entry error |
> | **Consistency** | Same data across systems | Source has 1M rows, target has 999,500 → 500 rows lost in transit |
> | **Timeliness** | Data arrives on time | Dashboard shows yesterday's data at 2 PM instead of 6 AM SLA |
> | **Uniqueness** | No duplicates | Same transaction loaded twice → inflated revenue metrics |
> | **Validity** | Values follow business rules | Status = "ACTVE" (typo) instead of "ACTIVE" → broken downstream filters |

---

**Q12: How would you handle duplicate records in a pipeline?**
> **Prevention (best):**
> - Use idempotent writes (MERGE/upsert instead of INSERT)
> - Deduplicate at ingestion using unique keys
>
> **Detection:**
> ```sql
> SELECT transaction_id, COUNT(*) as cnt
> FROM `project.dataset.transactions`
> GROUP BY transaction_id
> HAVING cnt > 1;
> ```
>
> **Resolution strategies:**
> 1. **Keep latest**: Use `ROW_NUMBER()` partitioned by key, ordered by timestamp DESC
> ```sql
> SELECT * FROM (
>     SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY load_time DESC) as rn
>     FROM `project.dataset.transactions`
> ) WHERE rn = 1;
> ```
> 2. **Merge/Upsert**: Use BigQuery MERGE statement
> ```sql
> MERGE target T USING source S ON T.txn_id = S.txn_id
> WHEN MATCHED THEN UPDATE SET T.amount = S.amount, T.updated_at = S.updated_at
> WHEN NOT MATCHED THEN INSERT VALUES (S.txn_id, S.amount, S.updated_at);
> ```
> 3. **Dedup table**: Maintain a separate deduplication table with seen IDs

---

**Q13: What is data reconciliation and how do you implement it?**
> **Data reconciliation** = verifying that data in the target matches the source after transformation/loading.
>
> **Three levels of reconciliation:**
> 1. **Row count**: Source rows = Target rows (± expected filter/aggregation)
> 2. **Value checksum**: SUM(amount) in source = SUM(amount) in target
> 3. **Sample validation**: Random sample of records compared field-by-field
>
> **Implementation pattern:**
> ```sql
> -- Automated daily reconciliation
> SELECT
>     'row_count' as check_type,
>     s.cnt as source_count,
>     t.cnt as target_count,
>     ABS(s.cnt - t.cnt) as difference,
>     CASE WHEN ABS(s.cnt - t.cnt) <= s.cnt * 0.001 THEN 'PASS' ELSE 'FAIL' END as status
> FROM
>     (SELECT COUNT(*) cnt FROM `raw.transactions` WHERE date = @run_date) s,
>     (SELECT COUNT(*) cnt FROM `curated.transactions` WHERE date = @run_date) t
> UNION ALL
> SELECT
>     'amount_sum' as check_type,
>     CAST(s.total as INT64),
>     CAST(t.total as INT64),
>     ABS(s.total - t.total),
>     CASE WHEN ABS(s.total - t.total) < 0.01 THEN 'PASS' ELSE 'FAIL' END
> FROM
>     (SELECT SUM(amount) total FROM `raw.transactions` WHERE date = @run_date) s,
>     (SELECT SUM(amount) total FROM `curated.transactions` WHERE date = @run_date) t;
> ```
>
> **Alert on failure**: Pipe results to Cloud Monitoring → PagerDuty/Slack alert.

---

**Q14: What is Great Expectations and how does it fit into a data pipeline?**
> Great Expectations (GX) is a Python framework for defining, running, and documenting data quality tests ("expectations").
>
> **How it fits:**
> ```
> Ingest → GX Validation → Transform → GX Validation → Load
>          (raw checks)                   (business rules)
> ```
>
> **Key concepts:**
> - **Expectation**: A single testable assertion (e.g., column is not null)
> - **Expectation Suite**: A collection of expectations for a dataset
> - **Checkpoint**: Runs a suite against data and produces results
> - **Data Docs**: Auto-generated HTML reports showing pass/fail history
>
> **Integration with Airflow:**
> ```python
> from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
>
> gx_validate = GreatExpectationsOperator(
>     task_id="validate_transactions",
>     data_context_root_dir="/opt/airflow/gx",
>     checkpoint_name="transactions_checkpoint",
> )
> ```
>
> **vs. dbt tests**: GX is Python-native and works on DataFrames/files; dbt tests are SQL-native and work on warehouse tables. Use both when appropriate.

---

**Q15: How do you monitor data quality in production and set up alerting?**
> **Monitoring stack:**
> 1. **Automated checks**: Run quality SQL after every pipeline run (Airflow sensors/checks)
> 2. **Metrics logging**: Write check results to a `data_quality_metrics` table
> 3. **Dashboards**: Grafana/Looker dashboard showing quality trends over time
> 4. **Alerting**: Cloud Monitoring custom metrics → alert policies → PagerDuty/Slack
>
> **What to alert on:**
> - Null percentage exceeds threshold (e.g., > 5%)
> - Row count drops by > 20% vs previous day
> - Data freshness exceeds SLA (e.g., no new data in 2 hours)
> - Duplicate rate spikes
> - Schema drift detected (unexpected new columns)
>
> **Example metric table:**
> ```sql
> CREATE TABLE `project.monitoring.dq_metrics` (
>     check_date DATE,
>     table_name STRING,
>     check_name STRING,
>     check_value FLOAT64,
>     threshold FLOAT64,
>     status STRING,  -- PASS / FAIL / WARN
>     run_id STRING
> )
> PARTITION BY check_date;
> ```
>
> **Anomaly detection**: Use statistical methods (Z-score, moving average) to detect unusual patterns without hardcoded thresholds.
