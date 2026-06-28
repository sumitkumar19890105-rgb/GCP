# BigQuery Optimization and Performance Tuning

---

## Table of Contents

1. [What is BigQuery? (In-Depth Definition)](#1-what-is-bigquery-in-depth-definition)
2. [Table Design](#2-table-design)
3. [Query Optimization](#3-query-optimization)
4. [Cost Optimization](#4-cost-optimization)
5. [Advanced Features](#5-advanced-features)
6. [Interview Questions — BigQuery](#6-interview-questions--bigquery)

---

## 1. What is BigQuery? (In-Depth Definition)

**BigQuery** is Google Cloud's fully managed, serverless, petabyte-scale data warehouse designed for fast SQL analytics over large datasets. It separates storage and compute, allowing independent scaling of each.

### Core Architecture

```
┌─────────────────────────────────────────────────────┐
│                   BigQuery Service                    │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────────┐         ┌──────────────────────┐  │
│  │   Dremel     │         │    Colossus          │  │
│  │  (Compute)   │         │   (Storage)          │  │
│  │              │         │                      │  │
│  │  Execution   │◄───────►│  Columnar format     │  │
│  │  engine      │  Borg   │  (Capacitor)         │  │
│  │  (slots)     │  Network│  Distributed FS      │  │
│  └──────────────┘  (Jupiter)└────────────────────┘  │
│                                                      │
└─────────────────────────────────────────────────────┘
```

| Component | Role |
|-----------|------|
| **Dremel** | Distributed execution engine that processes SQL queries using slots (units of compute) |
| **Colossus** | Google's distributed file system storing data in columnar format (Capacitor) |
| **Jupiter** | Petabit-scale network connecting compute and storage |
| **Borg** | Cluster management system that allocates compute resources |

### Key Characteristics

| Feature | Description |
|---------|-------------|
| **Serverless** | No infrastructure to manage — no VMs, no clusters, no tuning |
| **Columnar Storage** | Only reads columns referenced in query → massive I/O savings |
| **Separation of Storage & Compute** | Scale each independently; pay for what you use |
| **Automatic Scaling** | Thousands of slots can be allocated transparently for a single query |
| **Built-in Caching** | Repeated queries on unchanged data return cached results (free) |
| **ANSI SQL** | Standard SQL with extensions (STRUCT, ARRAY, ML, GIS) |
| **Streaming + Batch** | Supports both real-time streaming inserts and batch loading |

### How BigQuery Executes a Query

1. **Parse & Plan**: Query is parsed into an execution tree
2. **Root Server**: Coordinates the distributed execution
3. **Leaf Nodes (Slots)**: Each slot reads a portion of data from Colossus (columnar → only needed columns)
4. **Shuffle (Mixer)**: Intermediate results are redistributed for JOINs/aggregations
5. **Aggregation**: Results flow up the tree, get merged, and return to client

### BigQuery vs Traditional Data Warehouses

| Aspect | BigQuery | Traditional DWH (Teradata, Oracle) | Snowflake |
|--------|----------|-----------------------------------|-----------|
| Infrastructure | Serverless | On-prem / VM-based | Cloud, semi-serverless |
| Scaling | Automatic | Manual (add nodes) | Manual (resize warehouse) |
| Pricing | Per-TB scanned or flat-rate slots | License + hardware | Per-second compute + storage |
| Storage format | Columnar (Capacitor) | Row or columnar | Columnar (micro-partitions) |
| Concurrency | High (auto-scales) | Limited by resources | Limited by warehouse size |
| Maintenance | Zero | High (indexes, vacuuming) | Low |

### When to Use BigQuery

| Use Case | Why BigQuery Fits |
|----------|-------------------|
| **Ad-hoc analytics** | Serverless, no cluster to spin up, instant queries |
| **Data warehousing** | Petabyte-scale, supports star schema / denormalized models |
| **Real-time dashboards** | BI Engine (in-memory), streaming inserts |
| **ML on warehouse data** | BigQuery ML — train models with SQL |
| **Log/event analytics** | Handles semi-structured data (JSON, nested/repeated) natively |
| **Data lake analytics** | Query external tables (GCS, Bigtable, Drive) without loading |

### When NOT to Use BigQuery

| Scenario | Better Alternative |
|----------|-------------------|
| Low-latency transactional (OLTP) | Cloud SQL, Spanner, AlloyDB |
| Key-value lookups (< 10ms) | Bigtable, Memorystore |
| Small datasets (< 1GB) | Cloud SQL, Sheets, local DB |
| Complex graph queries | Neo4j, JanusGraph |
| Sub-second streaming analytics | Apache Flink, Dataflow streaming |

### Pricing Model

| Component | On-Demand | Flat-Rate (Editions) |
|-----------|-----------|---------------------|
| **Queries** | $6.25/TB scanned (first 1TB/month free) | Fixed $/slot-hour (Standard, Enterprise, Enterprise Plus) |
| **Storage (Active)** | $0.02/GB/month | Same |
| **Storage (Long-term)** | $0.01/GB/month (auto after 90 days) | Same |
| **Streaming inserts** | $0.012/200MB | Included in slots |

**Key cost insight**: BigQuery charges by **bytes scanned**, so:
- SELECT only needed columns (not `SELECT *`)
- Partition tables → prune entire segments
- Cluster tables → skip irrelevant blocks
- Use cached results when possible (free)

### Key Takeaway for Interviews

> "BigQuery is a serverless, columnar data warehouse that separates storage (Colossus) from compute (Dremel). This means I can store petabytes cheaply and only pay for compute when I query. It's optimized for analytical workloads — wide scans over billions of rows. I optimize cost by partitioning (eliminate segments), clustering (skip blocks), selecting only needed columns, and using materialized views for repeated aggregations."

---

## 2. Table Design

### Partitioning
```sql
-- Time-based partitioning (most common)
CREATE TABLE `project.dataset.events`
(
  event_id STRING,
  user_id STRING,
  event_type STRING,
  event_timestamp TIMESTAMP,
  payload STRING
)
PARTITION BY DATE(event_timestamp)
OPTIONS(
  partition_expiration_days=365,  -- Auto-delete after 1 year
  require_partition_filter=true   -- Force partition filter in queries
);

-- Integer range partitioning
CREATE TABLE `project.dataset.customers`
(
  customer_id INT64,
  name STRING,
  segment STRING
)
PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 1000000, 10000));
```

### Clustering
```sql
-- Cluster by frequently filtered/joined columns (max 4)
CREATE TABLE `project.dataset.transactions`
(
  transaction_id STRING,
  customer_id STRING,
  merchant_category STRING,
  region STRING,
  amount NUMERIC,
  transaction_date DATE
)
PARTITION BY transaction_date
CLUSTER BY region, merchant_category, customer_id;
-- Order matters: put most-filtered column first
```

### When to Use What

| Technique | Best For | Limit |
|-----------|----------|-------|
| Partitioning | Date-range filters, data lifecycle | 4000 partitions |
| Clustering | High-cardinality filters, JOINs | 4 columns |
| Both | Large tables with date + dimension filters | Combined |

---

## 3. Query Optimization

### Anti-Patterns and Fixes

```sql
-- ❌ BAD: SELECT * (scans all columns)
SELECT * FROM `project.dataset.large_table` WHERE date = '2026-01-01';

-- ✅ GOOD: Select only needed columns
SELECT customer_id, amount, status
FROM `project.dataset.large_table`
WHERE date = '2026-01-01';

-- ❌ BAD: No partition filter
SELECT SUM(amount) FROM `project.dataset.transactions`;

-- ✅ GOOD: Always filter on partition column
SELECT SUM(amount) FROM `project.dataset.transactions`
WHERE transaction_date >= '2026-01-01';

-- ❌ BAD: Using DISTINCT on large result sets
SELECT DISTINCT customer_id FROM `project.dataset.events`;

-- ✅ GOOD: Use GROUP BY or APPROX_COUNT_DISTINCT
SELECT customer_id FROM `project.dataset.events` GROUP BY customer_id;
SELECT APPROX_COUNT_DISTINCT(customer_id) FROM `project.dataset.events`;

-- ❌ BAD: Cross join / cartesian product
SELECT * FROM table_a, table_b;

-- ✅ GOOD: Explicit JOIN with conditions
SELECT a.*, b.name
FROM table_a a
JOIN table_b b ON a.id = b.id;
```

### JOIN Optimization
```sql
-- Place smaller table on the RIGHT side of JOIN (or use hints)
SELECT /*+ BROADCAST(small_table) */
    t.*, s.category_name
FROM `project.dataset.transactions` t
JOIN `project.dataset.categories` s ON t.category_id = s.id;

-- Use approximate joins for large-scale analytics
SELECT
    t.region,
    APPROX_TOP_COUNT(t.merchant, 10) as top_merchants
FROM `project.dataset.transactions` t
GROUP BY t.region;
```

### Window Functions (Common in Interviews)
```sql
-- Running total
SELECT
    customer_id,
    transaction_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY transaction_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total
FROM `project.dataset.transactions`;

-- Rank / Deduplication
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY updated_at DESC
        ) as rn
    FROM `project.dataset.customer_changes`
)
WHERE rn = 1;  -- Latest record per customer

-- Lag/Lead for change detection
SELECT
    customer_id,
    balance,
    LAG(balance) OVER (PARTITION BY customer_id ORDER BY month) as prev_balance,
    balance - LAG(balance) OVER (PARTITION BY customer_id ORDER BY month) as change
FROM `project.dataset.monthly_balances`;
```

---

## 4. Cost Optimization

### Strategies
| Strategy | Impact | How |
|----------|--------|-----|
| Partition pruning | High | Always filter on partition column |
| Column pruning | High | SELECT only needed columns |
| Materialized views | Medium-High | Pre-compute expensive aggregations; BigQuery auto-routes matching queries to MV instead of scanning base table |
| BI Engine | Medium | In-memory acceleration for dashboard queries (sub-second response) |
| Flat-rate pricing | Variable | Slots for predictable high usage |
| Storage tiers | Low-Medium | Long-term storage auto-applied after 90 days |

### Monitoring Cost
```sql
-- Check query cost before running (dry run)
-- In bq CLI: bq query --dry_run "SELECT ..."

-- Query audit log for expensive queries
SELECT
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes / POW(1024, 4) as tb_billed,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query.query as query_text
FROM `project.dataset.cloudaudit_googleapis_com_data_access`
WHERE DATE(timestamp) = CURRENT_DATE()
ORDER BY tb_billed DESC
LIMIT 20;
```

---

## 5. Advanced Features

### Materialized Views

**Definition**: A Materialized View (MV) is a **pre-computed, physically stored** result set of a query that BigQuery automatically maintains and refreshes. Unlike a regular view (which re-runs the query every time), a materialized view stores the aggregated results on disk and serves them directly.

**Purpose**:
- **Eliminate redundant computation** — repeated expensive aggregations (SUM, COUNT, AVG) are computed once and reused
- **Reduce query cost** — BigQuery automatically routes queries to the MV when it can satisfy the request, scanning far less data
- **Improve dashboard performance** — BI tools hitting the same aggregations benefit from sub-second responses
- **Zero maintenance** — BigQuery auto-refreshes MVs incrementally (only processes new/changed data)

**How It Works Internally**:
1. You define the MV with an aggregation query
2. BigQuery computes and stores the result
3. When a user query matches the MV's pattern, BigQuery **automatically rewrites** the query to use the MV (even if the user didn't reference it)
4. Incremental refresh: only new data since last refresh is processed (not full recomputation)

**When to Use**:
| Scenario | Benefit |
|----------|---------|
| Dashboard aggregations (daily totals, regional summaries) | 10-100x faster queries |
| Repeated GROUP BY on large tables | Massive cost savings |
| Base table is append-only or rarely updated | Efficient incremental refresh |
| Multiple teams query same aggregations | Compute once, serve many |

**When NOT to Use**:
| Scenario | Why |
|----------|-----|
| Frequently updated base tables (streaming heavy) | Refresh overhead may negate benefits |
| Queries with complex JOINs across many tables | MVs support limited JOIN patterns |
| One-off ad-hoc queries | No reuse benefit |
| Base table has frequent DELETEs/UPDATEs | Full refresh required (expensive) |

**Limitations in BigQuery**:
- Only supports aggregation queries (GROUP BY with SUM, COUNT, AVG, etc.)
- Limited JOIN support (single table or with dimension tables)
- Cannot use `HAVING`, `ORDER BY`, or non-deterministic functions
- Max 20 MVs per base table

**Example — Creating and Using**:
```sql
-- Create: Pre-compute daily regional summaries
CREATE MATERIALIZED VIEW `project.dataset.daily_summary`
OPTIONS(enable_refresh=true, refresh_interval_minutes=30)
AS
SELECT
    DATE(transaction_date) as day,
    region,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM `project.dataset.transactions`
GROUP BY 1, 2;

-- Query: BigQuery auto-routes to MV (user doesn't need to know MV exists)
SELECT region, SUM(amount) as total
FROM `project.dataset.transactions`
WHERE transaction_date >= '2026-06-01'
GROUP BY region;
-- ↑ BigQuery rewrites this to read from daily_summary (costs near-zero)

-- Force manual refresh
CALL BQ.REFRESH_MATERIALIZED_VIEW('project.dataset.daily_summary');
```

**Cost Impact Example**:
| Without MV | With MV |
|-----------|---------|
| Query scans 5TB daily = ~$31/day | Query reads MV = ~$0.01/day |
| 10 users × 5TB = 50TB/day | 10 users × same cached MV |
| $187.50/day | $0.10/day + refresh cost |

**Materialized View vs Regular View vs Table**:
| Aspect | Regular View | Materialized View | Table Copy |
|--------|-------------|-------------------|------------|
| Storage | None (just SQL) | Stores pre-computed results | Full copy |
| Query cost | Full scan every time | Near-zero (reads stored result) | Full scan |
| Freshness | Always current | Slight lag (refresh interval) | Stale until rebuilt |
| Maintenance | None | Auto-refresh | Manual pipeline |
| Best for | Abstraction/security | Repeated aggregations | Snapshots |

### BigQuery ML
```sql
-- Train a model
CREATE OR REPLACE MODEL `project.dataset.churn_model`
OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['churned']) AS
SELECT
    tenure_months,
    monthly_charges,
    total_charges,
    contract_type,
    churned
FROM `project.dataset.customer_features`;

-- Predict
SELECT * FROM ML.PREDICT(MODEL `project.dataset.churn_model`,
    (SELECT * FROM `project.dataset.new_customers`));

-- Evaluate
SELECT * FROM ML.EVALUATE(MODEL `project.dataset.churn_model`);
```

### Scripting and Procedures
```sql
-- Stored procedure for SCD Type 2
CREATE OR REPLACE PROCEDURE `project.dataset.merge_customers`()
BEGIN
    -- Close existing records
    UPDATE `project.dataset.customers_scd2` t
    SET t.end_date = CURRENT_TIMESTAMP(), t.is_current = FALSE
    WHERE t.is_current = TRUE
    AND t.customer_id IN (
        SELECT customer_id FROM `project.staging.customers_delta`
    );
    
    -- Insert new records
    INSERT INTO `project.dataset.customers_scd2`
    SELECT *, CURRENT_TIMESTAMP() as start_date, NULL as end_date, TRUE as is_current
    FROM `project.staging.customers_delta`;
END;
```

---

## 6. Interview Questions — BigQuery

**Q: How would you optimize a query that scans 10TB daily?**
> Partition by date, cluster by most-filtered columns, use materialized views for repeated aggregations, ensure queries filter on partition column, and consider BI Engine for dashboard queries.

**Q: Explain the difference between partitioning and clustering.**
> Partitioning physically divides the table into segments (max 4000), eliminating entire segments from scans. Clustering sorts data within partitions by up to 4 columns, allowing BQ to skip irrelevant blocks. Use partitioning for coarse filters (date ranges) and clustering for fine-grained filters (customer_id, region).

**Q: How does BigQuery pricing work?**
> Two models: On-demand ($5/TB scanned) and flat-rate (reserved slots at fixed monthly cost). Storage is $0.02/GB/month (active) or $0.01/GB/month (long-term, 90+ days untouched). Streaming inserts cost $0.01/200MB.
