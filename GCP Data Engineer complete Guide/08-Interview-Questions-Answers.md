# Interview Questions with Strong Answers

## Category 1: Architecture & Design

### Q1: Design a real-time fraud detection system on GCP.
**Strong Answer:**
> **Architecture:**
> - Payment events → **Pub/Sub** (ingestion buffer, handles spikes)
> - Pub/Sub → **Dataflow streaming** pipeline:
>   - Enrich with customer profile (BigQuery side input, refreshed hourly)
>   - Sliding window (60s window, 10s slide): count transactions per customer
>   - Rules engine: flag if >5 txns in 1 min, amount > 3x average, new country
>   - ML scoring: call Vertex AI endpoint for probability
> - High-risk → **Pub/Sub alert topic** → Cloud Function → block + notify
> - All events → **BigQuery streaming insert** (for analytics/investigation)
> - Historical analysis → **BigQuery** with materialized views for dashboards
>
> **Key decisions:**
> - Pub/Sub over Kafka: serverless, no cluster management, handles variable load
> - Dataflow over Spark Streaming: exactly-once, auto-scaling, unified batch+stream
> - BigQuery streaming inserts: accepts ~100K rows/sec, queryable within seconds

---

### Q2: How would you migrate a 50TB Oracle data warehouse to BigQuery?
**Strong Answer:**
> **Phase 1 — Assessment (Week 1-2):**
> - Catalog all tables, views, stored procedures, dependencies
> - Identify data types that need mapping (Oracle → BQ)
> - Map Oracle PL/SQL to BigQuery SQL / scheduled queries
>
> **Phase 2 — Infrastructure (Week 2-3):**
> - Set up BQ datasets (raw, curated, serving) with appropriate IAM
> - Configure VPN/Interconnect for secure data transfer
> - Set up Cloud Composer for orchestration
>
> **Phase 3 — Initial Load (Week 3-5):**
> - Use **BigQuery Data Transfer Service** or **Datastream** for bulk load
> - For 50TB: parallel export from Oracle (by partition/range), upload to GCS, then BQ load jobs
> - Estimated time: ~24-48h with parallel workers
>
> **Phase 4 — Incremental/CDC (Week 5-7):**
> - **Datastream** for real-time CDC (Oracle → GCS → BQ)
> - MERGE statements for SCD handling
>
> **Phase 5 — Validation & Cutover (Week 7-8):**
> - Row count reconciliation for every table
> - Checksum validation on key columns
> - Run parallel with both systems for 1 sprint
> - Cutover with rollback plan

---

### Q3: Explain how you'd build a data lake on GCP.
**Strong Answer:**
> **Structure:**
> ```
> gs://company-datalake/
> ├── landing/          (raw, as-received, immutable)
> ├── raw/              (partitioned by source/date)
> ├── curated/          (cleaned, deduplicated, typed)
> ├── enriched/         (joined, business logic applied)
> └── serving/          (aggregated, ML features)
> ```
>
> **Key principles:**
> - **Immutability**: Landing zone is append-only, never modify source data
> - **Schema-on-read**: Store as Parquet/Avro, apply schema at query time
> - **Lifecycle policies**: Move to Nearline after 30 days, Coldline after 90
> - **Cataloging**: Data Catalog for discovery, DLP for PII tagging
> - **Access**: IAM at bucket/folder level, separate projects per env
>
> **BigQuery as Lakehouse:**
> - External tables over GCS (query without loading)
> - BigLake for unified governance across GCS + BQ
> - Iceberg/Delta format support for ACID on data lake

---

## Category 2: Coding & SQL

### Q4: Write a SQL query to find customers with declining monthly spend over 3 consecutive months.
```sql
WITH monthly_spend AS (
    SELECT
        customer_id,
        DATE_TRUNC(transaction_date, MONTH) as month,
        SUM(amount) as total_spend
    FROM `project.dataset.transactions`
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    GROUP BY customer_id, DATE_TRUNC(transaction_date, MONTH)
),
with_lag AS (
    SELECT
        customer_id,
        month,
        total_spend,
        LAG(total_spend, 1) OVER (PARTITION BY customer_id ORDER BY month) as prev_month,
        LAG(total_spend, 2) OVER (PARTITION BY customer_id ORDER BY month) as two_months_ago
    FROM monthly_spend
)
SELECT
    customer_id,
    month as latest_month,
    total_spend as current_spend,
    prev_month,
    two_months_ago
FROM with_lag
WHERE total_spend < prev_month
  AND prev_month < two_months_ago
ORDER BY (two_months_ago - total_spend) DESC;  -- Biggest decliners first
```

---

### Q5: Write Python to handle late-arriving data in a streaming pipeline.
```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AccumulationMode
)

with beam.Pipeline(options=options) as p:
    (
        p
        | 'ReadPubSub' >> beam.io.ReadFromPubSub(
            topic='projects/my-project/topics/events',
            timestamp_attribute='event_time'  # Use event time, not processing time
        )
        | 'ParseJSON' >> beam.Map(lambda msg: json.loads(msg))
        | 'Window' >> beam.WindowInto(
            FixedWindows(300),  # 5-minute windows
            allowed_lateness=3600,  # Accept data up to 1 hour late
            trigger=AfterWatermark(
                early=AfterProcessingTime(60),  # Emit early results every 60s
                late=AfterProcessingTime(300),  # Re-emit for late data every 5min
            ),
            accumulation_mode=AccumulationMode.ACCUMULATING  # Include all data in re-emissions
        )
        | 'Aggregate' >> beam.CombinePerKey(sum)
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            'project:dataset.windowed_metrics',
            write_disposition='WRITE_APPEND'
        )
    )
```

---

### Q6: Implement an idempotent data load in Python.
```python
from google.cloud import bigquery
from datetime import date

def idempotent_load(project, dataset, table, source_uri, load_date: date):
    """
    Load data idempotently: delete existing data for the date, then insert.
    Safe to re-run without duplicates.
    """
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    
    # Step 1: Delete existing data for this date (idempotency)
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE load_date = '{load_date.isoformat()}'
    """
    client.query(delete_query).result()
    
    # Step 2: Load new data
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
    load_job.result()  # Wait for completion
    
    # Step 3: Validate
    count_query = f"""
        SELECT COUNT(*) as cnt FROM `{table_id}`
        WHERE load_date = '{load_date.isoformat()}'
    """
    result = list(client.query(count_query).result())
    row_count = result[0].cnt
    
    if row_count == 0:
        raise RuntimeError(f"Load resulted in 0 rows for {load_date}")
    
    return row_count
```

---

## Category 3: System Design & Trade-offs

### Q7: When would you choose Dataproc over Dataflow?
**Strong Answer:**
> **Choose Dataproc when:**
> - Existing PySpark/Scala Spark codebase (migration cost too high)
> - Need Spark ML libraries (MLlib, SparkML)
> - Need interactive analysis (Spark Shell, Jupyter on cluster)
> - Workload is predictable and long-running (cost-effective with preemptible VMs)
> - Need full Hadoop ecosystem (Hive, HBase, Presto)
>
> **Choose Dataflow when:**
> - New pipeline (no existing code to port)
> - Unified batch + streaming model needed
> - Need exactly-once processing guarantees
> - Variable workload (auto-scales to zero)
> - Don't want to manage clusters
>
> **Real-world example:** At a finance company, we used Dataproc for existing Spark risk calculation models (complex ML), and Dataflow for new real-time CDC pipelines from source systems to BigQuery.

---

### Q8: How do you handle data backfills efficiently?
**Strong Answer:**
> **Strategy:**
> 1. **Partition-aware backfill**: Process one date partition at a time
> 2. **Parallel execution**: Run multiple dates concurrently (Airflow `max_active_tasks`)
> 3. **Idempotent writes**: DELETE-then-INSERT pattern (safe to re-run)
> 4. **Resource management**: Use separate BQ reservations to avoid impacting prod queries
>
> **Airflow pattern:**
> ```python
> with DAG('backfill_transactions', 
>          start_date=datetime(2025, 1, 1),
>          end_date=datetime(2026, 6, 1),
>          schedule_interval='@daily',
>          max_active_runs=5,  # 5 dates in parallel
>          catchup=True) as dag:
>     ...
> ```
>
> **BigQuery optimization**: For large backfills, use BQ load jobs (free) instead of INSERT...SELECT (costs money on scanned bytes).

---

### Q9: How do you ensure exactly-once processing in a streaming pipeline?
**Strong Answer:**
> **Challenges:** Network failures, worker restarts, Pub/Sub redelivery
>
> **Solutions by layer:**
> 1. **Pub/Sub**: Provides at-least-once delivery. Use `ack_deadline` appropriately.
> 2. **Dataflow**: Built-in exactly-once via checkpointing and bundle retries. Framework handles deduplication.
> 3. **Sink (BigQuery)**: 
>    - Use `insertId` for streaming inserts (dedup window: ~10 min)
>    - For strict dedup: write to GCS first, then load with MERGE
> 4. **Application-level**: 
>    - Include a unique event ID in every message
>    - MERGE into target with `ON event_id` (idempotent)
>    - Maintain a "processed events" table for lookup

---

## Category 4: Behavioral / Scenario

### Q10: Tell me about a time a pipeline failed in production.
**Strong Answer (STAR format):**
> **Situation:** Our daily loan reconciliation pipeline (Dataflow → BigQuery) started failing at 3 AM with timeout errors, affecting morning reports.
>
> **Task:** Identify root cause and restore within SLA (6 AM).
>
> **Action:**
> 1. Checked Dataflow job logs — saw BigQuery streaming insert quota exceeded
> 2. Root cause: upstream system sent 10x normal volume due to a batch retry
> 3. Immediate fix: switched from streaming inserts to batch load (GCS → BQ load job)
> 4. Long-term: Added Pub/Sub between source and Dataflow as a buffer, implemented rate limiting, added volume anomaly alerts
>
> **Result:** Restored within 1.5 hours. The volume spike alert has since caught 3 similar incidents before they caused failures.

---

### Q11: How do you handle a situation where the business wants real-time data but the budget is limited?
**Strong Answer:**
> **Assessment:** What does "real-time" mean to them? Usually it's "faster than current" not "sub-second."
>
> **Options by latency vs cost:**
> | Latency | Approach | Monthly Cost |
> |---------|----------|-------------|
> | Sub-second | Pub/Sub → Dataflow streaming → BQ | $$$$ |
> | 5-15 min | Micro-batching (every 5 min) via Composer | $$ |
> | 1 hour | Hourly scheduled queries in BQ | $ |
> | Same-day | BQ scheduled queries (every 4h) | ¢ |
>
> **Recommendation:** Start with micro-batching (Cloud Scheduler + Cloud Function triggers BQ load every 5-15 minutes). It handles 90% of "real-time" needs at 10% of the streaming cost. If specific events need true real-time (fraud, alerts), route only those through the streaming path.

---

## Category 5: Advanced Scenario-Based Questions (Deep Dive)

### Q12: Your BigQuery pipeline suddenly costs $15K/day instead of the usual $2K/day. Walk me through your investigation.

**Detailed Answer:**

> **Step 1: Identify the expensive queries (First 15 minutes)**
> ```sql
> -- Check INFORMATION_SCHEMA for recent expensive jobs
> SELECT
>     user_email,
>     job_id,
>     query,
>     total_bytes_processed / POW(1024, 4) as tb_processed,
>     total_bytes_processed / POW(1024, 4) * 5 as estimated_cost_usd,
>     creation_time,
>     total_slot_ms / 1000 / 60 as slot_minutes
> FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
> WHERE DATE(creation_time) = CURRENT_DATE()
>   AND statement_type = 'SELECT'
> ORDER BY total_bytes_processed DESC
> LIMIT 20;
> ```
>
> **Step 2: Common root causes I'd check:**
> 1. **Missing partition filter:** A scheduled query lost its WHERE clause on the partition column (full table scan instead of 1 day = 365x cost increase)
> 2. **Accidental cartesian join:** A JOIN condition was dropped, creating N*M rows
> 3. **Looker dashboard explosion:** Someone shared a dashboard with 50 tiles, each doing full scans, to a distribution list
> 4. **Backfill gone wrong:** A catchup=True DAG triggered 180 days of reruns simultaneously
> 5. **SELECT * in pipeline:** New column added to source made "cheap" query now scan a huge BYTES column
>
> **Step 3: Immediate containment:**
> - Identify and kill the expensive recurring job
> - Add `require_partition_filter=true` to affected tables
> - Set up BigQuery custom quotas per user/service account (bytes/day limit)
>
> **Step 4: Prevention:**
> ```sql
> -- Create a cost alert: scheduled query that checks daily spend
> -- Alert if projected daily cost > 2x rolling 7-day average
> SELECT
>     DATE(creation_time) as day,
>     SUM(total_bytes_processed) / POW(1024, 4) * 5 as daily_cost,
>     AVG(SUM(total_bytes_processed) / POW(1024, 4) * 5) OVER (
>         ORDER BY DATE(creation_time) ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
>     ) as avg_7d_cost
> FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
> GROUP BY 1
> HAVING daily_cost > 2 * avg_7d_cost;
> ```
>
> **Real-world example:** At my previous role, a developer added a Looker explore without partition filters on a 200TB table. Each dashboard load scanned 200TB = $1,000. With 15 analysts hitting it throughout the day = $15K. Fixed by adding a mandatory date filter in Looker and `require_partition_filter` on the table.

---

### Q13: Design a pipeline that processes 500M events/day with exactly-once guarantees and handles 3-hour source outages gracefully.

**Detailed Answer:**

> **Architecture:**
> ```
> Source Systems (via API/CDC)
>     │
>     ▼
> Pub/Sub (7-day retention, ordering by key)
>     │
>     ├── Subscription 1: Dataflow Streaming (real-time path)
>     │     │
>     │     ├── Deduplication (event_id + window)
>     │     ├── Enrichment (BigTable side input for low-latency lookups)
>     │     ├── Windowed aggregation (5-min tumbling windows)
>     │     └── Write to BigQuery (streaming buffer)
>     │
>     └── Subscription 2: Dataflow Batch (reconciliation path, runs daily)
>           │
>           └── Full replay of day's data → Compare with streaming output → Alert on diff
> ```
>
> **Handling exactly-once:**
> 1. **Pub/Sub → Dataflow:** Dataflow's streaming engine provides exactly-once via checkpointing. If a worker crashes, it replays from the last checkpoint. Pub/Sub acknowledges only after Dataflow confirms processing.
> 2. **Dataflow → BigQuery:** Use `insertId` for streaming inserts (10-minute dedup window). For critical data, use a MERGE-based approach:
>    ```python
>    # Write to GCS first (guaranteed), then MERGE into BQ
>    events | beam.io.WriteToText('gs://buffer/events/')
>    # Separate job MERGEs GCS → BQ on unique event_id
>    ```
> 3. **Application-level dedup:** Maintain a dedup table:
>    ```sql
>    -- Before processing, check if event was already handled
>    MERGE `project.serving.events` AS target
>    USING `project.staging.new_events` AS source
>    ON target.event_id = source.event_id
>    WHEN NOT MATCHED THEN INSERT (...)
>    -- Matched = already processed, skip
>    ```
>
> **Handling 3-hour outage:**
> - **Pub/Sub absorbs the gap:** With 7-day retention and 500M/day ≈ 6K events/sec. During 3-hour outage, ~65M events buffer in Pub/Sub (well within limits).
> - **Auto-recovery:** When source resumes, Dataflow auto-scales up (based on backlog size) to process the buffered events. Configure `maxNumWorkers=50` to handle burst.
> - **Backpressure handling:** Dataflow automatically applies backpressure to Pub/Sub pull rate if sinks (BigQuery) are slow.
> - **Alerting:** Alert if `oldest_unacked_message_age > 30 minutes` (normal processing should keep this < 1 minute).
>
> **Cost estimate (500M events/day):**
> | Component | Monthly Cost |
> |-----------|-------------|
> | Pub/Sub (ingestion) | ~$300 (10KB avg msg) |
> | Dataflow (4 workers avg) | ~$2,000 |
> | BigQuery (streaming + storage) | ~$1,500 |
> | BigTable (enrichment lookups) | ~$800 |
> | **Total** | **~$4,600/month** |

---

### Q14: You inherit a legacy pipeline with no tests, no documentation, 47 DAGs, and 3 incidents/week. How do you stabilize it?

**Detailed Answer:**

> **Week 1-2: Triage and Visibility**
> 1. **Inventory:** List all 47 DAGs with: owner (if known), schedule, downstream consumers, last failure date, average runtime.
> 2. **Classify by criticality:**
>    - **P1 (12 DAGs):** Feed customer-facing reports, regulatory, or revenue-impacting systems
>    - **P2 (20 DAGs):** Internal analytics, non-urgent
>    - **P3 (15 DAGs):** Unknown consumers, possibly dead/unused
> 3. **Add monitoring immediately** (no code changes needed):
>    - Cloud Monitoring dashboard: success/failure per DAG
>    - Alert on any P1 DAG failure (PagerDuty)
>    - Alert on P2 DAG if failed > 2 consecutive runs
>    - Weekly email: pipeline health scorecard
>
> **Week 3-4: Stabilize Top Offenders**
> 4. **Identify the "top 5 flakiest DAGs"** (highest failure rate):
>    ```sql
>    SELECT dag_id, COUNT(*) as failures, 
>           COUNT(*) / total_runs as failure_rate
>    FROM airflow_task_instances
>    WHERE state = 'failed' AND execution_date > DATE_SUB(NOW(), INTERVAL 30 DAY)
>    GROUP BY dag_id ORDER BY failures DESC LIMIT 5;
>    ```
> 5. **For each:** 
>    - Add retries (usually 2 retries, 5-min delay fixes transient issues)
>    - Add timeouts (prevent infinite hangs)
>    - Fix obvious bugs (connection strings, missing files)
>    - Add `on_failure_callback` to send meaningful alerts
>
> **Month 2: Add Tests (Incrementally)**
> 6. **For P1 DAGs only:** Add data quality checks AFTER the DAG runs:
>    - Row count > 0
>    - No duplicates on primary key
>    - Amount columns > 0
>    - Date columns within expected range
> 7. **Pattern:** Don't rewrite the DAG — add a downstream "validation" task:
>    ```python
>    existing_task >> BigQueryCheckOperator(task_id='validate', sql='...')
>    ```
>
> **Month 3: Document and Standardize**
> 8. Create a DAG template with: standard error handling, logging, alerting
> 9. Document the top 10 DAGs (architecture diagram + runbook)
> 10. Retire the P3 DAGs (disable, wait 2 weeks, if nobody complains → delete)
>
> **Result Timeline:**
> | Week | Incidents/Week | Action |
> |------|---------------|--------|
> | 0 | 3 | Starting state |
> | 2 | 3 | Monitoring added (faster detection, not fewer incidents) |
> | 4 | 1.5 | Retries + fixes for top 5 |
> | 8 | 0.5 | Quality checks catch issues before impact |
> | 12 | 0.2 | Stabilized, documented, team confidence up |

---

### Q15: Design a multi-tenant data platform where each team owns their domain but shares infrastructure.

**Detailed Answer:**

> **Architecture: Data Mesh on GCP**
>
> ```
> ┌─────────────────────────────────────────────────────────────┐
> │                    Platform Layer (Central Team)              │
> ├─────────────────────────────────────────────────────────────┤
> │  Shared: Composer, Dataflow templates, CI/CD, Monitoring    │
> │  Shared: IAM policies, VPC-SC, encryption keys              │
> │  Shared: Data Catalog, lineage, quality framework           │
> └──────────┬──────────────┬──────────────┬───────────────────┘
>            │              │              │
> ┌──────────▼────┐ ┌──────▼──────┐ ┌────▼──────────┐
> │ Domain: Loans │ │ Domain: Cards│ │ Domain: Payments│
> │               │ │              │ │                 │
> │ BQ Dataset:   │ │ BQ Dataset:  │ │ BQ Dataset:     │
> │ loans_raw     │ │ cards_raw    │ │ payments_raw    │
> │ loans_curated │ │ cards_curated│ │ payments_curated│
> │ loans_serving │ │ cards_serving│ │ payments_serving│
> │               │ │              │ │                 │
> │ Own DAGs      │ │ Own DAGs     │ │ Own DAGs        │
> │ Own dbt models│ │ Own dbt models│ │ Own dbt models  │
> │ Own SLAs      │ │ Own SLAs     │ │ Own SLAs        │
> └───────────────┘ └──────────────┘ └─────────────────┘
> ```
>
> **Multi-Tenancy Implementation:**
>
> 1. **Project-level isolation:**
>    ```
>    my-org-platform (shared infra)
>    my-org-loans-prod (domain: loans)
>    my-org-cards-prod (domain: cards)
>    my-org-payments-prod (domain: payments)
>    ```
>
> 2. **BigQuery cross-project access:**
>    ```sql
>    -- Domain publishes "data products" via authorized views
>    CREATE VIEW `loans-prod.public.loan_summary` AS
>    SELECT loan_id, amount, status, region  -- Only expose safe columns
>    FROM `loans-prod.internal.full_loans`
>    WHERE pii_removed = TRUE;
>    
>    -- Grant access to consumers
>    GRANT `roles/bigquery.dataViewer` 
>    ON DATASET `loans-prod.public`
>    TO 'group:cards-team@company.com';
>    ```
>
> 3. **Shared quality framework:**
>    ```python
>    # Each domain uses the same quality check macro
>    # Platform provides the framework, domains define their checks
>    
>    # platform/macros/quality_check.py
>    def run_quality_checks(table, checks):
>        results = []
>        for check in checks:
>            result = execute_check(table, check)
>            results.append(result)
>            publish_to_catalog(table, check, result)  # Central visibility
>        return all(r.passed for r in results)
>    ```
>
> 4. **Cost allocation:**
>    - BigQuery reservations per domain (predictable budget)
>    - Labels on all resources: `team=loans`, `env=prod`
>    - Monthly cost report per domain via billing export
>
> **Key trade-offs:**
> - **Autonomy vs Consistency:** Domains choose their tools (dbt, custom SQL) but must publish to standard catalog
> - **Isolation vs Collaboration:** Separate projects for security, authorized views for sharing
> - **Freedom vs Governance:** Teams can build freely but must pass platform quality gates before publishing data products

---

### Q16: A Dataflow streaming pipeline has 45-minute lag during peak hours (normally <1 min). The business is losing real-time visibility. Diagnose and fix.

**Detailed Answer:**

> **Diagnosis Process (Systematic):**
>
> **Step 1: Check Dataflow metrics dashboard**
> - System lag: 45 min (confirmed)
> - Backlog (unprocessed elements): 2.7M messages
> - Current workers: 10 (max configured: 10) ← Problem! At max already.
> - Worker CPU: 95% ← Saturated
> - Worker memory: 78% ← OK
>
> **Step 2: Identify the bottleneck stage**
> - In Dataflow UI, click on the pipeline graph
> - Stage "EnrichWithCustomerProfile": throughput = 500 elements/sec (should be 5000)
> - This stage does a BigQuery lookup for each event ← API call per record!
>
> **Step 3: Root cause identified**
> The enrichment step calls BigQuery for every single event (1:1 API call). During peak hours (10x normal traffic), this becomes the bottleneck because:
> - BigQuery API has rate limits
> - Each call takes 200-500ms (network latency)
> - Sequential processing within the DoFn
>
> **Fix (Immediate — 1 hour):**
> ```python
> # BEFORE (slow): API call per element
> class EnrichDoFn(beam.DoFn):
>     def process(self, element):
>         customer = bigquery_client.query(f"SELECT * FROM customers WHERE id='{element['cust_id']}'")
>         element['customer_name'] = customer.name
>         yield element
>
> # AFTER (fast): Use side input (cached lookup table)
> customer_lookup = (
>     p | 'LoadCustomers' >> beam.io.ReadFromBigQuery(
>         query='SELECT customer_id, name, segment FROM `project.dataset.customers`'
>     )
>     | 'ToDict' >> beam.combiners.ToDict()  # {customer_id: row}
> )
>
> class EnrichDoFn(beam.DoFn):
>     def process(self, element, customers=beam.DoFn.SideInputParam):
>         customer = customers.get(element['cust_id'], {})
>         element['customer_name'] = customer.get('name', 'Unknown')
>         yield element
>
> enriched = events | 'Enrich' >> beam.ParDo(EnrichDoFn(), customers=beam.pvalue.AsDict(customer_lookup))
> ```
>
> **Fix (Long-term):**
> 1. Move customer lookup to **Bigtable** (single-digit ms latency, auto-scales)
> 2. Refresh the side input every 15 minutes (not stale)
> 3. Increase `maxNumWorkers` from 10 to 30 (auto-scale handles spikes)
> 4. Add an alert: system_lag > 5 minutes → PagerDuty
>
> **Result:**
> - Lag: 45 min → 30 seconds
> - Throughput: 500 elements/sec → 15,000 elements/sec
> - Cost: +$200/month (Bigtable) but saved 2 engineering hours/week in incident response

---

### Q17: You need to implement GDPR "Right to Deletion" across a data lake with 500 tables. How?

**Detailed Answer:**

> **Challenge:** When a customer requests data deletion, you must remove their data from ALL 500 tables within 30 days. Data is in GCS (Parquet), BigQuery, and backup systems.
>
> **Architecture:**
> ```
> Deletion Request (API/Support ticket)
>     │
>     ▼
> Deletion Queue (Pub/Sub) → Deletion Service (Cloud Run)
>     │
>     ├── BigQuery tables: DELETE WHERE customer_id = X
>     ├── GCS Parquet: Rewrite partitions excluding customer
>     ├── Backups: Mark for exclusion in restore procedures
>     └── Audit Log: Record deletion completion (for compliance proof)
> ```
>
> **Implementation Details:**
>
> 1. **Customer data inventory (pre-work):**
>    ```sql
>    -- Build a registry: which tables contain customer_id
>    -- Use Data Catalog + manual mapping
>    CREATE TABLE `governance.customer_data_map` (
>        table_id STRING,       -- 'project.dataset.table'
>        customer_id_column STRING,  -- 'cust_id' or 'customer_id'
>        storage_type STRING,   -- 'bigquery' or 'gcs'
>        gcs_path STRING,       -- For GCS tables
>        priority STRING        -- 'P1' (PII-heavy) or 'P2'
>    );
>    ```
>
> 2. **BigQuery deletion (straightforward):**
>    ```python
>    def delete_from_bigquery(customer_id: str, tables: list):
>        client = bigquery.Client()
>        for table in tables:
>            query = f"""
>                DELETE FROM `{table['table_id']}`
>                WHERE {table['customer_id_column']} = '{customer_id}'
>            """
>            job = client.query(query)
>            job.result()
>            log_deletion(table['table_id'], customer_id, job.num_dml_affected_rows)
>    ```
>
> 3. **GCS Parquet deletion (complex — Parquet is immutable):**
>    ```python
>    def delete_from_gcs_parquet(customer_id: str, gcs_path: str, partition_col: str):
>        """Rewrite affected partitions excluding the customer."""
>        # Read the partition
>        df = spark.read.parquet(gcs_path)
>        
>        # Find which partitions contain this customer
>        affected_partitions = df.filter(col("customer_id") == customer_id) \
>            .select(partition_col).distinct().collect()
>        
>        # Rewrite only affected partitions (don't touch the rest)
>        for partition in affected_partitions:
>            partition_path = f"{gcs_path}/{partition_col}={partition[0]}"
>            partition_df = spark.read.parquet(partition_path)
>            cleaned_df = partition_df.filter(col("customer_id") != customer_id)
>            cleaned_df.write.mode("overwrite").parquet(partition_path)
>    ```
>
> 4. **Crypto-shredding (advanced alternative):**
>    ```
>    Instead of deleting records, encrypt customer PII with a per-customer key.
>    To "delete": destroy the encryption key → data becomes unreadable.
>    
>    Advantages: No file rewriting, instant "deletion", works at scale
>    Disadvantage: Requires encrypting at write time (design upfront)
>    ```
>
> 5. **Verification and audit:**
>    ```sql
>    -- After deletion, verify customer is gone
>    SELECT table_id, COUNT(*) as remaining_records
>    FROM (
>        SELECT 'table_1' as table_id, customer_id FROM `project.dataset.table_1`
>        UNION ALL
>        SELECT 'table_2', customer_id FROM `project.dataset.table_2`
>        -- ... all tables
>    )
>    WHERE customer_id = 'CUSTOMER_TO_DELETE'
>    GROUP BY table_id;
>    -- Should return 0 rows
>    ```
>
> **SLA:** Process within 72 hours of request (buffer for verification before 30-day deadline).

---

### Q18: Your team runs 200 Airflow DAGs. How do you implement a zero-downtime deployment strategy for DAG updates?

**Detailed Answer:**

> **Problem:** Deploying new DAG code while tasks are running can cause:
> - Import errors (breaking running tasks)
> - Version mismatch (task started with old code, retries with new code)
> - Lost state (if DAG structure changes mid-run)
>
> **Strategy: Blue-Green DAG Deployment**
>
> ```
> ┌─────────────────────────────────────────────┐
> │         Cloud Composer (Airflow)             │
> ├─────────────────────────────────────────────┤
> │  DAGs Bucket: gs://composer-bucket/dags/    │
> │                                              │
> │  /dags/                                      │
> │  ├── active/ (symlink → v2/)                │
> │  ├── v1/ (previous version)                 │
> │  └── v2/ (current version)                  │
> └─────────────────────────────────────────────┘
> ```
>
> **Implementation:**
>
> 1. **Version-stamped deploys:**
>    ```bash
>    # CI/CD pipeline
>    VERSION=$(git rev-parse --short HEAD)
>    gsutil rsync -r dags/ gs://composer-bucket/dags/v_${VERSION}/
>    
>    # Atomic switch: update the import path
>    # In Composer 2: update environment variables
>    gcloud composer environments update prod-composer \
>        --update-env-variables=DAG_VERSION=v_${VERSION}
>    ```
>
> 2. **Graceful rollout:**
>    ```python
>    # deploy.py — Zero-downtime deployment script
>    def deploy_dags(new_version):
>        # Step 1: Upload new DAGs alongside old ones
>        upload_to_gcs(f"dags/{new_version}/")
>        
>        # Step 2: Wait for scheduler to parse new DAGs (check import errors)
>        wait_for_parsing(timeout=300)
>        errors = check_import_errors()
>        if errors:
>            rollback(new_version)
>            raise DeploymentError(f"Import errors: {errors}")
>        
>        # Step 3: Pause DAGs that are about to be updated
>        affected_dags = get_changed_dags(old_version, new_version)
>        pause_dags(affected_dags)
>        
>        # Step 4: Wait for in-flight tasks to complete (max 30 min)
>        wait_for_running_tasks(affected_dags, timeout=1800)
>        
>        # Step 5: Switch to new version
>        switch_active_version(new_version)
>        
>        # Step 6: Unpause DAGs
>        unpause_dags(affected_dags)
>        
>        # Step 7: Monitor for 10 minutes
>        monitor_health(duration=600)
>    ```
>
> 3. **Rollback plan:**
>    ```bash
>    # If issues detected within 30 minutes:
>    gcloud composer environments update prod-composer \
>        --update-env-variables=DAG_VERSION=v_previous
>    # Old DAGs still exist in GCS — instant rollback
>    ```
>
> 4. **Testing in CI:**
>    ```python
>    # tests/test_dag_integrity.py
>    from airflow.models import DagBag
>    
>    def test_no_import_errors():
>        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
>        assert len(dag_bag.import_errors) == 0, f"Import errors: {dag_bag.import_errors}"
>    
>    def test_all_dags_have_owner():
>        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
>        for dag_id, dag in dag_bag.dags.items():
>            assert dag.default_args.get('owner') != 'airflow', f"{dag_id} has no owner"
>    
>    def test_no_dag_cycles():
>        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
>        for dag_id, dag in dag_bag.dags.items():
>            assert not dag.test_cycle(), f"{dag_id} has a cycle"
>    ```
