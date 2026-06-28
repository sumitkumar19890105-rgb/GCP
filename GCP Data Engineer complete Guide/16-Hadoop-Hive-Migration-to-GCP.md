# Hadoop/Hive Migration to GCP (On-Premises → Cloud)

## Problem: How do you migrate petabytes of Hive data from on-prem Hadoop to GCP without downtime?

**Definition:** A Hadoop-to-GCP migration moves historical data (full load), then keeps GCP in sync with daily incremental loads until the **cutoff date** — when on-prem is decommissioned and GCP becomes the source of truth.

---

## Why Companies Migrate from Hadoop to GCP

| On-Prem Hadoop Pain | GCP Benefit |
|--------------------|-----------:|
| Hardware refresh every 3-5 years ($$$) | No hardware — serverless (BigQuery) |
| Cluster tuning, HDFS rebalancing | Fully managed — zero ops |
| Fixed capacity (can't burst) | Auto-scale on demand |
| HDFS replication factor = 3x storage cost | GCS handles replication transparently |
| Hive metastore maintenance | BigQuery = storage + compute + catalog in one |
| YARN resource contention | Isolated workloads (no noisy neighbors) |
| Hadoop admin team (5-10 people) | No admin needed — GCP manages infra |

---

## Migration Strategy Overview

```
┌───────────────────────────────────────────────────────────────────┐
│                    MIGRATION TIMELINE                               │
├─────────────────┬─────────────────────────┬───────────────────────┤
│   PHASE 1       │      PHASE 2            │     PHASE 3           │
│   Historical    │      Incremental        │     Cutover           │
│   Load          │      Sync               │                       │
│   (one-time)    │      (daily until cut)  │                       │
│                 │                         │                       │
│   Hive ──────▶ GCS ──▶ BigQuery          │  Switch consumers     │
│   (full dump)   │   (delta loads)         │  Decommission Hadoop  │
│                 │                         │                       │
│   Week 1-2      │      Week 3 → Cutoff   │     Cutoff Day        │
└─────────────────┴─────────────────────────┴───────────────────────┘
```

---

## Phase-by-Phase Approach

| Phase | What | Duration | Risk |
|-------|------|----------|------|
| **Phase 1: Historical Load** | Full dump of all Hive tables to GCS → BigQuery | 1-4 weeks | Network bandwidth, data corruption |
| **Phase 2: Incremental Sync** | Daily delta loads to keep GCP in sync | Weeks to months | Data consistency, late-arriving records |
| **Phase 3: Validation** | Compare row counts, checksums, query results | 1-2 weeks | Discrepancies in edge cases |
| **Phase 4: Cutover** | Switch all consumers to GCP, stop Hadoop jobs | 1 day | Rollback plan if issues found |
| **Phase 5: Decommission** | Turn off Hadoop cluster after bake period | After 2-4 weeks | None (keep backup) |

---

## Phase 1: Historical Load (Full Dump)

### Method A: Export → Transfer → Load (Command Line)

```bash
# ═══════════════════════════════════════════════════════════════
# STEP 1: Export Hive table as Parquet to HDFS staging
# Run on Hadoop cluster
# ═══════════════════════════════════════════════════════════════

hive -e "
    SET hive.exec.compress.output=true;
    SET parquet.compression=SNAPPY;
    
    INSERT OVERWRITE DIRECTORY '/staging/migration/loans/'
    STORED AS PARQUET
    SELECT * FROM loan_db.loans;
"

# ═══════════════════════════════════════════════════════════════
# STEP 2: Transfer HDFS → GCS (choose one method)
# ═══════════════════════════════════════════════════════════════

# Option A: hadoop distcp (uses Hadoop cluster bandwidth)
hadoop distcp \
    -m 50 \
    -bandwidth 100 \
    hdfs:///staging/migration/loans/ \
    gs://your-project-migration/raw/loans/

# Option B: Google Transfer Service (managed, handles retries)
gcloud transfer jobs create \
    hdfs:///staging/migration/loans/ \
    gs://your-project-migration/raw/loans/ \
    --source-agent-pool=my-on-prem-agents

# ═══════════════════════════════════════════════════════════════
# STEP 3: Load Parquet from GCS → BigQuery
# ═══════════════════════════════════════════════════════════════

bq load \
    --source_format=PARQUET \
    --autodetect \
    --time_partitioning_field=application_date \
    --clustering_fields=region,loan_type \
    project:loan_analytics.loans \
    "gs://your-project-migration/raw/loans/*.parquet"
```

### Method B: Dataproc Spark (Better for Large Tables)

```python
# historical_load_spark.py
# Read from Hive → Write to BigQuery (with validation)

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder \
    .appName("hive_to_bigquery_historical") \
    .enableHiveSupport() \
    .getOrCreate()

TEMP_GCS_BUCKET = "your-project-migration-temp"

# Tables to migrate
TABLES = [
    {"hive": "loan_db.loans", "bq": "project.loan_analytics.loans"},
    {"hive": "loan_db.customers", "bq": "project.loan_analytics.customers"},
    {"hive": "analytics_db.transactions", "bq": "project.loan_analytics.transactions"},
]

for table in TABLES:
    print(f"Migrating: {table['hive']} → {table['bq']}")
    
    # Read from Hive
    df = spark.sql(f"SELECT * FROM {table['hive']}")
    source_count = df.count()
    print(f"  Source rows: {source_count:,}")
    
    # Add migration metadata (useful for tracking what came from where)
    df = df.withColumn("_migration_ts", current_timestamp()) \
           .withColumn("_source", lit("hadoop_on_prem"))
    
    # Write to BigQuery
    df.write.format("bigquery") \
        .option("table", table['bq']) \
        .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
        .mode("overwrite") \
        .save()
    
    # Verify
    target_count = spark.read.format("bigquery").option("table", table['bq']).load().count()
    
    if target_count == source_count:
        print(f"  ✓ Verified: {target_count:,} rows")
    else:
        raise ValueError(f"  ❌ MISMATCH: Source={source_count:,}, Target={target_count:,}")

print("═══ HISTORICAL LOAD COMPLETE ═══")
```

---

## Phase 2: Incremental Load (Daily Sync Until Cutoff)

### Two Approaches Based on Table Design

| Approach | When to Use | How It Works |
|----------|------------|--------------|
| **Watermark-based** | Table has `updated_at` or `modified_date` column | Extract rows WHERE updated_at > last_loaded |
| **Partition-based** | Table is partitioned by date (dt=2026-06-22) | Extract only new partitions not yet in BigQuery |

---

### Approach 1: Watermark-based Incremental (Tables with updated_at)

```python
# incremental_sync.py
# Runs daily via Cloud Composer until MIGRATION_CUTOFF_DATE

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, current_timestamp, lit
from google.cloud import bigquery
from datetime import datetime

spark = SparkSession.builder.appName("incremental_sync").enableHiveSupport().getOrCreate()

CUTOFF_DATE = "2026-08-01"
TEMP_BUCKET = "your-project-migration-temp"
TODAY = datetime.now().strftime("%Y-%m-%d")

# ─── Stop if past cutoff ───
if TODAY > CUTOFF_DATE:
    print(f"Past cutoff ({CUTOFF_DATE}). No sync needed.")
    spark.stop()
    exit(0)


def sync_table(hive_table, bq_table, watermark_col="updated_at", key_col="loan_id"):
    """
    1. Check last loaded watermark in BigQuery
    2. Extract new/changed records from Hive since that watermark
    3. MERGE into BigQuery (insert new + update changed)
    """
    print(f"\n─── Syncing: {hive_table} → {bq_table} ───")
    
    # Step 1: Get last watermark from BigQuery (where did we stop last time?)
    df_bq = spark.read.format("bigquery").option("table", bq_table).load()
    last_watermark = df_bq.agg(spark_max(watermark_col)).collect()[0][0]
    last_watermark = str(last_watermark) if last_watermark else "1900-01-01"
    print(f"  Last watermark: {last_watermark}")
    
    # Step 2: Extract only NEW/CHANGED records from Hive
    df_delta = spark.sql(f"""
        SELECT * FROM {hive_table}
        WHERE {watermark_col} > '{last_watermark}'
    """)
    
    delta_count = df_delta.count()
    if delta_count == 0:
        print(f"  ✓ Already in sync. No new records.")
        return
    
    print(f"  New/changed records: {delta_count:,}")
    
    # Step 3: Write delta to a staging table
    staging_table = bq_table + "_staging"
    df_delta.withColumn("_migration_ts", current_timestamp()) \
        .write.format("bigquery") \
        .option("table", staging_table) \
        .option("temporaryGcsBucket", TEMP_BUCKET) \
        .mode("overwrite") \
        .save()
    
    # Step 4: MERGE staging → target (handles both inserts and updates)
    client = bigquery.Client()
    merge_sql = f"""
        MERGE `{bq_table}` T
        USING `{staging_table}` S
        ON T.{key_col} = S.{key_col}
        WHEN MATCHED THEN UPDATE SET
            T.status = S.status,
            T.amount = S.amount,
            T.{watermark_col} = S.{watermark_col},
            T._migration_ts = S._migration_ts
        WHEN NOT MATCHED THEN INSERT ROW
    """
    job = client.query(merge_sql)
    job.result()
    print(f"  ✓ MERGED: {job.num_dml_affected_rows} rows affected")


# ─── Sync all tables ───
sync_table("loan_db.loans", "project.loan_analytics.loans", "updated_at", "loan_id")
sync_table("loan_db.customers", "project.loan_analytics.customers", "modified_date", "customer_id")

print("\n═══ DAILY SYNC COMPLETE ═══")
```

---

### Approach 2: Partition-based Incremental (Date-partitioned Hive Tables)

```python
# For tables partitioned by date — just load new partitions

def sync_new_partitions(hive_table, bq_table, partition_col="dt"):
    """Extract only Hive partitions that don't exist in BigQuery yet."""
    
    # Get partitions already in BigQuery
    df_bq = spark.read.format("bigquery").option("table", bq_table).load()
    loaded_dates = set(str(d) for d in df_bq.select(partition_col).distinct().rdd.flatMap(lambda x: x).collect())
    
    # Get all Hive partitions
    hive_parts = spark.sql(f"SHOW PARTITIONS {hive_table}")
    hive_dates = set(row[0].split("=")[1] for row in hive_parts.collect())
    
    # Find new ones
    new_dates = sorted(hive_dates - loaded_dates)
    
    if not new_dates:
        print(f"  ✓ No new partitions")
        return
    
    print(f"  New partitions: {new_dates}")
    
    # Load only new partitions
    filter_list = ",".join(f"'{d}'" for d in new_dates)
    df_new = spark.sql(f"SELECT * FROM {hive_table} WHERE {partition_col} IN ({filter_list})")
    
    df_new.write.format("bigquery") \
        .option("table", bq_table) \
        .option("temporaryGcsBucket", TEMP_BUCKET) \
        .mode("append") \
        .save()
    
    print(f"  ✓ Loaded {len(new_dates)} partitions ({df_new.count():,} rows)")

# Use for append-only tables (like transactions)
sync_new_partitions("analytics_db.transactions", "project.loan_analytics.transactions", "transaction_date")
```

---

## Phase 3: Validation

```python
# Quick validation: compare source vs target
def validate(hive_table, bq_table):
    df_h = spark.sql(f"SELECT * FROM {hive_table}")
    df_b = spark.read.format("bigquery").option("table", bq_table).load()
    
    h_count = df_h.count()
    b_count = df_b.count()
    
    print(f"{hive_table}: Hive={h_count:,} | BQ={b_count:,} | {'✓' if h_count == b_count else '❌'}")
    
    # Also compare a checksum (sum of amount column)
    h_sum = df_h.agg({"amount": "sum"}).collect()[0][0] or 0
    b_sum = df_b.agg({"amount": "sum"}).collect()[0][0] or 0
    print(f"  SUM(amount): Hive={h_sum:,.2f} | BQ={b_sum:,.2f} | {'✓' if abs(h_sum-b_sum) < 1 else '❌'}")
```

---

## Phase 4: Cloud Composer DAG (Orchestrating the Migration)

```python
# dags/hadoop_migration.py
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime, timedelta

CUTOFF = "2026-08-01"

def check_cutoff(**ctx):
    """Stop syncing if we've passed the migration cutoff date."""
    if ctx['ds'] > CUTOFF:
        return 'done'
    return 'sync'

with DAG('hadoop_migration', schedule_interval='0 3 * * *',
         start_date=datetime(2026, 6, 22), catchup=False,
         tags=['migration', 'hadoop']) as dag:

    # Task 1: Should we still be syncing?
    branch = BranchPythonOperator(task_id='check_cutoff', python_callable=check_cutoff)

    # Task 2: Run incremental sync via Dataproc
    sync = DataprocSubmitPySparkJobOperator(
        task_id='sync',
        main='gs://bucket/migration/incremental_sync.py',
        cluster_name='migration-cluster',
        region='us-central1',
    )

    # Task 3: Migration is done
    done = PythonOperator(task_id='done', python_callable=lambda: print("Migration complete!"))

    branch >> [sync, done]
```

---

## Migration Tools Comparison

| Tool | Best For | Speed | Complexity |
|------|----------|-------|-----------|
| **hadoop distcp** | Large files, network-connected clusters | Fast (parallel) | Low |
| **Google Transfer Service** | Managed, large-scale, on-prem agents | Medium | Low |
| **Dataproc Spark** | Complex transforms during migration | Medium | Medium |
| **BigQuery Data Transfer** | Direct Hive to BQ (limited formats) | Fast | Low |
| **Cloud Interconnect + distcp** | Dedicated high-bandwidth link | Fastest | High (setup) |
| **Transfer Appliance** | 100+ TB, slow network | Shipping time | Low (physical) |

---

## Network & Bandwidth Planning

```
Data Volume: 50 TB
Network: 1 Gbps dedicated link (Cloud Interconnect)

Transfer time = 50 TB / 1 Gbps
             = 50,000 GB / 125 MB/s
             = 400,000 seconds
             ≈ 4.6 days (at full bandwidth)

With 10 Gbps: ~11 hours
With compression (2:1): ~5.5 hours at 10 Gbps
```

**Rule of thumb:**
- < 10 TB → Transfer over internet (days, but simple)
- 10-100 TB → Cloud Interconnect (dedicated link)
- 100+ TB → Transfer Appliance (physical device shipped by Google)

---

## Common Migration Gotchas

| Problem | Cause | Fix |
|---------|-------|-----|
| Data type mismatch | Hive `STRING` vs BQ `INT64` | Explicit CAST in Spark before writing |
| Null handling | Hive stores `\N` for null | `.na.replace("\\N", None)` |
| Partition format | Hive `dt=2026-06-22` vs BQ date | Extract date from partition column |
| Decimal precision | Hive `DECIMAL(38,18)` | Use BQ `BIGNUMERIC` or round to `NUMERIC(38,9)` |
| Complex types | Arrays, Maps in Hive | Flatten arrays to REPEATED, maps to STRUCT |
| Timezone | Hive UTC vs app local time | Explicit `from_utc_timestamp()` conversion |
| Duplicates after retry | Spark job retried, data appended twice | Deduplicate before MERGE using ROW_NUMBER |
| Metastore mapping | `hive_db.table` vs `project.dataset.table` | Maintain a mapping config file |
| ORC format | BQ prefers Parquet | Convert ORC to Parquet during export |
| Hive views | Views reference other tables | Recreate as BigQuery views with new table refs |

---

## Hive-to-BigQuery Type Mapping

| Hive Type | BigQuery Type | Notes |
|-----------|--------------|-------|
| `STRING` | `STRING` | Direct mapping |
| `INT` | `INT64` | Hive INT is 32-bit, BQ is 64-bit (safe) |
| `BIGINT` | `INT64` | Direct mapping |
| `FLOAT` | `FLOAT64` | Direct mapping |
| `DOUBLE` | `FLOAT64` | Direct mapping |
| `DECIMAL(p,s)` | `NUMERIC(38,9)` or `BIGNUMERIC` | Precision loss if s > 9 |
| `DATE` | `DATE` | Direct mapping |
| `TIMESTAMP` | `TIMESTAMP` | Check timezone handling |
| `BOOLEAN` | `BOOL` | Direct mapping |
| `ARRAY<T>` | `ARRAY<T>` (REPEATED) | Supported natively |
| `MAP<K,V>` | `ARRAY<STRUCT<key K, value V>>` | Flatten map to struct array |
| `STRUCT<...>` | `STRUCT<...>` | Supported natively |
| `BINARY` | `BYTES` | Direct mapping |

---

## Checklist: Before You Start Migration

- [ ] Inventory all Hive tables (count, sizes, partitions, dependencies)
- [ ] Identify tables by type: append-only vs mutable (determines incremental strategy)
- [ ] Map Hive schemas to BigQuery schemas (handle type mismatches)
- [ ] Set up network connectivity (VPN, Interconnect, or Transfer Appliance)
- [ ] Create GCS buckets for staging (`migration-raw`, `migration-temp`)
- [ ] Create BigQuery datasets with correct regions
- [ ] Set up Dataproc cluster with Hive metastore connectivity
- [ ] Test with ONE small table end-to-end before running all tables
- [ ] Define cutoff date and communicate to all stakeholders
- [ ] Identify all downstream consumers (reports, dashboards, APIs) to switch on cutoff
- [ ] Create rollback plan (what if GCP has issues on cutoff day?)
- [ ] Set up monitoring/alerting for the incremental sync DAG

---

## Interview Perspective — Hadoop Migration

> "I migrated Hive to BigQuery in phases. Phase 1: full historical load using Dataproc Spark — read from Hive, add metadata, write to BigQuery, validate row counts. Phase 2: daily incremental sync using watermark columns (`updated_at`) — extract only new/changed records, MERGE into BigQuery via a staging table. This runs on a Composer DAG that auto-stops after the cutoff date. Phase 3: validation comparing counts and checksums. On cutoff day, we switch consumers, monitor for 2 weeks, then decommission Hadoop."

---

## Interview Follow-up Questions

**Q: "How do you handle late-arriving data after cutoff?"**
> "I keep the sync running 2 weeks past cutoff. Any late records still get picked up. After 2 weeks of zero new records, we decommission."

**Q: "What if row counts don't match?"**
> "Usually timing — Hive nightly jobs hadn't finished when I extracted. I re-run next day and it matches. For persistent mismatches, I check the dead-letter table for filtered records (null IDs, corrupt data)."

**Q: "How do you handle Hive tables with complex types (arrays, maps, structs)?"**
> "BigQuery supports ARRAY and STRUCT natively. I map Hive arrays to BQ REPEATED fields, and Hive structs to BQ STRUCT. For Maps, I explode them into a struct with key/value fields. I test the schema mapping on a sample before running the full load."

**Q: "What about Hive UDFs used in views/queries?"**
> "I inventory all Hive UDFs, then rewrite them as BigQuery UDFs (JavaScript or SQL). For complex ones, I use BigQuery Remote Functions that call Cloud Functions. I validate output matches for 1000 sample rows before deploying."

**Q: "How did you estimate the migration timeline?"**
> "I calculate transfer time from data volume and network bandwidth. For 50TB over a 10Gbps link with compression, that's ~6 hours for the historical load. Then I add 1-2 weeks for validation and consumer switchover. The incremental sync phase depends on how long it takes the business to validate and approve cutover — typically 4-8 weeks."

**Q: "What was the biggest challenge?"**
> "Schema differences. Hive is very permissive — it allows NULL everywhere, loose typing, and schema-on-read. BigQuery is stricter. We had to add explicit type casting, null handling, and validation at every step. The second challenge was coordinating cutover across 15+ downstream teams who consumed Hive data."
