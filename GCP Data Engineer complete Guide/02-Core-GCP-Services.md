# Core GCP Services for Data Engineering

## Table of Contents

| # | Service | Purpose |
|---|---------|---------|
| 1 | [BigQuery](#1-bigquery--serverless-data-warehouse) | Serverless data warehouse — SQL analytics at petabyte scale |
| 2 | [Cloud Storage (GCS)](#2-cloud-storage-gcs--object-storage--data-lake) | Object storage — data lake foundation |
| 3 | [Pub/Sub](#3-pubsub--messaging-and-event-streaming) | Messaging — real-time event ingestion |
| 4 | [Dataflow](#4-dataflow--stream-and-batch-processing-apache-beam) | Serverless ETL — batch + streaming (Apache Beam) |
| 5 | [Dataproc](#5-dataproc--managed-sparkhadoop) | Managed Spark/Hadoop clusters |
| 6 | [Cloud Composer](#6-cloud-composer--managed-apache-airflow) | Pipeline orchestration (Airflow) |
| 7 | [Other Services](#7-other-important-services) | Datastream, Cloud Functions, Vertex AI, Data Catalog, Cloud DLP |
| — | [Quick Reference](#quick-reference-which-service-when) | Which service to use when (decision table) |

---

## 1. BigQuery — Serverless Data Warehouse

**Definition:** BigQuery is Google Cloud's fully managed, serverless, petabyte-scale data warehouse that enables super-fast SQL queries using the processing power of Google's infrastructure.

**Purpose:** Store and analyze massive datasets (terabytes to petabytes) without managing servers. Used for analytics, reporting, dashboards, and ML — where you need to query large volumes of structured/semi-structured data quickly and cost-effectively.

**When to Use:** You need a central analytics warehouse, ad-hoc SQL queries on large data, scheduled reporting, or ML on structured data (BQML).

### Key Features
- Serverless, petabyte-scale analytics
- Columnar storage (Capacitor format)
- Separation of compute and storage
- Built-in ML (BigQuery ML)
- Streaming inserts + batch loads
- Partitioning and clustering for optimization

### Common Commands
```sql
-- Create partitioned + clustered table
CREATE TABLE `project.dataset.transactions`
(
  transaction_id STRING,
  customer_id STRING,
  amount NUMERIC,
  transaction_date DATE,
  region STRING
)
PARTITION BY transaction_date
CLUSTER BY region, customer_id;

-- Query with partition filter (cost-efficient)
SELECT customer_id, SUM(amount) as total
FROM `project.dataset.transactions`
WHERE transaction_date BETWEEN '2026-01-01' AND '2026-06-30'
  AND region = 'US'
GROUP BY customer_id;

-- Scheduled query
CREATE SCHEDULED QUERY `daily_aggregation`
OPTIONS(schedule='every 24 hours', destination_table='project.dataset.daily_summary')
AS
SELECT DATE(transaction_date) as day, COUNT(*) as txn_count, SUM(amount) as total
FROM `project.dataset.transactions`
WHERE transaction_date = CURRENT_DATE() - 1
GROUP BY 1;
```

### Optimization Techniques
- **Partitioning**: By date/timestamp (most common), integer range, or ingestion time
- **Clustering**: Up to 4 columns, auto-re-clustering
- **Materialized Views**: Pre-computed results, auto-refreshed
- **BI Engine**: In-memory acceleration for dashboards
- **Slot Reservations**: Predictable cost for enterprise

---

## 2. Cloud Storage (GCS) — Object Storage / Data Lake

**Definition:** Cloud Storage (GCS) is Google Cloud's unified object storage service for storing any amount of data (files, images, backups, logs) as objects in buckets.

**Purpose:** Acts as the **data lake foundation** — the central landing zone for all raw data before processing. Also used for staging, archival, backup, and serving static assets.

**When to Use:** You need to store files (CSV, Parquet, JSON, Avro), create a data lake, stage data between pipeline steps, or archive historical data cheaply.

### Storage Classes
| Class | Use Case | Availability |
|-------|----------|-------------|
| Standard | Frequently accessed data | 99.99% |
| Nearline | Once/month access | 99.9% |
| Coldline | Once/quarter access | 99.9% |
| Archive | Once/year access | 99.9% |

### Data Lake Pattern
```
gs://company-data-lake/
├── raw/                    # Ingested as-is
│   ├── source1/YYYY/MM/DD/
│   └── source2/YYYY/MM/DD/
├── curated/                # Cleaned, validated
│   ├── domain1/
│   └── domain2/
├── processed/              # Analytics-ready
│   └── aggregations/
└── archive/                # Historical, cold storage
```

### Python Example — Upload/Download
```python
from google.cloud import storage

def upload_to_gcs(bucket_name, source_file, destination_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")

def list_blobs_with_prefix(bucket_name, prefix):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]
```

---

## 3. Pub/Sub — Messaging and Event Streaming

**Definition:** Cloud Pub/Sub is a fully managed, real-time messaging service that allows independent applications to send and receive messages asynchronously (publisher-subscriber pattern).

**Purpose:** Decouple event producers from consumers. Ingests millions of events per second for real-time analytics, event-driven architectures, and streaming pipelines. Guarantees at-least-once delivery with message retention up to 31 days.

**When to Use:** You need real-time event ingestion (clickstreams, IoT, transactions), asynchronous communication between microservices, or a buffer between data producers and consumers.

### Key Concepts
- **Topic**: Channel where publishers send messages
- **Subscription**: How subscribers receive messages (pull or push)
- **At-least-once delivery**: Messages may be delivered more than once
- **Ordering**: Optional message ordering by key
- **Dead Letter Topics**: Failed messages after max retries

### Python Publisher
```python
from google.cloud import pubsub_v1
import json

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("my-project", "transactions-topic")

def publish_event(data: dict):
    message = json.dumps(data).encode("utf-8")
    future = publisher.publish(
        topic_path,
        message,
        source="payment-service",
        event_type="transaction"
    )
    return future.result()

# Usage
publish_event({"txn_id": "T123", "amount": 500.0, "currency": "USD"})
```

### Python Subscriber
```python
from google.cloud import pubsub_v1
import json

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path("my-project", "transactions-sub")

def callback(message):
    data = json.loads(message.data.decode("utf-8"))
    print(f"Received: {data}")
    # Process message
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
```

---

## 4. Dataflow — Stream and Batch Processing (Apache Beam)

**Definition:** Dataflow is a fully managed, serverless stream and batch data processing service based on the Apache Beam SDK. You write the pipeline logic; Google handles infrastructure, scaling, and fault tolerance.

**Purpose:** Transform, enrich, and move data at scale — both in real-time (streaming) and batch. Handles complex event processing, windowing, and exactly-once semantics without managing clusters.

**When to Use:** You need serverless ETL/ELT, real-time streaming pipelines (Pub/Sub → BigQuery), complex transformations with windowing/triggers, or exactly-once processing guarantees.

### Key Features
- Unified batch + streaming model
- Auto-scaling (serverless)
- Exactly-once processing
- Windowing for streaming data

### Python Batch Pipeline
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    '--project=my-project',
    '--runner=DataflowRunner',
    '--region=us-central1',
    '--temp_location=gs://my-bucket/temp',
    '--staging_location=gs://my-bucket/staging',
])

with beam.Pipeline(options=options) as p:
    (
        p
        | 'ReadCSV' >> beam.io.ReadFromText('gs://my-bucket/raw/data.csv', skip_header_lines=1)
        | 'ParseCSV' >> beam.Map(lambda line: line.split(','))
        | 'FilterValid' >> beam.Filter(lambda row: len(row) == 5 and row[2].replace('.','').isdigit())
        | 'Transform' >> beam.Map(lambda row: {
            'id': row[0],
            'name': row[1],
            'amount': float(row[2]),
            'date': row[3],
            'region': row[4]
        })
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            'my-project:dataset.table',
            schema='id:STRING,name:STRING,amount:FLOAT,date:STRING,region:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
```

### Streaming Pipeline with Windowing
```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows, SlidingWindows

with beam.Pipeline(options=options) as p:
    (
        p
        | 'ReadPubSub' >> beam.io.ReadFromPubSub(topic='projects/my-project/topics/events')
        | 'ParseJSON' >> beam.Map(lambda msg: json.loads(msg))
        | 'AddTimestamp' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['event_time']))
        | 'Window5Min' >> beam.WindowInto(FixedWindows(300))  # 5-minute windows
        | 'CountByRegion' >> beam.combiners.Count.PerKey(lambda x: x['region'])
        | 'WriteToBQ' >> beam.io.WriteToBigQuery(...)
    )
```

---

## 5. Dataproc — Managed Spark/Hadoop

**Definition:** Dataproc is a fully managed service for running Apache Spark, Hadoop, Hive, Pig, and Presto clusters on GCP. Clusters can be created in ~90 seconds and auto-deleted after jobs complete.

**Purpose:** Run existing Spark/Hadoop workloads on GCP without re-writing them. Ideal for teams migrating from on-prem Hadoop or running ML workloads (PySpark ML, Spark MLlib) that aren't supported in Dataflow.

**When to Use:** You have existing Spark code, need PySpark ML, want cluster-level control, are migrating from on-prem Hadoop, or need interactive Spark (Jupyter notebooks on Dataproc).

### When to Use Dataproc vs Dataflow
| Criteria | Dataproc | Dataflow |
|----------|----------|----------|
| Existing Spark code | ✅ | ❌ (rewrite in Beam) |
| ML workloads (PySpark ML) | ✅ | ❌ |
| Serverless | ❌ (cluster-based) | ✅ |
| Streaming | Spark Structured Streaming | Apache Beam |
| Cost model | Cluster uptime | Per-job |

### PySpark on Dataproc
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when

spark = SparkSession.builder.appName("LoanAnalysis").getOrCreate()

# Read from GCS
df = spark.read.parquet("gs://data-lake/curated/loans/")

# Transform
result = (
    df
    .filter(col("loan_status").isNotNull())
    .groupBy("region", "loan_type")
    .agg(
        count("*").alias("total_loans"),
        sum("principal").alias("total_principal"),
        sum(when(col("loan_status") == "DEFAULT", 1).otherwise(0)).alias("defaults")
    )
    .withColumn("default_rate", col("defaults") / col("total_loans"))
)

# Write to BigQuery
result.write \
    .format("bigquery") \
    .option("table", "project.dataset.loan_summary") \
    .option("temporaryGcsBucket", "my-temp-bucket") \
    .mode("overwrite") \
    .save()
```

---

## 6. Cloud Composer — Managed Apache Airflow

**Definition:** Cloud Composer is a fully managed workflow orchestration service built on Apache Airflow. It schedules, monitors, and manages complex data pipelines as DAGs (Directed Acyclic Graphs).

**Purpose:** Orchestrate multi-step data pipelines across GCP services (and external systems). Handles scheduling, dependencies, retries, alerting, and cross-service coordination — the "conductor" of your data platform.

**When to Use:** You need to schedule and orchestrate multi-step ETL/ELT pipelines, manage dependencies between tasks, trigger jobs across BigQuery/Dataflow/Dataproc/GCS, or need visibility into pipeline health.

### Key Features
- Managed Airflow 2.x
- Integrates with all GCP services
- DAG versioning via GCS
- Auto-scaling workers (Composer 2)

### Example DAG
```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

with DAG(
    'daily_loan_pipeline',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['loans', 'production'],
) as dag:

    load_raw = GCSToBigQueryOperator(
        task_id='load_raw_data',
        bucket='data-lake',
        source_objects=['raw/loans/{{ ds }}/*.parquet'],
        destination_project_dataset_table='project.raw.loans',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
    )

    transform = BigQueryInsertJobOperator(
        task_id='transform_data',
        configuration={
            'query': {
                'query': """
                    CREATE OR REPLACE TABLE `project.curated.loans` AS
                    SELECT *, 
                        CASE WHEN days_past_due > 90 THEN 'DEFAULT' ELSE 'CURRENT' END as status
                    FROM `project.raw.loans`
                    WHERE load_date = '{{ ds }}'
                """,
                'useLegacySql': False,
            }
        },
    )

    load_raw >> transform
```

---

## 7. Other Important Services

### Datastream
**Definition:** Serverless Change Data Capture (CDC) and replication service.
**Purpose:** Replicate data in real-time from operational databases (MySQL, PostgreSQL, Oracle) into BigQuery or GCS — keeping your warehouse in sync with source systems without building custom pipelines.
**When to Use:** You need real-time database replication, CDC for incremental loads, or a low-latency path from OLTP → OLAP.

- CDC from MySQL, PostgreSQL, Oracle → BigQuery/GCS
- Real-time replication, serverless

### Cloud Functions / Cloud Run
**Definition:** Cloud Functions = serverless event-driven functions (single-purpose). Cloud Run = serverless containerized applications (any language/framework).
**Purpose:** Run lightweight, event-triggered code without managing servers. Used for data validation on file upload, alerting, API endpoints, and glue logic between services.
**When to Use:** You need to trigger processing on GCS file upload, send alerts on Pub/Sub events, build REST APIs for data access, or run small transformation tasks.

- Event-driven lightweight processing
- Triggered by GCS uploads, Pub/Sub messages, HTTP

### Vertex AI (Basics for DE)
**Definition:** Google's unified ML platform for building, deploying, and managing machine learning models at scale.
**Purpose:** For data engineers — provides Feature Store (centralized feature serving), ML Pipelines (training orchestration), and integration points where DE pipelines feed ML models.
**When to Use:** You need to serve ML features to models in real-time, orchestrate training pipelines, or manage model versions.

- Feature Store: Centralized feature management
- Pipelines: ML training orchestration
- Serving: Model endpoints

### Data Catalog
**Definition:** Fully managed metadata management and data discovery service.
**Purpose:** Make data findable and understandable across the organization. Auto-discovers datasets, tags sensitive columns (PII), tracks data lineage, and provides a searchable catalog.
**When to Use:** You need data governance, PII tagging, data lineage tracking, or a self-service catalog for analysts to find datasets.

- Metadata management, data discovery
- Auto-tagging with DLP (PII detection)
- Lineage tracking

### Cloud DLP (Data Loss Prevention)
**Definition:** Service for discovering, classifying, and protecting sensitive data (PII, PHI, PCI).
**Purpose:** Automatically detect and redact/mask sensitive information (SSN, credit cards, emails, phone numbers) in BigQuery tables, GCS files, and Datastore — critical for compliance (GDPR, HIPAA, PCI-DSS).
**When to Use:** You need to scan data for PII before loading to analytics, de-identify sensitive columns, or prove compliance during audits.

- Detect and redact PII (SSN, credit card, email)
- De-identification transforms
- Integrate with BigQuery and GCS

---

## Quick Reference: Which Service When?

| Scenario | Service |
|----------|--------|
| Store raw files (CSV, Parquet, JSON) | **GCS** |
| Query structured data at scale (SQL) | **BigQuery** |
| Real-time event ingestion | **Pub/Sub** |
| Serverless ETL (batch or streaming) | **Dataflow** |
| Existing Spark/Hadoop code | **Dataproc** |
| Orchestrate multi-step pipelines | **Cloud Composer** |
| Real-time CDC from databases | **Datastream** |
| Event-triggered lightweight code | **Cloud Functions** |
| Containerized APIs/microservices | **Cloud Run** |
| ML feature management | **Vertex AI** |
| Data governance & discovery | **Data Catalog** |
| PII detection & masking | **Cloud DLP** |
