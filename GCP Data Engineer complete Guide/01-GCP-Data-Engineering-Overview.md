# GCP Data Engineering Overview

## What is GCP Data Engineering?

GCP Data Engineering involves designing, building, and managing data pipelines and infrastructure on Google Cloud Platform to enable analytics, machine learning, and business intelligence.

## Core Pillars

| Pillar | GCP Services |
|--------|-------------|
| **Ingestion** | Pub/Sub, Cloud Storage, Datastream, Transfer Service |
| **Processing** | Dataflow (Apache Beam), Dataproc (Spark), Cloud Functions |
| **Storage** | BigQuery, Cloud Storage, Bigtable, Firestore, Spanner |
| **Orchestration** | Cloud Composer (Airflow), Cloud Scheduler, Workflows |
| **Serving** | BigQuery, Looker, Vertex AI, Data Studio |
| **Governance** | Data Catalog, DLP API, IAM, VPC Service Controls |

## Data Engineering Lifecycle on GCP

```
Sources → Ingestion → Processing → Storage → Serving → Monitoring
  │          │            │           │          │          │
  ├── APIs   ├── Pub/Sub  ├── Dataflow├── BQ     ├── Looker ├── Cloud Monitoring
  ├── DBs    ├── GCS      ├── Dataproc├── GCS    ├── APIs   ├── Logging
  └── Files  └── Datastream└── Functions└── Bigtable└── ML   └── Alerting
```

## Key Concepts for Interviews

### 1. Serverless vs Managed vs Self-Managed

| Type | Examples | When to Use |
|------|----------|-------------|
| Serverless | BigQuery, Dataflow, Cloud Functions | No infra management needed, variable workloads |
| Managed | Dataproc, Cloud Composer, GKE | Need control over config, existing Spark/Hadoop code |
| Self-Managed | Compute Engine + custom tools | Legacy systems, full control requirements |

### 2. Batch vs Streaming

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| Latency | Minutes to hours | Seconds to minutes |
| Tools | Dataflow batch, Dataproc, BQ scheduled | Dataflow streaming, Pub/Sub |
| Use Case | Reports, ETL, backfill | Real-time dashboards, alerts, fraud detection |
| Cost | Lower (scheduled) | Higher (always-on) |

### 3. ELT vs ETL on GCP

- **ETL** (Transform before load): Dataflow/Dataproc → BigQuery
- **ELT** (Load then transform): GCS → BigQuery → SQL transforms (dbt/scheduled queries)
- **Modern trend**: ELT is preferred on GCP because BigQuery is extremely powerful for transformations

## GCP Data Engineering vs AWS vs Azure

| Capability | GCP | AWS | Azure |
|-----------|-----|-----|-------|
| Warehouse | BigQuery | Redshift | Synapse |
| Stream Processing | Dataflow | Kinesis + Lambda | Stream Analytics |
| Batch Processing | Dataproc | EMR | HDInsight |
| Orchestration | Cloud Composer | MWAA / Step Functions | Data Factory |
| Object Storage | Cloud Storage | S3 | Blob Storage |
| Messaging | Pub/Sub | SQS/SNS/Kinesis | Event Hub |

## Interview Tip

> "GCP's differentiator is **serverless-first** philosophy. BigQuery separates compute from storage, Dataflow auto-scales, and you rarely manage clusters. This means lower operational overhead but requires understanding of cost optimization and query design."
