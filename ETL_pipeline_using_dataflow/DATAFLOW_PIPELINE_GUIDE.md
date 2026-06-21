# Dataflow Pipeline Project Guide
## Complete Setup & Execution Guide

---

## 📑 Table of Contents

1. [Project Overview](#project-overview)
2. [Folder Structure](#folder-structure)
3. [Prerequisites & Setup](#prerequisites--setup)
4. [Batch Pipeline Execution](#batch-pipeline-execution)
5. [Streaming Pipeline Execution](#streaming-pipeline-execution)
6. [Composer DAG Deployment](#composer-dag-deployment)
7. [Monitoring & Debugging](#monitoring--debugging)
8. [Common Issues & Solutions](#common-issues--solutions)

---

## 📋 Project Overview

This dataflow_pipeline project provides **production-ready templates** for:

- **Batch Pipelines**: Process large datasets from Cloud Storage
- **Streaming Pipelines**: Real-time processing from Pub/Sub
- **Composer Integration**: Orchestrate pipelines with Airflow/Composer
- **Data Transformation**: Extract, transform, load (ETL) patterns
- **Data Validation**: Built-in validation and error handling

### Key Files Structure

```
dataflow_pipeline/
├── batch_pipelines/          # Batch pipeline implementations
│   ├── batch_word_count_pipeline.py
│   └── batch_data_transformation_pipeline.py
├── streaming_pipelines/      # Streaming pipeline implementations
│   ├── streaming_event_processing_pipeline.py
│   └── streaming_session_aggregation_pipeline.py
├── composer_dags/            # Airflow DAGs for Composer
│   ├── batch_dataflow_dag.py
│   ├── streaming_dataflow_dag.py
│   └── transformation_etl_dag.py
├── templates/                # Configuration & deployment templates
│   ├── config_template.py
│   └── deployment_commands.txt
└── DATAFLOW_PIPELINE_GUIDE.md  # This file
```

---

## 📁 Folder Structure

### **batch_pipelines/**
Templates for batch data processing:
- **batch_word_count_pipeline.py**: Word count analysis on text files
- **batch_data_transformation_pipeline.py**: Data validation and transformation

**Use when:**
- Processing historical data
- ETL operations
- Scheduled daily/weekly jobs

### **streaming_pipelines/**
Templates for real-time data processing:
- **streaming_event_processing_pipeline.py**: Real-time event aggregation
- **streaming_session_aggregation_pipeline.py**: User session analysis

**Use when:**
- Real-time analytics required
- Continuous data streams
- Alert-on-event systems

### **composer_dags/**
Airflow DAGs for orchestration:
- **batch_dataflow_dag.py**: Orchestrate batch jobs
- **streaming_dataflow_dag.py**: Manage streaming jobs
- **transformation_etl_dag.py**: Complete ETL workflow

**Use when:**
- Need scheduled job execution
- Complex multi-step workflows
- Dependency management

### **templates/**
- **config_template.py**: Environment configuration
- **deployment_commands.txt**: Ready-to-use CLI commands

---

## 🔧 Prerequisites & Setup

### 1. **GCP Project Setup**

```bash
# Create GCP project
gcloud projects create your-project-id

# Set project
gcloud config set project your-project-id

# Enable required APIs
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable composer.googleapis.com
gcloud services enable storage-api.googleapis.com
```

### 2. **Create Service Account**

```bash
# Create service account
gcloud iam service-accounts create dataflow-runner \
    --display-name="Dataflow Service Account"

# Grant permissions
gcloud projects add-iam-policy-binding your-project-id \
    --member=serviceAccount:dataflow-runner@your-project-id.iam.gserviceaccount.com \
    --role=roles/dataflow.admin

gcloud projects add-iam-policy-binding your-project-id \
    --member=serviceAccount:dataflow-runner@your-project-id.iam.gserviceaccount.com \
    --role=roles/bigquery.admin

gcloud projects add-iam-policy-binding your-project-id \
    --member=serviceAccount:dataflow-runner@your-project-id.iam.gserviceaccount.com \
    --role=roles/pubsub.editor

# Create key
gcloud iam service-accounts keys create ~/dataflow-key.json \
    --iam-account=dataflow-runner@your-project-id.iam.gserviceaccount.com

# Set credential
export GOOGLE_APPLICATION_CREDENTIALS=~/dataflow-key.json
```

### 3. **Create Cloud Storage Bucket**

```bash
# Create bucket
gsutil mb gs://your-project-id-dataflow-bucket

# Create subdirectories
gsutil mb gs://your-project-id-dataflow-bucket/input
gsutil mb gs://your-project-id-dataflow-bucket/output
gsutil mb gs://your-project-id-dataflow-bucket/temp
gsutil mb gs://your-project-id-dataflow-bucket/staging
```

### 4. **Create BigQuery Dataset**

```bash
# Create dataset
bq mk --dataset \
    --location=US \
    --description="Dataflow outputs" \
    your-project-id:dataflow_output

# Create tables (if needed)
bq mk --table \
    your-project-id:dataflow_output.word_count \
    word:STRING,count:INTEGER,processing_timestamp:TIMESTAMP
```

### 5. **Create Pub/Sub Topic (for streaming)**

```bash
# Create topic
gcloud pubsub topics create events-stream

# Create subscription
gcloud pubsub subscriptions create events-subscription \
    --topic=events-stream
```

### 6. **Install Python Dependencies**

```bash
# Create virtual environment
python -m venv dataflow-env
source dataflow-env/bin/activate  # On Windows: dataflow-env\Scripts\activate

# Install packages
pip install --upgrade pip
pip install apache-beam[gcp]==2.54.0
pip install google-cloud-bigquery==3.13.0
pip install google-cloud-pubsub==2.18.4
pip install google-cloud-storage==2.10.0
pip install python-dateutil==2.8.2
```

---

## 🚀 Batch Pipeline Execution

### Option 1: Local Testing (DirectRunner)

```bash
# Test locally with sample data
python batch_pipelines/batch_word_count_pipeline.py \
    --runner DirectRunner \
    --input sample_data/*.txt \
    --output project:dataset.word_count_test
```

### Option 2: Cloud Dataflow (Production)

```bash
# Run on Dataflow
python batch_pipelines/batch_word_count_pipeline.py \
    --runner DataflowRunner \
    --project your-project-id \
    --region us-central1 \
    --temp_location gs://your-project-id-dataflow-bucket/temp \
    --input gs://your-project-id-dataflow-bucket/input/*.txt \
    --output your-project-id:dataflow_output.word_count \
    --num_workers 4 \
    --requirements_file requirements.txt
```

### Option 3: Data Transformation Pipeline

```bash
# Transform data from BigQuery
python batch_pipelines/batch_data_transformation_pipeline.py \
    --runner DataflowRunner \
    --project your-project-id \
    --region us-central1 \
    --temp_location gs://your-project-id-dataflow-bucket/temp \
    --input_table your-project-id:raw_data.source_table \
    --output_table your-project-id:processed_data.output_table \
    --num_workers 2
```

### Monitor Batch Job

```bash
# List active jobs
gcloud dataflow jobs list --region us-central1

# Get job status
gcloud dataflow jobs describe JOB_ID --region us-central1

# View logs
gcloud dataflow jobs log JOB_ID --region us-central1 --lines 50

# Cancel job (if needed)
gcloud dataflow jobs cancel JOB_ID --region us-central1
```

---

## 📡 Streaming Pipeline Execution

### Option 1: Local Testing (DirectRunner)

```bash
# Test streaming locally
python streaming_pipelines/streaming_event_processing_pipeline.py \
    --runner DirectRunner \
    --input_topic projects/your-project-id/topics/test-topic \
    --output_table your-project-id:test_dataset.test_output \
    --window_duration 30
```

### Option 2: Cloud Dataflow (Production)

```bash
# Run streaming pipeline with Streaming Engine
python streaming_pipelines/streaming_event_processing_pipeline.py \
    --runner DataflowRunner \
    --project your-project-id \
    --region us-central1 \
    --temp_location gs://your-project-id-dataflow-bucket/temp \
    --input_topic projects/your-project-id/topics/events-stream \
    --output_table your-project-id:streaming_output.real_time_events \
    --window_duration 60 \
    --window_type fixed \
    --num_workers 2 \
    --max_num_workers 10
```

### Option 3: Session Aggregation

```bash
# Group events into sessions
python streaming_pipelines/streaming_session_aggregation_pipeline.py \
    --runner DataflowRunner \
    --project your-project-id \
    --region us-central1 \
    --temp_location gs://your-project-id-dataflow-bucket/temp \
    --input_topic projects/your-project-id/topics/user-events \
    --output_table your-project-id:analytics.user_sessions \
    --session_gap 300  # 5 minutes
```

### Publish Test Data

```bash
# Send test messages to Pub/Sub
gcloud pubsub topics publish events-stream \
    --message '{"event_type":"click","value":100,"timestamp":"2024-01-01T00:00:00Z"}'

# Publish multiple messages
for i in {1..10}; do
  gcloud pubsub topics publish events-stream \
    --message "{\"event_type\":\"event$i\",\"value\":$i,\"timestamp\":\"2024-01-01T00:00:$(printf '%02d' $i)Z\"}"
done
```

### Monitor Streaming Job

```bash
# List jobs (streaming jobs run continuously)
gcloud dataflow jobs list --region us-central1

# Watch job metrics
gcloud dataflow jobs show JOB_ID --region us-central1 --full

# Check throughput and latency
gcloud dataflow jobs show JOB_ID --region us-central1 \
    --view=job_view_all
```

---

## 🎯 Composer DAG Deployment

### Step 1: Set Up Composer Environment

```bash
# Create Composer environment
gcloud composer environments create dataflow-composer \
    --location us-central1 \
    --machine-type n1-standard-4 \
    --python-version 3

# View environment details
gcloud composer environments describe dataflow-composer \
    --location us-central1
```

### Step 2: Get Composer Bucket

```bash
# List Composer buckets
gsutil ls

# Your DAG bucket will be: gs://your-project-storage.appspot.com/dags
COMPOSER_DAG_BUCKET=$(gcloud composer environments describe dataflow-composer \
    --location us-central1 \
    --format='value(config.dagGcsPrefix)')

echo "DAG Bucket: $COMPOSER_DAG_BUCKET"
```

### Step 3: Deploy DAGs

```bash
# Copy DAGs to Composer
gsutil cp composer_dags/*.py $COMPOSER_DAG_BUCKET

# Verify upload
gsutil ls $COMPOSER_DAG_BUCKET
```

### Step 4: Run DAGs from Composer

#### Option A: Trigger via CLI

```bash
# Trigger batch pipeline DAG
gcloud composer environments run dataflow-composer \
    --location us-central1 \
    dags trigger -- batch_dataflow_word_count

# Trigger streaming pipeline DAG
gcloud composer environments run dataflow-composer \
    --location us-central1 \
    dags trigger -- streaming_dataflow_pipeline

# Trigger ETL pipeline DAG
gcloud composer environments run dataflow-composer \
    --location us-central1 \
    dags trigger -- transformation_etl_pipeline
```

#### Option B: Via Airflow UI

```bash
# Get Airflow web server address
gcloud composer environments describe dataflow-composer \
    --location us-central1 \
    --format='value(config.airflowUri)'

# Open in browser and trigger DAGs manually
```

### Step 5: Monitor DAG Execution

```bash
# List DAG runs
gcloud composer environments run dataflow-composer \
    --location us-central1 \
    dags list-runs -- batch_dataflow_word_count

# View DAG logs
gcloud composer environments run dataflow-composer \
    --location us-central1 \
    tasks logs -- batch_dataflow_word_count \
    run_dataflow_batch_pipeline \
    2024-01-01T00:00:00
```

---

## 📊 Monitoring & Debugging

### Dataflow Job Monitoring

```bash
# View job metrics
gcloud dataflow jobs show JOB_ID --region us-central1 --full

# Monitor autoscaling
gcloud dataflow jobs show JOB_ID --region us-central1 \
    --view job_view_all | grep -E "workerPool|autoScaling"

# Check data throughput
gcloud dataflow jobs show JOB_ID --region us-central1 \
    | grep -E "elements|bytes"
```

### BigQuery Monitoring

```bash
# Query output table
bq query --use_legacy_sql=false '
  SELECT *
  FROM `your-project-id.dataflow_output.word_count`
  ORDER BY processing_timestamp DESC
  LIMIT 10
'

# Monitor data freshness
bq query --use_legacy_sql=false '
  SELECT 
    MAX(processing_timestamp) as latest_data,
    COUNT(*) as row_count,
    CURRENT_TIMESTAMP() as check_time
  FROM `your-project-id.dataflow_output.word_count`
'
```

### Logging & Debugging

```bash
# Stream logs in real-time
gcloud dataflow jobs log JOB_ID --region us-central1 --follow

# Filter logs by severity
gcloud dataflow jobs log JOB_ID --region us-central1 \
    | grep ERROR

# Export logs to Cloud Storage
gcloud logging read "resource.type=dataflow_step AND resource.labels.job_id=JOB_ID" \
    --format json > logs.json
```

---

## 🐛 Common Issues & Solutions

### Issue 1: "Permission Denied" Errors

**Solution:**
```bash
# Verify service account permissions
gcloud projects get-iam-policy your-project-id \
    --flatten="bindings[].members" \
    --filter="bindings.members:dataflow-runner*"

# Add missing roles
gcloud projects add-iam-policy-binding your-project-id \
    --member=serviceAccount:dataflow-runner@your-project-id.iam.gserviceaccount.com \
    --role=roles/dataflow.admin
```

### Issue 2: "Bucket Not Found" Error

**Solution:**
```bash
# Verify bucket exists
gsutil ls gs://your-bucket

# Create if missing
gsutil mb gs://your-bucket

# Create subdirectories
gsutil mb gs://your-bucket/temp
gsutil mb gs://your-bucket/staging
```

### Issue 3: "JSON Parsing Error" in Streaming

**Solution:**
```bash
# Validate JSON format before publishing
echo '{"event_type":"test","value":100}' | jq .

# Publish with proper JSON
gcloud pubsub topics publish events-stream \
    --message '{"event_type":"valid_json","timestamp":"2024-01-01T00:00:00Z"}'
```

### Issue 4: Slow Job Execution

**Solution:**
```bash
# Increase worker count
--num_workers 8 --max_num_workers 20

# Enable Streaming Engine (streaming only)
--enable_streaming_engine

# Use better machine types
--worker_machine_type n1-standard-8

# Check for data skew
bq query '
  SELECT key, COUNT(*) as count
  FROM `project:dataset.table`
  GROUP BY key
  ORDER BY count DESC
'
```

### Issue 5: BigQuery Insert Errors

**Solution:**
```bash
# Check table schema
bq show --schema --format=prettyjson \
    project:dataset.table

# Verify data types match schema
# Use create_disposition and write_disposition properly:
# - CREATE_IF_NEEDED: Auto-create table
# - WRITE_APPEND: Add to existing table
# - WRITE_TRUNCATE: Clear and replace
```

---

## 📚 Additional Resources

### Related Project: Original Dataflow Project
See `../dataflow_realtime_project/` for:
- Advanced pipeline examples
- Windowing strategies
- Late data handling
- Complete documentation

### Apache Beam Documentation
- [Apache Beam Python SDK](https://beam.apache.org/documentation/sdks/python/)
- [Dataflow Templates](https://cloud.google.com/dataflow/docs/concepts/dataflow-templates)

### GCP Documentation
- [Google Cloud Dataflow](https://cloud.google.com/dataflow/docs)
- [Cloud Composer](https://cloud.google.com/composer/docs)
- [Pub/Sub Streaming](https://cloud.google.com/pubsub/docs/quickstarts)

---

## 🎓 Best Practices

1. **Always test locally first** with DirectRunner before deploying to Dataflow
2. **Use environment variables** for sensitive configurations
3. **Implement proper error handling** and dead-letter queues
4. **Monitor job metrics** continuously in production
5. **Set up alerts** for failed pipelines and data quality issues
6. **Version your pipelines** and maintain rollback procedures
7. **Document your transformations** and data lineage
8. **Use IAM roles** with minimal required permissions

---

**Last Updated:** 2024
**Version:** 1.0
