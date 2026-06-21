# GCP Dataflow Tutorial: Step-by-Step Walkthrough

## Tutorial Overview

This tutorial walks you through building, deploying, and monitoring a real-time Dataflow pipeline from scratch.

**Time Required**: 45-60 minutes  
**Prerequisites**: GCP account, gcloud CLI, Python 3.7+

---

## Part 1: Local Setup (15 minutes)

### Step 1.1: Create Python Virtual Environment

```bash
# Navigate to project
cd dataflow_realtime_project

# Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# Verify activation
python --version  # Should show Python 3.7+
```

### Step 1.2: Install Dependencies

```bash
# Install Apache Beam and GCP libraries
pip install -r requirements.txt

# Verify installation
python -c "import apache_beam; print(apache_beam.__version__)"
```

### Step 1.3: Examine Sample Data

```bash
# View transactions data
head -5 sample_data/transactions.csv
# Output:
# transaction_id,user_id,timestamp,product_category,amount_usd,region
# TXN001,USR123,2024-06-12T09:15:32Z,Electronics,149.99,North America

# Count records
wc -l sample_data/transactions.csv  # 26 lines (25 data + 1 header)
```

---

## Part 2: Google Cloud Setup (20 minutes)

### Step 2.1: Initialize GCP Project

```bash
# Set project ID
export PROJECT_ID=your-gcp-project-id
export REGION=us-central1

# Authenticate
gcloud auth application-default login
# Browser will open for authentication

# Set default project
gcloud config set project $PROJECT_ID

# Verify
gcloud config get-value project
```

### Step 2.2: Enable Required APIs

```bash
# Enable APIs (may take 1-2 minutes)
gcloud services enable dataflow.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage-api.googleapis.com
gcloud services enable logging.googleapis.com

# Verify APIs are enabled
gcloud services list --enabled | grep -E "dataflow|pubsub|bigquery"
```

### Step 2.3: Create Cloud Storage Bucket

```bash
# Create bucket for Dataflow staging
gsutil mb -p $PROJECT_ID -l $REGION \
  gs://$PROJECT_ID-dataflow-bucket

# Create subdirectories
gsutil mkdir gs://$PROJECT_ID-dataflow-bucket/temp
gsutil mkdir gs://$PROJECT_ID-dataflow-bucket/staging

# List bucket
gsutil ls gs://$PROJECT_ID-dataflow-bucket/
```

### Step 2.4: Create Pub/Sub Topics

```bash
# Create transactions topic
gcloud pubsub topics create dataflow-transactions

# Create subscription
gcloud pubsub subscriptions create dataflow-transactions-sub \
  --topic=dataflow-transactions

# Verify creation
gcloud pubsub topics list
gcloud pubsub subscriptions list --filter="topic:dataflow-transactions"
```

### Step 2.5: Create BigQuery Dataset and Table

```bash
# Create dataset
bq mk --dataset --location=US dataflow_demo

# Create table with schema
bq mk --table \
  dataflow_demo.hourly_sales_summary \
  bigquery_schemas/aggregated_sales_schema.json

# Verify creation
bq ls dataflow_demo
bq show dataflow_demo.hourly_sales_summary
```

---

## Part 3: Local Testing (10 minutes)

### Step 3.1: Publish Test Data to Pub/Sub

```bash
# Publish single test message
gcloud pubsub topics publish dataflow-transactions \
  --message='{"transaction_id":"TXN001","user_id":"USR123","timestamp":"2024-06-12T09:15:32Z","product_category":"Electronics","amount_usd":149.99,"region":"North America"}'

# Verify message was published
echo "Message published. Check Pub/Sub metrics in Console."
```

### Step 3.2: Run Pipeline Locally

```bash
# Run with DirectRunner (local execution)
python pipeline_code/tumbling_window_pipeline.py \
  --runner=DirectRunner \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
  --output_table=$PROJECT_ID:dataflow_demo.hourly_sales_summary

# Expected output:
# - Pipeline setup messages
# - Processing messages
# - Completion message
# Duration: 2-3 minutes
```

### Step 3.3: Check BigQuery Results

```bash
# Query results (wait a minute after pipeline completes)
bq query --use_legacy_sql=false --max_rows=10 '
  SELECT 
    window_start,
    region,
    total_sales,
    transaction_count,
    avg_transaction_value
  FROM `'$PROJECT_ID'.dataflow_demo.hourly_sales_summary`
  ORDER BY window_start DESC'

# Expected: Row with sales data
```

---

## Part 4: Publish Batch Data (5 minutes)

### Step 4.1: Prepare Data for Publishing

```bash
# Create simple publisher script
cat > publish_data.py << 'EOF'
import csv
import json
import subprocess
import sys

def publish_csv_to_pubsub(csv_file, topic_name):
    """Publish CSV records to Pub/Sub topic"""
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            msg = json.dumps(row)
            subprocess.run([
                'gcloud', 'pubsub', 'topics', 'publish', topic_name,
                '--message', msg
            ], check=True)
            count += 1
            print(f"Published record {count}")
    print(f"Total records published: {count}")

if __name__ == '__main__':
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'sample_data/transactions.csv'
    topic = 'dataflow-transactions'
    publish_csv_to_pubsub(csv_file, topic)
EOF

python publish_data.py sample_data/transactions.csv
```

### Step 4.2: Monitor Pub/Sub

```bash
# Check subscription metrics
gcloud pubsub subscriptions describe dataflow-transactions-sub \
  --format="value(topic,ackDeadlineSeconds)"

# Check topic metrics
gcloud monitoring time-series list \
  --filter='metric.type="pubsub.googleapis.com/topic/publish_message_operation_count" AND resource.labels.topic_id="dataflow-transactions"' \
  --format=table
```

---

## Part 5: Deploy to Dataflow (15 minutes)

### Step 5.1: Create Service Account (Optional but Recommended)

```bash
# Create service account
gcloud iam service-accounts create dataflow-runner \
  --display-name="Dataflow Pipeline Runner"

# Get email
SA_EMAIL=dataflow-runner@$PROJECT_ID.iam.gserviceaccount.com

# Grant required roles
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/dataflow.admin

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/pubsub.editor

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/storage.objectAdmin
```

### Step 5.2: Deploy Pipeline to Dataflow

```bash
# Deploy tumbling window pipeline
python pipeline_code/tumbling_window_pipeline.py \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=gs://$PROJECT_ID-dataflow-bucket/temp \
  --staging_location=gs://$PROJECT_ID-dataflow-bucket/staging \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
  --output_table=$PROJECT_ID:dataflow_demo.hourly_sales_summary \
  --num_workers=2 \
  --worker_machine_type=n1-standard-2 \
  --disk_size_gb=50

# Expected output:
# - Dataflow job created
# - Job ID returned
# - Workers starting up
```

### Step 5.3: Monitor Job Deployment

```bash
# Get job ID
export JOB_ID=$(gcloud dataflow jobs list --region=$REGION --format="value(id)" | head -1)

# Watch job status
gcloud dataflow jobs describe $JOB_ID --region=$REGION

# Alternative: Monitor in Cloud Console
echo "https://console.cloud.google.com/dataflow/jobs/$REGION/$JOB_ID"

# Check worker status
gcloud dataflow jobs describe $JOB_ID --region=$REGION \
  --format="value(currentWorkerCount, totalWorkers)"
```

### Step 5.4: Publish More Data

```bash
# While pipeline is running, publish more messages
gcloud pubsub topics publish dataflow-transactions \
  --message='{"transaction_id":"TXN_NEW","user_id":"USR999","timestamp":"2024-06-12T14:00:00Z","product_category":"Electronics","amount_usd":299.99,"region":"Europe"}'

# Check logs
gcloud logging read \
  "resource.type=dataflow_step AND resource.labels.job_id=$JOB_ID" \
  --limit=20 \
  --format=text
```

---

## Part 6: Monitor and Query Results (10 minutes)

### Step 6.1: Query Aggregated Results

```bash
# Query hourly sales summary
bq query --use_legacy_sql=false '
  SELECT 
    window_start,
    region,
    total_sales,
    transaction_count,
    avg_transaction_value,
    top_category,
    processing_time
  FROM `'$PROJECT_ID'.dataflow_demo.hourly_sales_summary`
  WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
  ORDER BY window_start DESC'

# Expected columns:
# window_start | region | total_sales | transaction_count | avg_transaction_value | top_category | processing_time
```

### Step 6.2: Analyze Late Data

```bash
# Query to understand data freshness
bq query --use_legacy_sql=false '
  SELECT 
    COUNT(*) as total_records,
    MAX(window_start) as most_recent_window,
    MIN(window_start) as oldest_window,
    TIMESTAMP_DIFF(
      CURRENT_TIMESTAMP(), 
      MAX(processing_time), 
      MINUTE
    ) as data_age_minutes
  FROM `'$PROJECT_ID'.dataflow_demo.hourly_sales_summary`'
```

### Step 6.3: Monitor Pipeline Metrics

```bash
# Get current metrics
gcloud dataflow jobs describe $JOB_ID --region=$REGION \
  --format="table(
    id,
    name,
    state,
    currentWorkerCount,
    totalWorkers,
    createTime
  )"

# Get execution metrics
gcloud dataflow jobs show $JOB_ID --region=$REGION \
  --format="table(
    location_name,
    type,
    value
  )" 2>/dev/null || echo "Metrics not yet available"
```

---

## Part 7: Experiment with Other Windowing Strategies (Optional)

### Step 7.1: Try Sliding Windows

```bash
# First, create sensor data topic
gcloud pubsub topics create dataflow-sensors
gcloud pubsub subscriptions create dataflow-sensors-sub \
  --topic=dataflow-sensors

# Publish sensor data
python -c "
import csv, json, subprocess
with open('sample_data/sensor_data.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        subprocess.run([
            'gcloud', 'pubsub', 'topics', 'publish',
            'dataflow-sensors', '--message', json.dumps(row)
        ])
"

# Deploy sliding window pipeline
python pipeline_code/sliding_window_pipeline.py \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=gs://$PROJECT_ID-dataflow-bucket/temp \
  --staging_location=gs://$PROJECT_ID-dataflow-bucket/staging \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-sensors \
  --output_table=$PROJECT_ID:dataflow_demo.sensor_metrics
```

### Step 7.2: Try Session Windows

```bash
# Create activity topic
gcloud pubsub topics create dataflow-activities
gcloud pubsub subscriptions create dataflow-activities-sub \
  --topic=dataflow-activities

# Publish activity data
python -c "
import csv, json, subprocess
with open('sample_data/user_activity.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        subprocess.run([
            'gcloud', 'pubsub', 'topics', 'publish',
            'dataflow-activities', '--message', json.dumps(row)
        ])
"

# Deploy session window pipeline
python pipeline_code/session_window_pipeline.py \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=gs://$PROJECT_ID-dataflow-bucket/temp \
  --staging_location=gs://$PROJECT_ID-dataflow-bucket/staging \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-activities \
  --output_table=$PROJECT_ID:dataflow_demo.user_sessions
```

---

## Part 8: Cleanup (Optional)

### Step 8.1: Delete Pipeline Jobs

```bash
# Drain and cancel job (preserves data)
gcloud dataflow jobs update $JOB_ID \
  --region=$REGION \
  --update-option update_drain_active_steps

# Or force cancel (immediately stops)
gcloud dataflow jobs cancel $JOB_ID --region=$REGION
```

### Step 8.2: Clean Up Resources

```bash
# Delete Pub/Sub resources
gcloud pubsub subscriptions delete dataflow-transactions-sub
gcloud pubsub topics delete dataflow-transactions

# Delete BigQuery dataset
bq rm -r -d dataflow_demo

# Delete GCS bucket
gsutil -m rm -r gs://$PROJECT_ID-dataflow-bucket

# Delete service account
gcloud iam service-accounts delete $SA_EMAIL --quiet
```

---

## Troubleshooting Guide

### Issue: "Authentication Required"
**Solution:**
```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project $PROJECT_ID
```

### Issue: "Topic Not Found"
**Solution:**
```bash
# Verify topic exists
gcloud pubsub topics describe dataflow-transactions
# If not found, create it
gcloud pubsub topics create dataflow-transactions
```

### Issue: "Table Not Found in BigQuery"
**Solution:**
```bash
# Verify dataset exists
bq ls dataflow_demo
# Create if needed
bq mk --dataset dataflow_demo
```

### Issue: "No Results in BigQuery After Pipeline Runs"
**Solutions:**
1. Check if pipeline is still running
2. Wait 2-3 minutes for results
3. Verify Pub/Sub has published data
4. Check pipeline logs

---

## Next Steps After Tutorial

1. **Modify Pipeline Logic**
   - Change window durations
   - Add custom transformations
   - Modify aggregation logic

2. **Add Error Handling**
   - Invalid record filtering
   - Dead letter queues
   - Alert configuration

3. **Optimize Performance**
   - Adjust number of workers
   - Tune batch sizes
   - Optimize BigQuery queries

4. **Add New Data Sources**
   - Connect additional topics
   - Implement joins
   - Add side inputs

5. **Production Hardening**
   - Implement retries
   - Add monitoring alerts
   - Set up logging

---

**Congratulations! You've successfully built and deployed a real-time Dataflow pipeline!**

For more details, refer to:
- GCP_DATAFLOW_REALTIME_GUIDE.md
- QUICK_REFERENCE.md
- Individual pipeline code files
