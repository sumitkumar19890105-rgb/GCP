# GCP Configuration Guide

## Environment Setup

### 1. Set Required Environment Variables

```bash
# GCP Project
export PROJECT_ID=your-gcp-project-id
export REGION=us-central1
export BUCKET_NAME=$PROJECT_ID-dataflow-bucket

# Dataflow Settings
export NUM_WORKERS=2
export WORKER_MACHINE_TYPE=n1-standard-2
export DISK_SIZE_GB=50

# BigQuery Settings
export BQ_DATASET=dataflow_demo
export BQ_LOCATION=US
```

### 2. Google Cloud Setup Commands

```bash
# Set default project
gcloud config set project $PROJECT_ID

# Set default region
gcloud config set compute/region $REGION

# Enable required APIs
gcloud services enable \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  storage-api.googleapis.com \
  cloudlogging.googleapis.com \
  monitoring.googleapis.com

# Authenticate
gcloud auth application-default login
```

## Pub/Sub Setup

### Topic and Subscription Creation

```bash
# Transactions Topic
gcloud pubsub topics create dataflow-transactions \
  --message-retention-duration=24h

gcloud pubsub subscriptions create dataflow-transactions-sub \
  --topic=dataflow-transactions \
  --ack-deadline=60 \
  --message-retention-duration=7d

# Sensors Topic
gcloud pubsub topics create dataflow-sensors

gcloud pubsub subscriptions create dataflow-sensors-sub \
  --topic=dataflow-sensors

# Activities Topic
gcloud pubsub topics create dataflow-activities

gcloud pubsub subscriptions create dataflow-activities-sub \
  --topic=dataflow-activities
```

### Publishing Test Messages

```bash
# Single transaction message
gcloud pubsub topics publish dataflow-transactions \
  --message='{"transaction_id":"TXN001","user_id":"USR123","timestamp":"2024-06-12T09:15:32Z","product_category":"Electronics","amount_usd":149.99,"region":"North America"}'

# Batch publish from CSV
python -c "
import csv
import json
import subprocess

with open('sample_data/transactions.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        msg = json.dumps(row)
        subprocess.run([
            'gcloud', 'pubsub', 'topics', 'publish',
            'dataflow-transactions', '--message', msg
        ])
"
```

## BigQuery Setup

### Dataset Creation

```bash
# Create dataset
bq mk \
  --dataset \
  --location=$BQ_LOCATION \
  --description="Dataflow demo dataset" \
  $BQ_DATASET

# Create tables from schemas
bq mk \
  --table \
  $BQ_DATASET.transactions_raw \
  bigquery_schemas/transactions_schema.json

bq mk \
  --table \
  $BQ_DATASET.hourly_sales_summary \
  bigquery_schemas/aggregated_sales_schema.json

bq mk \
  --table \
  $BQ_DATASET.sensor_metrics \
  bigquery_schemas/sensor_data_schema.json
```

### Enable Streaming Inserts

```bash
# BigQuery streaming is enabled by default
# Verify table settings
bq show --schema --format=prettyjson $BQ_DATASET.hourly_sales_summary
```

## Cloud Storage Setup

### Create Staging Bucket

```bash
# Create bucket for Dataflow staging
gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME

# Create subdirectories
gsutil -m mkdir \
  gs://$BUCKET_NAME/staging \
  gs://$BUCKET_NAME/temp \
  gs://$BUCKET_NAME/logs
```

## Service Account Configuration

### Create Dataflow Service Account

```bash
# Create service account
gcloud iam service-accounts create dataflow-runner \
  --display-name="Dataflow Pipeline Runner"

# Get service account email
SA_EMAIL=dataflow-runner@$PROJECT_ID.iam.gserviceaccount.com

# Grant necessary roles
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/dataflow.admin

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/dataflow.worker

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/pubsub.editor

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member=serviceAccount:$SA_EMAIL \
  --role=roles/storage.admin
```

## Pipeline Execution Options

### Local Execution (Testing)

```bash
python pipeline_code/tumbling_window_pipeline.py \
  --runner=DirectRunner \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
  --output_table=$PROJECT_ID:$BQ_DATASET.hourly_sales_summary
```

### Cloud Dataflow Execution (Production)

```bash
python pipeline_code/tumbling_window_pipeline.py \
  --runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --temp_location=gs://$BUCKET_NAME/temp \
  --staging_location=gs://$BUCKET_NAME/staging \
  --service_account_email=$SA_EMAIL \
  --num_workers=$NUM_WORKERS \
  --worker_machine_type=$WORKER_MACHINE_TYPE \
  --disk_size_gb=$DISK_SIZE_GB \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
  --output_table=$PROJECT_ID:$BQ_DATASET.hourly_sales_summary
```

## Monitoring Setup

### Cloud Logging

```bash
# Create log sink for Dataflow
gcloud logging sinks create dataflow-logs \
  bigquery.googleapis.com/projects/$PROJECT_ID/datasets/$BQ_DATASET \
  --log-filter='resource.type="dataflow_step"'
```

### Cloud Monitoring Alerts

```bash
# Create alert for failed jobs
gcloud alpha monitoring policies create \
  --notification-channels=CHANNEL_ID \
  --display-name="Dataflow Job Failures" \
  --condition-display-name="Job Failure" \
  --condition-threshold-value=1 \
  --condition-threshold-duration=60s
```

## Cleaning Up Resources

```bash
# Delete Pub/Sub topics
gcloud pubsub topics delete dataflow-transactions
gcloud pubsub topics delete dataflow-sensors
gcloud pubsub topics delete dataflow-activities

# Delete BigQuery dataset
bq rm -r -d -f $BQ_DATASET

# Delete GCS bucket
gsutil -m rm -r gs://$BUCKET_NAME

# Delete service account
gcloud iam service-accounts delete $SA_EMAIL

# Disable APIs (optional)
gcloud services disable dataflow.googleapis.com
```

## Troubleshooting

### Check Service Account Permissions

```bash
# List service account roles
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:$SA_EMAIL"
```

### View Pipeline Metrics

```bash
# Export metrics to CSV
gcloud monitoring time-series list \
  --filter='metric.type="dataflow.googleapis.com/job/user_total_workers"' \
  --format=csv
```

### Debug Local Pipeline

```bash
# Run with verbose logging
export BEAM_LOG_LEVEL=DEBUG
python -u pipeline_code/tumbling_window_pipeline.py \
  --runner=DirectRunner \
  --input_topic=... \
  --output_table=... \
  2>&1 | tee pipeline.log
```
