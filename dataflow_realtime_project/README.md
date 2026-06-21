# GCP Dataflow Real-Time Processing Project

Complete guide for building real-time data pipelines using Google Cloud Dataflow with Apache Beam.

## Project Overview

This project demonstrates:
- **Three windowing strategies**: Tumbling (Fixed), Sliding, and Session windows
- **Late data handling**: Watermarks, allowed lateness, and trigger configurations
- **Real-time ingestion**: Google Pub/Sub integration
- **Data storage**: BigQuery integration with streaming inserts
- **Multiple data sources**: E-commerce transactions, IoT sensors, and user activity logs

## Project Structure

```
dataflow_realtime_project/
├── GCP_DATAFLOW_REALTIME_GUIDE.md    # Complete guide with examples
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── sample_data/
│   ├── transactions.csv              # E-commerce transaction data
│   ├── sensor_data.csv               # IoT sensor readings
│   └── user_activity.csv             # User activity logs
├── pipeline_code/
│   ├── __init__.py
│   ├── base_pipeline.py              # Base classes and utilities
│   ├── tumbling_window_pipeline.py   # Fixed window aggregation
│   ├── sliding_window_pipeline.py    # Moving average calculations
│   ├── session_window_pipeline.py    # User session analysis
│   ├── advanced_pipeline_late_data.py # Late data & watermark handling
│   └── transforms.py                 # Reusable transformations
└── bigquery_schemas/
    ├── transactions_schema.json      # Raw transaction schema
    ├── aggregated_sales_schema.json  # Sales summary schema
    └── sensor_data_schema.json       # Sensor metrics schema
```

## Quick Start

### Prerequisites

1. **Google Cloud Project**
   ```bash
   export PROJECT_ID=your-gcp-project-id
   gcloud config set project $PROJECT_ID
   ```

2. **Enable APIs**
   ```bash
   gcloud services enable dataflow.googleapis.com
   gcloud services enable pubsub.googleapis.com
   gcloud services enable bigquery.googleapis.com
   ```

3. **Create Service Account**
   ```bash
   gcloud iam service-accounts create dataflow-runner \
     --display-name="Dataflow Runner"
   
   gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member="serviceAccount:dataflow-runner@$PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/dataflow.admin"
   ```

### Installation

```bash
# Clone or navigate to project directory
cd dataflow_realtime_project

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Setup Guide

### 1. Create Pub/Sub Topics

```bash
# For sales transactions
gcloud pubsub topics create dataflow-transactions
gcloud pubsub subscriptions create dataflow-transactions-sub \
  --topic=dataflow-transactions

# For sensor data
gcloud pubsub topics create dataflow-sensors
gcloud pubsub subscriptions create dataflow-sensors-sub \
  --topic=dataflow-sensors

# For user activity
gcloud pubsub topics create dataflow-activities
gcloud pubsub subscriptions create dataflow-activities-sub \
  --topic=dataflow-activities
```

### 2. Create BigQuery Dataset and Tables

```bash
# Create dataset
bq mk --dataset --location=US dataflow_demo

# Create tables
bq mk --table dataflow_demo.transactions_raw \
  bigquery_schemas/transactions_schema.json

bq mk --table dataflow_demo.hourly_sales_summary \
  bigquery_schemas/aggregated_sales_schema.json

bq mk --table dataflow_demo.sensor_metrics \
  bigquery_schemas/sensor_data_schema.json

bq mk --table dataflow_demo.user_sessions
```

### 3. Publish Sample Data to Pub/Sub

```bash
# Publish transactions
while read line; do
  gcloud pubsub topics publish dataflow-transactions --message="$line"
done < sample_data/transactions.csv

# Publish sensor data
while read line; do
  gcloud pubsub topics publish dataflow-sensors --message="$line"
done < sample_data/sensor_data.csv

# Publish activity logs
while read line; do
  gcloud pubsub topics publish dataflow-activities --message="$line"
done < sample_data/user_activity.csv
```

## Running Pipelines

### Local Testing

```bash
# Test tumbling window pipeline
python pipeline_code/tumbling_window_pipeline.py \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
  --output_table=$PROJECT_ID:dataflow_demo.hourly_sales_summary

# Test sliding window pipeline
python pipeline_code/sliding_window_pipeline.py \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-sensors \
  --output_table=$PROJECT_ID:dataflow_demo.sensor_metrics

# Test session window pipeline
python pipeline_code/session_window_pipeline.py \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-activities \
  --output_table=$PROJECT_ID:dataflow_demo.user_sessions
```

### Deploy to Google Cloud Dataflow

```bash
# Create GCS bucket for staging
gsutil mb gs://$PROJECT_ID-dataflow-bucket

# Deploy pipeline
python pipeline_code/tumbling_window_pipeline.py \
  --project=$PROJECT_ID \
  --runner=DataflowRunner \
  --region=us-central1 \
  --temp_location=gs://$PROJECT_ID-dataflow-bucket/temp \
  --staging_location=gs://$PROJECT_ID-dataflow-bucket/staging \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
  --output_table=$PROJECT_ID:dataflow_demo.hourly_sales_summary
```

## Windowing Strategies

### Tumbling Window (Fixed)
- **Duration**: 1 hour (3600 seconds)
- **Use Case**: Hourly sales reports by region
- **File**: `pipeline_code/tumbling_window_pipeline.py`

```bash
python pipeline_code/tumbling_window_pipeline.py \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
  --output_table=$PROJECT_ID:dataflow_demo.hourly_sales_summary \
  --window_duration=3600
```

### Sliding Window
- **Window Size**: 10 minutes (600 seconds)
- **Slide Period**: 5 minutes (300 seconds)
- **Use Case**: Moving averages for sensor data
- **File**: `pipeline_code/sliding_window_pipeline.py`

```bash
python pipeline_code/sliding_window_pipeline.py \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-sensors \
  --output_table=$PROJECT_ID:dataflow_demo.sensor_metrics \
  --window_size=600 \
  --window_period=300
```

### Session Window
- **Gap Duration**: 30 minutes (1800 seconds)
- **Use Case**: User session analysis
- **File**: `pipeline_code/session_window_pipeline.py`

```bash
python pipeline_code/session_window_pipeline.py \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-activities \
  --output_table=$PROJECT_ID:dataflow_demo.user_sessions \
  --gap_duration=1800
```

## Late Data & Watermark Handling

### Pipeline with Late Data Support
- **Allowed Lateness**: 10 minutes (600 seconds)
- **File**: `pipeline_code/advanced_pipeline_late_data.py`

```bash
python pipeline_code/advanced_pipeline_late_data.py \
  --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
  --output_table=$PROJECT_ID:dataflow_demo.transactions_with_late_data \
  --allowed_lateness=600 \
  --accumulation_mode=ACCUMULATING
```

### Accumulation Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| ACCUMULATING | Retains previous results | Sales totals |
| DISCARDING | Discards previous results | Distinct counts |
| ACCUMULATING_AND_RETRACTING | Retracts previous results | Corrections |

## Monitoring & Debugging

### Check Pipeline Status

```bash
# List active jobs
gcloud dataflow jobs list

# Get job details
gcloud dataflow jobs describe JOB_ID

# View logs
gcloud logging read "resource.type=dataflow_step AND resource.labels.job_id=JOB_ID"
```

### Query Results in BigQuery

```bash
# Check sales summary
bq query --use_legacy_sql=false '
  SELECT 
    window_start,
    region,
    total_sales,
    transaction_count,
    avg_transaction_value
  FROM `'$PROJECT_ID'.dataflow_demo.hourly_sales_summary`
  ORDER BY window_start DESC
  LIMIT 10'

# Check sensor metrics
bq query --use_legacy_sql=false '
  SELECT 
    window_start,
    sensor_id,
    location,
    avg_temperature,
    avg_humidity
  FROM `'$PROJECT_ID'.dataflow_demo.sensor_metrics`
  ORDER BY window_start DESC
  LIMIT 10'
```

## Sample Data Formats

### Transaction Data (E-Commerce)
```csv
transaction_id,user_id,timestamp,product_category,amount_usd,region
TXN001,USR123,2024-06-12T09:15:32Z,Electronics,149.99,North America
```

### Sensor Data (IoT)
```csv
sensor_id,device_id,timestamp,temperature,humidity,location
SENSOR001,IOT_DEV_001,2024-06-12T10:00:00Z,22.5,45.2,Building_A_Floor_1
```

### User Activity Data
```csv
event_id,user_id,timestamp,event_type,event_action,session_id
EVT001,USR123,2024-06-12T14:00:00Z,PAGE_VIEW,home,SESS_123_001
```

## Common Issues & Solutions

### Issue: "Credentials not found"
```bash
# Set up authentication
gcloud auth application-default login
```

### Issue: "Topic not found"
```bash
# Verify topic exists
gcloud pubsub topics list

# Check topic subscription
gcloud pubsub subscriptions list --filter="topic:dataflow-transactions"
```

### Issue: "Table not found in BigQuery"
```bash
# List all tables in dataset
bq ls -t dataflow_demo

# Check table schema
bq show dataflow_demo.hourly_sales_summary
```

## Performance Tuning

### Dataflow Runner Options

```bash
--num_workers=4               # Number of worker VMs
--worker_machine_type=n1-standard-4  # Machine type
--disk_size_gb=100           # Worker disk size
--max_num_workers=10         # Autoscaling max
--service_account=EMAIL      # Service account
```

### Pipeline Optimization

1. **Batch Size**: Adjust based on message frequency
2. **Window Duration**: Balance latency vs throughput
3. **Allowed Lateness**: Set based on SLA requirements
4. **Triggers**: Configure early/late triggers for updates

## Next Steps

1. Customize pipeline transforms in `pipeline_code/transforms.py`
2. Add more complex aggregations for your use case
3. Implement custom triggers based on business requirements
4. Set up alerts and dashboards in Cloud Monitoring
5. Optimize BigQuery partitioning for cost efficiency

## Resources

- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Google Cloud Dataflow](https://cloud.google.com/dataflow)
- [Pub/Sub Documentation](https://cloud.google.com/pubsub)
- [BigQuery Documentation](https://cloud.google.com/bigquery)
- [Beam SQL](https://beam.apache.org/documentation/dsls/sql/overview/)

## License

This project is provided as-is for educational purposes.
