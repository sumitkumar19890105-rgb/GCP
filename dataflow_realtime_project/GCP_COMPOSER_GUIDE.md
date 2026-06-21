# Running Dataflow Jobs from GCP Composer (Apache Airflow)

This guide provides step-by-step instructions on how to run Dataflow pipelines using Google Cloud Composer (managed Apache Airflow) with templates and code organization best practices.

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Project Structure & Code Organization](#project-structure--code-organization)
4. [GCP Composer Setup](#gcp-composer-setup)
5. [Creating DAG Files](#creating-dag-files)
6. [Deploying Code to Composer](#deploying-code-to-composer)
7. [Running Jobs](#running-jobs)
8. [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## Overview

GCP Composer is a managed Apache Airflow service that allows you to orchestrate and schedule Dataflow jobs. Instead of manually running pipelines, you define workflows in Python (DAGs - Directed Acyclic Graphs) that schedule and trigger your Dataflow jobs automatically.

### Benefits:
- **Orchestration**: Chain multiple Dataflow jobs and other GCP services
- **Scheduling**: Run jobs on a schedule (hourly, daily, monthly, etc.)
- **Monitoring**: Built-in logging and failure alerts
- **Retry Logic**: Automatic retries with exponential backoff
- **Dependencies**: Define task dependencies and parallel execution

---

## Prerequisites

### GCP Requirements:
1. **Active GCP Project** with billing enabled
2. **Required APIs Enabled:**
   ```bash
   gcloud services enable \
     composer.googleapis.com \
     dataflow.googleapis.com \
     pubsub.googleapis.com \
     bigquery.googleapis.com \
     storage-api.googleapis.com
   ```

3. **Roles & Permissions** (Service Account must have):
   - `roles/dataflow.admin` - Run Dataflow jobs
   - `roles/bigquery.dataEditor` - Write to BigQuery
   - `roles/pubsub.editor` - Access Pub/Sub topics
   - `roles/iam.serviceAccountUser` - Use service accounts

### Local Requirements:
- Google Cloud SDK (`gcloud` CLI)
- Python 3.8+
- Apache Airflow understanding (basic familiarity)

---

## Project Structure & Code Organization

### Recommended Directory Structure in Composer

```
gs://your-bucket/dags/
├── dataflow_dags/
│   ├── tumbling_window_dag.py
│   ├── sliding_window_dag.py
│   ├── session_window_dag.py
│   └── advanced_pipeline_dag.py
├── templates/
│   ├── tumbling_window_template.py
│   ├── sliding_window_template.py
│   ├── session_window_template.py
│   └── advanced_template.py
└── utils/
    ├── config.py
    ├── helpers.py
    └── transformations.py

gs://your-bucket/data/
├── configs/
│   ├── tumbling_config.json
│   ├── sliding_config.json
│   └── session_config.json
└── sample_data/
    ├── transactions.csv
    ├── sensor_data.csv
    └── user_activity.csv
```

### Local Project Structure (Before Upload)

```
dataflow_realtime_project/
├── composer_dags/                  # NEW: DAG files for Composer
│   ├── __init__.py
│   ├── config.py                   # Shared configuration
│   ├── tumbling_window_dag.py
│   ├── sliding_window_dag.py
│   ├── session_window_dag.py
│   └── advanced_pipeline_dag.py
├── composer_templates/             # NEW: Parameterized templates
│   ├── tumbling_window_template.py
│   ├── sliding_window_template.py
│   ├── session_window_template.py
│   └── advanced_template.py
├── pipeline_code/                  # Existing pipeline code
│   ├── base_pipeline.py
│   ├── tumbling_window_pipeline.py
│   ├── sliding_window_pipeline.py
│   ├── session_window_pipeline.py
│   ├── advanced_pipeline_late_data.py
│   └── transforms.py
├── composer_configs/               # NEW: Configuration files
│   ├── dev_config.json
│   ├── prod_config.json
│   └── pipeline_params.yaml
└── requirements.txt
```

---

## GCP Composer Setup

### Step 1: Create a Composer Environment

```bash
# Set variables
export PROJECT_ID=your-gcp-project-id
export COMPOSER_ENV=dataflow-orchestrator
export REGION=us-central1
export BUCKET_NAME=$PROJECT_ID-composer-bucket

# Create Composer environment (this takes 10-15 minutes)
gcloud composer environments create $COMPOSER_ENV \
  --location=$REGION \
  --python-version=3 \
  --machine-type=n1-standard-4 \
  --node-count=3 \
  --project=$PROJECT_ID

# Wait for creation to complete
gcloud composer environments describe $COMPOSER_ENV --location=$REGION
```

### Step 2: Get Composer Bucket Details

```bash
# Get the storage bucket used by Composer
export COMPOSER_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
  --location=$REGION \
  --format='value(config.dagGcsPrefix)' | sed 's|/dags||')

echo "Composer Bucket: $COMPOSER_BUCKET"
# Example: gs://us-central1-dataflow-orchestrator-a1b2c3d4-bucket

# This bucket has structure:
# gs://bucket/dags/           <- Upload DAG files here
# gs://bucket/data/           <- Upload data/config files here
# gs://bucket/plugins/        <- Upload custom plugins here
```

### Step 3: Install Additional Packages (If Needed)

```bash
# Check installed packages
gcloud composer environments describe $COMPOSER_ENV \
  --location=$REGION --format="value(config.pypiPackages)"

# Add Apache Beam package if needed
gcloud composer environments update $COMPOSER_ENV \
  --location=$REGION \
  --update-pypi-packages-from-file requirements.txt
```

---

## Creating DAG Files

### Understanding DAGs

A DAG (Directed Acyclic Graph) defines:
- **Tasks**: Individual operations (e.g., run a Dataflow job)
- **Dependencies**: Order of execution
- **Schedule**: When the DAG runs

### Example 1: Simple Tumbling Window DAG

Create `composer_dags/tumbling_window_dag.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator
)
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# DAG definition
with DAG(
    dag_id='tumbling_window_pipeline',
    default_args=default_args,
    description='Tumbling window aggregation for sales data',
    schedule_interval='0 */1 * * *',  # Every hour
    catchup=False,
    tags=['dataflow', 'realtime', 'sales'],
) as dag:
    
    # Configuration
    PROJECT_ID = 'your-project-id'
    REGION = 'us-central1'
    BUCKET = f'gs://{PROJECT_ID}-dataflow-bucket'
    TEMPLATE_PATH = f'{BUCKET}/templates/tumbling_window_template'
    BQ_DATASET = 'dataflow_demo'
    
    # Task 1: Create BigQuery Dataset (runs only once)
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id=BQ_DATASET,
        project_id=PROJECT_ID,
        exists_ok=True,  # Don't fail if exists
    )
    
    # Task 2: Create BigQuery Table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_bq_table',
        dataset_id=BQ_DATASET,
        table_id='aggregated_sales',
        project_id=PROJECT_ID,
        schema_fields=[
            {"name": "window_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "window_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "total_sales", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "transaction_count", "type": "INT64", "mode": "NULLABLE"},
            {"name": "region", "type": "STRING", "mode": "NULLABLE"},
        ],
        exists_ok=True,
    )
    
    # Task 3: Run Dataflow Job
    run_dataflow_job = DataflowTemplatedJobStartOperator(
        task_id='run_tumbling_window_job',
        template_location=TEMPLATE_PATH,
        project_id=PROJECT_ID,
        location=REGION,
        job_name='tumbling-window-{{ ds_nodash }}',  # Date-based job name
        parameters={
            'input_topic': 'projects/your-project-id/topics/dataflow-transactions',
            'output_table': f'{PROJECT_ID}:{BQ_DATASET}.aggregated_sales',
            'window_size': '3600',  # 1 hour in seconds
        },
        environment={
            'machineType': 'n1-standard-2',
            'numWorkers': 2,
            'maxWorkers': 5,
            'diskSizeGb': 50,
            'tempLocation': f'{BUCKET}/temp',
        },
        wait_until_finished=False,  # Don't block DAG while job runs
    )
    
    # Define dependencies
    create_dataset >> create_table >> run_dataflow_job
```

### Example 2: Advanced DAG with Multiple Jobs

Create `composer_dags/advanced_pipeline_dag.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='advanced_multi_pipeline',
    default_args=default_args,
    description='Multi-pipeline orchestration with late data handling',
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    tags=['dataflow', 'advanced', 'multi-job'],
) as dag:
    
    PROJECT_ID = 'your-project-id'
    REGION = 'us-central1'
    BUCKET = f'gs://{PROJECT_ID}-dataflow-bucket'
    
    # Configuration
    config = {
        'project': PROJECT_ID,
        'region': REGION,
        'bucket': BUCKET,
        'dataset': 'dataflow_demo',
    }
    
    # Dummy start task
    start = DummyOperator(task_id='start')
    
    # Task group for data ingestion
    with TaskGroup('data_ingestion') as ingest_group:
        ingest_transactions = DataflowTemplatedJobStartOperator(
            task_id='ingest_transactions',
            template_location=f"{BUCKET}/templates/transactions_template",
            project_id=PROJECT_ID,
            location=REGION,
            job_name='ingest-txn-{{ ds }}',
            wait_until_finished=False,
        )
        
        ingest_sensors = DataflowTemplatedJobStartOperator(
            task_id='ingest_sensors',
            template_location=f"{BUCKET}/templates/sensors_template",
            project_id=PROJECT_ID,
            location=REGION,
            job_name='ingest-sensors-{{ ds }}',
            wait_until_finished=False,
        )
    
    # Task group for processing
    with TaskGroup('processing') as process_group:
        tumbling = DataflowTemplatedJobStartOperator(
            task_id='tumbling_window',
            template_location=f"{BUCKET}/templates/tumbling_template",
            project_id=PROJECT_ID,
            location=REGION,
            job_name='tumbling-{{ ds }}',
            wait_until_finished=False,
        )
        
        sliding = DataflowTemplatedJobStartOperator(
            task_id='sliding_window',
            template_location=f"{BUCKET}/templates/sliding_template",
            project_id=PROJECT_ID,
            location=REGION,
            job_name='sliding-{{ ds }}',
            wait_until_finished=False,
        )
        
        session = DataflowTemplatedJobStartOperator(
            task_id='session_window',
            template_location=f"{BUCKET}/templates/session_template",
            project_id=PROJECT_ID,
            location=REGION,
            job_name='session-{{ ds }}',
            wait_until_finished=False,
        )
    
    # Python function for validation
    def validate_output(**context):
        print("Validating output data...")
        return True
    
    validate = PythonOperator(
        task_id='validate_output',
        python_callable=validate_output,
    )
    
    # Dummy end task
    end = DummyOperator(task_id='end')
    
    # Define workflow
    start >> ingest_group >> process_group >> validate >> end
```

---

## Deploying Code to Composer

### Step 1: Prepare Local Files

```bash
# Navigate to your project
cd dataflow_realtime_project

# Create necessary directories locally
mkdir -p composer_dags
mkdir -p composer_templates
mkdir -p composer_configs
```

### Step 2: Upload DAG Files

```bash
# Option A: Upload individual files
gcloud composer environments storage dags import \
  --environment=$COMPOSER_ENV \
  --location=$REGION \
  --source=composer_dags/tumbling_window_dag.py

# Option B: Upload entire directory
gsutil -m cp -r composer_dags/* gs://${COMPOSER_BUCKET}/dags/

# Verify upload
gsutil ls gs://${COMPOSER_BUCKET}/dags/
```

### Step 3: Upload Templates & Utilities

```bash
# Upload pipeline templates
gsutil -m cp -r composer_templates/* gs://${COMPOSER_BUCKET}/data/templates/

# Upload configuration files
gsutil -m cp -r composer_configs/* gs://${COMPOSER_BUCKET}/data/configs/

# Upload utility modules
gsutil cp requirements.txt gs://${COMPOSER_BUCKET}/data/

# Upload sample data
gsutil -m cp -r sample_data/* gs://${COMPOSER_BUCKET}/data/sample_data/
```

### Step 4: Update Composer Environment (If New Dependencies)

```bash
# If requirements.txt has new packages
gcloud composer environments update $COMPOSER_ENV \
  --location=$REGION \
  --update-pypi-packages-from-file requirements.txt \
  --async
```

### Step 5: Upload Custom Python Modules (Optional)

```bash
# If using custom utilities, upload to plugins
gsutil -m cp pipeline_code/*.py gs://${COMPOSER_BUCKET}/plugins/pipeline_code/

# Or keep in dags directory
gsutil -m cp pipeline_code/*.py gs://${COMPOSER_BUCKET}/dags/utils/
```

---

## Running Jobs

### Method 1: Automatic Scheduling

1. **DAGs are automatically detected** by Composer once uploaded to `dags/` folder
2. **Check DAG Status:**
   ```bash
   gcloud composer environments run $COMPOSER_ENV \
     --location=$REGION \
     dags list
   ```

3. **Enable the DAG** (if disabled):
   ```bash
   gcloud composer environments run $COMPOSER_ENV \
     --location=$REGION \
     dags unpause -- tumbling_window_pipeline
   ```

4. **DAG will run** according to the schedule defined in Python
   - Example: `schedule_interval='0 */1 * * *'` runs every hour at minute 0

### Method 2: Manual Trigger via CLI

```bash
# Trigger a DAG run manually
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  dags trigger -- tumbling_window_pipeline

# With specific execution date
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  dags trigger -- tumbling_window_pipeline \
  --execution-date=2024-06-15T10:00:00

# Trigger with configuration overrides
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  dags trigger -- tumbling_window_pipeline \
  --conf '{"env": "prod", "workers": 5}'
```

### Method 3: Trigger via Web UI

1. Open Composer Web UI:
   ```bash
   gcloud composer environments describe $COMPOSER_ENV \
     --location=$REGION \
     --format='value(config.airflowUri)'
   ```

2. Click on the DAG name
3. Click the **▶️ Play** button (Trigger DAG)
4. Optionally modify parameters
5. Click **Trigger**

### Method 4: Programmatic Trigger (Cloud Functions)

Create `cloud_function_trigger.py`:

```python
import requests
import json
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

def trigger_dag(request):
    """HTTP Cloud Function to trigger Composer DAG"""
    
    PROJECT_ID = 'your-project-id'
    REGION = 'us-central1'
    COMPOSER_ENV = 'dataflow-orchestrator'
    DAG_ID = 'tumbling_window_pipeline'
    
    # Get access token
    credentials = Credentials.from_service_account_file(
        'service-account-key.json'
    )
    credentials.refresh(Request())
    access_token = credentials.token
    
    # Get Composer URL
    webserver_url = f'https://console.cloud.google.com/composer/environments/detail/{REGION}/{COMPOSER_ENV}'
    
    # Alternative: Use Airflow API
    airflow_url = f'https://{REGION}-{COMPOSER_ENV}-composer.googleusercontent.com'
    
    # Trigger DAG
    endpoint = f'{airflow_url}/api/v1/dags/{DAG_ID}/dagRuns'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    data = {
        'conf': {
            'env': 'production',
            'workers': 5
        }
    }
    
    response = requests.post(endpoint, headers=headers, json=data)
    return response.json()
```

---

## Monitoring & Troubleshooting

### View DAG Logs

```bash
# List all DAG runs
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  dags list-runs -- -d tumbling_window_pipeline

# Show DAG run status
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  dags list-runs -- -d tumbling_window_pipeline --state success

# Get task logs
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  tasks logs tumbling_window_pipeline run_tumbling_window_job 2024-06-15
```

### Real-Time Monitoring via Cloud Logging

```bash
# Stream Composer logs
gcloud logging read "resource.type=cloud-composer" \
  --project=$PROJECT_ID \
  --limit=50 \
  --format=json \
  --stream

# Filter by DAG
gcloud logging read "resource.type=cloud-composer AND jsonPayload.dag_id=tumbling_window_pipeline" \
  --project=$PROJECT_ID \
  --limit=50
```

### Check Dataflow Job Status

```bash
# List running Dataflow jobs
gcloud dataflow jobs list --project=$PROJECT_ID --filter="STATE:RUNNING"

# Show job details
gcloud dataflow jobs describe <JOB_ID> --project=$PROJECT_ID

# Stream job logs
gcloud dataflow jobs show <JOB_ID> --project=$PROJECT_ID --full
```

### Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| DAG not appearing in UI | Check DAG syntax, look in Cloud Logging |
| Task failures | Check Dataflow job logs in Cloud Console |
| Timeout errors | Increase `wait_until_finished` timeout or set to `False` |
| Permission denied | Verify service account roles and bucket permissions |
| Template not found | Check template path and bucket location |
| Stuck in queued state | Check Composer environment resource availability |

---

## Best Practices

### 1. Environment Configuration

```python
# Use environment variables in DAGs
import os

PROJECT_ID = os.getenv('GCP_PROJECT', 'your-project-id')
REGION = os.getenv('GCP_REGION', 'us-central1')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'dev')  # dev, staging, prod
```

### 2. Parameterized Templates

Always make templates configurable:

```python
run_job = DataflowTemplatedJobStartOperator(
    task_id='run_job',
    template_location=TEMPLATE_PATH,
    parameters={
        'input_topic': f'projects/{PROJECT_ID}/topics/input-{ENVIRONMENT}',
        'output_dataset': f'{PROJECT_ID}:dataflow_{ENVIRONMENT}',
        'window_size': '3600',  # Make configurable
    },
)
```

### 3. Error Handling & Retries

```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_error_notification,
    'email_on_failure': True,
    'email': ['team@company.com'],
}
```

### 4. Version Control

Keep all DAGs and templates in Git:

```bash
# .gitignore
*.pyc
__pycache__/
.env
venv/
```

### 5. Testing Locally

Test DAGs before uploading:

```bash
# Test DAG syntax
python -m py_compile composer_dags/tumbling_window_dag.py

# Test imports
python -c "from composer_dags.tumbling_window_dag import dag; print(dag.dag_id)"

# Run Airflow locally
airflow dags test tumbling_window_pipeline 2024-06-15
```

---

## Summary

| Step | Command |
|------|---------|
| 1. Create Composer | `gcloud composer environments create ...` |
| 2. Prepare code | Create DAG files locally |
| 3. Upload DAGs | `gsutil cp composer_dags/* gs://bucket/dags/` |
| 4. Enable DAG | `gcloud composer environments run ... dags unpause` |
| 5. Trigger job | Use Web UI, CLI, or programmatically |
| 6. Monitor | Check Cloud Logging and Dataflow jobs |

---

## Additional Resources

- [Google Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [Apache Airflow DAG Structure](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)
- [Cloud Dataflow Operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataflow.html)
- [Composer Troubleshooting](https://cloud.google.com/composer/docs/troubleshooting)
