"""
Sample Composer DAG for Tumbling Window Dataflow Job
Location: gs://your-bucket/dags/tumbling_window_dag.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator
)
from airflow.operators.dummy_operator import DummyOperator

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================
PROJECT_ID = 'your-gcp-project-id'  # Change this
REGION = 'us-central1'
BUCKET = f'gs://{PROJECT_ID}-dataflow-bucket'  # Must exist
BQ_DATASET = 'dataflow_demo'
BQ_TABLE = 'aggregated_sales'
TEMPLATE_PATH = f'{BUCKET}/templates/tumbling_window_template'

# ============================================================================
# DAG DEFAULT ARGUMENTS
# ============================================================================
default_args = {
    'owner': 'data-engineering',
    'description': 'Tumbling Window Dataflow Job via Composer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@company.com'],  # Change this
    'email_on_failure': True,
    'email_on_retry': False,
}

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    dag_id='tumbling_window_pipeline',
    default_args=default_args,
    description='Tumbling window aggregation for sales data - runs hourly',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    catchup=False,  # Don't backfill old runs
    tags=['dataflow', 'realtime', 'sales', 'tumbling-window'],
    max_active_runs=1,  # Only one run at a time
) as dag:
    
    # Task 1: Start marker
    start_task = DummyOperator(task_id='start')
    
    # Task 2: Create BigQuery Dataset (idempotent - safe to run multiple times)
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bq_dataset',
        dataset_id=BQ_DATASET,
        project_id=PROJECT_ID,
        location='US',
        exists_ok=True,  # Don't fail if dataset already exists
        description='Dataset for Dataflow real-time aggregations',
    )
    
    # Task 3: Create BigQuery Table with schema
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_bq_table',
        dataset_id=BQ_DATASET,
        table_id=BQ_TABLE,
        project_id=PROJECT_ID,
        exists_ok=True,
        schema_fields=[
            {"name": "window_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "window_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "total_sales", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "transaction_count", "type": "INT64", "mode": "NULLABLE"},
            {"name": "average_transaction", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "max_transaction", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "min_transaction", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
        description='Aggregated sales data by hour and region',
    )
    
    # Task 4: Run Dataflow Tumbling Window Job
    run_dataflow_job = DataflowTemplatedJobStartOperator(
        task_id='run_tumbling_window_job',
        template_location=TEMPLATE_PATH,
        project_id=PROJECT_ID,
        location=REGION,
        job_name='tumbling-window-{{ ds_nodash }}',  # Date-based name (e.g., tumbling-window-20240615)
        
        # Parameters passed to the template
        parameters={
            'input_topic': f'projects/{PROJECT_ID}/topics/dataflow-transactions',
            'output_table': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}',
            'window_size': '3600',  # 1 hour in seconds
        },
        
        # Compute environment configuration
        environment={
            'machineType': 'n1-standard-2',
            'numWorkers': 2,
            'maxWorkers': 5,
            'diskSizeGb': 50,
            'tempLocation': f'{BUCKET}/temp',
            'stagingLocation': f'{BUCKET}/staging',
            'serviceAccountEmail': f'dataflow-runner@{PROJECT_ID}.iam.gserviceaccount.com',
        },
        
        # Don't wait for job to finish (set to True if you want to wait)
        wait_until_finished=False,
        
        # Additional options
        append_job_name=False,
    )
    
    # Task 5: End marker
    end_task = DummyOperator(task_id='end')
    
    # ========================================================================
    # DEFINE WORKFLOW DEPENDENCIES
    # ========================================================================
    # Run in sequence: start -> dataset -> table -> dataflow job -> end
    start_task >> create_dataset >> create_table >> run_dataflow_job >> end_task

# ============================================================================
# TESTING THE DAG LOCALLY
# ============================================================================
# To test this DAG on your local machine before uploading:
#
# 1. Ensure you have Apache Airflow installed:
#    pip install apache-airflow apache-airflow-providers-google
#
# 2. Test DAG syntax:
#    python -m py_compile tumbling_window_dag.py
#
# 3. Test DAG import:
#    python -c "from tumbling_window_dag import dag; print(dag.dag_id)"
#
# 4. Test with Airflow (if local Airflow setup exists):
#    airflow dags test tumbling_window_pipeline 2024-06-15
#
# ============================================================================
# DEPLOYING TO COMPOSER
# ============================================================================
# Upload this file to your Composer environment:
#
# gcloud composer environments storage dags import \
#   --environment=dataflow-orchestrator \
#   --location=us-central1 \
#   --source=tumbling_window_dag.py
#
# Or use gsutil:
# gsutil cp tumbling_window_dag.py gs://YOUR-COMPOSER-BUCKET/dags/
#
# ============================================================================
