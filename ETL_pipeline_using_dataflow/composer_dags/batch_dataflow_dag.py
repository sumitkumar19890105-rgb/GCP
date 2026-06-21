# ============================================================================
# BATCH DATAFLOW DAG - Cloud Composer / Apache Airflow Orchestration
# ============================================================================
# 
# Location: Upload to Cloud Composer DAG bucket:
#    gs://your-project-composer-bucket/dags/batch_dataflow_dag.py
#
# This DAG orchestrates a batch Dataflow job with the following workflow:
# 1. Setup: Create BigQuery dataset and tables
# 2. Execute: Launch Dataflow job for batch processing
# 3. Verify: Query and validate output
# 4. Cleanup: Optional cleanup tasks
#
# Schedule: Daily at 2 AM UTC (configurable)
# Timeout: 1 hour
#
# Key Concepts:
# - DAG (Directed Acyclic Graph): Definition of workflow with tasks
# - Tasks: Individual units of work (create table, run job, verify)
# - Dependencies: Tasks run in specific order (A -> B -> C)
#

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryGetDataOperator
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable


# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES FOR YOUR ENVIRONMENT
# ============================================================================
PROJECT_ID = 'your-gcp-project-id'  # Change this to your actual project
REGION = 'us-central1'              # GCP region for Dataflow workers
BUCKET_NAME = f'{PROJECT_ID}-dataflow-bucket'  # Must exist in your project
TEMPLATE_PATH = f'gs://{BUCKET_NAME}/templates/batch_word_count_template'  # Path to Dataflow template

# BigQuery configuration
BQ_DATASET = 'dataflow_batch_output'    # Dataset name
BQ_TABLE = 'word_count_results'         # Table name
BQ_DATASET_LOCATION = 'US'              # BigQuery location

# Input/output paths
INPUT_PATH = f'gs://{BUCKET_NAME}/input/documents/*.txt'  # Where to read from
TEMP_LOCATION = f'gs://{BUCKET_NAME}/temp'  # Temporary staging location

# ============================================================================
# DAG DEFAULT ARGUMENTS
# ============================================================================
# These apply to all tasks in the DAG unless overridden
default_args = {
    'owner': 'data-engineering',  # Team responsible
    'description': 'Batch Dataflow Pipeline Orchestration',  # What this DAG does
    'depends_on_past': False,     # Don't wait for previous DAG runs
    'start_date': datetime(2024, 1, 1),  # When DAG becomes active
    'email': ['your-email@company.com'],  # Alerts go here
    'email_on_failure': True,     # Send email if task fails
    'email_on_retry': False,      # Don't email on retries
    'retries': 2,                 # Automatically retry failed tasks
    'retry_delay': timedelta(minutes=5),  # Wait 5 min before retrying
    'execution_timeout': timedelta(hours=1),  # Max time for task
}

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    dag_id='batch_dataflow_word_count',
    default_args=default_args,
    description='Batch Dataflow Pipeline - Word Count',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    catchup=False,
    tags=['dataflow', 'batch', 'word-count'],
    max_active_runs=1,  # Prevent concurrent runs
) as dag:
    
    # ========================================================================
    # SETUP TASKS
    # ========================================================================
    start_pipeline = DummyOperator(task_id='start_pipeline')
    
    # Create BigQuery dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bigquery_dataset',
        dataset_id=BQ_DATASET,
        location=BQ_DATASET_LOCATION,
        exists_ok=True,
        gcp_conn_id='google_cloud_default',
    )
    
    # Create BigQuery table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_bigquery_table',
        dataset_id=BQ_DATASET,
        table_id=BQ_TABLE,
        schema_fields=[
            {'name': 'word', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'count', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'word_length', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'processing_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'batch_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        exists_ok=True,
    )
    
    # ========================================================================
    # DATAFLOW EXECUTION TASK
    # ========================================================================
    def get_dataflow_job_name():
        """Generate unique job name with timestamp"""
        return f"batch-word-count-{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    run_dataflow_batch = DataflowTemplatedJobStartOperator(
        task_id='run_dataflow_batch_pipeline',  # Task ID in Airflow UI
        template_google_cloud_options={
            'projectId': PROJECT_ID,  # GCP project ID
            'location': REGION,       # Region for Dataflow workers
        },
        parameters={
            'inputFile': INPUT_PATH,  # Input file pattern for Dataflow pipeline
            'output': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}',  # Output BigQuery table
        },
        job_name=f'batch-word-count-{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}',  # Unique job name with timestamp
        append_job_name=False,  # Use exact name provided
        environment={
            'numWorkers': 2,        # Start with 2 workers
            'maxWorkers': 4,        # Scale up to max 4 if needed
            'workerMachineType': 'n1-standard-4',  # Machine type for workers
            'zone': f'{REGION}-a',  # GCP zone
            'tempLocation': TEMP_LOCATION,  # Temp storage for intermediate files
        },
        wait_until_finished=True,  # Block until Dataflow job completes
        gcp_conn_id='google_cloud_default',  # Use default GCP credentials
    )
    
    # ========================================================================
    # VERIFICATION AND MONITORING - Validate output
    # ========================================================================
    # Query the output table to verify data was written correctly
    verify_output = BigQueryGetDataOperator(
        task_id='verify_dataflow_output',  # Task to fetch and display data
        dataset_id=BQ_DATASET,             # Which dataset to query
        table_id=BQ_TABLE,                 # Which table to query
        max_results=5,                     # Return first 5 rows
        selected_fields='word,count',      # Only fetch these columns
        gcp_conn_id='google_cloud_default',
    )
    
    # ========================================================================
    # CLEANUP AND COMPLETION - End of pipeline
    # ========================================================================
    # End marker task (no-op, just signals completion)
    end_pipeline = DummyOperator(task_id='end_pipeline')
    
    # ========================================================================
    # TASK DEPENDENCIES - Define workflow execution order
    # ========================================================================
    # Using Airflow's >> operator to define dependencies
    # Flow:
    # 1. start_pipeline (entry point)
    # 2. create_dataset and create_table (parallel)
    # 3. run_dataflow_batch (waits for setup tasks)
    # 4. verify_output (validates results)
    # 5. end_pipeline (completion)
    start_pipeline >> [create_dataset, create_table] >> run_dataflow_batch >> verify_output >> end_pipeline
