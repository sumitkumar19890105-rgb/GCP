"""
Composer DAG - Run Batch Dataflow Pipeline
This DAG orchestrates a batch Dataflow job using Composer/Airflow

Location: Upload to Cloud Composer DAG bucket:
    gs://your-project-composer-bucket/dags/batch_dataflow_dag.py

Schedule: Daily at 2 AM UTC (adjust as needed)
Timeout: 1 hour
"""

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
# CONFIGURATION
# ============================================================================
PROJECT_ID = 'your-gcp-project-id'
REGION = 'us-central1'
BUCKET_NAME = f'{PROJECT_ID}-dataflow-bucket'
TEMPLATE_PATH = f'gs://{BUCKET_NAME}/templates/batch_word_count_template'

BQ_DATASET = 'dataflow_batch_output'
BQ_TABLE = 'word_count_results'
BQ_DATASET_LOCATION = 'US'

INPUT_PATH = f'gs://{BUCKET_NAME}/input/documents/*.txt'
TEMP_LOCATION = f'gs://{BUCKET_NAME}/temp'

# ============================================================================
# DAG DEFAULT ARGUMENTS
# ============================================================================
default_args = {
    'owner': 'data-engineering',
    'description': 'Batch Dataflow Pipeline Orchestration',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
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
        task_id='run_dataflow_batch_pipeline',
        template_google_cloud_options={
            'projectId': PROJECT_ID,
            'location': REGION,
        },
        parameters={
            'inputFile': INPUT_PATH,
            'output': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}',
        },
        # Using classic template - adjust based on your setup
        # For Flex Templates, use: template='gs://path/to/flex/template/metadata'
        job_name=f'batch-word-count-{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}',
        append_job_name=False,
        environment={
            'numWorkers': 2,
            'maxWorkers': 4,
            'workerMachineType': 'n1-standard-4',
            'zone': f'{REGION}-a',
            'tempLocation': TEMP_LOCATION,
        },
        wait_until_finished=True,
        gcp_conn_id='google_cloud_default',
    )
    
    # ========================================================================
    # VERIFICATION AND MONITORING TASKS
    # ========================================================================
    verify_output = BigQueryGetDataOperator(
        task_id='verify_dataflow_output',
        dataset_id=BQ_DATASET,
        table_id=BQ_TABLE,
        max_results=5,
        selected_fields='word,count',
        gcp_conn_id='google_cloud_default',
    )
    
    # ========================================================================
    # CLEANUP AND COMPLETION TASKS
    # ========================================================================
    end_pipeline = DummyOperator(task_id='end_pipeline')
    
    # ========================================================================
    # TASK DEPENDENCIES
    # ========================================================================
    start_pipeline >> [create_dataset, create_table] >> run_dataflow_batch >> verify_output >> end_pipeline
