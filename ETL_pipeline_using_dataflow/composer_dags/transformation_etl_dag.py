"""
Composer DAG - Data Transformation Pipeline
Demonstrates ETL pattern with validation, transformation, and loading

Location: gs://your-project-composer-bucket/dags/transformation_etl_dag.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


# ============================================================================
# CONFIGURATION
# ============================================================================
PROJECT_ID = 'your-gcp-project-id'
REGION = 'us-central1'
BUCKET_NAME = f'{PROJECT_ID}-etl-bucket'

BQ_DATASET = 'raw_data'
BQ_STAGING_TABLE = 'staging_events'
BQ_PROCESSED_TABLE = 'processed_events'

# ============================================================================
# DAG DEFAULT ARGUMENTS
# ============================================================================
default_args = {
    'owner': 'data-engineering',
    'description': 'ETL Pipeline with Dataflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    dag_id='transformation_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline - Extract, Transform, Load Data',
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    tags=['etl', 'dataflow', 'transformation'],
) as dag:
    
    """
    ETL Pipeline Stages:
    1. Extract: Read from Cloud Storage
    2. Transform: Clean, validate, enrich data using Dataflow
    3. Load: Write processed data to BigQuery
    4. Validate: Verify data quality
    """
    
    # ========================================================================
    # EXTRACT STAGE
    # ========================================================================
    start_pipeline = DummyOperator(task_id='start_etl_pipeline')
    
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bigquery_dataset',
        dataset_id=BQ_DATASET,
        location='US',
        exists_ok=True,
    )
    
    extract_data = DummyOperator(task_id='extract_from_source')
    
    # ========================================================================
    # TRANSFORM STAGE - Using Dataflow
    # ========================================================================
    transform_data = DataflowTemplatedJobStartOperator(
        task_id='transform_with_dataflow',
        template_google_cloud_options={
            'projectId': PROJECT_ID,
            'location': REGION,
        },
        parameters={
            'inputTable': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_STAGING_TABLE}',
            'outputTable': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_PROCESSED_TABLE}',
        },
        job_name=f'etl-transform-{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}',
        append_job_name=False,
        environment={
            'numWorkers': 2,
            'maxWorkers': 5,
            'workerMachineType': 'n1-standard-4',
            'zone': f'{REGION}-a',
            'tempLocation': f'gs://{BUCKET_NAME}/temp',
        },
        wait_until_finished=True,
    )
    
    # ========================================================================
    # LOAD STAGE
    # ========================================================================
    load_data = DummyOperator(task_id='load_to_warehouse')
    
    # ========================================================================
    # VALIDATION STAGE
    # ========================================================================
    def validate_data_quality(**context):
        """Validate data quality after transformation"""
        logging.info("Validating data quality...")
        # In production, run data quality checks
        return 'data_quality_passed'
    
    validate_quality = BranchPythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
    )
    
    quality_passed = DummyOperator(task_id='data_quality_passed')
    quality_failed = DummyOperator(task_id='data_quality_failed')
    
    # ========================================================================
    # COMPLETION
    # ========================================================================
    end_pipeline = DummyOperator(task_id='end_etl_pipeline')
    
    # ========================================================================
    # TASK DEPENDENCIES
    # ========================================================================
    (start_pipeline 
     >> create_dataset 
     >> extract_data 
     >> transform_data 
     >> load_data 
     >> validate_quality)
    
    validate_quality >> [quality_passed, quality_failed] >> end_pipeline
