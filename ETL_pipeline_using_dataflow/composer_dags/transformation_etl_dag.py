"""
Composer DAG - Data Transformation Pipeline
Demonstrates ETL pattern with validation, transformation, and loading

ETL = Extract, Transform, Load
1. Extract: Read data from source (BigQuery, Cloud Storage, etc.)
2. Transform: Apply business logic and data quality checks
3. Load: Write processed data to destination

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
PROJECT_ID = 'your-gcp-project-id'  # Your GCP project
REGION = 'us-central1'               # Dataflow region
BUCKET_NAME = f'{PROJECT_ID}-etl-bucket'  # Storage bucket for ETL

# BigQuery datasets and tables
BQ_DATASET = 'raw_data'              # Input dataset (raw data)
BQ_STAGING_TABLE = 'staging_events'  # Staging table (raw input)
BQ_PROCESSED_TABLE = 'processed_events'  # Output table (processed data)

# ============================================================================
# DAG DEFAULT ARGUMENTS
# ============================================================================
default_args = {
    'owner': 'data-engineering',
    'description': 'ETL Pipeline with Dataflow',
    'depends_on_past': False,  # Don't chain DAG runs together
    'start_date': datetime(2024, 1, 1),  # When DAG becomes active
    'email': ['your-email@company.com'],  # Alert email
    'email_on_failure': True,  # Email on errors
    'retries': 2,  # Auto-retry failed tasks
    'retry_delay': timedelta(minutes=5),  # Wait before retry
}

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    dag_id='transformation_etl_pipeline',  # Unique DAG ID
    default_args=default_args,
    description='ETL Pipeline - Extract, Transform, Load Data',
    schedule_interval='0 0 * * *',  # Daily at midnight UTC
    catchup=False,  # Don't backfill
    tags=['etl', 'dataflow', 'transformation'],
) as dag:
    
    """
    Complete ETL Pipeline Pattern:
    
    E (Extract):
    - Read data from source (BigQuery staging table)
    - Load into memory/pipeline
    
    T (Transform):
    - Validate data quality
    - Apply business logic
    - Enrich with computed fields
    
    L (Load):
    - Write to destination (BigQuery processed table)
    - Append or replace based on requirements
    
    V (Validate):
    - Check output data quality
    - Alert if issues found
    """
    
    # ========================================================================
    # EXTRACT STAGE - Prepare input data
    # ========================================================================
    start_pipeline = DummyOperator(task_id='start_etl_pipeline')  # Entry point
    
    # Create output BigQuery dataset if it doesn't exist
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bigquery_dataset',
        dataset_id=BQ_DATASET,
        location='US',
        exists_ok=True,
    )
    
    # Dummy task representing data extraction
    # In production, this might be a task that:
    # - Queries source systems
    # - Exports data from external APIs
    # - Copies files from Cloud Storage
    extract_data = DummyOperator(task_id='extract_from_source')
    
    # ========================================================================
    # TRANSFORM STAGE - Apply business logic using Dataflow
    # ========================================================================
    # Launch Dataflow job to transform data
    # This reads from input table, processes, writes to output table
    transform_data = DataflowTemplatedJobStartOperator(
        task_id='transform_with_dataflow',  # Transform using Dataflow
        template_google_cloud_options={
            'projectId': PROJECT_ID,
            'location': REGION,
        },
        parameters={
            'inputTable': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_STAGING_TABLE}',  # Read from staging
            'outputTable': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_PROCESSED_TABLE}',  # Write to processed
        },
        job_name=f'etl-transform-{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}',
        append_job_name=False,
        environment={
            'numWorkers': 2,  # Workers for Dataflow job
            'maxWorkers': 5,  # Auto-scale limit
            'workerMachineType': 'n1-standard-4',
            'zone': f'{REGION}-a',
            'tempLocation': f'gs://{BUCKET_NAME}/temp',
        },
        wait_until_finished=True,  # Wait for job to complete
    )
    
    # ========================================================================
    # LOAD STAGE - Write to destination
    # ========================================================================
    # Dummy task representing data loading
    # In production, this might be:
    # - Additional BigQuery tasks
    # - Pub/Sub publishing
    # - File exports
    load_data = DummyOperator(task_id='load_to_warehouse')
    
    # ========================================================================
    # VALIDATION STAGE - Quality checks
    # ========================================================================
    def validate_data_quality(**context):
        """Validate output data quality
        
        In production, this would:
        - Check row counts (did we lose data?)
        - Validate data types
        - Check for nulls in critical fields
        - Compare against expected ranges
        
        Args:
            context: Airflow context with DAG execution info
            
        Returns:
            str: Task ID to execute next (passed to BranchPythonOperator)
        """
        logging.info("Validating data quality...")
        # Add your data quality checks here
        # Return 'data_quality_passed' or 'data_quality_failed'
        return 'data_quality_passed'
    
    # Branch based on data quality validation
    validate_quality = BranchPythonOperator(
        task_id='validate_data_quality',  # Decision point
        python_callable=validate_data_quality,  # Function to decide branch
    )
    
    # Success path: Quality checks passed
    quality_passed = DummyOperator(task_id='data_quality_passed')
    
    # Failure path: Quality checks failed
    # In production, this might trigger alerts or rollbacks
    quality_failed = DummyOperator(task_id='data_quality_failed')
    
    # ========================================================================
    # COMPLETION
    # ========================================================================
    end_pipeline = DummyOperator(task_id='end_etl_pipeline')
    
    # ========================================================================
    # TASK DEPENDENCIES - Define ETL workflow
    # ========================================================================
    # Linear flow for setup and main pipeline:
    # start -> create_dataset -> extract -> transform -> load -> validate
    #
    # Branching at validation:
    # validate -> passed -> end
    # validate -> failed -> end
    #
    # This ensures all paths lead to completion
    (start_pipeline 
     >> create_dataset 
     >> extract_data 
     >> transform_data 
     >> load_data 
     >> validate_quality)
    
    # Branch to different outcomes based on quality check
    validate_quality >> [quality_passed, quality_failed] >> end_pipeline
