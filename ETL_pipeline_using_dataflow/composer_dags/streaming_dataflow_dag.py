"""
Composer DAG - Run Streaming Dataflow Pipeline
This DAG creates and monitors a long-running streaming Dataflow job

Key difference from batch DAGs:
- Streaming pipelines run CONTINUOUSLY (not on a schedule)
- They process data as it arrives (low latency)
- Never actually "complete" - they run until manually stopped

Location: Upload to Cloud Composer DAG bucket:
    gs://your-project-composer-bucket/dags/streaming_dataflow_dag.py

Schedule: None (on-demand manual trigger)
Note: After starting, the streaming job runs in the background indefinitely
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
    DataflowStopJobOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.exceptions import AirflowException
from google.cloud import dataflow_v1beta3
import logging


# ============================================================================
# CONFIGURATION - Streaming Pipeline Settings
# ============================================================================
PROJECT_ID = 'your-gcp-project-id'  # GCP project ID
REGION = 'us-central1'               # Region for workers
BUCKET_NAME = f'{PROJECT_ID}-streaming-bucket'  # Cloud Storage bucket

# BigQuery output configuration
BQ_DATASET = 'dataflow_streaming_output'  # Dataset for results
BQ_TABLE = 'real_time_events'             # Table for results

# Pub/Sub input configuration (where events come from)
INPUT_TOPIC = f'projects/{PROJECT_ID}/topics/events-stream'  # Source topic
TEMP_LOCATION = f'gs://{BUCKET_NAME}/temp'  # Temp storage

# ============================================================================
# DAG DEFAULT ARGUMENTS - Same as batch DAGs
# ============================================================================
default_args = {
    'owner': 'data-engineering',
    'description': 'Streaming Dataflow Pipeline Orchestration',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@company.com'],
    'email_on_failure': True,
    'retries': 1,  # Fewer retries for streaming (often long-running)
}

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    dag_id='streaming_dataflow_pipeline',  # Unique DAG identifier
    default_args=default_args,
    description='Streaming Dataflow Pipeline - Real-time Event Processing',
    schedule_interval=None,  # None = manual trigger only (no schedule)
    catchup=False,
    tags=['dataflow', 'streaming', 'realtime'],  # Tags for organization
) as dag:
    
    """
    Streaming Pipeline DAG Pattern:
    
    Unlike batch DAGs, streaming DAGs:
    - Don't run on a schedule (manual or external trigger)
    - Launch a job that runs indefinitely
    - Monitor the job while it's running
    - Can be stopped/restarted as needed
    
    Workflow:
    1. Setup: Create BigQuery dataset
    2. Launch: Start streaming Dataflow job
    3. Monitor: Check job status and health
    """
    
    # ========================================================================
    # SETUP TASKS
    # ========================================================================
    start_setup = DummyOperator(task_id='start_setup')
    
    # Create BigQuery dataset for streaming outputs
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bigquery_dataset',
        dataset_id=BQ_DATASET,
        location='US',
        exists_ok=True,
    )
    
    # ========================================================================
    # STREAMING JOB STARTUP - Launch the continuous pipeline
    # ========================================================================
    launch_streaming_job = DataflowTemplatedJobStartOperator(
        task_id='launch_streaming_dataflow_job',
        template_google_cloud_options={
            'projectId': PROJECT_ID,
            'location': REGION,
        },
        parameters={
            'inputTopic': INPUT_TOPIC,  # Where to read events from
            'outputTable': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}',  # Where to write results
            'windowDuration': '60',  # 60-second windows for aggregation
        },
        job_name=f'streaming-pipeline-{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}',
        append_job_name=False,
        environment={
            'numWorkers': 2,  # Starting workers
            'maxWorkers': 10,  # Scale up to 10 if needed
            'workerMachineType': 'n1-standard-4',
            'enableStreamingEngine': True,  # CRITICAL: For low-latency streaming
            'autoscalingAlgorithm': 'THROUGHPUT_BASED',  # Auto-scale based on backlog
            'zone': f'{REGION}-a',
            'tempLocation': TEMP_LOCATION,
        },
        wait_until_finished=False,  # Don't wait - streaming runs indefinitely
        gcp_conn_id='google_cloud_default',
    )
    
    # ========================================================================
    # MONITORING AND HEALTH CHECK
    # ========================================================================
    def check_job_status(**context):
        """Monitor streaming job status
        
        This function is called after the streaming job is launched.
        In production, you might:
        - Query job metrics (throughput, latency)
        - Check for errors or warnings
        - Validate data output
        - Send alerts if issues found
        """
        # Extract job ID from previous task
        job_id = context['ti'].xcom_pull(task_ids='launch_streaming_dataflow_job')
        logger = logging.getLogger(__name__)
        
        logger.info(f"Streaming job ID: {job_id}")
        logger.info("Streaming pipeline is running continuously...")
        
        return job_id
    
    monitor_job = PythonOperator(
        task_id='monitor_streaming_job',
        python_callable=check_job_status,
        provide_context=True,  # Pass DAG context to function
    )
    
    # ========================================================================
    # COMPLETION
    # ========================================================================
    end_setup = DummyOperator(task_id='end_setup')
    
    # ========================================================================
    # TASK DEPENDENCIES - Streaming workflow
    # ========================================================================
    # Sequence:
    # 1. start_setup
    # 2. create_dataset (prepare BigQuery)
    # 3. launch_streaming_job (starts continuous processing)
    # 4. monitor_job (health check)
    # 5. end_setup
    start_setup >> create_dataset >> launch_streaming_job >> monitor_job >> end_setup
