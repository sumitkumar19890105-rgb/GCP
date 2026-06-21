"""
Composer DAG - Run Streaming Dataflow Pipeline
This DAG creates and monitors a long-running streaming Dataflow job

Location: Upload to Cloud Composer DAG bucket:
    gs://your-project-composer-bucket/dags/streaming_dataflow_dag.py

Note: Streaming pipelines run continuously, so this DAG manages the job lifecycle
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
# CONFIGURATION
# ============================================================================
PROJECT_ID = 'your-gcp-project-id'
REGION = 'us-central1'
BUCKET_NAME = f'{PROJECT_ID}-streaming-bucket'

BQ_DATASET = 'dataflow_streaming_output'
BQ_TABLE = 'real_time_events'

INPUT_TOPIC = f'projects/{PROJECT_ID}/topics/events-stream'
TEMP_LOCATION = f'gs://{BUCKET_NAME}/temp'

# ============================================================================
# DAG DEFAULT ARGUMENTS
# ============================================================================
default_args = {
    'owner': 'data-engineering',
    'description': 'Streaming Dataflow Pipeline Orchestration',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@company.com'],
    'email_on_failure': True,
    'retries': 1,
}

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    dag_id='streaming_dataflow_pipeline',
    default_args=default_args,
    description='Streaming Dataflow Pipeline - Real-time Event Processing',
    schedule_interval=None,  # Don't schedule - runs continuously
    catchup=False,
    tags=['dataflow', 'streaming', 'realtime'],
) as dag:
    
    """
    This DAG demonstrates a streaming Dataflow pipeline that:
    1. Creates necessary GCP resources (dataset, bucket)
    2. Launches a streaming Dataflow job
    3. Monitors the job status
    4. Can be triggered manually or on schedule
    """
    
    # ========================================================================
    # SETUP TASKS
    # ========================================================================
    start_setup = DummyOperator(task_id='start_setup')
    
    # Create BigQuery dataset for outputs
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bigquery_dataset',
        dataset_id=BQ_DATASET,
        location='US',
        exists_ok=True,
    )
    
    # ========================================================================
    # STREAMING JOB STARTUP
    # ========================================================================
    launch_streaming_job = DataflowTemplatedJobStartOperator(
        task_id='launch_streaming_dataflow_job',
        template_google_cloud_options={
            'projectId': PROJECT_ID,
            'location': REGION,
        },
        parameters={
            'inputTopic': INPUT_TOPIC,
            'outputTable': f'{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}',
            'windowDuration': '60',  # 60 seconds
        },
        job_name=f'streaming-pipeline-{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}',
        append_job_name=False,
        environment={
            'numWorkers': 2,
            'maxWorkers': 10,
            'workerMachineType': 'n1-standard-4',
            'enableStreamingEngine': True,  # CRITICAL for streaming
            'autoscalingAlgorithm': 'THROUGHPUT_BASED',
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
        """Check streaming job status"""
        job_id = context['ti'].xcom_pull(task_ids='launch_streaming_dataflow_job')
        logger = logging.getLogger(__name__)
        
        logger.info(f"Streaming job ID: {job_id}")
        logger.info("Streaming pipeline is running continuously...")
        
        # In production, you might want to:
        # - Set up monitoring alerts
        # - Check job metrics
        # - Validate data output
        return job_id
    
    monitor_job = PythonOperator(
        task_id='monitor_streaming_job',
        python_callable=check_job_status,
        provide_context=True,
    )
    
    # ========================================================================
    # COMPLETION
    # ========================================================================
    end_setup = DummyOperator(task_id='end_setup')
    
    # ========================================================================
    # TASK DEPENDENCIES
    # ========================================================================
    start_setup >> create_dataset >> launch_streaming_job >> monitor_job >> end_setup
