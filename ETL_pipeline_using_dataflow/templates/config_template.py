"""
Dataflow Pipeline Configuration Template
Use this as a base for your environment configuration

Location: config.py or environment variables
"""

# ============================================================================
# GCP PROJECT CONFIGURATION
# ============================================================================
class DataflowConfig:
    """Base configuration for Dataflow pipelines"""
    
    # Project and region settings
    GCP_PROJECT_ID = 'your-gcp-project-id'  # Set via environment variable
    GCP_REGION = 'us-central1'
    GCP_ZONE = f'{GCP_REGION}-a'
    
    # Storage configuration
    GCS_BUCKET = f'{GCP_PROJECT_ID}-dataflow-bucket'
    GCS_TEMP_LOCATION = f'gs://{GCS_BUCKET}/temp'
    GCS_STAGING_LOCATION = f'gs://{GCS_BUCKET}/staging'
    
    # BigQuery configuration
    BQ_DATASET_RAW = 'raw_data'
    BQ_DATASET_PROCESSED = 'processed_data'
    BQ_DATASET_LOCATION = 'US'
    
    # Pub/Sub configuration
    PUBSUB_INPUT_TOPIC = f'projects/{GCP_PROJECT_ID}/topics/events-input'
    PUBSUB_DEADLETTER_TOPIC = f'projects/{GCP_PROJECT_ID}/topics/events-deadletter'
    
    # Worker configuration
    WORKER_MACHINE_TYPE = 'n1-standard-4'
    NUM_WORKERS = 2
    MAX_NUM_WORKERS = 10
    
    # Dataflow job configuration
    JOB_LABELS = {
        'environment': 'production',
        'team': 'data-engineering',
        'version': '1.0',
    }
    
    # Streaming specific
    ENABLE_STREAMING_ENGINE = True
    AUTOSCALING_ALGORITHM = 'THROUGHPUT_BASED'
    
    # Batch specific
    SETUP_FILE = 'setup.py'
    REQUIREMENTS_FILE = 'requirements.txt'


# ============================================================================
# BATCH PIPELINE CONFIGURATION
# ============================================================================
class BatchPipelineConfig(DataflowConfig):
    """Configuration for batch pipelines"""
    
    # Input/Output
    INPUT_PATH = f'gs://{DataflowConfig.GCS_BUCKET}/input/batch/*.csv'
    OUTPUT_TABLE = f'{DataflowConfig.GCP_PROJECT_ID}:{DataflowConfig.BQ_DATASET_PROCESSED}.batch_output'
    
    # Batch specific settings
    NUM_WORKERS = 4
    MAX_NUM_WORKERS = 8
    RUNNER = 'DataflowRunner'
    
    # Job configuration
    JOB_PREFIX = 'batch-pipeline'


# ============================================================================
# STREAMING PIPELINE CONFIGURATION
# ============================================================================
class StreamingPipelineConfig(DataflowConfig):
    """Configuration for streaming pipelines"""
    
    # Input/Output
    INPUT_TOPIC = DataflowConfig.PUBSUB_INPUT_TOPIC
    OUTPUT_TABLE = f'{DataflowConfig.GCP_PROJECT_ID}:{DataflowConfig.BQ_DATASET_PROCESSED}.streaming_output'
    
    # Streaming specific settings
    NUM_WORKERS = 2
    MAX_NUM_WORKERS = 10
    ENABLE_STREAMING_ENGINE = True
    AUTOSCALING_ALGORITHM = 'THROUGHPUT_BASED'
    RUNNER = 'DataflowRunner'
    
    # Window configuration
    WINDOW_DURATION = 60  # seconds
    WINDOW_TYPE = 'fixed'  # 'fixed', 'sliding', or 'session'
    
    # Job configuration
    JOB_PREFIX = 'streaming-pipeline'


# ============================================================================
# DEVELOPMENT/LOCAL CONFIGURATION
# ============================================================================
class LocalDevConfig(DataflowConfig):
    """Configuration for local development"""
    
    GCP_PROJECT_ID = 'your-dev-project'
    RUNNER = 'DirectRunner'
    NUM_WORKERS = 0  # Not applicable for DirectRunner
    
    # Point to local/dev resources
    INPUT_PATH = 'local_data/*.csv'
    OUTPUT_TABLE = f'{GCP_PROJECT_ID}:{DataflowConfig.BQ_DATASET_PROCESSED}.dev_output'


# ============================================================================
# HELPER FUNCTIONS - Create pipeline options easily
# ============================================================================
def get_pipeline_options(config_class, runner='DataflowRunner', **kwargs):
    """
    Create Apache Beam pipeline options from a configuration class.
    
    This function encapsulates the boilerplate code for setting up
    pipeline options, making it reusable across multiple pipelines.
    
    Args:
        config_class: Configuration class (DataflowConfig, BatchPipelineConfig, etc.)
        runner: Which runner to use ('DirectRunner' or 'DataflowRunner')
        **kwargs: Additional pipeline options to override defaults
        
    Returns:
        PipelineOptions: Configured options ready for use in pipeline
        
    Example:
        options = get_pipeline_options(BatchPipelineConfig, runner='DataflowRunner')
        with beam.Pipeline(options=options) as p:
            # Build pipeline here
    """
    from apache_beam.options.pipeline_options import (
        PipelineOptions,
        GoogleCloudOptions,
        WorkerOptions,
    )
    
    config = config_class()  # Instantiate configuration
    options = PipelineOptions(**kwargs)  # Create base options
    
    # Configure for cloud Dataflow execution
    if runner == 'DataflowRunner':
        # Set Google Cloud options
        gcp_options = options.view_as(GoogleCloudOptions)
        gcp_options.project = config.GCP_PROJECT_ID
        gcp_options.region = config.GCP_REGION
        gcp_options.temp_location = config.GCS_TEMP_LOCATION
        
        # Enable streaming engine if supported by config
        if hasattr(config, 'ENABLE_STREAMING_ENGINE'):
            gcp_options.enable_streaming_engine = config.ENABLE_STREAMING_ENGINE
        
        # Configure worker resources
        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = config.NUM_WORKERS
        worker_options.max_num_workers = config.MAX_NUM_WORKERS
        worker_options.machine_type = config.WORKER_MACHINE_TYPE
    
    return options


# ============================================================================
# ENVIRONMENT-BASED CONFIGURATION SELECTION
# ============================================================================
# This allows switching between environments using the ENVIRONMENT variable
# Example: export ENVIRONMENT=development
import os

ENV = os.getenv('ENVIRONMENT', 'production')  # Default to production if not set

# Select appropriate configuration based on environment
if ENV == 'development':
    CONFIG = LocalDevConfig  # Use local development config
elif ENV == 'staging':
    CONFIG = DataflowConfig  # Use base config for staging
else:
    CONFIG = DataflowConfig  # Use base config for production (default)
