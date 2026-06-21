"""
Batch Dataflow Pipeline - Data Transformation with Validation
Reads from multiple sources, transforms data, validates, and writes to BigQuery

Use cases:
- ETL processes
- Data migration
- Daily batch processing
- Scheduled data transformation
"""

import argparse
import logging
from datetime import datetime
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidateRecordFn(beam.DoFn):
    """
    Validate input records - ensures data quality before processing.
    
    This transformation:
    - Checks for required fields
    - Validates data types
    - Logs warnings for invalid records
    - Filters out bad data early in the pipeline
    """
    
    def process(self, element):
        """Validate required fields and data types
        
        Args:
            element: Dictionary record from input source
            
        Yields:
            dict: Valid record, or nothing if validation fails
        """
        try:
            # Define which fields are mandatory
            required_fields = ['id', 'timestamp', 'value']
            
            # Check that all required fields are present
            if not all(field in element for field in required_fields):
                logger.warning(f"Record missing required fields: {element}")
                return  # Skip this record (filter it out)
            
            # Validate that numeric field is actually a number
            if not isinstance(element['value'], (int, float)):
                logger.warning(f"Invalid value type: {element}")
                return  # Skip this record
            
            # If all validations pass, emit the record
            yield element
            
        except Exception as e:
            # Catch any unexpected errors and log them
            logger.error(f"Validation error: {e}")


class TransformFn(beam.DoFn):
    """Apply business logic transformations to records.
    
    This transformation:
    - Calculates derived fields (processed_value)
    - Adds processing metadata
    - Enriches records with computed data
    """
    
    def process(self, element):
        """Apply transformations to create new fields
        
        Args:
            element: Validated input record
            
        Yields:
            dict: Transformed record with new computed fields
        """
        try:
            # Calculate derived metrics
            element['processed_value'] = element['value'] * 1.1  # Example: Apply 10% multiplier
            element['processing_date'] = datetime.utcnow().isoformat()  # Record when processed
            element['data_quality_score'] = 0.95  # Could be calculated based on validations
            
            yield element
            
        except Exception as e:
            logger.error(f"Transform error: {e}")


class EnrichDataFn(beam.DoFn):
    """Enrich records with additional operational metadata.
    
    This transformation adds:
    - Batch run identifiers for tracking
    - Source system information
    - Processing timestamps for auditing
    """
    
    def process(self, element):
        """Add enrichment fields for operational tracking
        
        Args:
            element: Transformed record
            
        Yields:
            dict: Enriched record with operational metadata
        """
        try:
            # Add operational metadata for tracking and audit trails
            element['batch_id'] = datetime.utcnow().strftime('%Y%m%d_%H%M%S')  # Unique batch identifier
            element['source_system'] = 'batch_pipeline'  # Track which system produced this
            element['enrichment_timestamp'] = datetime.utcnow().isoformat()  # Audit trail
            
            yield element
            
        except Exception as e:
            logger.error(f"Enrichment error: {e}")


def run(argv=None):
    """Execute the batch data transformation pipeline.
    
    Pipeline flow:
    1. Read data from BigQuery (source table)
    2. Validate each record (quality checks)
    3. Transform records (business logic)
    4. Enrich with metadata (audit trail)
    5. Write to BigQuery (output table)
    
    Args:
        argv: Command-line arguments
    """
    
    parser = argparse.ArgumentParser(
        description='Batch Data Transformation Pipeline'
    )
    
    # ====== Data Source/Destination ======
    # Define where input data comes from and where output goes
    parser.add_argument(
        '--input_table',
        dest='input_table',
        required=True,
        help='Input BigQuery table (e.g., project:dataset.source_table)'
    )
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='Output BigQuery table (e.g., project:dataset.output_table)'
    )
    
    # ====== Execution Configuration ======
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',
        choices=['DirectRunner', 'DataflowRunner']
    )
    parser.add_argument('--project', dest='project', help='GCP Project ID')
    parser.add_argument('--region', dest='region', default='us-central1')
    parser.add_argument('--temp_location', dest='temp_location')
    parser.add_argument('--num_workers', dest='num_workers', type=int, default=2)
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # ====== Pipeline Options ======
    # Configure how the pipeline runs
    options = PipelineOptions(pipeline_args)
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = known_args.runner
    
    # Configure for cloud execution if using DataflowRunner
    if known_args.runner == 'DataflowRunner':
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project
        google_cloud_options.region = known_args.region
        google_cloud_options.temp_location = known_args.temp_location
        
        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = known_args.num_workers
    
    # ====== Output BigQuery Schema ======
    # Define the structure of records in the output table
    # Each line represents: field_name:type:mode
    output_schema = '''
        id:STRING,                           # Unique identifier
        timestamp:TIMESTAMP,                 # Original timestamp
        value:FLOAT64,                       # Original numeric value
        processed_value:FLOAT64,             # Calculated value
        processing_date:TIMESTAMP,           # When it was processed
        data_quality_score:FLOAT64,          # Quality metric
        batch_id:STRING,                     # Which batch run this came from
        source_system:STRING,                # Which system produced it
        enrichment_timestamp:TIMESTAMP       # When enrichment occurred
    '''
    
    # ====== BUILD AND EXECUTE PIPELINE ======
    with beam.Pipeline(options=options) as p:
        (
            p
            # STAGE 1: Read data from BigQuery table
            # This reads all rows from the specified input table
            | 'ReadFromBigQuery' >> ReadFromBigQuery(
                table=known_args.input_table,
                use_standard_sql=True  # Use modern BigQuery SQL dialect
            )
            
            # STAGE 2: Validate records
            # Filters out records with missing fields or invalid data types
            | 'Validate' >> beam.ParDo(ValidateRecordFn())
            
            # STAGE 3: Transform data
            # Applies business logic (calculations, enrichment)
            | 'Transform' >> beam.ParDo(TransformFn())
            
            # STAGE 4: Enrich with metadata
            # Adds operational fields for tracking and auditing
            | 'Enrich' >> beam.ParDo(EnrichDataFn())
            
            # STAGE 5: Write to BigQuery
            # Writes transformed records to output table
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=output_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE  # Replace existing data
            )
        )
    
    logger.info("Batch transformation pipeline completed")


if __name__ == '__main__':
    run()
