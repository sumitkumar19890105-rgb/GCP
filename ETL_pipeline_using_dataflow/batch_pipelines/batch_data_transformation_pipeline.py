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
    """Validate input records"""
    
    def process(self, element):
        """Validate required fields"""
        try:
            # Example validation - customize based on your schema
            required_fields = ['id', 'timestamp', 'value']
            
            if not all(field in element for field in required_fields):
                logger.warning(f"Record missing required fields: {element}")
                return
            
            # Validate data types
            if not isinstance(element['value'], (int, float)):
                logger.warning(f"Invalid value type: {element}")
                return
            
            yield element
            
        except Exception as e:
            logger.error(f"Validation error: {e}")


class TransformFn(beam.DoFn):
    """Transform data"""
    
    def process(self, element):
        """Apply transformations"""
        try:
            # Add computed fields
            element['processed_value'] = element['value'] * 1.1
            element['processing_date'] = datetime.utcnow().isoformat()
            element['data_quality_score'] = 0.95
            
            yield element
            
        except Exception as e:
            logger.error(f"Transform error: {e}")


class EnrichDataFn(beam.DoFn):
    """Enrich data with additional information"""
    
    def process(self, element):
        """Add enrichment fields"""
        try:
            # Add enrichment logic
            element['batch_id'] = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            element['source_system'] = 'batch_pipeline'
            element['enrichment_timestamp'] = datetime.utcnow().isoformat()
            
            yield element
            
        except Exception as e:
            logger.error(f"Enrichment error: {e}")


def run(argv=None):
    """Run the batch transformation pipeline"""
    
    parser = argparse.ArgumentParser(
        description='Batch Data Transformation Pipeline'
    )
    
    # Required arguments
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
    
    # Dataflow arguments
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
    
    # Create pipeline options
    options = PipelineOptions(pipeline_args)
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = known_args.runner
    
    if known_args.runner == 'DataflowRunner':
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project
        google_cloud_options.region = known_args.region
        google_cloud_options.temp_location = known_args.temp_location
        
        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = known_args.num_workers
    
    # Define output schema
    output_schema = '''
        id:STRING,
        timestamp:TIMESTAMP,
        value:FLOAT64,
        processed_value:FLOAT64,
        processing_date:TIMESTAMP,
        data_quality_score:FLOAT64,
        batch_id:STRING,
        source_system:STRING,
        enrichment_timestamp:TIMESTAMP
    '''
    
    # Build and run pipeline
    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromBigQuery' >> ReadFromBigQuery(
             table=known_args.input_table,
             use_standard_sql=True
         )
         | 'Validate' >> beam.ParDo(ValidateRecordFn())
         | 'Transform' >> beam.ParDo(TransformFn())
         | 'Enrich' >> beam.ParDo(EnrichDataFn())
         | 'WriteToBigQuery' >> WriteToBigQuery(
             table=known_args.output_table,
             schema=output_schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
         )
        )
    
    logger.info("Batch transformation pipeline completed")


if __name__ == '__main__':
    run()
