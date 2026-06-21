"""
Batch Dataflow Pipeline Template - Word Count Example
This pipeline reads from Cloud Storage, processes text, and writes results to BigQuery.

Run locally:
    python batch_word_count_pipeline.py --input gs://your-bucket/input/*.txt --output project:dataset.output_table

Run on Dataflow:
    python batch_word_count_pipeline.py \
        --input gs://your-bucket/input/*.txt \
        --output project:dataset.output_table \
        --runner DataflowRunner \
        --project your-project-id \
        --region us-central1 \
        --temp_location gs://your-bucket/temp
"""

import argparse
import logging
from datetime import datetime
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    WorkerOptions,
    SetupOptions
)
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.transforms.window import GlobalWindow


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NormalizeFn(beam.DoFn):
    """Normalize text input"""
    
    def process(self, line):
        """Convert to lowercase and remove non-alphanumeric characters"""
        if line.strip():
            # Remove non-alphanumeric, convert to lowercase
            normalized = re.sub(r'[^\w\s]', '', line.lower())
            if normalized.strip():
                yield normalized


class FilterAndCountFn(beam.DoFn):
    """Filter short words and count occurrences"""
    
    MIN_WORD_LENGTH = 3
    
    def process(self, element):
        """Filter and format output"""
        word, count = element
        
        # Filter words based on minimum length
        if len(word) >= self.MIN_WORD_LENGTH:
            yield {
                'word': word,
                'count': count,
                'word_length': len(word),
                'processing_timestamp': datetime.utcnow().isoformat(),
                'batch_id': datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            }


def run(argv=None):
    """Run the batch pipeline"""
    
    parser = argparse.ArgumentParser(
        description='Batch Word Count Dataflow Pipeline'
    )
    
    # Input/Output arguments
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input Cloud Storage pattern (e.g., gs://bucket/path/*.txt)'
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output BigQuery table (e.g., project:dataset.table)'
    )
    
    # Dataflow arguments
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',
        choices=['DirectRunner', 'DataflowRunner'],
        help='Apache Beam runner'
    )
    parser.add_argument(
        '--project',
        dest='project',
        help='GCP Project ID (required for DataflowRunner)'
    )
    parser.add_argument(
        '--region',
        dest='region',
        default='us-central1',
        help='GCP region for Dataflow'
    )
    parser.add_argument(
        '--temp_location',
        dest='temp_location',
        help='Temporary location in Cloud Storage (required for DataflowRunner)'
    )
    parser.add_argument(
        '--num_workers',
        dest='num_workers',
        type=int,
        default=2,
        help='Number of workers for Dataflow'
    )
    
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
        worker_options.machine_type = 'n1-standard-4'
        
        setup_options = options.view_as(SetupOptions)
        setup_options.save_main_session = True
    
    # Define BigQuery schema
    bq_schema = 'word:STRING, count:INTEGER, word_length:INTEGER, processing_timestamp:TIMESTAMP, batch_id:STRING'
    
    # Build and run pipeline
    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromGCS' >> ReadFromText(known_args.input)
         | 'Normalize' >> beam.ParDo(NormalizeFn())
         | 'SplitWords' >> beam.FlatMap(lambda line: line.split())
         | 'PairWithOne' >> beam.Map(lambda word: (word, 1))
         | 'SumCounts' >> beam.CombinePerKey(sum)
         | 'FilterAndFormat' >> beam.ParDo(FilterAndCountFn())
         | 'WriteToBigQuery' >> WriteToBigQuery(
             table=known_args.output,
             schema=bq_schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             method='STREAMING_INSERTS'
         )
        )
    
    logger.info(f"Batch pipeline completed successfully")


if __name__ == '__main__':
    run()
