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
    """
    Normalize text input - converts to lowercase and removes punctuation.
    
    This is a ParDo (parallel do) transformation that processes each line independently.
    """
    
    def process(self, line):
        """Convert to lowercase and remove non-alphanumeric characters
        
        Args:
            line: String input line to normalize
            
        Yields:
            str: Normalized line with lowercase letters and spaces only
        """
        # Skip empty lines
        if line.strip():
            # Remove non-alphanumeric characters (keep only letters, numbers, spaces)
            # \w matches word characters, \s matches whitespace
            normalized = re.sub(r'[^\w\s]', '', line.lower())
            
            # Only yield if normalized line has content
            if normalized.strip():
                yield normalized


class FilterAndCountFn(beam.DoFn):
    """
    Filter short words and prepare output for BigQuery.
    
    This transformation:
    - Filters out words shorter than MIN_WORD_LENGTH
    - Adds metadata (word length, timestamps)
    - Creates structured output for BigQuery insertion
    """
    
    MIN_WORD_LENGTH = 3  # Only keep words with 3+ characters
    
    def process(self, element):
        """Filter and format output for BigQuery storage
        
        Args:
            element: Tuple of (word, count) from previous aggregation
            
        Yields:
            dict: Structured record ready for BigQuery with fields:
                - word: The word string
                - count: Frequency count
                - word_length: Length of word (for analysis)
                - processing_timestamp: UTC timestamp when processed
                - batch_id: Unique identifier for this batch run
        """
        word, count = element
        
        # Filter out very short words (noise filtering)
        if len(word) >= self.MIN_WORD_LENGTH:
            yield {
                'word': word,
                'count': count,
                'word_length': len(word),  # Track word length for analysis
                'processing_timestamp': datetime.utcnow().isoformat(),  # For tracking pipeline run
                'batch_id': datetime.utcnow().strftime('%Y%m%d_%H%M%S')  # Unique batch identifier
            }


def run(argv=None):
    """
    Main pipeline execution function.
    
    This function:
    1. Parses command-line arguments
    2. Configures pipeline options based on runner type
    3. Builds and executes the pipeline
    4. Returns upon successful completion or error
    
    Args:
        argv: Command-line arguments (defaults to sys.argv if None)
    """
    
    # Create argument parser for CLI configuration
    parser = argparse.ArgumentParser(
        description='Batch Word Count Dataflow Pipeline'
    )
    
    # ====== Input/Output Arguments ======
    # These define where data comes from and where results go
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
    
    # ====== Execution Configuration ======
    # DirectRunner: Local testing (single machine)
    # DataflowRunner: Production (distributed GCP service)
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',
        choices=['DirectRunner', 'DataflowRunner'],
        help='Apache Beam runner: DirectRunner (local) or DataflowRunner (cloud)'
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
    
    # Parse arguments - separate known args from unknown Beam args
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # ====== Pipeline Options Configuration ======
    # Create base pipeline options from parsed arguments
    options = PipelineOptions(pipeline_args)
    
    # Set the runner type (DirectRunner for local, DataflowRunner for cloud)
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = known_args.runner
    
    # Configure cloud-specific options if using DataflowRunner
    if known_args.runner == 'DataflowRunner':
        # Configure Google Cloud Platform settings
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project  # GCP project ID
        google_cloud_options.region = known_args.region    # GCP region for workers
        google_cloud_options.temp_location = known_args.temp_location  # Temp staging location
        
        # Configure worker resources
        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = known_args.num_workers  # Starting number of workers
        worker_options.machine_type = 'n1-standard-4'  # Machine type for workers
        
        # Setup options for saving Python environment
        setup_options = options.view_as(SetupOptions)
        setup_options.save_main_session = True  # Save main session for distributed workers
    
    # ====== BigQuery Output Schema ======
    # Define the structure of records written to BigQuery
    bq_schema = (
        'word:STRING, '
        'count:INTEGER, '
        'word_length:INTEGER, '
        'processing_timestamp:TIMESTAMP, '
        'batch_id:STRING'
    )
    
    # ====== BUILD AND EXECUTE PIPELINE ======
    # This creates a pipeline processing graph and executes it
    with beam.Pipeline(options=options) as p:
        # Pipeline definition using pipe (|) operator
        # Each stage transforms data and passes to the next
        (
            p
            # STAGE 1: Read text files from Cloud Storage
            # Inputs: All .txt files matching the input pattern
            # Output: Individual lines as strings
            | 'ReadFromGCS' >> ReadFromText(known_args.input)
            
            # STAGE 2: Normalize text (lowercase, remove punctuation)
            # Input: Text lines
            # Output: Normalized text lines
            | 'Normalize' >> beam.ParDo(NormalizeFn())
            
            # STAGE 3: Split lines into words
            # Input: Lines of text
            # Output: Individual words (FlatMap expands each line to multiple elements)
            | 'SplitWords' >> beam.FlatMap(lambda line: line.split())
            
            # STAGE 4: Create key-value pairs (word, count=1)
            # Input: Individual words
            # Output: Tuples of (word, 1) for later aggregation
            | 'PairWithOne' >> beam.Map(lambda word: (word, 1))
            
            # STAGE 5: Sum counts per word (GROUP BY word, SUM count)
            # Input: (word, 1) pairs
            # Output: (word, total_count) after combining all counts for each word
            | 'SumCounts' >> beam.CombinePerKey(sum)
            
            # STAGE 6: Filter and add metadata
            # Input: (word, count) tuples
            # Output: Formatted dictionaries ready for BigQuery
            | 'FilterAndFormat' >> beam.ParDo(FilterAndCountFn())
            
            # STAGE 7: Write results to BigQuery
            # Input: Formatted dictionaries matching bq_schema
            # Output: Inserted rows in BigQuery table
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=known_args.output,
                schema=bq_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  # Create table if missing
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,  # Append to existing data
                method='STREAMING_INSERTS'  # Use streaming inserts (fast)
            )
        )
    
    # Log successful completion
    logger.info("Batch pipeline completed successfully")


if __name__ == '__main__':
    run()
