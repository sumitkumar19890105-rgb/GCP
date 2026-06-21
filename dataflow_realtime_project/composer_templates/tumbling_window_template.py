"""
Dataflow Template for Tumbling Window Aggregation
Location: gs://your-bucket/templates/tumbling_window_template.py

This template can be used by:
1. GCP Composer DAGs (via DataflowTemplatedJobStartOperator)
2. Direct CLI commands
3. Cloud Functions
4. Any orchestration tool that supports Dataflow templates

To create a template:
gcloud dataflow flex-template build \
  gs://YOUR-BUCKET/templates/tumbling_window_template \
  --image-gcr-path=gcr.io/YOUR-PROJECT/dataflow-template:latest \
  --sdk-language=PYTHON \
  --flex-template-base-image=PYTHON3 \
  --py-path=. \
  --env=FLEX_TEMPLATE_PYTHON_PY_FILE=tumbling_window_template.py \
  --env=FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt \
  --env=FLEX_TEMPLATE_PYTHON_CMD=tumbling_window_template.py
"""

import argparse
import json
import logging
from datetime import datetime
from typing import Any, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    WorkerOptions,
)
from apache_beam.transforms import window
from apache_beam.utils.timestamp import Timestamp

# ============================================================================
# CONFIGURATION
# ============================================================================
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# ============================================================================
# CUSTOM TRANSFORMS
# ============================================================================


class ParseJSONAndAddTimestamp(beam.DoFn):
    """Parse JSON message and extract timestamp from payload"""
    
    def process(self, element: str):
        try:
            data = json.loads(element)
            timestamp_str = data.get('timestamp')
            
            if not timestamp_str:
                LOGGER.warning(f"No timestamp in message: {element}")
                return
            
            # Parse ISO format timestamp (e.g., "2024-06-12T09:15:32Z")
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            timestamp = Timestamp.from_utc_datetime(dt)
            
            # Yield TimestampedValue so beam knows the event time
            yield beam.window.TimestampedValue(data, timestamp.micros / 1e6)
        except Exception as e:
            LOGGER.error(f"Error parsing message: {element}, error: {e}")


class AggregateByRegion(beam.CombineFn):
    """Combine function to aggregate sales by region"""
    
    def create_accumulator(self):
        return {
            'total_sales': 0.0,
            'count': 0,
            'min_amount': float('inf'),
            'max_amount': float('-inf'),
            'transactions': []
        }
    
    def add_input(self, accumulator, element):
        amount = float(element.get('amount_usd', 0))
        
        accumulator['total_sales'] += amount
        accumulator['count'] += 1
        accumulator['min_amount'] = min(accumulator['min_amount'], amount)
        accumulator['max_amount'] = max(accumulator['max_amount'], amount)
        accumulator['transactions'].append(element)
        
        return accumulator
    
    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged['total_sales'] += acc['total_sales']
            merged['count'] += acc['count']
            merged['min_amount'] = min(merged['min_amount'], acc['min_amount'])
            merged['max_amount'] = max(merged['max_amount'], acc['max_amount'])
            merged['transactions'].extend(acc['transactions'])
        return merged
    
    def extract_output(self, accumulator):
        return accumulator


class FormatOutput(beam.DoFn):
    """Format aggregation results for BigQuery"""
    
    def process(self, element, window=beam.transforms.window.GlobalWindow()):
        region, aggregates = element
        
        total_sales = aggregates['total_sales']
        count = aggregates['count']
        min_amount = aggregates['min_amount']
        max_amount = aggregates['max_amount']
        average = total_sales / count if count > 0 else 0
        
        # Get window boundaries
        window_start = window.start
        window_end = window.end
        
        output_row = {
            'window_start': Timestamp(window_start / 1e6).to_utc_datetime().isoformat() + 'Z',
            'window_end': Timestamp(window_end / 1e6).to_utc_datetime().isoformat() + 'Z',
            'region': region,
            'total_sales': total_sales,
            'transaction_count': count,
            'average_transaction': round(average, 2),
            'max_transaction': max_amount,
            'min_transaction': min_amount,
            'processing_time': datetime.utcnow().isoformat() + 'Z'
        }
        
        yield output_row


# ============================================================================
# PIPELINE DEFINITION
# ============================================================================


def create_pipeline(argv=None):
    """Create and return the Beam pipeline"""
    
    parser = argparse.ArgumentParser(description='Tumbling Window Dataflow Template')
    
    # Required arguments
    parser.add_argument(
        '--input_topic',
        required=True,
        help='Pub/Sub input topic (format: projects/PROJECT/topics/TOPIC)'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery output table (format: PROJECT:DATASET.TABLE)'
    )
    
    # Optional arguments
    parser.add_argument(
        '--window_size',
        type=int,
        default=3600,
        help='Window size in seconds (default: 3600 = 1 hour)'
    )
    parser.add_argument(
        '--allowed_lateness',
        type=int,
        default=600,
        help='Allowed lateness in seconds (default: 600 = 10 minutes)'
    )
    
    args, pipeline_args = parser.parse_known_args(argv)
    
    # Create pipeline options
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    # Get project from options
    project = options.view_as(StandardOptions).project
    
    LOGGER.info(f"Creating pipeline with configuration:")
    LOGGER.info(f"  Input Topic: {args.input_topic}")
    LOGGER.info(f"  Output Table: {args.output_table}")
    LOGGER.info(f"  Window Size: {args.window_size}s")
    LOGGER.info(f"  Allowed Lateness: {args.allowed_lateness}s")
    
    # Create pipeline
    p = beam.Pipeline(options=options)
    
    (
        p
        # Read from Pub/Sub
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=args.input_topic)
        
        # Parse JSON and add timestamp
        | 'ParseJSON' >> beam.ParDo(ParseJSONAndAddTimestamp())
        
        # Apply tumbling window
        | 'TumblingWindow' >> beam.WindowInto(
            window.FixedWindows(args.window_size),
            allowed_lateness=args.allowed_lateness,
            trigger=beam.transforms.trigger.DefaultTrigger(),
            accumulation_mode=beam.transforms.accumulation_mode.AccumulationMode.ACCUMULATING,
        )
        
        # Group by region and aggregate
        | 'GroupByRegion' >> beam.Map(lambda x: (x.get('region', 'UNKNOWN'), x))
        | 'CombineByRegion' >> beam.CombinePerKey(AggregateByRegion())
        
        # Format output
        | 'FormatOutput' >> beam.ParDo(FormatOutput())
        
        # Write to BigQuery
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=args.output_table,
            schema='AUTODETECT',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )
    
    return p


# ============================================================================
# ENTRYPOINT
# ============================================================================


def run(argv=None):
    """Run the pipeline"""
    pipeline = create_pipeline(argv)
    result = pipeline.run()
    
    # For flex templates, don't wait for completion
    # result.wait_until_finish()


if __name__ == '__main__':
    run()
