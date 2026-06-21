"""
Streaming Dataflow Pipeline - Real-time Event Processing
Reads from Pub/Sub, applies transformations, and writes to BigQuery

Use cases:
- Real-time analytics
- Event streaming
- Continuous data processing
- Live dashboards
"""

import argparse
import logging
from datetime import datetime
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.transforms.window import FixedWindows, SlidingWindows
from apache_beam.utils.timestamp import Timestamp


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ParseEventFn(beam.DoFn):
    """Parse JSON events from Pub/Sub"""
    
    def process(self, element):
        """Decode and parse JSON"""
        try:
            # Decode bytes to string
            if isinstance(element, bytes):
                element = element.decode('utf-8')
            
            # Parse JSON
            record = json.loads(element)
            
            # Extract timestamp
            if 'timestamp' in record:
                yield beam.utils.timestamp.Timestamp.from_rfc3339(record['timestamp'])
            else:
                yield beam.utils.timestamp.Timestamp.now()
            
            yield record
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
        except Exception as e:
            logger.error(f"Parsing error: {e}")


class WindowAggregationFn(beam.DoFn):
    """Aggregate events within time windows"""
    
    def process(self, element, window=beam.DoFn.WindowParam):
        """Aggregate window results"""
        key, events = element
        events = list(events)
        
        if not events:
            return
        
        # Calculate aggregations
        count = len(events)
        values = [e.get('value', 0) for e in events if isinstance(e, dict)]
        
        result = {
            'window_start': Timestamp(window.start).to_rfc3339(),
            'window_end': Timestamp(window.end).to_rfc3339(),
            'key': key,
            'event_count': count,
            'total_value': sum(values) if values else 0,
            'avg_value': sum(values) / len(values) if values else 0,
            'max_value': max(values) if values else 0,
            'min_value': min(values) if values else 0,
            'processing_timestamp': datetime.utcnow().isoformat()
        }
        
        yield result


class FilterEventsFn(beam.DoFn):
    """Filter events based on criteria"""
    
    def process(self, element):
        """Filter invalid events"""
        if not isinstance(element, dict):
            return
        
        # Add validation logic
        if 'event_type' in element and 'value' in element:
            yield element
        else:
            logger.warning(f"Event missing required fields: {element}")


def run(argv=None):
    """Run the streaming pipeline"""
    
    parser = argparse.ArgumentParser(
        description='Streaming Real-time Event Processing Pipeline'
    )
    
    # Input/Output arguments
    parser.add_argument(
        '--input_topic',
        dest='input_topic',
        required=True,
        help='Input Pub/Sub topic (e.g., projects/project-id/topics/topic-name)'
    )
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='Output BigQuery table (e.g., project:dataset.table)'
    )
    
    # Window arguments
    parser.add_argument(
        '--window_duration',
        dest='window_duration',
        type=int,
        default=60,
        help='Fixed window duration in seconds (default: 60)'
    )
    parser.add_argument(
        '--window_type',
        dest='window_type',
        default='fixed',
        choices=['fixed', 'sliding'],
        help='Type of windowing'
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
    parser.add_argument('--max_num_workers', dest='max_num_workers', type=int, default=10)
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Create pipeline options
    options = PipelineOptions(pipeline_args)
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = known_args.runner
    
    if known_args.runner == 'DataflowRunner':
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project
        google_cloud_options.region = known_args.region
        google_cloud_options.temp_location = known_args.temp_location
        google_cloud_options.enable_streaming_engine = True
        
        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = known_args.num_workers
        worker_options.max_num_workers = known_args.max_num_workers
        worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'
    
    # Define BigQuery schema
    bq_schema = '''
        window_start:TIMESTAMP,
        window_end:TIMESTAMP,
        key:STRING,
        event_count:INTEGER,
        total_value:FLOAT64,
        avg_value:FLOAT64,
        max_value:FLOAT64,
        min_value:FLOAT64,
        processing_timestamp:TIMESTAMP
    '''
    
    # Select windowing strategy
    if known_args.window_type == 'sliding':
        windowing = SlidingWindows(
            size=known_args.window_duration,
            period=known_args.window_duration // 2
        )
    else:
        windowing = FixedWindows(known_args.window_duration)
    
    # Build and run pipeline
    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromPubSub' >> ReadFromPubSub(topic=known_args.input_topic)
         | 'ParseEvents' >> beam.FlatMap(lambda x: [json.loads(x)])
         | 'FilterEvents' >> beam.ParDo(FilterEventsFn())
         | f'Window_{known_args.window_type}' >> beam.WindowInto(windowing)
         | 'ExtractKey' >> beam.Map(lambda x: (x.get('event_type', 'unknown'), x))
         | 'GroupByKey' >> beam.GroupByKey()
         | 'AggregateWindow' >> beam.ParDo(WindowAggregationFn())
         | 'WriteToBigQuery' >> WriteToBigQuery(
             table=known_args.output_table,
             schema=bq_schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             method='STREAMING_INSERTS'
         )
        )
    
    logger.info("Streaming pipeline started")


if __name__ == '__main__':
    run()
