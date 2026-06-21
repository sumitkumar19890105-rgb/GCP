"""
Streaming Pipeline - Real-time Session Aggregation
Groups events by session and calculates metrics

Use cases:
- User session analysis
- Real-time user behavior tracking
- Session-based aggregations
- User activity monitoring
"""

import argparse
import logging
from datetime import datetime
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.transforms.window import Sessions
from apache_beam.utils.timestamp import Timestamp


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnrichEventFn(beam.DoFn):
    """Enrich events with additional metadata"""
    
    def process(self, element):
        """Add enrichment fields"""
        if isinstance(element, bytes):
            element = element.decode('utf-8')
        
        try:
            record = json.loads(element)
            record['ingestion_time'] = datetime.utcnow().isoformat()
            record['pipeline_version'] = '1.0'
            yield record
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing event: {e}")


class SessionAggregationFn(beam.DoFn):
    """Calculate session metrics"""
    
    def process(self, element, window=beam.DoFn.WindowParam):
        """Aggregate session data"""
        user_id, events = element
        events = list(events)
        
        if not events:
            return
        
        # Calculate session metrics
        session_events = len(events)
        session_duration = (window.end - window.start)
        event_types = {}
        
        for event in events:
            if isinstance(event, dict):
                event_type = event.get('event_type', 'unknown')
                event_types[event_type] = event_types.get(event_type, 0) + 1
        
        result = {
            'session_start': Timestamp(window.start).to_rfc3339(),
            'session_end': Timestamp(window.end).to_rfc3339(),
            'user_id': user_id,
            'session_duration_seconds': int(session_duration),
            'total_events': session_events,
            'event_types': json.dumps(event_types),
            'most_common_event': max(event_types.items(), key=lambda x: x[1])[0] if event_types else 'unknown',
            'processing_timestamp': datetime.utcnow().isoformat()
        }
        
        yield result


def run(argv=None):
    """Run the streaming session pipeline"""
    
    parser = argparse.ArgumentParser(
        description='Streaming Session Aggregation Pipeline'
    )
    
    # Input/Output arguments
    parser.add_argument(
        '--input_topic',
        dest='input_topic',
        required=True,
        help='Input Pub/Sub topic'
    )
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='Output BigQuery table'
    )
    
    # Session window arguments
    parser.add_argument(
        '--session_gap',
        dest='session_gap',
        type=int,
        default=300,
        help='Session inactivity gap in seconds (default: 300 = 5 minutes)'
    )
    
    # Dataflow arguments
    parser.add_argument(
        '--runner',
        dest='runner',
        default='DirectRunner',
        choices=['DirectRunner', 'DataflowRunner']
    )
    parser.add_argument('--project', dest='project')
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
        google_cloud_options.enable_streaming_engine = True
        
        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = known_args.num_workers
    
    # Define BigQuery schema
    bq_schema = '''
        session_start:TIMESTAMP,
        session_end:TIMESTAMP,
        user_id:STRING,
        session_duration_seconds:INTEGER,
        total_events:INTEGER,
        event_types:STRING,
        most_common_event:STRING,
        processing_timestamp:TIMESTAMP
    '''
    
    # Build and run pipeline
    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromPubSub' >> ReadFromPubSub(topic=known_args.input_topic)
         | 'EnrichEvents' >> beam.ParDo(EnrichEventFn())
         | 'SessionWindow' >> beam.WindowInto(
             Sessions(known_args.session_gap)
         )
         | 'ExtractUserId' >> beam.Map(lambda x: (x.get('user_id', 'anonymous'), x))
         | 'GroupByUser' >> beam.GroupByKey()
         | 'AggregateSession' >> beam.ParDo(SessionAggregationFn())
         | 'WriteToBigQuery' >> WriteToBigQuery(
             table=known_args.output_table,
             schema=bq_schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
         )
        )
    
    logger.info("Streaming session pipeline started")


if __name__ == '__main__':
    run()
