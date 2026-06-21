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
    """Enrich events with ingestion metadata.
    
    This transformation adds timestamps and version info
    useful for tracking and debugging.
    """
    
    def process(self, element):
        """Add enrichment fields
        
        Args:
            element: Raw Pub/Sub message (bytes)
            
        Yields:
            dict: Parsed record with enrichment fields added
        """
        # Decode bytes to string if needed
        if isinstance(element, bytes):
            element = element.decode('utf-8')
        
        try:
            # Parse JSON
            record = json.loads(element)
            # Add operational metadata
            record['ingestion_time'] = datetime.utcnow().isoformat()
            record['pipeline_version'] = '1.0'
            yield record
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing event: {e}")


class SessionAggregationFn(beam.DoFn):
    """Calculate session metrics and statistics.
    
    Sessions are defined by a time gap - if there's > session_gap seconds
    between events for the same user, it's a new session.
    
    This is perfect for user behavior analysis, e.g.:
    - User clicks for 5 minutes (session 1)
    - Leaves for 10 minutes (gap)
    - Clicks again (session 2)
    """
    
    def process(self, element, window=beam.DoFn.WindowParam):
        """Aggregate session data into statistics
        
        Args:
            element: Tuple of (user_id, list_of_events) for this session
            window: Session window object containing start/end times
            
        Yields:
            dict: Session summary with statistics
        """
        user_id, events = element
        # Convert iterator to list for processing
        events = list(events)
        
        # Skip empty sessions
        if not events:
            return
        
        # Calculate session metrics
        session_events = len(events)  # How many events in session
        session_duration = (window.end - window.start)  # How long was the session
        event_types = {}  # Counter for event types
        
        # Count occurrences of each event type
        for event in events:
            if isinstance(event, dict):
                event_type = event.get('event_type', 'unknown')
                event_types[event_type] = event_types.get(event_type, 0) + 1
        
        # Build session summary
        result = {
            'session_start': Timestamp(window.start).to_rfc3339(),           # When user activity started
            'session_end': Timestamp(window.end).to_rfc3339(),               # When activity ended
            'user_id': user_id,                                             # Which user
            'session_duration_seconds': int(session_duration),              # Total session length
            'total_events': session_events,                                 # Event count
            'event_types': json.dumps(event_types),                         # Event type breakdown
            'most_common_event': max(event_types.items(), key=lambda x: x[1])[0] if event_types else 'unknown',
            'processing_timestamp': datetime.utcnow().isoformat()           # When we processed
        }
        
        yield result


def run(argv=None):
    """Execute the streaming session aggregation pipeline.
    
    This pipeline analyzes user behavior by grouping events into sessions.
    A session ends when there's a gap of more than session_gap seconds
    without events from the same user.
    
    Example use case: Analyze web user sessions
    - User logs in (event 1)
    - Clicks around for 5 minutes
    - Leaves browser idle for 20 minutes
    - Returns and clicks more (new session started)
    
    Args:
        argv: Command-line arguments
    """
    
    parser = argparse.ArgumentParser(
        description='Streaming Session Aggregation Pipeline'
    )
    
    # ====== Input/Output Configuration ======
    parser.add_argument(
        '--input_topic',
        dest='input_topic',
        required=True,
        help='Input Pub/Sub topic (continuous stream of user events)'
    )
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='Output BigQuery table (stores session summaries)'
    )
    
    # ====== Session Window Configuration ======
    # Critical parameter: defines what constitutes a session boundary
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
    
    # ====== Pipeline Options ======
    options = PipelineOptions(pipeline_args)
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = known_args.runner
    
    # Configure for cloud streaming execution
    if known_args.runner == 'DataflowRunner':
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project
        google_cloud_options.region = known_args.region
        google_cloud_options.temp_location = known_args.temp_location
        # Enable Streaming Engine for low-latency processing
        google_cloud_options.enable_streaming_engine = True
        
        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = known_args.num_workers
    
    # ====== Output BigQuery Schema ======
    # Schema for session summary records
    bq_schema = '''
        session_start:TIMESTAMP,           # When session began
        session_end:TIMESTAMP,             # When session ended
        user_id:STRING,                    # Which user
        session_duration_seconds:INTEGER,  # How long was session (in seconds)
        total_events:INTEGER,              # Number of events in session
        event_types:STRING,                # JSON of event type counts
        most_common_event:STRING,          # Most frequent event type
        processing_timestamp:TIMESTAMP     # When we processed it
    '''
    
    # ====== BUILD AND EXECUTE PIPELINE ======
    with beam.Pipeline(options=options) as p:
        (
            p
            # STAGE 1: Read continuous stream from Pub/Sub
            | 'ReadFromPubSub' >> ReadFromPubSub(topic=known_args.input_topic)
            
            # STAGE 2: Enrich events with metadata
            | 'EnrichEvents' >> beam.ParDo(EnrichEventFn())
            
            # STAGE 3: Apply session windowing
            # Sessions: dynamic windows based on time gaps
            # If gap > session_gap between events for same user = new session
            | 'SessionWindow' >> beam.WindowInto(
                Sessions(known_args.session_gap)  # Gap threshold for sessions
            )
            
            # STAGE 4: Extract user ID as grouping key
            # Creates (user_id, event) tuples for GroupByKey
            | 'ExtractUserId' >> beam.Map(lambda x: (x.get('user_id', 'anonymous'), x))
            
            # STAGE 5: Group events by user within each session
            # Output: (user_id, [events]) for that session
            | 'GroupByUser' >> beam.GroupByKey()
            
            # STAGE 6: Calculate session statistics
            # Aggregates event counts, types, duration, etc.
            | 'AggregateSession' >> beam.ParDo(SessionAggregationFn())
            
            # STAGE 7: Write session summaries to BigQuery
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
