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
    """Parse JSON events from Pub/Sub with error handling.
    
    This transformation:
    - Decodes bytes to UTF-8 strings
    - Parses JSON messages
    - Extracts timestamps for windowing
    - Handles parsing errors gracefully
    """
    
    def process(self, element):
        """Decode and parse JSON events from Pub/Sub
        
        Args:
            element: Raw bytes from Pub/Sub message
            
        Yields:
            dict: Parsed JSON record, or nothing if parse fails
        """
        try:
            # Pub/Sub delivers messages as bytes, convert to string
            if isinstance(element, bytes):
                element = element.decode('utf-8')
            
            # Parse JSON string to dictionary
            record = json.loads(element)
            
            # Extract timestamp for windowing (critical for stream processing)
            # Beam uses timestamps to group events into windows
            if 'timestamp' in record:
                # Parse RFC 3339 timestamp (standard ISO format)
                yield beam.utils.timestamp.Timestamp.from_rfc3339(record['timestamp'])
            else:
                # Use current server time if no timestamp in record
                yield beam.utils.timestamp.Timestamp.now()
            
            yield record
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
        except Exception as e:
            logger.error(f"Parsing error: {e}")


class WindowAggregationFn(beam.DoFn):
    """Aggregate events within time windows.
    
    This transformation:
    - Groups events that fell in the same window
    - Calculates aggregate statistics
    - Creates summary records for each window
    
    Key Concept: Windowing divides infinite streams into finite chunks
    for analysis. Events are grouped by timestamp windows (e.g., 1-min)
    """
    
    def process(self, element, window=beam.DoFn.WindowParam):
        """Aggregate window results into statistics
        
        Args:
            element: Tuple of (key, list_of_events) grouped by this key
            window: Window object containing start/end times
            
        Yields:
            dict: Aggregated statistics for this window
        """
        key, events = element
        # Convert iterator to list so we can iterate multiple times
        events = list(events)
        
        # Skip empty windows
        if not events:
            return
        
        # Calculate aggregations
        count = len(events)  # Number of events in window
        # Extract numeric values from each event (with fallback)
        values = [e.get('value', 0) for e in events if isinstance(e, dict)]
        
        # Build aggregation result
        result = {
            'window_start': Timestamp(window.start).to_rfc3339(),  # When window started
            'window_end': Timestamp(window.end).to_rfc3339(),      # When window ended
            'key': key,                                            # Grouping key
            'event_count': count,                                  # Number of events
            'total_value': sum(values) if values else 0,           # Sum of all values
            'avg_value': sum(values) / len(values) if values else 0,  # Average
            'max_value': max(values) if values else 0,             # Maximum
            'min_value': min(values) if values else 0,             # Minimum
            'processing_timestamp': datetime.utcnow().isoformat()  # When we processed it
        }
        
        yield result


class FilterEventsFn(beam.DoFn):
    """Filter events based on validation criteria.
    
    This transformation removes invalid or incomplete events
    to ensure only good data reaches aggregation.
    """
    
    def process(self, element):
        """Filter invalid events
        
        Args:
            element: Potentially invalid record
            
        Yields:
            dict: Valid record, or nothing if invalid
        """
        # Skip non-dict elements (e.g., parsing errors)
        if not isinstance(element, dict):
            return
        
        # Only keep events with required fields
        # Customize these fields based on your data schema
        if 'event_type' in element and 'value' in element:
            yield element
        else:
            logger.warning(f"Event missing required fields: {element}")


def run(argv=None):
    """Execute the streaming event processing pipeline.
    
    This pipeline demonstrates real-time stream processing:
    1. Continuously reads events from Pub/Sub
    2. Windows events into time buckets (e.g., 60-second windows)
    3. Aggregates statistics within each window
    4. Writes results to BigQuery in real-time
    
    Unlike batch pipelines, this runs continuously and processes
    data as it arrives (latency of seconds to minutes).
    
    Args:
        argv: Command-line arguments
    """
    
    parser = argparse.ArgumentParser(
        description='Streaming Real-time Event Processing Pipeline'
    )
    
    # ====== Input/Output Configuration ======
    # Pub/Sub is the input source (streaming), BigQuery is the output
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
    
    # ====== Window Configuration ======
    # Controls how events are grouped together for analysis
    parser.add_argument(
        '--window_duration',
        dest='window_duration',
        type=int,
        default=60,
        help='Fixed window duration in seconds (default: 60 = 1 minute)'
    )
    parser.add_argument(
        '--window_type',
        dest='window_type',
        default='fixed',
        choices=['fixed', 'sliding'],
        help='Type of windowing: fixed or sliding'
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
    
    # ====== Pipeline Options ======
    options = PipelineOptions(pipeline_args)
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = known_args.runner
    
    # Configure for cloud execution with streaming optimizations
    if known_args.runner == 'DataflowRunner':
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = known_args.project
        google_cloud_options.region = known_args.region
        google_cloud_options.temp_location = known_args.temp_location
        # CRITICAL: Enable Streaming Engine for low-latency streaming processing
        google_cloud_options.enable_streaming_engine = True
        
        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = known_args.num_workers
        worker_options.max_num_workers = known_args.max_num_workers
        # Use throughput-based autoscaling (scales based on backlog)
        worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'
    
    # ====== Output BigQuery Schema ======
    # Schema for aggregated results
    bq_schema = '''
        window_start:TIMESTAMP,        # Window start time
        window_end:TIMESTAMP,          # Window end time
        key:STRING,                    # Grouping key (e.g., event_type)
        event_count:INTEGER,           # Number of events in window
        total_value:FLOAT64,           # Sum of values
        avg_value:FLOAT64,             # Average value
        max_value:FLOAT64,             # Maximum value
        min_value:FLOAT64,             # Minimum value
        processing_timestamp:TIMESTAMP # When we processed it
    '''
    
    # ====== Select Windowing Strategy ======
    # Choose between fixed or sliding windows based on requirements
    # Fixed windows: Non-overlapping buckets (e.g., 0-60s, 60-120s)
    # Sliding windows: Overlapping buckets (e.g., 0-60s, 30-90s, 60-120s)
    if known_args.window_type == 'sliding':
        # Sliding window: size + period (slide period)
        windowing = SlidingWindows(
            size=known_args.window_duration,
            period=known_args.window_duration // 2  # 50% overlap
        )
    else:
        # Fixed window: simple non-overlapping time buckets
        windowing = FixedWindows(known_args.window_duration)
    
    # ====== BUILD AND EXECUTE PIPELINE ======
    with beam.Pipeline(options=options) as p:
        (
            p
            # STAGE 1: Read continuously from Pub/Sub
            # This connects to Pub/Sub and reads messages as they arrive
            | 'ReadFromPubSub' >> ReadFromPubSub(topic=known_args.input_topic)
            
            # STAGE 2: Parse events
            # Converts JSON strings to dictionaries and extracts timestamps
            | 'ParseEvents' >> beam.FlatMap(lambda x: [json.loads(x)])
            
            # STAGE 3: Filter invalid events
            # Removes events that don't have required fields
            | 'FilterEvents' >> beam.ParDo(FilterEventsFn())
            
            # STAGE 4: Apply windowing
            # Groups events by time window (e.g., "events from minute 1")
            # This is what makes streaming aggregation possible
            | f'Window_{known_args.window_type}' >> beam.WindowInto(windowing)
            
            # STAGE 5: Extract key for grouping
            # Creates (key, event) tuples for GroupByKey
            # Key could be event_type, user_id, sensor_id, etc.
            | 'ExtractKey' >> beam.Map(lambda x: (x.get('event_type', 'unknown'), x))
            
            # STAGE 6: Group by key
            # Collects all events with the same key in the same window
            # Output: (key, [events]) for aggregation
            | 'GroupByKey' >> beam.GroupByKey()
            
            # STAGE 7: Aggregate window data
            # Calculates statistics (sum, avg, min, max) per window
            | 'AggregateWindow' >> beam.ParDo(WindowAggregationFn())
            
            # STAGE 8: Write to BigQuery
            # Inserts results into BigQuery in real-time (streaming inserts)
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=known_args.output_table,
                schema=bq_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method='STREAMING_INSERTS'  # Fast insertion for streaming data
            )
        )
    
    logger.info("Streaming pipeline started")


if __name__ == '__main__':
    run()
