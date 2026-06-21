"""
Session Window Pipeline
Groups user activity events into sessions based on inactivity gaps
"""

import argparse
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import Sessions
from apache_beam.utils.timestamp import Timestamp

from base_pipeline import BasePipeline, parse_json_with_timestamp


class CreateSessionSummaryFn(beam.DoFn):
    """Create session summary from grouped activities"""
    
    def process(self, element, window=beam.DoFn.WindowParam):
        """
        Process user activities and create session summary
        
        Args:
            element: (user_id, [activities])
            window: Session window information
        """
        user_id, activities = element
        activities = sorted(
            list(activities),
            key=lambda x: x['timestamp']
        )
        
        if not activities:
            return
        
        # Extract event types
        event_types = [a.get('event_type', 'UNKNOWN') for a in activities]
        event_count_by_type = {}
        for et in event_types:
            event_count_by_type[et] = event_count_by_type.get(et, 0) + 1
        
        # Get session info
        session_id = activities[0].get('session_id', 'UNKNOWN')
        session_start = activities[0]['timestamp']
        session_end = activities[-1]['timestamp']
        
        result = {
            'window_start': Timestamp(window.start).to_rfc3339(),
            'window_end': Timestamp(window.end).to_rfc3339(),
            'user_id': user_id,
            'session_id': session_id,
            'session_start': session_start,
            'session_end': session_end,
            'event_count': len(activities),
            'event_types': list(event_types),
            'event_summary': event_count_by_type,
            'processing_time': datetime.utcnow().isoformat()
        }
        
        yield result


class SessionWindowPipeline(BasePipeline):
    """Session window pipeline for user activity analysis"""
    
    def __init__(self, project=None, runner='DirectRunner', region=None,
                 input_topic=None, output_table=None, gap_duration=1800):
        super().__init__(project, runner, region)
        self.input_topic = input_topic
        self.output_table = output_table
        self.gap_duration = gap_duration  # 30 minutes
    
    def run(self):
        """Execute the pipeline"""
        with beam.Pipeline(options=self.options) as p:
            (p
             | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                 topic=self.input_topic)
             | 'ParseActivity' >> beam.FlatMap(
                 parse_json_with_timestamp, timestamp_field='timestamp')
             | 'ExtractUser' >> beam.Map(lambda x: (x['user_id'], x))
             | 'SessionWindow' >> beam.WindowInto(
                 Sessions(gap_duration=self.gap_duration))
             | 'GroupByUser' >> beam.GroupByKey()
             | 'CreateSessionSummary' >> beam.ParDo(CreateSessionSummaryFn())
             | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                 table=self.output_table,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )


def main():
    parser = argparse.ArgumentParser(
        description='Session Window User Activity Pipeline'
    )
    parser.add_argument(
        '--project',
        required=False,
        help='GCP Project ID'
    )
    parser.add_argument(
        '--runner',
        default='DirectRunner',
        help='Runner: DirectRunner or DataflowRunner'
    )
    parser.add_argument(
        '--region',
        default='us-central1',
        help='GCP region'
    )
    parser.add_argument(
        '--input_topic',
        required=True,
        help='Pub/Sub topic for input'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery output table'
    )
    parser.add_argument(
        '--gap_duration',
        type=int,
        default=1800,
        help='Inactivity gap duration in seconds'
    )
    
    args = parser.parse_args()
    
    pipeline = SessionWindowPipeline(
        project=args.project,
        runner=args.runner,
        region=args.region,
        input_topic=args.input_topic,
        output_table=args.output_table,
        gap_duration=args.gap_duration
    )
    
    pipeline.run()


if __name__ == '__main__':
    main()
