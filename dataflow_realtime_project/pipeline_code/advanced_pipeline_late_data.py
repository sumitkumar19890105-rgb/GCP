"""
Advanced Pipeline with Late Data Handling and Watermark Triggers
Demonstrates allowed lateness and custom trigger configurations
"""

import argparse
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows, AfterWatermark, AfterCount, Repeatedly
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.utils.timestamp import Timestamp

from base_pipeline import BasePipeline, parse_json_with_timestamp


class ProcessWithWatermarkFn(beam.DoFn):
    """Process data with watermark information"""
    
    def process(self, element, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam):
        """
        Process element and include timing information
        
        Args:
            element: Data element
            window: Window information
            timestamp: Event timestamp
        """
        element['window_start'] = Timestamp(window.start).to_rfc3339()
        element['window_end'] = Timestamp(window.end).to_rfc3339()
        element['event_timestamp'] = Timestamp(timestamp).to_rfc3339()
        element['processing_time'] = datetime.utcnow().isoformat()
        
        yield element


class LateDataHandlingPipeline(BasePipeline):
    """Pipeline with late data and watermark handling"""
    
    def __init__(self, project=None, runner='DirectRunner', region=None,
                 input_topic=None, output_table=None,
                 window_duration=3600, allowed_lateness=600,
                 early_count_trigger=10, late_count_trigger=5):
        super().__init__(project, runner, region)
        self.input_topic = input_topic
        self.output_table = output_table
        self.window_duration = window_duration      # 1 hour
        self.allowed_lateness = allowed_lateness    # 10 minutes
        self.early_count_trigger = early_count_trigger
        self.late_count_trigger = late_count_trigger
    
    def run(self):
        """Execute the pipeline with late data handling"""
        with beam.Pipeline(options=self.options) as p:
            (p
             | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                 topic=self.input_topic)
             | 'ParseTransaction' >> beam.FlatMap(
                 parse_json_with_timestamp, timestamp_field='timestamp')
             | 'WindowWithLateData' >> beam.WindowInto(
                 FixedWindows(self.window_duration),
                 trigger=Repeatedly(
                     AfterWatermark(
                         early=AfterCount(self.early_count_trigger),
                         late=AfterCount(self.late_count_trigger)
                     )
                 ),
                 accumulation_mode=AccumulationMode.ACCUMULATING,
                 allowed_lateness=self.allowed_lateness)
             | 'ProcessWithTiming' >> beam.ParDo(ProcessWithWatermarkFn())
             | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                 table=self.output_table,
                 schema='bigquery_schemas/transactions_schema.json',
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )


class DiscardingAccumulationPipeline(BasePipeline):
    """Pipeline using DISCARDING accumulation mode for late data"""
    
    def __init__(self, project=None, runner='DirectRunner', region=None,
                 input_topic=None, output_table=None, 
                 window_duration=3600, allowed_lateness=300):
        super().__init__(project, runner, region)
        self.input_topic = input_topic
        self.output_table = output_table
        self.window_duration = window_duration
        self.allowed_lateness = allowed_lateness
    
    def run(self):
        """Execute with discarding accumulation"""
        with beam.Pipeline(options=self.options) as p:
            (p
             | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                 topic=self.input_topic)
             | 'ParseTransaction' >> beam.FlatMap(
                 parse_json_with_timestamp, timestamp_field='timestamp')
             | 'WindowWithDiscarding' >> beam.WindowInto(
                 FixedWindows(self.window_duration),
                 trigger=AfterWatermark(late=AfterCount(1)),
                 accumulation_mode=AccumulationMode.DISCARDING,
                 allowed_lateness=self.allowed_lateness)
             | 'ProcessWithTiming' >> beam.ParDo(ProcessWithWatermarkFn())
             | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                 table=self.output_table,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )


def main():
    parser = argparse.ArgumentParser(
        description='Late Data and Watermark Pipeline'
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
        '--window_duration',
        type=int,
        default=3600,
        help='Window duration in seconds'
    )
    parser.add_argument(
        '--allowed_lateness',
        type=int,
        default=600,
        help='Allowed lateness in seconds'
    )
    parser.add_argument(
        '--accumulation_mode',
        default='ACCUMULATING',
        choices=['ACCUMULATING', 'DISCARDING', 'ACCUMULATING_AND_RETRACTING'],
        help='Accumulation mode for late data'
    )
    
    args = parser.parse_args()
    
    if args.accumulation_mode == 'DISCARDING':
        pipeline = DiscardingAccumulationPipeline(
            project=args.project,
            runner=args.runner,
            region=args.region,
            input_topic=args.input_topic,
            output_table=args.output_table,
            window_duration=args.window_duration,
            allowed_lateness=args.allowed_lateness
        )
    else:
        pipeline = LateDataHandlingPipeline(
            project=args.project,
            runner=args.runner,
            region=args.region,
            input_topic=args.input_topic,
            output_table=args.output_table,
            window_duration=args.window_duration,
            allowed_lateness=args.allowed_lateness
        )
    
    pipeline.run()


if __name__ == '__main__':
    main()
