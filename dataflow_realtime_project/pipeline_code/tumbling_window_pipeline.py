"""
Tumbling (Fixed) Window Pipeline
Aggregates sales data by region for 1-hour windows
"""

import argparse
from datetime import datetime
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.core import ParDo, WindowedValue
from apache_beam.utils.timestamp import Timestamp

from base_pipeline import BasePipeline, parse_json_with_timestamp, WindowMetrics


class AggregateByRegionFn(beam.DoFn):
    """Aggregate sales by region within a tumbling window"""
    
    def process(self, element, window=beam.DoFn.WindowParam):
        """
        Process grouped transactions and create aggregated summary
        
        Args:
            element: (region, [transactions])
            window: Window information
        """
        region, transactions = element
        transactions = list(transactions)
        
        if not transactions:
            return
        
        # Calculate aggregations
        amounts = [t['amount_usd'] for t in transactions]
        total_sales = sum(amounts)
        count = len(transactions)
        avg_value = total_sales / count
        
        # Get most common category
        categories = {}
        for t in transactions:
            cat = t.get('product_category', 'Unknown')
            categories[cat] = categories.get(cat, 0) + 1
        top_category = max(categories.items(), key=lambda x: x[1])[0] if categories else 'Unknown'
        
        result = {
            'window_start': Timestamp(window.start).to_rfc3339(),
            'window_end': Timestamp(window.end).to_rfc3339(),
            'region': region,
            'total_sales': total_sales,
            'transaction_count': count,
            'avg_transaction_value': round(avg_value, 2),
            'top_category': top_category,
            'processing_time': datetime.utcnow().isoformat()
        }
        
        yield result


class TumblingWindowPipeline(BasePipeline):
    """Tumbling window pipeline for sales aggregation"""
    
    def __init__(self, project=None, runner='DirectRunner', region=None, 
                 input_topic=None, output_table=None, window_duration=3600):
        super().__init__(project, runner, region)
        self.input_topic = input_topic
        self.output_table = output_table
        self.window_duration = window_duration  # Default 1 hour
    
    def run(self):
        """Execute the pipeline"""
        with beam.Pipeline(options=self.options) as p:
            (p
             | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                 topic=self.input_topic)
             | 'ParseTransaction' >> beam.FlatMap(
                 parse_json_with_timestamp, timestamp_field='timestamp')
             | 'TumblingWindow' >> beam.WindowInto(
                 FixedWindows(self.window_duration))
             | 'ExtractRegion' >> beam.Map(lambda x: (x['region'], x))
             | 'GroupByRegion' >> beam.GroupByKey()
             | 'AggregateByRegion' >> beam.ParDo(AggregateByRegionFn())
             | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                 table=self.output_table,
                 schema='bigquery_schemas/aggregated_sales_schema.json',
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )


def main():
    parser = argparse.ArgumentParser(
        description='Tumbling Window Sales Aggregation Pipeline'
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
    
    args = parser.parse_args()
    
    pipeline = TumblingWindowPipeline(
        project=args.project,
        runner=args.runner,
        region=args.region,
        input_topic=args.input_topic,
        output_table=args.output_table,
        window_duration=args.window_duration
    )
    
    pipeline.run()


if __name__ == '__main__':
    main()
