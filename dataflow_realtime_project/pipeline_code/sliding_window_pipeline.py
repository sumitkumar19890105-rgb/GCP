"""
Sliding Window Pipeline
Computes moving averages for IoT sensor data
"""

import argparse
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import SlidingWindows
from apache_beam.utils.timestamp import Timestamp

from base_pipeline import BasePipeline, parse_json_with_timestamp


class CalculateSlidingAverageFn(beam.DoFn):
    """Calculate sliding window averages for sensor data"""
    
    def process(self, element, window=beam.DoFn.WindowParam):
        """
        Process sensor readings and calculate averages
        
        Args:
            element: (sensor_id, [readings])
            window: Window information
        """
        sensor_id, readings = element
        readings = list(readings)
        
        if not readings:
            return
        
        # Extract values
        temperatures = [r['temperature'] for r in readings]
        humidities = [r['humidity'] for r in readings]
        
        # Calculate statistics
        avg_temp = sum(temperatures) / len(temperatures)
        avg_humidity = sum(humidities) / len(humidities)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        
        result = {
            'window_start': Timestamp(window.start).to_rfc3339(),
            'window_end': Timestamp(window.end).to_rfc3339(),
            'sensor_id': sensor_id,
            'device_id': readings[0].get('device_id', 'Unknown'),
            'location': readings[0].get('location', 'Unknown'),
            'avg_temperature': round(avg_temp, 2),
            'avg_humidity': round(avg_humidity, 2),
            'max_temperature': round(max_temp, 2),
            'min_temperature': round(min_temp, 2),
            'reading_count': len(readings),
            'processing_time': datetime.utcnow().isoformat()
        }
        
        yield result


class SlidingWindowPipeline(BasePipeline):
    """Sliding window pipeline for sensor data aggregation"""
    
    def __init__(self, project=None, runner='DirectRunner', region=None,
                 input_topic=None, output_table=None, 
                 window_size=600, window_period=300):
        super().__init__(project, runner, region)
        self.input_topic = input_topic
        self.output_table = output_table
        self.window_size = window_size      # 10 minutes
        self.window_period = window_period  # 5 minute slide
    
    def run(self):
        """Execute the pipeline"""
        with beam.Pipeline(options=self.options) as p:
            (p
             | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                 topic=self.input_topic)
             | 'ParseSensorData' >> beam.FlatMap(
                 parse_json_with_timestamp, timestamp_field='timestamp')
             | 'SlidingWindow' >> beam.WindowInto(
                 SlidingWindows(size=self.window_size, period=self.window_period))
             | 'ExtractSensor' >> beam.Map(lambda x: (x['sensor_id'], x))
             | 'GroupBySensor' >> beam.GroupByKey()
             | 'CalculateAverage' >> beam.ParDo(CalculateSlidingAverageFn())
             | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                 table=self.output_table,
                 schema='bigquery_schemas/sensor_data_schema.json',
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )


def main():
    parser = argparse.ArgumentParser(
        description='Sliding Window Sensor Data Pipeline'
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
        '--window_size',
        type=int,
        default=600,
        help='Sliding window size in seconds'
    )
    parser.add_argument(
        '--window_period',
        type=int,
        default=300,
        help='Sliding window period in seconds'
    )
    
    args = parser.parse_args()
    
    pipeline = SlidingWindowPipeline(
        project=args.project,
        runner=args.runner,
        region=args.region,
        input_topic=args.input_topic,
        output_table=args.output_table,
        window_size=args.window_size,
        window_period=args.window_period
    )
    
    pipeline.run()


if __name__ == '__main__':
    main()
