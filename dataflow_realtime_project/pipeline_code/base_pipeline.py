"""
Base Pipeline Configuration and Common Utilities
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BasePipeline(ABC):
    """Base class for Dataflow pipelines"""
    
    def __init__(self, project=None, runner='DirectRunner', region=None):
        self.project = project
        self.runner = runner
        self.region = region
        self.options = self._create_options()
    
    def _create_options(self):
        """Create pipeline options"""
        options = PipelineOptions()
        
        if self.runner != 'DirectRunner':
            options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project = self.project
            options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = self.region
            options.view_as(beam.options.pipeline_options.WorkerOptions).worker_machine_type = 'n1-standard-2'
            options.view_as(beam.options.pipeline_options.WorkerOptions).num_workers = 2
        
        return options
    
    @abstractmethod
    def run(self):
        """Run the pipeline"""
        pass


def parse_json_with_timestamp(element, timestamp_field='timestamp'):
    """
    Parse JSON element and extract timestamp
    
    Args:
        element: JSON string
        timestamp_field: Field name containing timestamp
        
    Yields:
        TimestampedValue with parsed data
    """
    try:
        data = json.loads(element)
        if timestamp_field in data:
            ts_str = data[timestamp_field]
            timestamp = datetime.fromisoformat(
                ts_str.replace('Z', '+00:00')
            ).timestamp()
            yield beam.window.TimestampedValue(data, timestamp)
        else:
            logger.warning(f"Missing {timestamp_field} in data: {data}")
            yield beam.window.TimestampedValue(data, datetime.utcnow().timestamp())
    except Exception as e:
        logger.error(f"Error parsing JSON: {e}, element: {element}")


def format_for_bigquery(element):
    """Format element for BigQuery insertion"""
    if isinstance(element, dict):
        return element
    return {'value': element}


class WindowMetrics:
    """Utility for extracting window information"""
    
    @staticmethod
    def add_window_info(element, window):
        """Add window start and end times to element"""
        element['window_start'] = datetime.fromtimestamp(window.start).isoformat()
        element['window_end'] = datetime.fromtimestamp(window.end).isoformat()
        element['processing_time'] = datetime.utcnow().isoformat()
        return element
