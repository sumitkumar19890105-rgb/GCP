"""
Common transformation functions for Dataflow pipelines
"""

import json
from datetime import datetime
from typing import Dict, Any, List

import apache_beam as beam


class JSONParserDoFn(beam.DoFn):
    """Parse JSON strings to dictionaries"""
    
    def process(self, element):
        try:
            data = json.loads(element)
            yield data
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")


class TimestampAdderDoFn(beam.DoFn):
    """Add processing timestamp to elements"""
    
    def process(self, element):
        if isinstance(element, dict):
            element['processing_timestamp'] = datetime.utcnow().isoformat()
        yield element


class FieldFilterFn(beam.DoFn):
    """Filter elements based on field conditions"""
    
    def __init__(self, field: str, expected_value: Any = None, 
                 min_value: Any = None, max_value: Any = None):
        self.field = field
        self.expected_value = expected_value
        self.min_value = min_value
        self.max_value = max_value
    
    def process(self, element):
        if not isinstance(element, dict):
            return
        
        value = element.get(self.field)
        
        # Check exact match
        if self.expected_value is not None:
            if value == self.expected_value:
                yield element
        # Check range
        elif self.min_value is not None and self.max_value is not None:
            try:
                if self.min_value <= value <= self.max_value:
                    yield element
            except TypeError:
                pass


class AggregationFn(beam.DoFn):
    """Generic aggregation function"""
    
    def __init__(self, metric_field: str, metric_type: str = 'sum'):
        """
        Args:
            metric_field: Field to aggregate
            metric_type: 'sum', 'avg', 'count', 'min', 'max'
        """
        self.metric_field = metric_field
        self.metric_type = metric_type
    
    def process(self, element):
        key, values = element
        values = list(values)
        
        if not values:
            return
        
        # Extract metric values
        metrics = []
        for v in values:
            if isinstance(v, dict) and self.metric_field in v:
                try:
                    metrics.append(float(v[self.metric_field]))
                except (ValueError, TypeError):
                    pass
        
        if not metrics:
            return
        
        result = {'key': key}
        
        if self.metric_type == 'sum':
            result['metric_value'] = sum(metrics)
        elif self.metric_type == 'avg':
            result['metric_value'] = sum(metrics) / len(metrics)
        elif self.metric_type == 'count':
            result['metric_value'] = len(metrics)
        elif self.metric_type == 'min':
            result['metric_value'] = min(metrics)
        elif self.metric_type == 'max':
            result['metric_value'] = max(metrics)
        
        yield result


class DataEnrichmentFn(beam.DoFn):
    """Enrich data with additional fields"""
    
    def __init__(self, enrichment_map: Dict[str, Any]):
        """
        Args:
            enrichment_map: Dict of field:value pairs to add
        """
        self.enrichment_map = enrichment_map
    
    def process(self, element):
        if isinstance(element, dict):
            element.update(self.enrichment_map)
        yield element


class DataValidationFn(beam.DoFn):
    """Validate data and filter invalid records"""
    
    def __init__(self, required_fields: List[str], type_checks: Dict[str, type] = None):
        """
        Args:
            required_fields: List of required field names
            type_checks: Dict of field: expected_type pairs
        """
        self.required_fields = required_fields
        self.type_checks = type_checks or {}
    
    def process(self, element):
        if not isinstance(element, dict):
            return
        
        # Check required fields
        for field in self.required_fields:
            if field not in element:
                print(f"Missing required field: {field}")
                return
        
        # Check types
        for field, expected_type in self.type_checks.items():
            if field in element:
                if not isinstance(element[field], expected_type):
                    print(f"Type mismatch for {field}: expected {expected_type}")
                    return
        
        yield element


class DuplicateRemovalFn(beam.DoFn):
    """Remove duplicate records based on key fields"""
    
    def __init__(self, key_fields: List[str]):
        """
        Args:
            key_fields: Fields to use as duplicate key
        """
        self.key_fields = key_fields
        self.seen = set()
    
    def process(self, element):
        if isinstance(element, dict):
            key = tuple(element.get(f) for f in self.key_fields)
            if key not in self.seen:
                self.seen.add(key)
                yield element


# Common composite transforms

def ParseJSONAndFilter(required_fields: List[str] = None):
    """Composite transform: parse JSON and validate fields"""
    required_fields = required_fields or []
    
    return (
        beam.ParDo(JSONParserDoFn())
        | 'ValidateData' >> beam.ParDo(
            DataValidationFn(required_fields=required_fields)
        )
    )


def EnrichAndTimestamp(enrichment_map: Dict[str, Any]):
    """Composite transform: enrich data and add timestamp"""
    return (
        beam.ParDo(DataEnrichmentFn(enrichment_map))
        | 'AddTimestamp' >> beam.ParDo(TimestampAdderDoFn())
    )
