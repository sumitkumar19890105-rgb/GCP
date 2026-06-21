# GCP Dataflow Real-Time Processing Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Windowing Strategies](#windowing-strategies)
3. [Watermarks & Late Data](#watermarks--late-data)
4. [Sample Data](#sample-data)
5. [Project Architecture](#project-architecture)
6. [Pub/Sub Integration](#pubsub-integration)
7. [BigQuery Integration](#bigquery-integration)
8. [Pipeline Examples](#pipeline-examples)

---

## Introduction

Apache Beam (Google Cloud Dataflow) is a unified model for batch and real-time processing. This guide demonstrates:
- Multiple windowing strategies
- Handling late-arriving data
- Real-time data ingestion via Pub/Sub
- Data transformation and storage to BigQuery

**Key Concepts:**
- **Windowing**: Dividing unbounded data streams into finite chunks
- **Watermarks**: Tracking progress through event time
- **Triggers**: Determining when to output results
- **Late Data**: Handling events that arrive after the window has closed

---

## Windowing Strategies

### 1. **Tumbling (Fixed) Windows**
Fixed-size, non-overlapping windows. Each event belongs to exactly one window.

**Use Case:** Hourly sales aggregations, daily reports

```
Time: |--[0,1)--|--[1,2)--|--[2,3)--|--[3,4)--|
Data:  a b     c d     e f     g h
```

**Window Duration:** 1 hour  
**Slide Interval:** 1 hour (no overlap)

---

### 2. **Sliding Windows**
Overlapping windows that move at a fixed interval. Events can belong to multiple windows.

**Use Case:** Moving averages, trend detection

```
Time: |--[0,2)--|
      |    --[1,3)--|
      |       --[2,4)--|
```

**Window Duration:** 2 hours  
**Slide Interval:** 1 hour

---

### 3. **Session Windows**
Dynamic windows based on inactivity gaps. Windows close when no data arrives for a specified duration.

**Use Case:** User sessions, event sessions

```
User Activity: Event1 Event2 Event3 <gap> Event4 Event5
              |---Session 1---|      |---Session 2---|
```

**Gap Duration:** 30 minutes

---

## Watermarks & Late Data

### Watermarks
A watermark represents the pipeline's notion of input completeness. It marks the time beyond which no data is expected to arrive (for that partition).

```
Watermark Progress:
Time ===|====|====|====|====|====>
        Event Time
```

### Allowed Lateness
Allows late-arriving data to be included in window computations instead of being discarded.

```
Window [0-1) closes at watermark = 1
With allowed lateness = 30 seconds:
  - Data arriving before T=1:30 will update the window
  - Data arriving after T=1:30 will be dropped
```

### Trigger Strategies

| Trigger Type | Description |
|---|---|
| **Default Trigger** | Fires when watermark passes window boundary |
| **Early Trigger** | Fires before watermark (on schedule) |
| **Late Trigger** | Fires after watermark for late arrivals |
| **Composite Trigger** | Combination of multiple triggers |

---

## Sample Data

### E-Commerce Transaction Data

**CSV Format:** `transactions.csv`
```
transaction_id,user_id,timestamp,product_category,amount_usd,region
TXN001,USR123,2024-06-12T09:15:32Z,Electronics,149.99,North America
TXN002,USR456,2024-06-12T09:16:45Z,Clothing,89.50,Europe
TXN003,USR789,2024-06-12T09:17:12Z,Electronics,299.99,Asia
TXN004,USR123,2024-06-12T09:18:22Z,Books,24.99,North America
TXN005,USR999,2024-06-12T09:19:05Z,Electronics,199.99,North America
```

**Schema Details:**
- `transaction_id`: Unique transaction identifier
- `user_id`: Customer ID
- `timestamp`: Event time (RFC 3339 format)
- `product_category`: Category of purchased item
- `amount_usd`: Transaction amount
- `region`: Geographic region

### IoT Sensor Data

**CSV Format:** `sensor_data.csv`
```
sensor_id,device_id,timestamp,temperature,humidity,location
SENSOR001,IOT_DEV_001,2024-06-12T10:00:00Z,22.5,45.2,Building_A_Floor_1
SENSOR002,IOT_DEV_002,2024-06-12T10:00:15Z,23.1,48.9,Building_A_Floor_2
SENSOR003,IOT_DEV_003,2024-06-12T10:00:30Z,21.8,42.1,Building_B_Floor_1
SENSOR001,IOT_DEV_001,2024-06-12T10:01:00Z,22.7,45.5,Building_A_Floor_1
SENSOR002,IOT_DEV_002,2024-06-12T10:01:15Z,23.3,49.2,Building_A_Floor_2
```

**Schema Details:**
- `sensor_id`: Sensor identifier
- `device_id`: IoT device identifier
- `timestamp`: Reading timestamp (event time)
- `temperature`: Temperature in Celsius
- `humidity`: Humidity percentage
- `location`: Physical location

### User Activity Log Data

**CSV Format:** `user_activity.csv`
```
event_id,user_id,timestamp,event_type,event_action,session_id
EVT001,USR123,2024-06-12T14:00:00Z,PAGE_VIEW,home,SESS_123_001
EVT002,USR123,2024-06-12T14:00:45Z,CLICK,product_link,SESS_123_001
EVT003,USR123,2024-06-12T14:01:30Z,ADD_TO_CART,item_added,SESS_123_001
EVT004,USR456,2024-06-12T14:05:00Z,PAGE_VIEW,home,SESS_456_001
EVT005,USR456,2024-06-12T14:05:32Z,SEARCH,query_entered,SESS_456_001
EVT006,USR123,2024-06-12T14:35:00Z,PAGE_VIEW,home,SESS_123_002
```

---

## Project Architecture

### Directory Structure

```
dataflow_realtime_project/
├── README.md
├── requirements.txt
├── setup.py
├── config/
│   ├── beam_options.yaml
│   └── gcp_config.yaml
├── sample_data/
│   ├── transactions.csv
│   ├── sensor_data.csv
│   └── user_activity.csv
├── pipeline_code/
│   ├── __init__.py
│   ├── base_pipeline.py
│   ├── tumbling_window_pipeline.py
│   ├── sliding_window_pipeline.py
│   ├── session_window_pipeline.py
│   └── transforms.py
├── bigquery_schemas/
│   ├── transactions_schema.json
│   ├── sensor_data_schema.json
│   ├── user_activity_schema.json
│   └── aggregated_sales_schema.json
└── tests/
    └── test_transforms.py
```

---

## Pub/Sub Integration

### Topic & Subscription Setup

```bash
# Create Pub/Sub Topic
gcloud pubsub topics create dataflow-transactions

# Create Subscription
gcloud pubsub subscriptions create dataflow-transactions-sub \
  --topic=dataflow-transactions

# Publish Sample Message
gcloud pubsub topics publish dataflow-transactions \
  --message='{"transaction_id":"TXN001","user_id":"USR123","timestamp":"2024-06-12T09:15:32Z","product_category":"Electronics","amount_usd":149.99,"region":"North America"}'
```

### Message Format (JSON)

```json
{
  "transaction_id": "TXN001",
  "user_id": "USR123",
  "timestamp": "2024-06-12T09:15:32Z",
  "product_category": "Electronics",
  "amount_usd": 149.99,
  "region": "North America"
}
```

---

## BigQuery Integration

### BigQuery Setup

```bash
# Create Dataset
bq mk --dataset --location=US dataflow_demo

# Create Tables (schemas defined in bigquery_schemas/)
bq mk --table dataflow_demo.transactions_raw bigquery_schemas/transactions_schema.json

bq mk --table dataflow_demo.hourly_sales_summary bigquery_schemas/aggregated_sales_schema.json

bq mk --table dataflow_demo.sensor_metrics bigquery_schemas/sensor_data_schema.json
```

### Table Schemas

#### 1. **Transactions Raw Table**
```json
{
  "fields": [
    {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "product_category", "type": "STRING", "mode": "REQUIRED"},
    {"name": "amount_usd", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "region", "type": "STRING", "mode": "NULLABLE"},
    {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"}
  ]
}
```

#### 2. **Hourly Sales Summary Table**
```json
{
  "fields": [
    {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "region", "type": "STRING", "mode": "REQUIRED"},
    {"name": "total_sales", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "transaction_count", "type": "INT64", "mode": "REQUIRED"},
    {"name": "avg_transaction_value", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "top_category", "type": "STRING", "mode": "NULLABLE"},
    {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"}
  ]
}
```

#### 3. **Sensor Data Aggregated Table**
```json
{
  "fields": [
    {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "sensor_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "location", "type": "STRING", "mode": "REQUIRED"},
    {"name": "avg_temperature", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "avg_humidity", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "max_temperature", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "min_temperature", "type": "FLOAT64", "mode": "REQUIRED"},
    {"name": "reading_count", "type": "INT64", "mode": "REQUIRED"},
    {"name": "processing_time", "type": "TIMESTAMP", "mode": "REQUIRED"}
  ]
}
```

---

## Pipeline Examples

### Example 1: Tumbling Window Pipeline (Hourly Sales)

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from datetime import datetime
import json

def parse_transaction(element):
    """Parse JSON transaction message"""
    try:
        data = json.loads(element)
        # Add event timestamp
        yield beam.window.TimestampedValue(
            data, 
            timestamp=datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')).timestamp()
        )
    except Exception as e:
        print(f"Error parsing: {e}")

def aggregate_sales(elements):
    """Aggregate sales by region"""
    transactions = list(elements)
    total_sales = sum(t['amount_usd'] for t in transactions)
    count = len(transactions)
    
    return {
        'region': transactions[0]['region'],
        'total_sales': total_sales,
        'transaction_count': count,
        'avg_transaction_value': total_sales / count,
        'processing_time': datetime.utcnow().isoformat()
    }

def run_tumbling_window_pipeline():
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        (p 
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
             topic='projects/YOUR_PROJECT/topics/dataflow-transactions')
         | 'ParseTransaction' >> beam.FlatMap(parse_transaction)
         | 'TumblingWindow' >> beam.WindowInto(
             FixedWindows(3600))  # 1 hour windows
         | 'ExtractRegion' >> beam.Map(lambda x: (x['region'], x))
         | 'GroupByRegion' >> beam.GroupByKey()
         | 'AggregateByRegion' >> beam.Map(lambda x: aggregate_sales(x[1]))
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             table='YOUR_PROJECT:dataflow_demo.hourly_sales_summary',
             schema='bigquery_schemas/aggregated_sales_schema.json',
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    run_tumbling_window_pipeline()
```

### Example 2: Sliding Window Pipeline (Moving Average)

```python
import apache_beam as beam
from apache_beam.transforms.window import SlidingWindows
from datetime import datetime
import json

def parse_sensor_data(element):
    """Parse sensor data"""
    try:
        data = json.loads(element)
        yield beam.window.TimestampedValue(
            data,
            timestamp=datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')).timestamp()
        )
    except Exception as e:
        print(f"Error parsing: {e}")

def calculate_sliding_avg(elements):
    """Calculate moving average"""
    readings = list(elements)
    temps = [r['temperature'] for r in readings]
    humidities = [r['humidity'] for r in readings]
    
    return {
        'sensor_id': readings[0]['sensor_id'],
        'location': readings[0]['location'],
        'avg_temperature': sum(temps) / len(temps),
        'avg_humidity': sum(humidities) / len(humidities),
        'reading_count': len(readings),
        'processing_time': datetime.utcnow().isoformat()
    }

def run_sliding_window_pipeline():
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
             topic='projects/YOUR_PROJECT/topics/dataflow-sensors')
         | 'ParseSensorData' >> beam.FlatMap(parse_sensor_data)
         | 'SlidingWindow' >> beam.WindowInto(
             SlidingWindows(size=600, period=300))  # 10min window, 5min slide
         | 'ExtractSensor' >> beam.Map(lambda x: (x['sensor_id'], x))
         | 'GroupBySensor' >> beam.GroupByKey()
         | 'CalculateAverage' >> beam.Map(lambda x: calculate_sliding_avg(x[1]))
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             table='YOUR_PROJECT:dataflow_demo.sensor_metrics',
             schema='bigquery_schemas/sensor_data_schema.json',
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    run_sliding_window_pipeline()
```

### Example 3: Session Window Pipeline (User Sessions)

```python
import apache_beam as beam
from apache_beam.transforms.window import Sessions
from datetime import datetime
import json

def parse_activity(element):
    """Parse user activity"""
    try:
        data = json.loads(element)
        yield beam.window.TimestampedValue(
            data,
            timestamp=datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')).timestamp()
        )
    except Exception as e:
        print(f"Error parsing: {e}")

def session_summary(elements):
    """Summarize user session"""
    activities = sorted(
        list(elements), 
        key=lambda x: x['timestamp']
    )
    
    return {
        'user_id': activities[0]['user_id'],
        'session_id': activities[0]['session_id'],
        'session_start': activities[0]['timestamp'],
        'session_end': activities[-1]['timestamp'],
        'event_count': len(activities),
        'events': [a['event_type'] for a in activities],
        'processing_time': datetime.utcnow().isoformat()
    }

def run_session_window_pipeline():
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
             topic='projects/YOUR_PROJECT/topics/dataflow-activities')
         | 'ParseActivity' >> beam.FlatMap(parse_activity)
         | 'ExtractUser' >> beam.Map(lambda x: (x['user_id'], x))
         | 'SessionWindow' >> beam.WindowInto(
             Sessions(gap_duration=1800))  # 30 minute gap
         | 'GroupByUser' >> beam.GroupByKey()
         | 'CreateSessionSummary' >> beam.Map(lambda x: session_summary(x[1]))
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             table='YOUR_PROJECT:dataflow_demo.user_sessions',
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    run_session_window_pipeline()
```

### Example 4: Late Data & Watermark Handling

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows, AfterWatermark, AfterCount, Repeatedly
from apache_beam.transforms.trigger import AccumulationMode
from datetime import datetime
import json

def parse_transaction(element):
    """Parse transaction with timestamp"""
    data = json.loads(element)
    yield beam.window.TimestampedValue(
        data,
        timestamp=datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')).timestamp()
    )

def run_pipeline_with_late_data():
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
             topic='projects/YOUR_PROJECT/topics/dataflow-transactions')
         | 'ParseTransaction' >> beam.FlatMap(parse_transaction)
         | 'WindowWithLateData' >> beam.WindowInto(
             FixedWindows(3600),
             trigger=Repeatedly(
                 AfterWatermark(
                     early=AfterCount(10),  # Early trigger after 10 events
                     late=AfterCount(5)      # Late trigger for late data
                 )
             ),
             accumulation_mode=AccumulationMode.ACCUMULATING,
             allowed_lateness=600  # Allow 10 minutes of late data
         )
         | 'Aggregate' >> beam.CombinePerKey(
             beam.combiners.Sum()
         )
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             table='YOUR_PROJECT:dataflow_demo.transactions_with_late_data',
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    run_pipeline_with_late_data()
```

---

## Running the Pipeline

### Local Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline locally
python pipeline_code/tumbling_window_pipeline.py
```

### Deploy to Google Cloud Dataflow

```bash
# Set environment variables
export PROJECT_ID=your-gcp-project
export REGION=us-central1
export BUCKET=your-dataflow-bucket

# Create GCS bucket if needed
gsutil mb gs://$BUCKET

# Deploy pipeline
python pipeline_code/tumbling_window_pipeline.py \
  --project=$PROJECT_ID \
  --runner=DataflowRunner \
  --region=$REGION \
  --temp_location=gs://$BUCKET/temp \
  --staging_location=gs://$BUCKET/staging
```

---

## Key Takeaways

### Windowing Selection
| Scenario | Window Type |
|----------|-------------|
| Hourly reports | Tumbling |
| Moving averages | Sliding |
| User sessions | Session |
| Custom logic | Custom |

### Late Data Strategy
- Set appropriate `allowed_lateness` based on SLA
- Use accumulation mode strategically
- Monitor pipeline lag and watermark progression
- Test with synthetic late data

### BigQuery Best Practices
- Use partitioned tables for time-series data
- Stream insert for real-time updates
- Batch load for historical backfill
- Monitor costs and optimize queries

---

## References
- [Apache Beam Documentation](https://beam.apache.org/documentation/)
- [Google Cloud Dataflow](https://cloud.google.com/dataflow/docs)
- [Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [BigQuery Streaming Inserts](https://cloud.google.com/bigquery/streaming-data-into-bigquery)
