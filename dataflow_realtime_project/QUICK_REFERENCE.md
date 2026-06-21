# Dataflow Quick Reference Guide

## Windowing Comparison

```
TUMBLING WINDOW (Fixed)
|--[1h]--|--[1h]--|--[1h]--|
Events fall into exactly one window
Window 1: 09:00-10:00
Window 2: 10:00-11:00
Window 3: 11:00-12:00

SLIDING WINDOW
|--[10m]--|
  |--[10m]--|
    |--[10m]--|
Windows overlap by 5 minutes
Window 1: 09:00-09:10
Window 2: 09:05-09:15
Window 3: 09:10-09:20

SESSION WINDOW
Event1 Event2 Event3 <30m gap> Event4 Event5
|----Session 1-------|         |----Session 2----|
Windows based on inactivity
```

## Watermark Concepts

### What is a Watermark?
- Marks the pipeline's progress through event time
- Indicates which data has been fully processed
- Advances based on input data timestamps
- Late data: arrives after watermark for its window

### Allowed Lateness
- Time window remains open after watermark passes
- Late arrivals update results if within lateness window
- Default: 0 (no late data processing)

### Trigger Strategies

| Trigger | When | Output |
|---------|------|--------|
| Default | Watermark crosses window | FINAL |
| Early | Before watermark (frequent) | PANE |
| Late | After watermark (for late data) | PANE |
| Repeatedly | Multiple times | PANES |

## Code Snippets

### Parse JSON with Timestamp

```python
def parse_json_with_timestamp(element, timestamp_field='timestamp'):
    data = json.loads(element)
    timestamp = datetime.fromisoformat(
        data[timestamp_field].replace('Z', '+00:00')
    ).timestamp()
    yield beam.window.TimestampedValue(data, timestamp)
```

### Tumbling Window

```python
| 'Window' >> beam.WindowInto(FixedWindows(3600))  # 1 hour
```

### Sliding Window

```python
| 'Window' >> beam.WindowInto(
    SlidingWindows(size=600, period=300)  # 10min size, 5min slide
)
```

### Session Window

```python
| 'Window' >> beam.WindowInto(
    Sessions(gap_duration=1800)  # 30 minute gap
)
```

### Late Data Handling

```python
| 'Window' >> beam.WindowInto(
    FixedWindows(3600),
    trigger=AfterWatermark(late=AfterCount(1)),
    accumulation_mode=AccumulationMode.ACCUMULATING,
    allowed_lateness=600  # 10 minutes
)
```

## Common Patterns

### Aggregation by Key

```python
(p
 | 'Input' >> ...
 | 'Window' >> beam.WindowInto(FixedWindows(3600))
 | 'ExtractKey' >> beam.Map(lambda x: (x['region'], x))
 | 'GroupByKey' >> beam.GroupByKey()
 | 'Aggregate' >> beam.CombinePerKey(beam.combiners.Sum())
)
```

### Filter and Transform

```python
(p
 | 'Input' >> ...
 | 'Filter' >> beam.Filter(lambda x: x['amount'] > 100)
 | 'Transform' >> beam.Map(lambda x: {...})
)
```

### Flatten Multiple Sources

```python
(p
 | 'Source1' >> beam.Create([...]),
 | 'Source2' >> beam.Create([...])
) | 'Flatten' >> beam.Flatten()
```

## BigQuery Operations

### Stream Insert

```python
| 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
    table='project:dataset.table',
    schema='auto',
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
)
```

### Batch Load

```python
| 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
    table='project:dataset.table',
    load_job_project_id='project',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
)
```

## Pub/Sub Operations

### Read from Pub/Sub

```python
| 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
    topic='projects/project-id/topics/topic-name'
)
```

### Read from Subscription

```python
| 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
    subscription='projects/project-id/subscriptions/sub-name'
)
```

### Publish Results

```python
| 'PublishToPubSub' >> beam.io.WriteToPubSub(
    topic='projects/project-id/topics/output-topic'
)
```

## Deployment Commands

### Local Testing

```bash
python pipeline.py \
  --runner=DirectRunner \
  --input_topic=... \
  --output_table=...
```

### Deploy to Dataflow

```bash
python pipeline.py \
  --runner=DataflowRunner \
  --project=PROJECT_ID \
  --region=us-central1 \
  --temp_location=gs://bucket/temp \
  --staging_location=gs://bucket/staging \
  --input_topic=... \
  --output_table=...
```

## Performance Tuning

### Worker Configuration

```bash
--num_workers=4                    # Number of workers
--worker_machine_type=n1-standard-4 # Machine type
--disk_size_gb=100                 # Disk per worker
--max_num_workers=10               # Autoscaling limit
```

### Pipeline Parameters

- **Window Size**: Smaller = lower latency, higher cost
- **Allowed Lateness**: Larger = handles more late data
- **Batch Size**: Depends on message volume
- **Triggers**: Balance frequency vs efficiency

## Monitoring Commands

### List Jobs

```bash
gcloud dataflow jobs list --region=us-central1
```

### Get Job Details

```bash
gcloud dataflow jobs describe JOB_ID --region=us-central1
```

### View Metrics

```bash
gcloud dataflow jobs describe JOB_ID \
  --region=us-central1 \
  --format="value(currentWorkerCount, totalWorkers)"
```

### Check Logs

```bash
gcloud logging read \
  "resource.type=dataflow_step AND resource.labels.job_id=JOB_ID"
```

## BigQuery Queries

### Query Results

```sql
SELECT 
  window_start,
  region,
  total_sales,
  transaction_count
FROM `project.dataflow_demo.hourly_sales_summary`
WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY window_start DESC;
```

### Check Data Age

```sql
SELECT 
  MAX(window_end) as latest_data,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(window_end), MINUTE) as age_minutes
FROM `project.dataflow_demo.hourly_sales_summary`;
```

### Monitor Late Data

```sql
SELECT 
  window_start,
  window_end,
  processing_time,
  TIMESTAMP_DIFF(processing_time, window_end, SECOND) as lateness_seconds
FROM `project.dataflow_demo.hourly_sales_summary`
WHERE TIMESTAMP_DIFF(processing_time, window_end, SECOND) > 300
ORDER BY window_start DESC;
```

## Error Handling

### Common Issues

| Error | Cause | Solution |
|-------|-------|----------|
| Credentials not found | Auth not set | `gcloud auth login` |
| Topic not found | Wrong topic name | Verify with `gcloud pubsub topics list` |
| Schema mismatch | Wrong schema | Compare with table schema |
| Out of memory | Batch too large | Reduce window size/batch |
| Lag increasing | Low resources | Increase num_workers |

### Debugging Techniques

1. **Enable verbose logging**
   ```bash
   export BEAM_LOG_LEVEL=DEBUG
   ```

2. **Add logging in transforms**
   ```python
   | 'Debug' >> beam.Map(lambda x: print(f"Data: {x}") or x)
   ```

3. **Sample data**
   ```python
   | 'Sample' >> beam.Filter(lambda x: random.random() < 0.01)
   ```

4. **Test locally first**
   ```bash
   python pipeline.py --runner=DirectRunner
   ```

## Resources

- [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [Windowing in Beam](https://beam.apache.org/documentation/programming-guide/#windowing)
- [Dataflow Pricing](https://cloud.google.com/dataflow/pricing)
- [BigQuery Streaming](https://cloud.google.com/bigquery/streaming-data-into-bigquery)
