# Dataflow Realtime Project Analysis & Template Mapping

## Project Analysis Summary

### Original `dataflow_realtime_project` Structure

The original project in `../dataflow_realtime_project/` contains:

#### 📁 Documentation
- **GCP_DATAFLOW_REALTIME_GUIDE.md** - Comprehensive windowing guide
- **GCP_CONFIG_GUIDE.md** - Environment setup instructions
- **QUICK_REFERENCE.md** - Windowing comparison and snippets
- **README.md** - Quick start guide
- **STEP_BY_STEP_TUTORIAL.md** - Detailed tutorial

#### 💻 Pipeline Code Examples
1. **tumbling_window_pipeline.py** - Fixed 1-hour windows for sales aggregation
2. **sliding_window_pipeline.py** - Moving averages for sensor data
3. **session_window_pipeline.py** - User session grouping (30-min gap)
4. **advanced_pipeline_late_data.py** - Late data handling with watermarks
5. **base_pipeline.py** - Base classes and utilities
6. **transforms.py** - Reusable transformation functions

#### 📊 Sample Data
- `sensor_data.csv` - IoT readings (temp, humidity)
- `transactions.csv` - E-commerce data
- `user_activity.csv` - Web activity logs

#### 🏗️ BigQuery Schemas
- `aggregated_sales_schema.json`
- `sensor_data_schema.json`
- `transactions_schema.json`

#### ✈️ Cloud Composer Integration
- `tumbling_window_dag.py` - Composer DAG example
- `COMPOSER_QUICK_REFERENCE.md` - Composer setup guide

---

## 🔄 Template Mapping: Original → New Framework

### Batch Pipelines

| Original | New Template | Purpose |
|----------|--------------|---------|
| N/A | `batch_word_count_pipeline.py` | Simple batch processing example |
| N/A | `batch_data_transformation_pipeline.py` | Full ETL with validation |

**Key Differences:**
- Templates focus on **Cloud Storage input** (more common for batch)
- Simplified job submission process
- Built-in validation framework

### Streaming Pipelines

| Original | New Template | Purpose |
|----------|--------------|---------|
| `tumbling_window_pipeline.py` | `streaming_event_processing_pipeline.py` | Windowed aggregation |
| `sliding_window_pipeline.py` | Similar in `streaming_event_processing_pipeline.py` | Moving average support |
| `session_window_pipeline.py` | `streaming_session_aggregation_pipeline.py` | Session-based grouping |
| `advanced_pipeline_late_data.py` | Error handling in templates | Late data scenarios |

**Key Differences:**
- Templates include **more modular ParDo functions**
- **Flexible windowing strategy** selection via parameters
- **Better error handling** and filtering
- **Autoscaling configuration** for production

### Composer Integration

| Original | New Template | Approach |
|----------|--------------|----------|
| `tumbling_window_dag.py` | `batch_dataflow_dag.py` | Batch job orchestration |
| N/A | `streaming_dataflow_dag.py` | Streaming job management |
| N/A | `transformation_etl_dag.py` | Complex ETL workflows |

**Improvements:**
- **Templated job startup** (easier updates)
- **Better error handling and retries**
- **Monitoring and validation tasks**
- **Three DAG patterns** for different use cases

---

## 🎯 How to Use the New Templates

### Starting Point: Choose Your Use Case

1. **Batch Processing from Cloud Storage**
   → Use `batch_word_count_pipeline.py` or `batch_data_transformation_pipeline.py`
   → Deploy with: `batch_dataflow_dag.py`

2. **Real-Time Event Processing**
   → Use `streaming_event_processing_pipeline.py`
   → Deploy with: `streaming_dataflow_dag.py`

3. **User Session Analysis**
   → Use `streaming_session_aggregation_pipeline.py`
   → Deploy with: `streaming_dataflow_dag.py`

4. **Complex ETL Workflows**
   → Use `transformation_etl_dag.py` in Composer
   → Chains multiple transformation steps

### Customization Pattern

```python
# 1. Choose a base template
# 2. Copy to your project
# 3. Modify these key areas:

# A. Input/Output Sources
--input_topic projects/YOUR_PROJECT/topics/YOUR_TOPIC
--output_table YOUR_PROJECT:YOUR_DATASET.YOUR_TABLE

# B. Transform Functions
class YourTransformFn(beam.DoFn):
    def process(self, element):
        # Your logic here
        yield transformed_element

# C. Windowing Strategy
beam.WindowInto(FixedWindows(60))  # 1-minute windows

# D. Configuration
--num_workers 4
--max_num_workers 20
```

---

## 📊 Data Flow Comparison

### Original Project: Tumbling Window Example

```
Pub/Sub (Transactions)
    ↓
Parse JSON + Extract Timestamp
    ↓
Tumbling Window (1 hour)
    ↓
Group by Region
    ↓
Aggregate Sales Metrics
    ↓
BigQuery (sales_summary table)
```

### New Template: Event Processing Example

```
Pub/Sub (Events)
    ↓
Parse JSON (with error handling)
    ↓
Filter Events (validation)
    ↓
Windowing (Fixed/Sliding)
    ↓
Group by Key
    ↓
Aggregate Metrics (count, sum, avg, min, max)
    ↓
BigQuery (with schema)
```

**Differences:**
- ✅ More generic key extraction
- ✅ Built-in filtering/validation
- ✅ Multiple aggregation statistics
- ✅ Flexible window selection

---

## 🔑 Key Concepts from Original Project

### 1. Windowing Strategies

| Type | Original Example | New Template |
|------|------------------|--------------|
| **Tumbling** | `FixedWindows(3600)` | `FixedWindows(60)` |
| **Sliding** | `SlidingWindows(600, 300)` | `SlidingWindows(600, 300)` |
| **Session** | `Sessions(1800)` | `Sessions(300)` |

### 2. Watermark Handling

Original project shows:
```python
# Late data example with watermark
beam.WindowInto(
    FixedWindows(window_duration),
    allowed_lateness=timedelta(hours=1),
    trigger=Trigger.at_watermark()
)
```

New templates implement this in error handling and logging.

### 3. JSON Parsing with Timestamp

Original base class approach:
```python
def parse_json_with_timestamp(element, timestamp_field='timestamp'):
    # Extract and convert timestamp
```

New templates use:
```python
class ParseEventFn(beam.DoFn):
    # Enhanced with error logging
```

---

## 🚀 Quick Migration Guide

### From Original to New Templates

If you have code in the original project:

1. **Extract the transform logic**
   ```python
   # From: advanced_pipeline_late_data.py
   # Copy: class MyTransformFn(beam.DoFn)
   # To: streaming_pipelines/streaming_event_processing_pipeline.py
   ```

2. **Update input/output**
   ```python
   # Replace:
   input_topic = 'hardcoded/topic'
   
   # With:
   parser.add_argument('--input_topic', dest='input_topic', required=True)
   ```

3. **Add to DAG**
   ```python
   # Copy your customized pipeline to streaming_pipelines/
   # Add DAG task in composer_dags/streaming_dataflow_dag.py
   ```

---

## 📈 Recommended Pipeline Progression

### Stage 1: Learning
- Study original `dataflow_realtime_project`
- Understand windowing concepts
- Review BigQuery schemas

### Stage 2: Templates
- Choose template matching your use case
- Customize input/output
- Test locally with DirectRunner

### Stage 3: Production
- Deploy to Cloud Dataflow
- Monitor with provided commands
- Orchestrate with Composer DAGs

### Stage 4: Advanced
- Implement custom transforms
- Add monitoring/alerting
- Optimize performance

---

## 🔗 File Cross-Reference

### Configuration
- **Original**: Individual pipeline classes with hardcoded config
- **New**: `config_template.py` - Centralized configuration
  - Supports environment-based switching
  - Easy credential management
  - Reusable pipeline options

### Deployment
- **Original**: Manual pipeline commands
- **New**: `deployment_commands.txt` + Composer DAGs
  - Automated job submission
  - Error handling and retries
  - Monitoring integration

### Testing
- **Original**: Sample CSV files in `sample_data/`
- **New**: All templates work with sample data
  - DirectRunner mode for local testing
  - Easy test data generation
  - Integration test patterns

---

## 🎓 Learning Path

### Foundations (Original Project)
1. Read `GCP_DATAFLOW_REALTIME_GUIDE.md`
2. Study `tumbling_window_pipeline.py`
3. Understand windowing concepts

### Implementation (New Templates)
1. Review `batch_word_count_pipeline.py` for basics
2. Customize `streaming_event_processing_pipeline.py`
3. Deploy with `streaming_dataflow_dag.py`

### Production (Advanced)
1. Implement custom transforms
2. Set up monitoring/alerting
3. Optimize with autoscaling

---

## 📚 Template Statistics

| Metric | Original | New |
|--------|----------|-----|
| Pipeline Examples | 4 | 4 |
| Composer DAGs | 1 | 3 |
| Configuration Files | 0 | 2 |
| Total Lines of Code | ~800 | ~1500+ |
| Documented Examples | 4 | 6 |

---

## ✅ When to Use Each Resource

| Scenario | Use This |
|----------|----------|
| Learning windowing concepts | `dataflow_realtime_project` |
| Quick batch pipeline | `batch_word_count_pipeline.py` |
| Real-time analytics | `streaming_event_processing_pipeline.py` |
| User behavior analysis | `streaming_session_aggregation_pipeline.py` |
| Scheduled jobs | `batch_dataflow_dag.py` |
| Continuous processing | `streaming_dataflow_dag.py` |
| Complex workflows | `transformation_etl_dag.py` |
| Configuration management | `config_template.py` |

---

**For detailed usage, see:** `DATAFLOW_PIPELINE_GUIDE.md`
