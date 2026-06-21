# GCP Dataflow Real-Time Project - File Navigation Guide

## 📚 Complete File Index

### 🎯 Start Here

#### **PROJECT_SUMMARY.md** (This file was created to introduce the project)
- **Purpose**: Overview of what's included in the project
- **Audience**: Everyone - read first
- **Content**: Project checklist, learning paths, key concepts
- **Time**: 5 minutes

#### **README.md** (Quick Start Guide)
- **Purpose**: Get up and running quickly
- **Audience**: Users ready to begin
- **Content**: Installation, setup, running pipelines
- **Time**: 10-15 minutes

---

### 📖 Learning & Reference

#### **GCP_DATAFLOW_REALTIME_GUIDE.md** (Main Reference)
- **Purpose**: Comprehensive technical guide
- **Audience**: All technical levels
- **Sections**:
  - Windowing Strategies (Tumbling, Sliding, Session)
  - Watermarks & Late Data
  - Sample Data (CSV format)
  - Project Architecture
  - Pub/Sub Integration
  - BigQuery Integration
  - 4 Complete Pipeline Examples
- **Use**: Reference during development, learning resource
- **Time**: 30-45 minutes to read completely

#### **QUICK_REFERENCE.md** (Developer Cheat Sheet)
- **Purpose**: Fast lookups during coding
- **Audience**: Developers actively coding
- **Sections**:
  - ASCII windowing diagrams
  - Code snippets (copy-paste ready)
  - Common patterns
  - Deployment commands
  - Monitoring commands
  - Troubleshooting
- **Use**: Keep open while developing
- **Time**: Reference as needed

#### **STEP_BY_STEP_TUTORIAL.md** (Hands-On Walkthrough)
- **Purpose**: Guided practical experience
- **Audience**: First-time users
- **Sections**:
  - Part 1-8 with step-by-step instructions
  - Commands with expected outputs
  - Troubleshooting for each step
  - Experiment suggestions
- **Use**: Follow sequentially for first deployment
- **Time**: 45-60 minutes to complete

#### **GCP_CONFIG_GUIDE.md** (Environment Setup)
- **Purpose**: Detailed configuration instructions
- **Audience**: DevOps, infrastructure setup
- **Sections**:
  - GCP project setup
  - API enablement
  - Service accounts
  - Pub/Sub configuration
  - BigQuery setup
  - Cloud Storage configuration
  - Monitoring alerts
- **Use**: First-time GCP setup reference
- **Time**: 15-20 minutes to execute

#### **GCP_COMPOSER_GUIDE.md** (Orchestration with Apache Airflow) ⭐ NEW
- **Purpose**: Run Dataflow jobs from GCP Composer (managed Airflow)
- **Audience**: Platform engineers, DevOps, pipeline orchestration
- **Sections**:
  - Overview of Composer benefits
  - Prerequisites and GCP setup
  - Project structure & code organization
  - Composer environment creation
  - Creating DAG (Directed Acyclic Graph) files
  - Example DAGs (simple and advanced)
  - Deploying code to Composer
  - Running jobs (automatic, CLI, Web UI, programmatic)
  - Monitoring and troubleshooting
  - Best practices and testing
- **Use**: Schedule and orchestrate Dataflow jobs automatically
- **Time**: 30-40 minutes to understand, variable for implementation
- **Key Features Covered**:
  - DAG scheduling (hourly, daily, etc.)
  - Task dependencies
  - Error handling and retries
  - Multi-job pipelines
  - Integration with BigQuery and Pub/Sub

---

### 📊 Sample Data Files (CSV)

#### **sample_data/transactions.csv**
- **Purpose**: E-commerce transaction test data
- **Format**: CSV with headers
- **Records**: 25 sample transactions
- **Fields**: transaction_id, user_id, timestamp, product_category, amount_usd, region
- **Time Span**: 3 hours on 2024-06-12
- **Use**: Test tumbling window pipeline, sales aggregation
- **When**: Publishing to Pub/Sub for testing

#### **sample_data/sensor_data.csv**
- **Purpose**: IoT sensor readings test data
- **Format**: CSV with headers
- **Records**: 25 sensor readings
- **Fields**: sensor_id, device_id, timestamp, temperature, humidity, location
- **Time Span**: 7 minutes on 2024-06-12
- **Use**: Test sliding window pipeline, moving averages
- **When**: Publishing to Pub/Sub for sensor data pipeline

#### **sample_data/user_activity.csv**
- **Purpose**: User web activity test data
- **Format**: CSV with headers
- **Records**: 25 activity events
- **Fields**: event_id, user_id, timestamp, event_type, event_action, session_id
- **Time Span**: 40 minutes on 2024-06-12
- **Use**: Test session window pipeline, user behavior analysis
- **When**: Publishing to Pub/Sub for user activity pipeline

---

### 🔧 Pipeline Code Files

#### **pipeline_code/base_pipeline.py**
- **Purpose**: Foundational classes and utilities
- **Use**: Imported by other pipelines
- **Classes**:
  - `BasePipeline`: Abstract base class
  - `WindowMetrics`: Window utility functions
  - Parsing functions
- **When**: Understanding pipeline architecture
- **Dependency**: Required by all other pipelines

#### **pipeline_code/tumbling_window_pipeline.py**
- **Purpose**: Fixed 1-hour window aggregation
- **Use Case**: Hourly sales reports by region
- **Window**: 1 hour, non-overlapping
- **Input**: Transactions from Pub/Sub
- **Output**: Regional sales summaries to BigQuery
- **Run Locally**:
  ```bash
  python pipeline_code/tumbling_window_pipeline.py \
    --runner=DirectRunner \
    --input_topic=projects/$PROJECT_ID/topics/dataflow-transactions \
    --output_table=$PROJECT_ID:dataflow_demo.hourly_sales_summary
  ```
- **Deploy to Dataflow**:
  ```bash
  python pipeline_code/tumbling_window_pipeline.py \
    --runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=us-central1 \
    --temp_location=gs://bucket/temp \
    --staging_location=gs://bucket/staging \
    --input_topic=... \
    --output_table=...
  ```

#### **pipeline_code/sliding_window_pipeline.py**
- **Purpose**: Moving average calculations
- **Use Case**: Real-time sensor data monitoring
- **Window**: 10 minutes size, 5-minute slide (50% overlap)
- **Input**: Sensor readings from Pub/Sub
- **Output**: Sensor metrics (avg temp, humidity) to BigQuery
- **Key Features**: 
  - Overlapping windows for trend detection
  - Temperature/humidity statistics
  - Per-sensor aggregation

#### **pipeline_code/session_window_pipeline.py**
- **Purpose**: Dynamic user session grouping
- **Use Case**: User behavior and engagement analysis
- **Window**: Dynamic - closes on 30-minute inactivity gap
- **Input**: User activity events from Pub/Sub
- **Output**: Session summaries to BigQuery
- **Key Features**:
  - Gap-based windowing
  - Event counting by type
  - Session timeline capture

#### **pipeline_code/advanced_pipeline_late_data.py**
- **Purpose**: Handle delayed data arrivals
- **Use Case**: Real-world scenarios with late arrivals
- **Features**:
  - Watermark-based triggers
  - Multiple accumulation modes (ACCUMULATING/DISCARDING)
  - Allowed lateness configuration
- **Classes**:
  - `LateDataHandlingPipeline`: Main pipeline
  - `DiscardingAccumulationPipeline`: Alternative approach
- **Key Configuration**:
  - Allowed lateness: 10 minutes
  - Early triggers: 10 events
  - Late triggers: 5 events

#### **pipeline_code/transforms.py**
- **Purpose**: Reusable transformation functions
- **Use**: Import into custom pipelines
- **Classes**:
  - `JSONParserDoFn`: Parse JSON strings
  - `TimestampAdderDoFn`: Add processing timestamp
  - `FieldFilterFn`: Filter by field conditions
  - `AggregationFn`: Generic aggregation (sum/avg/min/max/count)
  - `DataEnrichmentFn`: Add fields to records
  - `DataValidationFn`: Validate required fields
  - `DuplicateRemovalFn`: Remove duplicate records
- **Composite Transforms**:
  - `ParseJSONAndFilter()`: Parse & validate
  - `EnrichAndTimestamp()`: Enrich & add time
- **When**: Building custom pipelines beyond examples

#### **pipeline_code/__init__.py**
- **Purpose**: Package initialization
- **Use**: Makes pipeline_code a Python package
- **When**: Importing from pipeline_code

---

### 📋 BigQuery Schema Files (JSON)

#### **bigquery_schemas/transactions_schema.json**
- **Purpose**: Schema for raw transactions table
- **Table**: `dataflow_demo.transactions_raw`
- **Fields**: 9 fields including window and processing time
- **Primary Key**: transaction_id (unique per window)
- **Use**: Create table: `bq mk --table dataflow_demo.transactions_raw ...`

#### **bigquery_schemas/aggregated_sales_schema.json**
- **Purpose**: Schema for sales summary table
- **Table**: `dataflow_demo.hourly_sales_summary`
- **Fields**: 8 fields with aggregations
- **Aggregations**: total_sales, transaction_count, avg_transaction_value
- **Grouping**: By window and region
- **Use**: Tumbling window pipeline output

#### **bigquery_schemas/sensor_data_schema.json**
- **Purpose**: Schema for sensor metrics table
- **Table**: `dataflow_demo.sensor_metrics`
- **Fields**: 11 fields with temperature/humidity statistics
- **Statistics**: avg, min, max temperatures; avg humidity
- **Grouping**: By window and sensor
- **Use**: Sliding window pipeline output

---

### ⚙️ Configuration Files

#### **requirements.txt**
- **Purpose**: Python package dependencies
- **Packages**: Apache Beam, GCP clients, utilities
- **Install**: `pip install -r requirements.txt`
- **Python Version**: 3.7+
- **Update**: Add custom dependencies here

---

## 🗺️ Navigation by Task

### I Want To...

#### **Understand the Concepts**
1. Read: QUICK_REFERENCE.md (Windowing section)
2. Read: GCP_DATAFLOW_REALTIME_GUIDE.md (Introduction)
3. Study: Diagram ASCII in QUICK_REFERENCE.md

#### **Set Up Everything**
1. Follow: STEP_BY_STEP_TUTORIAL.md (Parts 1-2)
2. Reference: GCP_CONFIG_GUIDE.md (for detailed commands)
3. Verify: Check GCP Console

#### **Run a Local Pipeline**
1. Follow: STEP_BY_STEP_TUTORIAL.md (Part 3)
2. Reference: QUICK_REFERENCE.md (Local Testing section)
3. Study: tumbling_window_pipeline.py (code)

#### **Deploy to Production**
1. Follow: STEP_BY_STEP_TUTORIAL.md (Part 5)
2. Reference: QUICK_REFERENCE.md (Deployment Commands)
3. Study: README.md (Deployment section)

#### **Monitor Results**
1. Follow: STEP_BY_STEP_TUTORIAL.md (Part 6)
2. Reference: QUICK_REFERENCE.md (Monitoring Commands)
3. Use: BigQuery query examples

#### **Customize for My Use Case**
1. Study: transforms.py (available functions)
2. Copy: Relevant pipeline example
3. Modify: Aggregation logic
4. Test: With DirectRunner locally

#### **Understand Late Data**
1. Read: GCP_DATAFLOW_REALTIME_GUIDE.md (Watermarks section)
2. Study: advanced_pipeline_late_data.py (code)
3. Reference: QUICK_REFERENCE.md (Late Data Handling)

#### **Troubleshoot Issues**
1. Check: QUICK_REFERENCE.md (Error Handling)
2. Check: STEP_BY_STEP_TUTORIAL.md (Troubleshooting)
3. Check: README.md (Common Issues)

---

## 🔄 File Dependencies

```
requirements.txt
├── base_pipeline.py
├── transforms.py
├── tumbling_window_pipeline.py
├── sliding_window_pipeline.py
├── session_window_pipeline.py
└── advanced_pipeline_late_data.py

bigquery_schemas/
├── transactions_schema.json
├── aggregated_sales_schema.json
└── sensor_data_schema.json

sample_data/
├── transactions.csv
├── sensor_data.csv
└── user_activity.csv
```

---

## ⏱️ Recommended Reading Order

### For 1-Hour Overview
1. PROJECT_SUMMARY.md (5 min)
2. README.md (10 min)
3. QUICK_REFERENCE.md (20 min)
4. tumbling_window_pipeline.py (15 min)
5. Explore sample_data/ (10 min)

### For Complete Understanding (2-3 hours)
1. PROJECT_SUMMARY.md (5 min)
2. STEP_BY_STEP_TUTORIAL.md parts 1-2 (20 min)
3. GCP_DATAFLOW_REALTIME_GUIDE.md (45 min)
4. QUICK_REFERENCE.md (15 min)
5. Study pipeline code files (30 min)
6. STEP_BY_STEP_TUTORIAL.md parts 3-5 (20 min)

### For Hands-On Deployment (1.5-2 hours)
1. GCP_CONFIG_GUIDE.md (20 min - execute commands)
2. STEP_BY_STEP_TUTORIAL.md (60-90 min - follow all parts)
3. README.md as reference (open in parallel)

---

## 🎓 Key Sections by Topic

### Windowing Strategies
- Main: GCP_DATAFLOW_REALTIME_GUIDE.md (Windowing Strategies section)
- Reference: QUICK_REFERENCE.md (Windowing Comparison)
- Code Examples: Individual pipeline files
- Diagrams: QUICK_REFERENCE.md and GCP_DATAFLOW_REALTIME_GUIDE.md

### Late Data & Watermarks
- Main: GCP_DATAFLOW_REALTIME_GUIDE.md (Watermarks & Late Data section)
- Code: advanced_pipeline_late_data.py
- Quick Ref: QUICK_REFERENCE.md (Watermark Concepts)
- Practical: STEP_BY_STEP_TUTORIAL.md (Part 4-6)

### Real-Time Integration
- Main: GCP_DATAFLOW_REALTIME_GUIDE.md (Pub/Sub Integration)
- Setup: GCP_CONFIG_GUIDE.md (Pub/Sub Setup section)
- Commands: QUICK_REFERENCE.md (Pub/Sub Operations)
- Practical: STEP_BY_STEP_TUTORIAL.md (Part 2, 4)

### BigQuery Integration
- Main: GCP_DATAFLOW_REALTIME_GUIDE.md (BigQuery Integration)
- Schemas: bigquery_schemas/ directory (JSON files)
- Setup: GCP_CONFIG_GUIDE.md (BigQuery Setup)
- Queries: QUICK_REFERENCE.md (BigQuery Queries)
- Practical: STEP_BY_STEP_TUTORIAL.md (Part 6)

### Deployment
- Local: README.md (Local Testing section)
- Cloud: README.md (Deploy to Dataflow section)
- Commands: QUICK_REFERENCE.md (Deployment Commands)
- Step-by-Step: STEP_BY_STEP_TUTORIAL.md (Part 5)
- Config: GCP_CONFIG_GUIDE.md (entire file)

### Monitoring
- Commands: QUICK_REFERENCE.md (Monitoring Commands)
- Practical: STEP_BY_STEP_TUTORIAL.md (Part 6)
- Config: GCP_CONFIG_GUIDE.md (Monitoring Setup)
- Queries: QUICK_REFERENCE.md (BigQuery Queries)

---

## 💡 Tips for Using This Project

1. **Start Simple**: Run tumbling_window_pipeline.py first
2. **Use QUICK_REFERENCE**: Keep it open while coding
3. **Follow Tutorial**: STEP_BY_STEP_TUTORIAL.md is comprehensive
4. **Save Schema Files**: Reference when modifying tables
5. **Keep Logs**: Check pipeline logs for debugging
6. **Version Control**: Treat pipeline_code/ as production code
7. **Test Locally**: Always test with DirectRunner first

---

## 📞 Quick Links

| Need | File | Section |
|------|------|---------|
| Overview | PROJECT_SUMMARY.md | - |
| Quick Start | README.md | - |
| Concepts | QUICK_REFERENCE.md | Windowing & Watermarks |
| Setup Steps | STEP_BY_STEP_TUTORIAL.md | Parts 1-2 |
| Code Examples | GCP_DATAFLOW_REALTIME_GUIDE.md | Pipeline Examples |
| Troubleshooting | QUICK_REFERENCE.md | Error Handling |
| Monitoring | QUICK_REFERENCE.md | Monitoring Commands |
| Sample Data | sample_data/ | CSV files |
| Schemas | bigquery_schemas/ | JSON files |
| Code | pipeline_code/ | Python files |

---

## ✅ Checklist Before Starting

- [ ] Read PROJECT_SUMMARY.md
- [ ] Read README.md
- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Authenticate with GCP: `gcloud auth application-default login`
- [ ] Enable APIs (follow GCP_CONFIG_GUIDE.md)
- [ ] Create Pub/Sub topics (follow STEP_BY_STEP_TUTORIAL.md Part 2.4)
- [ ] Create BigQuery dataset (follow STEP_BY_STEP_TUTORIAL.md Part 2.5)
- [ ] Run local pipeline test (follow STEP_BY_STEP_TUTORIAL.md Part 3)
- [ ] Deploy to Dataflow (follow STEP_BY_STEP_TUTORIAL.md Part 5)

**Good luck with your Dataflow project!**
