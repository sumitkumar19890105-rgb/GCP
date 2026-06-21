# Quick Reference - Dataflow Pipeline

## 🚀 Quick Start Commands

### Batch Pipeline (Local Test)
```bash
python batch_pipelines/batch_word_count_pipeline.py \
    --runner DirectRunner \
    --input sample_data/*.txt \
    --output project:dataset.output
```

### Batch Pipeline (Cloud Dataflow)
```bash
python batch_pipelines/batch_word_count_pipeline.py \
    --runner DataflowRunner \
    --project your-project-id \
    --region us-central1 \
    --temp_location gs://bucket/temp \
    --input gs://bucket/input/*.txt \
    --output your-project-id:dataset.output
```

### Streaming Pipeline (Cloud Dataflow)
```bash
python streaming_pipelines/streaming_event_processing_pipeline.py \
    --runner DataflowRunner \
    --project your-project-id \
    --region us-central1 \
    --temp_location gs://bucket/temp \
    --input_topic projects/project-id/topics/events \
    --output_table your-project-id:dataset.output
```

### Deploy to Composer
```bash
gsutil cp composer_dags/*.py gs://your-dag-bucket/dags/
```

### Trigger Composer DAG
```bash
gcloud composer environments run dataflow-composer \
    --location us-central1 \
    dags trigger -- batch_dataflow_word_count
```

---

## 📊 Monitoring Commands

```bash
# List jobs
gcloud dataflow jobs list --region us-central1

# Get job details
gcloud dataflow jobs describe JOB_ID --region us-central1

# View logs
gcloud dataflow jobs log JOB_ID --region us-central1

# Query results in BigQuery
bq query 'SELECT * FROM `project:dataset.output` LIMIT 10'
```

---

## 🔧 Configuration

Edit `templates/config_template.py` to set:
- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCS_BUCKET`
- `BQ_DATASET`
- `PUBSUB_TOPIC`

---

## 📁 File Structure

| File | Purpose |
|------|---------|
| `batch_pipelines/*.py` | Batch data processing |
| `streaming_pipelines/*.py` | Real-time event processing |
| `composer_dags/*.py` | Airflow orchestration |
| `templates/config_template.py` | Configuration base class |
| `templates/deployment_commands.txt` | All CLI commands |
| `DATAFLOW_PIPELINE_GUIDE.md` | Complete documentation |

---

## 🔑 Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--runner` | DirectRunner | DirectRunner or DataflowRunner |
| `--project` | - | GCP Project ID |
| `--region` | us-central1 | GCP Region |
| `--window_duration` | 60 | Window size in seconds |
| `--num_workers` | 2 | Number of workers |
| `--max_num_workers` | 10 | Maximum workers for scaling |

---

See **DATAFLOW_PIPELINE_GUIDE.md** for complete documentation.
