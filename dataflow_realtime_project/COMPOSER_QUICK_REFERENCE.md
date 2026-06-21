# GCP Composer Quick Reference & Cheat Sheet

## Quick Commands

### Create Composer Environment
```bash
gcloud composer environments create dataflow-orchestrator \
  --location=us-central1 \
  --python-version=3 \
  --machine-type=n1-standard-4 \
  --node-count=3 \
  --project=your-project-id
```

### Upload DAGs
```bash
# Single file
gcloud composer environments storage dags import \
  --environment=dataflow-orchestrator \
  --location=us-central1 \
  --source=tumbling_window_dag.py

# All files in directory
gsutil -m cp composer_dags/* gs://COMPOSER-BUCKET/dags/
```

### List DAGs
```bash
gcloud composer environments run dataflow-orchestrator \
  --location=us-central1 \
  dags list
```

### Enable/Disable DAG
```bash
# Enable
gcloud composer environments run dataflow-orchestrator \
  --location=us-central1 \
  dags unpause -- tumbling_window_pipeline

# Disable
gcloud composer environments run dataflow-orchestrator \
  --location=us-central1 \
  dags pause -- tumbling_window_pipeline
```

### Trigger DAG
```bash
gcloud composer environments run dataflow-orchestrator \
  --location=us-central1 \
  dags trigger -- tumbling_window_pipeline
```

### View DAG Runs
```bash
gcloud composer environments run dataflow-orchestrator \
  --location=us-central1 \
  dags list-runs -- -d tumbling_window_pipeline
```

### View Task Logs
```bash
gcloud composer environments run dataflow-orchestrator \
  --location=us-central1 \
  tasks logs tumbling_window_pipeline run_tumbling_window_job 2024-06-15
```

---

## Directory Structure Reference

| Location | Purpose | Upload Method |
|----------|---------|----------------|
| `gs://bucket/dags/` | DAG files | `gsutil cp *.py gs://bucket/dags/` |
| `gs://bucket/plugins/` | Custom operators | `gsutil cp -r plugins/ gs://bucket/plugins/` |
| `gs://bucket/data/` | Data/configs | `gsutil cp -r data/ gs://bucket/data/` |

---

## Common DAG Parameters

```python
# Schedule Intervals
'@hourly'                    # Every hour
'@daily'                     # Every day at midnight
'@weekly'                    # Every Sunday at midnight
'@monthly'                   # First day of month at midnight
'0 * * * *'                  # Every hour (cron format)
'0 9 * * 1-5'               # Weekdays at 9 AM

# Retry Configuration
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# Email Notifications
default_args = {
    'email': ['team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
```

---

## Template Triggering Examples

### From Composer DAG
```python
run_job = DataflowTemplatedJobStartOperator(
    task_id='run_job',
    template_location='gs://bucket/templates/my-template',
    project_id='project-id',
    location='us-central1',
    parameters={
        'input_topic': 'projects/project-id/topics/input',
        'output_table': 'project:dataset.table',
    },
)
```

### From CLI
```bash
gcloud dataflow jobs run my-job \
  --gcs-location=gs://bucket/templates/my-template \
  --parameters input_topic=projects/project/topics/input,output_table=project:dataset.table
```

---

## Monitoring

### Cloud Logging
```bash
# Stream Composer logs
gcloud logging read "resource.type=cloud-composer" \
  --project=project-id \
  --limit=50 \
  --format=json \
  --stream

# Filter by environment
gcloud logging read "resource.labels.environment_name=dataflow-orchestrator" \
  --project=project-id \
  --limit=50
```

### View Dataflow Jobs
```bash
# List running jobs
gcloud dataflow jobs list --project=project-id --filter="STATE:RUNNING"

# Get job details
gcloud dataflow jobs describe JOB_ID --project=project-id

# Stream job logs
gcloud dataflow jobs show JOB_ID --project=project-id --full
```

### Web UI Access
```bash
# Get Airflow Web UI URL
gcloud composer environments describe dataflow-orchestrator \
  --location=us-central1 \
  --format='value(config.airflowUri)'
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| DAG not appearing | Check DAG syntax, look at Cloud Logging |
| Task timeout | Set `wait_until_finished=False` in Dataflow operator |
| Permission errors | Check service account roles and IAM bindings |
| Template not found | Verify template path in GCS, check bucket name |
| Dataflow job failing | Check Dataflow job logs in Cloud Console |
| Out of memory | Increase `numWorkers` or `machineType` |

---

## Testing Locally

```bash
# Check DAG syntax
python -m py_compile tumbling_window_dag.py

# Test imports
python -c "from tumbling_window_dag import dag; print(dag.dag_id)"

# Test with local Airflow
airflow dags test tumbling_window_pipeline 2024-06-15

# List local DAGs
airflow dags list
```

---

## Deployment Checklist

- [ ] DAGs created and tested locally
- [ ] Templates parameterized and tested
- [ ] GCS bucket created with correct permissions
- [ ] Composer environment created
- [ ] DAGs uploaded to `gs://bucket/dags/`
- [ ] Templates available in GCS
- [ ] Service account has required permissions
- [ ] Pub/Sub topics created (if using)
- [ ] BigQuery datasets and tables created (if using)
- [ ] DAGs enabled in Composer UI
- [ ] Test run triggered manually
- [ ] Monitoring and alerts configured

---

## File Organization Quick Reference

```
LOCAL (Git Repository):
├── composer_dags/        ← DAG Python files
├── composer_templates/   ← Template Python files
├── composer_configs/     ← JSON/YAML configs
├── composer_setup/       ← Deployment scripts
└── pipeline_code/        ← Base pipeline code

GCS BUCKET:
├── dags/                 ← DAGs for Composer (upload composer_dags/*)
├── templates/            ← Dataflow templates (upload composer_templates/*)
├── data/                 ← Config/sample data (upload composer_configs/*, sample_data/*)
└── temp/                 ← Auto-generated (do not manage)
```

---

## Environment Variables in DAGs

```python
import os

# Get from environment or use defaults
PROJECT_ID = os.getenv('GCP_PROJECT', 'default-project')
REGION = os.getenv('GCP_REGION', 'us-central1')
ENVIRONMENT = os.getenv('ENVIRONMENT', 'dev')

# Pass to Composer during creation
gcloud composer environments create env-name \
  --location=us-central1 \
  --env-variables GCP_PROJECT=my-project,ENVIRONMENT=prod
```

---

## Common Errors & Solutions

### Error: "DAG is not found in Airflow"
```bash
# Solution: Upload DAG to correct location
gsutil cp my_dag.py gs://COMPOSER-BUCKET/dags/

# Verify upload
gsutil ls gs://COMPOSER-BUCKET/dags/
```

### Error: "template_location not found"
```bash
# Solution: Check template path in GCS
gsutil ls gs://bucket/templates/

# Make sure path is correct in DAG
template_location='gs://bucket/templates/my-template'
```

### Error: "Dataflow job timeout"
```python
# Solution: Set wait_until_finished=False
run_job = DataflowTemplatedJobStartOperator(
    task_id='run_job',
    template_location='...',
    wait_until_finished=False,  # Don't block DAG
)
```

### Error: "Permission denied: Service account"
```bash
# Solution: Grant required roles
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SA@PROJECT.iam.gserviceaccount.com" \
  --role="roles/dataflow.admin"
```

---

## Additional Resources

- [Google Cloud Composer Documentation](https://cloud.google.com/composer/docs)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Cloud Dataflow Operators for Airflow](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataflow.html)
- [GCP_COMPOSER_GUIDE.md](GCP_COMPOSER_GUIDE.md) - Full Composer Guide
