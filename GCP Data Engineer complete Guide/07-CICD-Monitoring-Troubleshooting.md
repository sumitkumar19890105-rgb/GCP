# CI/CD, Monitoring, and Troubleshooting

## 1. CI/CD for Data Pipelines

### Pipeline Deployment Architecture
```
Developer → Git Push → Cloud Build (CI) → Artifact Registry → Deploy (CD)
                           │
                           ├── Lint SQL / Python
                           ├── Unit tests
                           ├── Integration tests (BQ dry-run)
                           └── Deploy to Composer / Dataflow
```

### Cloud Build Configuration
```yaml
# cloudbuild.yaml
steps:
  # Step 1: Install dependencies
  - name: 'python:3.11'
    entrypoint: 'pip'
    args: ['install', '-r', 'requirements.txt', '-t', '/workspace/deps']

  # Step 2: Lint
  - name: 'python:3.11'
    entrypoint: 'python'
    args: ['-m', 'flake8', 'dags/', 'pipelines/']

  # Step 3: Unit tests
  - name: 'python:3.11'
    entrypoint: 'python'
    args: ['-m', 'pytest', 'tests/unit/', '-v']

  # Step 4: SQL validation (dry run)
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        for f in sql/*.sql; do
          bq query --dry_run --use_legacy_sql=false < "$f"
        done

  # Step 5: Deploy DAGs to Composer
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: 'gsutil'
    args: ['rsync', '-r', 'dags/', 'gs://composer-bucket/dags/']

  # Step 6: Deploy Dataflow templates
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        python pipelines/my_pipeline.py \
          --runner=DataflowRunner \
          --project=$PROJECT_ID \
          --template_location=gs://templates-bucket/my_pipeline \
          --staging_location=gs://staging-bucket/staging
```

### Testing Strategy

```python
# tests/unit/test_transforms.py
import pytest
from transforms import clean_amount, validate_customer_id

def test_clean_amount_valid():
    assert clean_amount("1,234.56") == 1234.56

def test_clean_amount_negative():
    assert clean_amount("-100.00") == -100.00

def test_clean_amount_invalid():
    with pytest.raises(ValueError):
        clean_amount("abc")

def test_validate_customer_id():
    assert validate_customer_id("CUST-12345") == True
    assert validate_customer_id("") == False
    assert validate_customer_id(None) == False


# tests/integration/test_bq_queries.py
from google.cloud import bigquery

def test_daily_aggregation_query():
    client = bigquery.Client()
    # Dry run to validate SQL syntax and check bytes scanned
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query = open("sql/daily_aggregation.sql").read()
    job = client.query(query, job_config=job_config)
    assert job.total_bytes_processed > 0
    # No exception means SQL is valid
```

### Terraform for Infrastructure
```hcl
# BigQuery dataset
resource "google_bigquery_dataset" "curated" {
  dataset_id = "curated"
  project    = var.project_id
  location   = "US"

  default_table_expiration_ms = null
  
  access {
    role          = "WRITER"
    user_by_email = google_service_account.etl_sa.email
  }
  access {
    role          = "READER"
    group_by_email = "[REDACTED_EMAIL_ADDRESS_5]"
  }
}

# Cloud Composer environment
resource "google_composer_environment" "production" {
  name    = "prod-composer"
  region  = "us-central1"
  project = var.project_id

  config {
    software_config {
      image_version = "composer-2.5.0-airflow-2.6.3"
      pypi_packages = {
        "great-expectations" = ">=0.17.0"
        "dbt-bigquery"       = ">=1.7.0"
      }
    }
    workloads_config {
      scheduler {
        cpu    = 2
        memory_gb = 4
        count  = 1
      }
      worker {
        cpu    = 2
        memory_gb = 4
        min_count = 1
        max_count = 6
      }
    }
  }
}
```

---

## 2. Monitoring and Alerting

### Cloud Monitoring Metrics for Data Pipelines

| Service | Key Metrics | Alert Condition |
|---------|------------|-----------------|
| BigQuery | Slot utilization, query duration, bytes scanned | Slot usage > 80% for 10min |
| Dataflow | System lag, backlog (unprocessed), error count | System lag > 5 min |
| Composer | DAG failures, task duration, scheduler heartbeat | Any DAG failure |
| Pub/Sub | Unacked messages, oldest unacked age | Age > 10 min |
| GCS | Request errors, latency | Error rate > 1% |

### Custom Alerting in Python
```python
from google.cloud import monitoring_v3
from google.protobuf import duration_pb2

def create_alert_policy(project_id, metric, threshold, display_name):
    client = monitoring_v3.AlertPolicyServiceClient()
    
    policy = monitoring_v3.AlertPolicy(
        display_name=display_name,
        conditions=[
            monitoring_v3.AlertPolicy.Condition(
                display_name=f"{display_name} condition",
                condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
                    filter=f'metric.type="{metric}"',
                    comparison=monitoring_v3.ComparisonType.COMPARISON_GT,
                    threshold_value=threshold,
                    duration=duration_pb2.Duration(seconds=300),
                    aggregations=[
                        monitoring_v3.Aggregation(
                            alignment_period=duration_pb2.Duration(seconds=60),
                            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
                        )
                    ],
                ),
            )
        ],
        notification_channels=[f"projects/{project_id}/notificationChannels/12345"],
    )
    
    return client.create_alert_policy(
        request={"name": f"projects/{project_id}", "alert_policy": policy}
    )
```

### Airflow Alerting Pattern
```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.python import PythonOperator

def alert_on_failure(context):
    """Send alert when task fails."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    
    message = f"ALERT: {dag_id}.{task_id} failed at {execution_date}"
    # Send to Slack/Teams/Email
    send_notification(message)

# Apply to all tasks
default_args = {
    'on_failure_callback': alert_on_failure,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

### Data Freshness Monitoring
```python
# Airflow sensor to check data freshness
from airflow.sensors.sql_sensor import SqlSensor

freshness_check = SqlSensor(
    task_id='check_data_freshness',
    conn_id='bigquery_default',
    sql="""
        SELECT COUNT(*) FROM `project.curated.transactions`
        WHERE DATE(load_timestamp) = CURRENT_DATE()
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), load_timestamp, MINUTE) < 120
    """,
    mode='reschedule',
    timeout=3600,
    poke_interval=300,
)
```

---

## 3. Troubleshooting Common Issues

### BigQuery Issues
| Problem | Cause | Solution |
|---------|-------|----------|
| Query timeout | Complex query, insufficient slots | Break into stages, use materialized views |
| Quota exceeded | Too many concurrent queries | Use reservations, queue jobs |
| Streaming buffer delays | Recent data not in partition | Query with `_PARTITIONTIME IS NULL` |
| High cost queries | Full table scans | Add partition filter, select fewer columns |

### Dataflow Issues
| Problem | Cause | Solution |
|---------|-------|----------|
| High system lag | Backpressure, slow sink | Scale workers, optimize transforms |
| OOM errors | Large elements in memory | Use side inputs, increase worker memory |
| Stuck pipeline | Watermark not advancing | Check for late data, set allowed lateness |
| Shuffle errors | Hot keys | Add salting/fanout before GroupByKey |

### Cloud Composer Issues
| Problem | Cause | Solution |
|---------|-------|----------|
| Scheduler lag | Too many DAGs, complex parsing | Reduce DAG complexity, increase scheduler resources |
| Worker OOM | Heavy tasks in worker | Use KubernetesPodOperator for heavy work |
| Task stuck in "queued" | Not enough workers | Increase max workers, check parallelism |
| Import errors | Missing packages | Add to pypi_packages config |

### Debugging Checklist
```
1. Check logs (Cloud Logging)
   - Filter by resource type (bigquery_resource, dataflow_step, etc.)
   - Look for ERROR severity

2. Check metrics (Cloud Monitoring)
   - Is the service healthy?
   - Are there resource constraints?

3. Check IAM
   - Does the service account have required permissions?
   - Are there org policy restrictions?

4. Check network
   - VPC connectivity
   - Firewall rules
   - Private Google Access enabled?

5. Check quotas
   - API quotas (requests/min)
   - Resource quotas (slots, workers, CPU)
```

---

## Interview Questions — Operations

**Q: How do you deploy a data pipeline change safely?**
> 1. Feature branch → PR with unit/integration tests
> 2. CI validates SQL syntax (dry-run), Python linting, unit tests
> 3. Deploy to staging environment first (separate BQ project)
> 4. Run integration tests against staging data
> 5. Merge → CD deploys DAGs to production Composer, templates to Dataflow
> 6. Monitor for 24h after deployment (freshness, quality checks, latency)

**Q: A Dataflow streaming pipeline has increasing lag. How do you troubleshoot?**
> 1. Check system lag metric — is it growing or stable?
> 2. Look at worker utilization — are workers at max CPU/memory?
> 3. Check for hot keys — one key getting most of the data?
> 4. Check the sink (BigQuery) — are streaming inserts throttled?
> 5. Solutions: increase max workers, add key salting, optimize transforms, check for memory leaks in DoFn

**Q: How do you monitor data pipeline health?**
> Three levels:
> 1. **Infrastructure**: Worker health, CPU, memory, network (Cloud Monitoring)
> 2. **Pipeline**: Execution time, success rate, lag (Airflow metrics + custom)
> 3. **Data**: Freshness, quality, reconciliation (SQL checks in pipeline)
> Alert on all three with appropriate thresholds and escalation paths.
