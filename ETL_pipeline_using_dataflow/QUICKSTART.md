# Setup & Quick Start Checklist

## ✅ Pre-Requisites Checklist

### Install & Configure
- [ ] Python 3.8+ installed
- [ ] Google Cloud SDK installed (`gcloud`)
- [ ] Service account created and authenticated
- [ ] Project ID set: `export PROJECT_ID=your-project-id`

### GCP Resources Created
- [ ] Cloud Storage bucket created
- [ ] BigQuery dataset created
- [ ] (Optional) Pub/Sub topic created for streaming
- [ ] (Optional) Cloud Composer environment created

---

## 🚀 5-Minute Quickstart

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Set Environment Variables
```bash
export PROJECT_ID=your-project-id
export REGION=us-central1
export BUCKET_NAME=${PROJECT_ID}-dataflow-bucket
export TEMP_LOCATION=gs://${BUCKET_NAME}/temp
```

### Step 3: Test Batch Pipeline Locally
```bash
python batch_pipelines/batch_word_count_pipeline.py \
    --runner DirectRunner \
    --input README.md \
    --output test_project:test_dataset.word_count
```

### Step 4: Run on Cloud Dataflow
```bash
python batch_pipelines/batch_word_count_pipeline.py \
    --runner DataflowRunner \
    --project ${PROJECT_ID} \
    --region ${REGION} \
    --temp_location ${TEMP_LOCATION} \
    --input gs://${BUCKET_NAME}/input/*.txt \
    --output ${PROJECT_ID}:dataflow_output.word_count
```

### Step 5: Monitor
```bash
# List jobs
gcloud dataflow jobs list --region ${REGION}

# Get job status
gcloud dataflow jobs describe JOB_ID --region ${REGION}
```

---

## 🎯 Choose Your Path

### Path A: Batch Processing
**Goal:** Process files from Cloud Storage

1. **Update config:**
   - Edit `templates/config_template.py`
   - Set `GCP_PROJECT_ID`, `GCS_BUCKET`, `BQ_DATASET`

2. **Prepare input data:**
   ```bash
   gsutil cp your_data.csv gs://${BUCKET_NAME}/input/
   ```

3. **Run pipeline:**
   ```bash
   python batch_pipelines/batch_word_count_pipeline.py \
       --runner DataflowRunner \
       --project ${PROJECT_ID} \
       --region ${REGION} \
       --temp_location ${TEMP_LOCATION} \
       --input gs://${BUCKET_NAME}/input/*.csv \
       --output ${PROJECT_ID}:dataflow_output.results
   ```

4. **Query results:**
   ```bash
   bq query 'SELECT * FROM `${PROJECT_ID}.dataflow_output.results` LIMIT 10'
   ```

### Path B: Streaming Processing
**Goal:** Real-time processing from Pub/Sub

1. **Create Pub/Sub topic:**
   ```bash
   gcloud pubsub topics create events-stream
   gcloud pubsub subscriptions create events-subscription --topic events-stream
   ```

2. **Run pipeline:**
   ```bash
   python streaming_pipelines/streaming_event_processing_pipeline.py \
       --runner DataflowRunner \
       --project ${PROJECT_ID} \
       --region ${REGION} \
       --temp_location ${TEMP_LOCATION} \
       --input_topic projects/${PROJECT_ID}/topics/events-stream \
       --output_table ${PROJECT_ID}:dataflow_output.streaming_events \
       --window_duration 60
   ```

3. **Publish test messages:**
   ```bash
   gcloud pubsub topics publish events-stream \
       --message '{"event_type":"test","value":100,"timestamp":"2024-01-01T00:00:00Z"}'
   ```

4. **Query results:**
   ```bash
   bq query 'SELECT * FROM `${PROJECT_ID}.dataflow_output.streaming_events` ORDER BY processing_timestamp DESC LIMIT 10'
   ```

### Path C: Using Composer
**Goal:** Orchestrate pipelines with Airflow

1. **Create Composer environment:**
   ```bash
   gcloud composer environments create dataflow-composer \
       --location ${REGION} \
       --machine-type n1-standard-4
   ```

2. **Get DAG bucket:**
   ```bash
   DAG_BUCKET=$(gcloud composer environments describe dataflow-composer \
       --location ${REGION} \
       --format='value(config.dagGcsPrefix)')
   ```

3. **Deploy DAGs:**
   ```bash
   gsutil cp composer_dags/*.py ${DAG_BUCKET}
   ```

4. **Trigger pipeline:**
   ```bash
   gcloud composer environments run dataflow-composer \
       --location ${REGION} \
       dags trigger -- batch_dataflow_word_count
   ```

---

## 📊 Common Commands Reference

| Task | Command |
|------|---------|
| **List jobs** | `gcloud dataflow jobs list --region ${REGION}` |
| **Get job status** | `gcloud dataflow jobs describe JOB_ID --region ${REGION}` |
| **View logs** | `gcloud dataflow jobs log JOB_ID --region ${REGION}` |
| **Cancel job** | `gcloud dataflow jobs cancel JOB_ID --region ${REGION}` |
| **Query BigQuery** | `bq query 'SELECT * FROM table'` |
| **Upload to GCS** | `gsutil cp file gs://bucket/path/` |
| **List GCS files** | `gsutil ls gs://bucket/` |
| **Create topic** | `gcloud pubsub topics create topic-name` |
| **Publish message** | `gcloud pubsub topics publish topic-name --message 'json'` |

---

## 🐛 Troubleshooting

### Problem: "Project not found"
```bash
# Solution: Set project ID
export PROJECT_ID=your-actual-project-id
gcloud config set project ${PROJECT_ID}
```

### Problem: "Bucket not found"
```bash
# Solution: Create bucket first
gsutil mb gs://${PROJECT_ID}-dataflow-bucket
gsutil mb gs://${PROJECT_ID}-dataflow-bucket/temp
```

### Problem: "Permission denied"
```bash
# Solution: Verify service account permissions
gcloud projects get-iam-policy ${PROJECT_ID} \
    --flatten="bindings[].members" \
    --filter="bindings.members:dataflow*"
```

### Problem: Slow job execution
```bash
# Solution: Increase workers and enable optimizations
--num_workers 8 \
--max_num_workers 20 \
--worker_machine_type n1-standard-8 \
--enable_streaming_engine  # for streaming
```

---

## 📚 Next Steps

1. **Read** `DATAFLOW_PIPELINE_GUIDE.md` for complete documentation
2. **Review** `PROJECT_ANALYSIS.md` to understand template mapping
3. **Explore** original `../dataflow_realtime_project/` for advanced examples
4. **Customize** templates for your use case
5. **Test** locally before deploying to production

---

## 🎓 Resource Links

- [Apache Beam Documentation](https://beam.apache.org/documentation/sdks/python/)
- [Google Cloud Dataflow](https://cloud.google.com/dataflow/docs)
- [Cloud Composer (Apache Airflow)](https://cloud.google.com/composer/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)

---

## ✨ Tips for Success

1. **Always test locally first** with `DirectRunner`
2. **Start small** - test with 1 worker before scaling
3. **Monitor from the start** - set up alerts early
4. **Use version control** - track pipeline changes
5. **Document your transformations** - include comments
6. **Validate data quality** - add checks in pipelines
7. **Set up proper logging** - capture errors early
8. **Automate with Composer** - reduce manual operations

---

**Ready to get started? Pick a path above and follow the steps!**
