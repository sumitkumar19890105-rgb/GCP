"""
Composer Configuration and Deployment Guide
Location: Keep in version control (git repo)

This file explains directory structure and deployment process for Composer
"""

# ============================================================================
# DIRECTORY STRUCTURE FOR COMPOSER SETUP
# ============================================================================

"""
Local Development Structure (Git Repository):
================================================

dataflow_realtime_project/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── composer_setup/                          ⭐ NEW - Deployment scripts
│   ├── deploy.sh                            # Main deployment script
│   ├── create_environment.sh                # Create Composer environment
│   ├── update_dags.sh                       # Update DAGs
│   └── config.env                           # Environment variables
│
├── composer_dags/                           ⭐ NEW - Airflow DAGs
│   ├── __init__.py
│   ├── config.py                            # Shared configuration
│   ├── tumbling_window_dag.py               # Tumbling window DAG
│   ├── sliding_window_dag.py                # Sliding window DAG
│   ├── session_window_dag.py                # Session window DAG
│   └── advanced_pipeline_dag.py             # Multi-job orchestration
│
├── composer_templates/                      ⭐ NEW - Dataflow templates
│   ├── tumbling_window_template.py
│   ├── sliding_window_template.py
│   ├── session_window_template.py
│   └── advanced_pipeline_template.py
│
├── composer_configs/                        ⭐ NEW - Configuration files
│   ├── dev_config.json
│   ├── staging_config.json
│   ├── prod_config.json
│   └── pipeline_parameters.yaml
│
├── pipeline_code/                           (Existing)
│   ├── base_pipeline.py
│   ├── tumbling_window_pipeline.py
│   ├── sliding_window_pipeline.py
│   ├── session_window_pipeline.py
│   ├── advanced_pipeline_late_data.py
│   ├── transforms.py
│   └── __init__.py
│
├── sample_data/                             (Existing)
│   ├── transactions.csv
│   ├── sensor_data.csv
│   └── user_activity.csv
│
└── bigquery_schemas/                        (Existing)
    ├── transactions_schema.json
    ├── aggregated_sales_schema.json
    └── sensor_data_schema.json


GCS Bucket Structure (Cloud Storage):
=====================================

gs://YOUR-PROJECT-dataflow-bucket/
│
├── dags/                                    ← Composer reads from here
│   ├── tumbling_window_dag.py
│   ├── sliding_window_dag.py
│   ├── session_window_dag.py
│   └── advanced_pipeline_dag.py
│
├── templates/                               ← Dataflow flex templates
│   ├── tumbling_window_template/
│   ├── sliding_window_template/
│   ├── session_window_template/
│   └── advanced_template/
│
├── data/                                    ← Data files
│   ├── configs/
│   │   ├── dev_config.json
│   │   ├── staging_config.json
│   │   └── prod_config.json
│   ├── sample_data/
│   │   ├── transactions.csv
│   │   ├── sensor_data.csv
│   │   └── user_activity.csv
│   └── schemas/
│       ├── transactions_schema.json
│       ├── aggregated_sales_schema.json
│       └── sensor_data_schema.json
│
├── temp/                                    ← Temporary Dataflow files
│   └── (auto-generated)
│
└── staging/                                 ← Dataflow staging location
    └── (auto-generated)
"""

# ============================================================================
# WHERE TO KEEP CODE & TEMPLATES - BEST PRACTICES
# ============================================================================

"""
1. SOURCE CONTROL (Git Repository)
   ====================================
   KEEP HERE:
   - All DAG Python files (composer_dags/)
   - All template files (composer_templates/)
   - Configuration files (composer_configs/)
   - Deployment scripts (composer_setup/)
   - requirements.txt
   - .gitignore
   - Documentation (GCP_COMPOSER_GUIDE.md, etc.)
   
   VERSION CONTROL STRUCTURE:
   .git/
       - Tracks all changes
       - Enables rollback
       - Enables collaboration
   
   Example Git Commands:
   $ git add composer_dags/
   $ git commit -m "Add tumbling window DAG"
   $ git push origin main

2. COMPOSER ENVIRONMENT (gs://BUCKET/dags/)
   ========================================
   KEEP HERE:
   - ONLY the DAG files that Composer needs to run
   - Upload from git repository
   - Automatically synced by Composer
   
   DO NOT KEEP:
   - Large data files
   - Template code (store separately, reference via GCS paths)
   
   Upload Process:
   $ gsutil cp composer_dags/*.py gs://YOUR-BUCKET/dags/
   
   Or use Composer commands:
   $ gcloud composer environments storage dags import \
       --environment=ENV_NAME --location=REGION --source=FILE.py

3. GCS BUCKET (gs://BUCKET/templates/, gs://BUCKET/data/)
   =======================================================
   KEEP HERE:
   - Dataflow template code (composed into containers)
   - Configuration files (referenced by DAGs)
   - Sample data for testing
   - BigQuery schemas
   
   Organization:
   gs://bucket/templates/TEMPLATE_NAME/
                    ├── main.py           (Flex template Python file)
                    ├── requirements.txt
                    └── Dockerfile        (if custom)
   
   gs://bucket/data/
                ├── configs/
                ├── sample_data/
                └── schemas/

4. VERSION CONTROL IGNORE (.gitignore)
   ===================================
   DO NOT COMMIT TO GIT:
   - __pycache__/
   - *.pyc
   - .env files with secrets
   - venv/ or virtualenv/
   - *.egg-info/
   - .DS_Store (macOS)
   
   Example .gitignore:
   __pycache__/
   *.py[cod]
   *$py.class
   *.so
   .env
   .env.local
   venv/
   env/
   .vscode/
   .idea/
"""

# ============================================================================
# ENVIRONMENT VARIABLES & CONFIGURATION FILES
# ============================================================================

"""
File: composer_configs/dev_config.json
===========================================
{
  "project": "your-project-dev",
  "region": "us-central1",
  "composer_env": "dataflow-orchestrator-dev",
  "bucket": "gs://your-project-dev-dataflow-bucket",
  "dataset": "dataflow_dev",
  "pubsub_topics": {
    "transactions": "projects/your-project-dev/topics/dataflow-transactions",
    "sensors": "projects/your-project-dev/topics/dataflow-sensors",
    "activities": "projects/your-project-dev/topics/dataflow-activities"
  },
  "dataflow_config": {
    "workers": 2,
    "max_workers": 5,
    "machine_type": "n1-standard-2",
    "disk_size": 50
  }
}

File: composer_configs/prod_config.json
===========================================
{
  "project": "your-project-prod",
  "region": "us-central1",
  "composer_env": "dataflow-orchestrator-prod",
  "bucket": "gs://your-project-prod-dataflow-bucket",
  "dataset": "dataflow_prod",
  "pubsub_topics": {
    "transactions": "projects/your-project-prod/topics/dataflow-transactions",
    "sensors": "projects/your-project-prod/topics/dataflow-sensors",
    "activities": "projects/your-project-prod/topics/dataflow-activities"
  },
  "dataflow_config": {
    "workers": 4,
    "max_workers": 10,
    "machine_type": "n1-standard-4",
    "disk_size": 100
  }
}
"""

# ============================================================================
# DEPLOYMENT SCRIPTS
# ============================================================================

"""
File: composer_setup/config.env
==================================
# Environment Configuration
# Copy to .env and update with your values

# GCP Project Settings
PROJECT_ID=your-gcp-project-id
REGION=us-central1
ENVIRONMENT=dev  # or prod, staging

# Composer Settings
COMPOSER_ENV=dataflow-orchestrator-${ENVIRONMENT}
COMPOSER_BUCKET_BASE=${PROJECT_ID}-dataflow

# GCS Buckets
BUCKET=gs://${COMPOSER_BUCKET_BASE}-${ENVIRONMENT}
TEMPLATE_BUCKET=${BUCKET}/templates
DATA_BUCKET=${BUCKET}/data

# BigQuery
BQ_DATASET=dataflow_${ENVIRONMENT}
BQ_LOCATION=US

# Service Account
SERVICE_ACCOUNT=dataflow-runner@${PROJECT_ID}.iam.gserviceaccount.com

# Dataflow Configuration
NUM_WORKERS=2
MAX_WORKERS=5
MACHINE_TYPE=n1-standard-2
DISK_SIZE_GB=50


File: composer_setup/deploy.sh
==================================
#!/bin/bash
# Complete deployment script for Composer

set -e  # Exit on error

# Load configuration
source config.env

echo "=========================================="
echo "Deploying Dataflow Composer Setup"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Environment: $ENVIRONMENT"
echo "Region: $REGION"
echo ""

# Step 1: Check if buckets exist
echo "[1/5] Checking Cloud Storage buckets..."
gsutil ls -b ${BUCKET} || {
    echo "Creating bucket: ${BUCKET}"
    gsutil mb -p ${PROJECT_ID} -l ${REGION} ${BUCKET}
}

# Step 2: Create directory structure in GCS
echo "[2/5] Creating directory structure in GCS..."
gsutil -m cp README.md ${BUCKET}/README.md
gsutil -m cp requirements.txt ${BUCKET}/data/requirements.txt

# Step 3: Upload DAG files
echo "[3/5] Uploading DAG files to Composer..."
gsutil -m cp -r ../composer_dags/* ${BUCKET}/dags/

# Step 4: Build and upload Dataflow templates
echo "[4/5] Uploading Dataflow templates..."
gsutil -m cp -r ../composer_templates/* ${BUCKET}/templates/

# Step 5: Upload configuration and data
echo "[5/5] Uploading configuration and sample data..."
gsutil -m cp ../composer_configs/*.json ${BUCKET}/data/configs/
gsutil -m cp ../sample_data/* ${BUCKET}/data/sample_data/

echo ""
echo "=========================================="
echo "✓ Deployment Complete!"
echo "=========================================="
echo ""
echo "Next Steps:"
echo "1. Enable DAGs in Composer Web UI"
echo "2. Trigger a test run"
echo "3. Monitor in Cloud Logging"
echo ""
echo "Dashboard: https://console.cloud.google.com/composer"
echo ""


File: composer_setup/create_environment.sh
==============================================
#!/bin/bash
# Create a new Composer environment

set -e

source config.env

echo "Creating Composer Environment: $COMPOSER_ENV"
echo "Region: $REGION"
echo "Machine Type: $MACHINE_TYPE"
echo ""

gcloud composer environments create $COMPOSER_ENV \\
  --location=$REGION \\
  --python-version=3 \\
  --machine-type=$MACHINE_TYPE \\
  --node-count=3 \\
  --project=$PROJECT_ID \\
  --env-variables="PROJECT_ID=$PROJECT_ID,ENVIRONMENT=$ENVIRONMENT"

echo ""
echo "Environment created! Waiting for it to be ready..."
gcloud composer environments describe $COMPOSER_ENV \\
  --location=$REGION \\
  --project=$PROJECT_ID

echo "✓ Composer environment is ready!"
"""

# ============================================================================
# RECOMMENDED GIT WORKFLOW
# ============================================================================

"""
1. Clone the repository:
   $ git clone your-repo-url
   $ cd dataflow_realtime_project

2. Create a feature branch:
   $ git checkout -b feature/new-dag-name

3. Make changes to DAGs:
   $ vim composer_dags/new_dag.py

4. Test locally:
   $ python -m py_compile composer_dags/new_dag.py

5. Commit changes:
   $ git add composer_dags/new_dag.py
   $ git commit -m "Add new DAG: new-dag-name"

6. Push to repository:
   $ git push origin feature/new-dag-name

7. Create Pull Request for review

8. After approval, merge to main:
   $ git checkout main
   $ git pull origin main
   $ git merge feature/new-dag-name

9. Deploy to Composer:
   $ cd composer_setup
   $ ./deploy.sh

10. Verify in Composer Web UI:
    https://console.cloud.google.com/composer
"""

# ============================================================================
# SUMMARY - CODE ORGANIZATION
# ============================================================================

"""
LOCATION REFERENCE:

1. GIT REPOSITORY (Source Control)
   ├── DAG Files: composer_dags/
   ├── Templates: composer_templates/
   ├── Configs: composer_configs/
   ├── Deployment Scripts: composer_setup/
   └── Documentation: *.md files

2. GCS BUCKET (Cloud Storage)
   ├── DAGs: gs://bucket/dags/
   ├── Templates: gs://bucket/templates/
   ├── Data: gs://bucket/data/
   └── Temp: gs://bucket/temp/

3. COMPOSER ENVIRONMENT
   └── Monitors: gs://bucket/dags/
       └── Auto-syncs with environment
       └── DAGs run on schedule/trigger

WORKFLOW:
    Local Development (Git) 
         ↓
    Push to Repository
         ↓
    Run Tests
         ↓
    Deploy to GCS
         ↓
    Composer Reads DAGs
         ↓
    Triggers Dataflow Jobs
"""
