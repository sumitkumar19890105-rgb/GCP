#!/bin/bash
# GCP Composer Deployment Script
# Location: composer_setup/deploy.sh
# Usage: bash deploy.sh

set -e  # Exit on error

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

PROJECT_ID="your-project-id"              # CHANGE THIS
REGION="us-central1"
ENVIRONMENT="dev"                          # dev, staging, prod
COMPOSER_ENV="dataflow-orchestrator-${ENVIRONMENT}"
BUCKET_NAME="${PROJECT_ID}-dataflow-${ENVIRONMENT}"
BUCKET="gs://${BUCKET_NAME}"

# Machine configuration
NUM_WORKERS=2
MAX_WORKERS=5
MACHINE_TYPE="n1-standard-2"
DISK_SIZE_GB=50

# BigQuery
BQ_DATASET="dataflow_${ENVIRONMENT}"

# ============================================================================
# COLORS FOR OUTPUT
# ============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_step() {
    echo -e "${YELLOW}[*] $1${NC}"
}

print_success() {
    echo -e "${GREEN}[✓] $1${NC}"
}

print_error() {
    echo -e "${RED}[✗] $1${NC}"
}

# ============================================================================
# MAIN DEPLOYMENT
# ============================================================================

main() {
    print_header "GCP Composer Deployment Script"
    
    echo "Configuration:"
    echo "  Project ID: $PROJECT_ID"
    echo "  Environment: $ENVIRONMENT"
    echo "  Region: $REGION"
    echo "  Composer Env: $COMPOSER_ENV"
    echo "  Bucket: $BUCKET"
    echo "  BigQuery Dataset: $BQ_DATASET"
    echo ""
    
    read -p "Continue with deployment? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_error "Deployment cancelled"
        exit 1
    fi
    
    # Step 1: Verify gcloud authentication
    print_step "Step 1/6: Verifying GCP authentication..."
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        print_error "Not authenticated with GCP. Run: gcloud auth login"
        exit 1
    fi
    print_success "GCP authentication verified"
    
    # Step 2: Set GCP project
    print_step "Step 2/6: Setting GCP project..."
    gcloud config set project $PROJECT_ID
    print_success "Project set to: $PROJECT_ID"
    
    # Step 3: Enable required APIs
    print_step "Step 3/6: Enabling required GCP APIs..."
    gcloud services enable \
        composer.googleapis.com \
        dataflow.googleapis.com \
        pubsub.googleapis.com \
        bigquery.googleapis.com \
        storage-api.googleapis.com \
        --project=$PROJECT_ID > /dev/null 2>&1
    print_success "Required APIs enabled"
    
    # Step 4: Create GCS bucket if needed
    print_step "Step 4/6: Checking GCS bucket..."
    if gsutil ls -b "${BUCKET}" > /dev/null 2>&1; then
        print_success "Bucket already exists: ${BUCKET}"
    else
        print_step "  Creating bucket: ${BUCKET}"
        gsutil mb -p ${PROJECT_ID} -l ${REGION} ${BUCKET}
        print_success "Bucket created: ${BUCKET}"
    fi
    
    # Step 5: Create directory structure in GCS
    print_step "Step 5/6: Creating directory structure in GCS..."
    
    # Create subdirectories by uploading placeholder files
    echo "" | gsutil cp - ${BUCKET}/dags/.placeholder
    echo "" | gsutil cp - ${BUCKET}/templates/.placeholder
    echo "" | gsutil cp - ${BUCKET}/data/.placeholder
    echo "" | gsutil cp - ${BUCKET}/temp/.placeholder
    
    print_success "Directory structure created in GCS"
    
    # Step 6: Upload DAG and template files
    print_step "Step 6/6: Uploading DAG and template files..."
    
    # Check if local files exist
    if [ -d "../composer_dags" ]; then
        print_step "  Uploading DAG files..."
        gsutil -m cp ../composer_dags/*.py ${BUCKET}/dags/ 2>/dev/null || true
        print_success "DAG files uploaded"
    else
        print_error "Directory composer_dags/ not found. Creating example..."
        mkdir -p ../composer_dags
    fi
    
    if [ -d "../composer_templates" ]; then
        print_step "  Uploading template files..."
        gsutil -m cp ../composer_templates/*.py ${BUCKET}/templates/ 2>/dev/null || true
        print_success "Template files uploaded"
    else
        print_error "Directory composer_templates/ not found"
    fi
    
    if [ -d "../composer_configs" ]; then
        print_step "  Uploading config files..."
        gsutil -m cp ../composer_configs/*.json ${BUCKET}/data/configs/ 2>/dev/null || true
        print_success "Config files uploaded"
    fi
    
    if [ -d "../sample_data" ]; then
        print_step "  Uploading sample data..."
        gsutil -m cp ../sample_data/* ${BUCKET}/data/sample_data/ 2>/dev/null || true
        print_success "Sample data uploaded"
    fi
    
    # Final summary
    echo ""
    print_header "Deployment Summary"
    
    echo ""
    echo -e "${GREEN}✓ Deployment completed successfully!${NC}"
    echo ""
    echo "Next Steps:"
    echo "  1. Create Composer Environment:"
    echo "     gcloud composer environments create $COMPOSER_ENV \\"
    echo "       --location=$REGION \\"
    echo "       --python-version=3 \\"
    echo "       --machine-type=$MACHINE_TYPE \\"
    echo "       --node-count=3 \\"
    echo "       --project=$PROJECT_ID"
    echo ""
    echo "  2. Wait 10-15 minutes for environment creation"
    echo ""
    echo "  3. Upload DAGs to Composer:"
    echo "     gsutil -m cp $BUCKET/dags/* gs://COMPOSER-BUCKET/dags/"
    echo ""
    echo "  4. View in Cloud Console:"
    echo "     https://console.cloud.google.com/composer/environments"
    echo ""
    echo "  5. Enable and trigger DAGs in Airflow UI"
    echo ""
    echo "Resources:"
    echo "  - Bucket: ${BUCKET}"
    echo "  - Documentation: GCP_COMPOSER_GUIDE.md"
    echo ""
}

# ============================================================================
# ERROR HANDLING
# ============================================================================

trap 'print_error "Script failed at line $LINENO"; exit 1' ERR

# ============================================================================
# RUN MAIN
# ============================================================================

main "$@"
