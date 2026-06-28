# Security, IAM, and Governance

## 1. IAM (Identity and Access Management)

### Principle of Least Privilege
```
Organization
  └── Folder (Business Unit)
       └── Project (Environment)
            └── Resources (BigQuery, GCS, etc.)
```

### Key Roles for Data Engineering

| Role | Permission | Use Case |
|------|-----------|----------|
| `roles/bigquery.dataViewer` | Read tables | Analysts |
| `roles/bigquery.dataEditor` | Read + Write tables | ETL service accounts |
| `roles/bigquery.jobUser` | Run queries | All data users |
| `roles/bigquery.admin` | Full BQ control | Platform team |
| `roles/storage.objectViewer` | Read GCS objects | Consumers |
| `roles/storage.objectCreator` | Write GCS objects | Ingestion pipelines |
| `roles/composer.worker` | Run Airflow tasks | Composer service account |

### Service Account Best Practices
```bash
# Create dedicated service account per pipeline
gcloud iam service-accounts create etl-loan-pipeline \
    --display-name="ETL Loan Pipeline SA"

# Grant minimal permissions
gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:etl-loan-pipeline@my-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:etl-loan-pipeline@my-project.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"
```

### Row-Level and Column-Level Security
```sql
-- Column-level security (policy tags via Data Catalog)
-- Tag PII columns, restrict access to specific groups

-- Row-level security
CREATE ROW ACCESS POLICY region_filter
ON `project.dataset.transactions`
GRANT TO ('group:us-team@company.com')
FILTER USING (region = 'US');

CREATE ROW ACCESS POLICY apac_filter
ON `project.dataset.transactions`
GRANT TO ('group:apac-team@company.com')
FILTER USING (region = 'APAC');
```

---

## 2. Secret Management

### Secret Manager
```python
from google.cloud import secretmanager

def get_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Usage in pipeline
db_password = get_secret("my-project", "oracle-db-password")
api_key = get_secret("my-project", "external-api-key")
```

### In Cloud Composer
```python
# Store secrets as Airflow Variables or Connections backed by Secret Manager
# airflow.cfg: secrets_backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend

from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("oracle_prod")  # Fetched from Secret Manager
```

---

## 3. Data Governance

### Data Catalog
```python
from google.cloud import datacatalog_v1

client = datacatalog_v1.DataCatalogClient()

# Create a tag template for data classification
template = datacatalog_v1.TagTemplate()
template.display_name = "Data Classification"
template.fields["data_classification"] = datacatalog_v1.TagTemplateField(
    display_name="Classification",
    type_=datacatalog_v1.FieldType(
        enum_type=datacatalog_v1.FieldType.EnumType(
            allowed_values=[
                datacatalog_v1.FieldType.EnumType.EnumValue(display_name="PUBLIC"),
                datacatalog_v1.FieldType.EnumType.EnumValue(display_name="INTERNAL"),
                datacatalog_v1.FieldType.EnumType.EnumValue(display_name="CONFIDENTIAL"),
                datacatalog_v1.FieldType.EnumType.EnumValue(display_name="RESTRICTED"),
            ]
        )
    ),
)
```

### Data Loss Prevention (DLP)
```python
from google.cloud import dlp_v2

dlp = dlp_v2.DlpServiceClient()

# Scan BigQuery table for PII
inspect_config = {
    "info_types": [
        {"name": "EMAIL_ADDRESS"},
        {"name": "PHONE_NUMBER"},
        {"name": "CREDIT_CARD_NUMBER"},
        {"name": "US_SOCIAL_SECURITY_NUMBER"},
    ],
    "min_likelihood": dlp_v2.Likelihood.LIKELY,
}

storage_config = {
    "big_query_options": {
        "table_reference": {
            "project_id": "my-project",
            "dataset_id": "raw",
            "table_id": "customers",
        }
    }
}

# De-identify (mask PII)
deidentify_config = {
    "record_transformations": {
        "field_transformations": [{
            "fields": [{"name": "email"}],
            "primitive_transformation": {
                "character_mask_config": {
                    "masking_character": "*",
                    "number_to_mask": 5,
                }
            }
        }]
    }
}
```

---

## 4. VPC Service Controls

```
┌─────────────────────────────────────────┐
│        VPC Service Perimeter            │
│                                          │
│  ┌──────────┐  ┌──────────┐            │
│  │ BigQuery │  │   GCS    │            │
│  └──────────┘  └──────────┘            │
│  ┌──────────┐  ┌──────────┐            │
│  │ Dataflow │  │ Pub/Sub  │            │
│  └──────────┘  └──────────┘            │
│                                          │
│  Only authorized networks/identities     │
│  can access these resources              │
└─────────────────────────────────────────┘
```

- Prevents data exfiltration
- Controls access at project/service boundary
- Critical for finance/regulated industries

---

## 5. Encryption

| Layer | Default | Custom |
|-------|---------|--------|
| At Rest | Google-managed keys (AES-256) | CMEK (Customer-Managed Encryption Keys via Cloud KMS) |
| In Transit | TLS 1.3 | Always enabled |
| Application | N/A | Client-side encryption before upload |

### CMEK Example
```bash
# Create a key ring and key
gcloud kms keyrings create my-keyring --location=us
gcloud kms keys create my-bq-key --location=us --keyring=my-keyring --purpose=encryption

# Create BigQuery dataset with CMEK
bq mk --dataset \
    --default_kms_key=projects/my-project/locations/us/keyRings/my-keyring/cryptoKeys/my-bq-key \
    my-project:secure_dataset
```

---

## 6. Audit Logging

```sql
-- Who accessed what data?
SELECT
    timestamp,
    protopayload_auditlog.authenticationInfo.principalEmail as user,
    protopayload_auditlog.resourceName as resource,
    protopayload_auditlog.methodName as action
FROM `project.dataset.cloudaudit_googleapis_com_data_access`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY timestamp DESC;
```

---

## Interview Questions — Security

**Q: How do you handle PII in a data pipeline?**
> 1. Identify PII using Cloud DLP API scans
> 2. Classify data using Data Catalog policy tags
> 3. Apply column-level security in BigQuery for restricted columns
> 4. Use tokenization or hashing for irreversible de-identification
> 5. Row-level security for geography-based restrictions
> 6. Audit access via Cloud Audit Logs

**Q: How do you manage secrets in a production pipeline?**
> Use Secret Manager (never hardcode credentials). In Airflow, configure the Secret Manager backend so connections and variables are automatically fetched. Service accounts should use Workload Identity (no key files in production).

**Q: What is VPC Service Controls and when would you use it?**
> VPC-SC creates a security perimeter around GCP resources, preventing data exfiltration. Used in finance/healthcare where data must not leave the defined boundary. Even authorized users can't copy data outside the perimeter.
