# Company-Wise Preparation Strategy

## 1. Startup (Series A-C, 10-200 people)

### What They Value
- **Speed**: Can you build and ship quickly?
- **Breadth**: Can you handle end-to-end (infra + pipeline + analytics)?
- **Pragmatism**: Trade-offs over perfection
- **Ownership**: You own it, you monitor it, you fix it at 2 AM

### Typical Stack
- BigQuery (warehouse) + GCS (lake)
- Cloud Functions / Cloud Run (lightweight processing)
- Airflow on Cloud Composer (or self-hosted)
- dbt for transforms
- Fivetran/Airbyte for ingestion
- Looker / Metabase for dashboards

### Interview Focus Areas
| Weight | Topic |
|--------|-------|
| 30% | System design (end-to-end pipeline) |
| 25% | Coding (Python, SQL) |
| 20% | Problem solving (ambiguous requirements) |
| 15% | GCP services knowledge |
| 10% | Culture fit (scrappy, autonomous) |

### Sample Questions
1. "Design a data pipeline for our e-commerce platform from scratch. We have 10M events/day."
2. "How would you set up analytics for a new product feature with no existing data?"
3. "Write a Python script to ingest data from this REST API, transform it, and load to BigQuery."
4. "We have a 5-person team. How do you balance tech debt vs shipping features?"

### How to Prepare
- Build a complete mini-project end-to-end (see Case Study section)
- Practice coding in Python + SQL (LeetCode medium level)
- Prepare stories about building things fast with limited resources
- Know dbt, basic Terraform, Docker
- Understand cost optimization (startups are cost-sensitive)

---

## 2. Product-Based Company (Google, Spotify, Grab, Flipkart, etc.)

### What They Value
- **Scale**: Can you handle billions of events?
- **Design depth**: Correct trade-offs at scale
- **Code quality**: Production-grade, tested, maintainable
- **Data modeling**: Efficient schemas for complex domains

### Typical Stack
- Custom frameworks + GCP managed services
- Dataflow (streaming at scale)
- BigQuery (petabyte analytics)
- Pub/Sub (event backbone)
- Dataproc (ML pipelines)
- Internal orchestration or Composer

### Interview Focus Areas
| Weight | Topic |
|--------|-------|
| 35% | System design (scale, reliability, trade-offs) |
| 30% | Coding (Python, Spark, SQL optimization) |
| 20% | Data modeling & architecture patterns |
| 10% | GCP services deep-dive |
| 5% | Behavioral (collaboration, ownership) |

### Sample Questions
1. "Design a real-time recommendation system that handles 1B events/day."
2. "How would you build a data quality framework that scales across 500 pipelines?"
3. "Optimize this Spark job that takes 4 hours — bring it under 30 minutes."
4. "Design the data model for a ride-sharing platform (drivers, riders, trips, pricing)."
5. "Write a Beam pipeline that deduplicates events with exactly-once semantics."

### How to Prepare
- Practice system design (Designing Data-Intensive Applications concepts)
- Deep-dive into one streaming framework (Beam or Spark Streaming)
- Master SQL window functions and optimization
- Study distributed systems concepts (CAP, eventual consistency, partitioning)
- Prepare 3-4 STAR stories about scale challenges

---

## 3. Enterprise / Finance Company (Goldman, JPMorgan, Amex, Banks)

### What They Value
- **Reliability**: 99.99% uptime, no data loss
- **Governance**: Lineage, audit trails, compliance
- **Security**: Encryption, access control, PII handling
- **Process**: CI/CD, change management, documentation
- **Domain knowledge**: Financial data, regulatory requirements

### Typical Stack
- BigQuery / legacy warehouses (Teradata, Oracle) being migrated
- Cloud Composer (Airflow) — heavy orchestration
- Dataflow / Dataproc (batch processing)
- VPC Service Controls, CMEK, DLP
- Data Catalog, lineage tracking
- Strict IAM, separate projects per env

### Interview Focus Areas
| Weight | Topic |
|--------|-------|
| 25% | Architecture (reliability, disaster recovery) |
| 25% | Security & governance (IAM, encryption, audit) |
| 20% | SQL & data modeling (finance domain) |
| 15% | Pipeline design (batch, CDC, reconciliation) |
| 10% | Behavioral (risk management, stakeholder mgmt) |
| 5% | CI/CD & operational excellence |

### Sample Questions
1. "How do you ensure no data loss in a pipeline that processes financial transactions?"
2. "Design a CDC pipeline from Oracle to BigQuery with full audit trail."
3. "How do you handle PII in a data lake while meeting GDPR/SOX requirements?"
4. "Explain your approach to disaster recovery for a critical data pipeline."
5. "How would you implement SCD Type 2 for customer dimension in BigQuery?"
6. "Walk me through your CI/CD process for deploying a production pipeline."

### How to Prepare
- Deep-dive into security (IAM, CMEK, VPC-SC, DLP)
- Study data governance (Data Catalog, lineage, classification)
- Master CDC patterns and SCD implementations
- Understand reconciliation and data quality controls
- Know financial concepts: T+1 settlement, EOD batch, regulatory reporting
- Prepare stories about handling production incidents responsibly

---

## Comparison Matrix

| Aspect | Startup | Product Company | Enterprise |
|--------|---------|-----------------|-----------|
| Interview rounds | 3-4 | 4-6 | 4-5 |
| Coding test | Take-home or live | Live (LC medium) | SQL-heavy + Python |
| System design | High-level E2E | Deep, scaled | Reliability-focused |
| Security questions | Rare | Moderate | Heavy |
| Domain knowledge | Nice-to-have | Moderate | Required |
| Behavioral | Culture fit | Leadership principles | Risk/process |
| Time to prepare | 1-2 weeks | 3-4 weeks | 2-3 weeks |

---

## Universal Preparation Tips

1. **Know your resume deeply** — every project you list will be questioned in detail
2. **Prepare 5 STAR stories** covering: scale challenge, failure recovery, cross-team collaboration, design decision, optimization
3. **Practice SQL daily** — window functions, CTEs, MERGE, optimization
4. **Build one end-to-end project** on GCP (even with free tier)
5. **Read the job description carefully** — tailor your examples to their tech stack
6. **Ask good questions** — about team size, data volume, current challenges, tech debt
