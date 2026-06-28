# 14-Day GCP Data Engineer Interview Preparation Roadmap

## Overview

| Week | Focus | Hours/Day |
|------|-------|-----------|
| Week 1 (Day 1-7) | Fundamentals + Core Services + SQL | 3-4h |
| Week 2 (Day 8-14) | Advanced Topics + System Design + Mock Interviews | 3-4h |

---

## Week 1: Foundations

### Day 1: GCP Overview + BigQuery Basics
**Study (2h):**
- Read: `01-GCP-Data-Engineering-Overview.md`
- Read: `02-Core-GCP-Services.md` (BigQuery section)
- Understand: serverless vs managed, ELT vs ETL

**Practice (1.5h):**
- Create a BigQuery dataset in free tier
- Load sample data (public datasets)
- Write 5 queries: SELECT, JOIN, GROUP BY, subquery, CTE

**Key Takeaway:** BigQuery separates compute from storage. Columnar = only scan columns you need.

---

### Day 2: SQL Deep Dive — Window Functions & Optimization
**Study (1.5h):**
- Read: `04-BigQuery-Optimization.md`
- Focus: Partitioning, clustering, query anti-patterns

**Practice (2h):**
```sql
-- Practice these patterns:
1. ROW_NUMBER() for deduplication
2. LAG/LEAD for change detection
3. Running totals with SUM() OVER
4. NTILE for percentile buckets
5. MERGE statement for SCD
6. Explain partitioning vs clustering trade-offs
```

**Key Takeaway:** Always filter on partition column. Cluster by high-cardinality filter columns.

---

### Day 3: Python for Data Engineering
**Study (1h):**
- Review Python patterns: generators, context managers, error handling
- BigQuery client library, GCS client library

**Practice (2.5h):**
```python
# Build these:
1. Script to read CSV → validate → upload to GCS → load to BQ
2. Idempotent load function (delete + insert)
3. Data quality check function
4. Pub/Sub publisher and subscriber
```

**Key Takeaway:** Know `google-cloud-bigquery`, `google-cloud-storage`, `google-cloud-pubsub` libraries cold.

---

### Day 4: Dataflow (Apache Beam)
**Study (2h):**
- Read: `02-Core-GCP-Services.md` (Dataflow section)
- Read: `10.a-Hands-On-Mini-Project.md` (real-time streaming pipeline example)
- Understand: PCollections, ParDo, DoFn, transforms, windowing
- Batch vs streaming modes

**Practice (2h):**
- Write a batch pipeline: Read CSV → Transform → Write BQ
- Add error handling (dead-letter pattern)
- Understand `with_outputs` for branching
- Study the Pub/Sub → Dataflow → BQ streaming example in file 10.a

**Key Takeaway:** Beam's power is unified model. Same code runs batch or streaming by changing runner options.

---

### Day 5: Cloud Composer (Airflow)
**Study (1.5h):**
- Read: `02-Core-GCP-Services.md` (Composer section)
- DAG structure, operators, sensors, XCom, trigger rules

**Practice (2h):**
```python
# Write DAGs for:
1. Simple 3-task pipeline (extract → transform → load)
2. Branching (if data exists → process, else → skip)
3. Dynamic task generation
4. Error handling with on_failure_callback
5. Data quality sensor
```

**Key Takeaway:** Know GCP-specific operators (BigQueryInsertJobOperator, GCSToBigQueryOperator). Understand trigger rules.

---

### Day 6: Data Modeling + Data Quality
**Study (2h):**
- Read: `06-Data-Modeling-Quality.md`
- Star schema vs denormalized, nested/repeated fields
- SCD Type 1 and Type 2 implementations

**Practice (1.5h):**
- Design a schema for a loan processing system
- Write a MERGE statement for SCD Type 2
- Implement 3 data quality checks in SQL

**Key Takeaway:** BigQuery favors denormalized wide tables. Use STRUCT/ARRAY for hierarchical data.

---

### Day 7: Architecture Patterns + Migration
**Study (2.5h):**
- Read: `03-Architecture-Patterns.md`
- Read: `16-Hadoop-Hive-Migration-to-GCP.md`
- Batch ELT, streaming, Lambda, CDC, Data Mesh, ETL/ELT with data cleaning
- Hadoop/Hive migration phases (historical load → incremental sync → cutover)
- Decision framework: when to use what

**Practice (1h):**
- Draw 4 architectures on paper/whiteboard:
  1. Daily batch pipeline (Oracle → BQ)
  2. Real-time event processing (Pub/Sub → Dataflow → BQ)
  3. CDC pipeline (source DB → BQ with MERGE)
  4. Hadoop/Hive migration (on-prem → GCS → BigQuery with incremental sync)

**Key Takeaway:** Be able to justify every component choice. "I chose X over Y because..."

---

## Week 2: Advanced + Interview Prep

### Day 8: Security, IAM, Governance
**Study (2h):**
- Read: `05-Security-IAM-Governance.md`
- IAM roles, service accounts, Secret Manager
- VPC Service Controls, CMEK, DLP

**Practice (1.5h):**
- Write IAM policy for a multi-team data platform
- Implement Secret Manager access in Python
- Explain column-level and row-level security

**Key Takeaway:** For enterprise interviews, security is 20%+ of the conversation. Know it cold.

---

### Day 9: CI/CD + Monitoring
**Study (2h):**
- Read: `07-CICD-Monitoring-Troubleshooting.md`
- Cloud Build, testing strategies, Terraform basics
- Cloud Monitoring, alerting, logging

**Practice (1.5h):**
- Write a `cloudbuild.yaml` for a pipeline
- Design monitoring dashboards (what metrics matter?)
- Troubleshooting scenarios (practice the checklist)

**Key Takeaway:** "How do you know your pipeline is healthy?" → Freshness, quality metrics, SLA monitoring.

---

### Day 10: System Design Practice
**Study (1h):**
- Review `08-Interview-Questions-Answers.md` (architecture questions)
- Review `13-Spark-Dataproc-Advanced.md` (OOM scenarios, preemptible clusters)

**Practice (2.5h) — Mock designs (25 min each):**
1. Design a real-time fraud detection system
2. Design a data lake for a finance company
3. Design a pipeline to migrate 50TB from Hadoop/Hive to BigQuery (refer to file 16)
4. Design a data quality framework for 100+ pipelines

**Framework for each:**
```
1. Clarify requirements (scale, latency, cost, compliance)
2. High-level architecture (draw boxes and arrows)
3. Deep-dive into critical path
4. Discuss trade-offs
5. Address failure scenarios
6. Cost estimation
```

---

### Day 11: Coding Practice
**Practice (3h):**

**SQL (1.5h):**
- 5 medium-hard problems (window functions, CTEs, recursive)
- Practice explaining your approach out loud

**Python (1.5h):**
- Write a complete mini-pipeline script (end-to-end)
- Implement: retry logic, idempotent writes, validation
- Apache Beam: write a pipeline from memory

---

### Day 12: Company-Specific Prep
**Study (1.5h):**
- Read: `09-Company-Wise-Strategy.md`
- Tailor preparation to your target company type

**Practice (2h):**
- Research the company's data stack (check job description, tech blog, LinkedIn)
- Prepare 5 STAR stories relevant to their challenges
- Prepare 5 questions to ask the interviewer

**Key Takeaway:** Match your examples to their domain. Finance → talk about reconciliation, compliance. Startup → talk about speed, trade-offs.

---

### Day 13: Mock Interview Day
**Practice (3-4h):**

**Round 1: Coding (45 min)**
- 1 SQL problem + 1 Python problem
- Timer on. Explain as you go.

**Round 2: System Design (45 min)**
- Pick a random design question, solve on whiteboard/paper
- Practice talking through trade-offs

**Round 3: Behavioral (30 min)**
- Answer 5 behavioral questions using STAR format
- Record yourself and review

**Round 4: Review & Gaps (1h)**
- What felt weak? Review those sections.

---

### Day 14: Final Review + Confidence Building
**Review (2h):**
- Skim all guide sections (focus on key takeaways)
- Review your STAR stories one more time
- Review the common interview questions and answers

**Mindset (1h):**
- It's okay to say "I don't know, but here's how I'd approach it"
- Ask clarifying questions (shows senior thinking)
- Think out loud during design questions
- Trade-offs > perfect answers

---

## Quick Reference Card (Print This)

### GCP Services Cheat Sheet
| Need | Service |
|------|---------|
| Store files | Cloud Storage |
| Stream events | Pub/Sub |
| Process batch/stream | Dataflow |
| Process Spark | Dataproc |
| Query/warehouse | BigQuery |
| Orchestrate | Cloud Composer |
| CDC | Datastream |
| Secrets | Secret Manager |
| Monitor | Cloud Monitoring |
| Catalog | Data Catalog |

### SQL Patterns to Know
1. Window functions (ROW_NUMBER, LAG, SUM OVER)
2. CTEs (WITH clause)
3. MERGE (upsert/SCD)
4. UNNEST (for arrays)
5. PARTITION BY + CLUSTER BY
6. Approximate functions (APPROX_COUNT_DISTINCT)

### Python Patterns to Know
1. BigQuery client (load, query, streaming insert)
2. GCS client (upload, download, list)
3. Pub/Sub (publish, subscribe)
4. Apache Beam (Pipeline, ParDo, DoFn, IO)
5. Error handling + retries
6. Idempotent operations

### Architecture Patterns
1. Batch ELT: GCS → BQ raw → BQ curated → BQ serving
2. Streaming: Pub/Sub → Dataflow → BQ + GCS
3. CDC: Datastream → GCS → BQ MERGE
4. Lambda: Batch + Speed layer → Serving layer
5. ETL/ELT: Raw → Clean → Validate → Load (with dead-letter)
6. Hadoop Migration: Hive → GCS → BQ (historical + incremental sync)

---

## 7-Day Compressed Roadmap (If Short on Time)

| Day | Focus |
|-----|-------|
| 1 | BigQuery (optimization, SQL window functions) |
| 2 | Dataflow + Pub/Sub (batch + streaming) — include file 10.a |
| 3 | Airflow/Composer + Architecture patterns + Hadoop migration (file 16) |
| 4 | Security + Data modeling + Quality |
| 5 | Spark/Dataproc (OOM debugging, preemptible clusters — file 13) + System design (2 problems) |
| 6 | Coding practice (SQL + Python) |
| 7 | Mock interview + behavioral + company research |
