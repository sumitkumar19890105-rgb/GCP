# GCP Data Engineer Interview Preparation Guide

## Table of Contents

| # | File | Topic |
|---|------|-------|
| 1 | [01-GCP-Data-Engineering-Overview.md](01-GCP-Data-Engineering-Overview.md) | GCP overview, core pillars, batch vs streaming, ELT vs ETL |
| 2 | [02-Core-GCP-Services.md](02-Core-GCP-Services.md) | BigQuery, GCS, Pub/Sub, Dataflow, Dataproc, Composer + code examples |
| 3 | [03-Architecture-Patterns.md](03-Architecture-Patterns.md) | Batch ELT, streaming, Lambda, CDC, Data Mesh, ML pipelines, ETL/ELT with data cleaning |
| 4 | [04-BigQuery-Optimization.md](04-BigQuery-Optimization.md) | Partitioning, clustering, query optimization, cost, BQML |
| 5 | [05-Security-IAM-Governance.md](05-Security-IAM-Governance.md) | IAM, Secret Manager, DLP, VPC-SC, encryption, audit |
| 6 | [06-Data-Modeling-Quality.md](06-Data-Modeling-Quality.md) | Star schema, denormalization, nested fields, SCD, quality checks |
| 7 | [07-CICD-Monitoring-Troubleshooting.md](07-CICD-Monitoring-Troubleshooting.md) | Cloud Build, Terraform, monitoring, alerting, debugging |
| 8 | [08-Interview-Questions-Answers.md](08-Interview-Questions-Answers.md) | 18 detailed Q&A covering design, coding, behavioral + advanced scenarios |
| 9 | [09-Company-Wise-Strategy.md](09-Company-Wise-Strategy.md) | Startup vs Product Co vs Enterprise prep strategies |
| 10 | [10-Hands-On-Mini-Project.md](10-Hands-On-Mini-Project.md) | Batch loan analytics pipeline with problem statements (GCS → Dataflow → BQ → Composer) |
| 10.a | [10.a-Hands-On-Mini-Project.md](10.a-Hands-On-Mini-Project.md) | Real-time streaming pipeline (Pub/Sub → Dataflow → BQ → Cloud Functions alerts) |
| 11 | [11-Preparation-Roadmap.md](11-Preparation-Roadmap.md) | 14-day and 7-day study plans with daily tasks |
| 12 | [12-Advanced-SQL-Practice.md](12-Advanced-SQL-Practice.md) | 20 progressively harder SQL problems (sessionization, gaps-and-islands, SCD) |
| 13 | [13-Spark-Dataproc-Advanced.md](13-Spark-Dataproc-Advanced.md) | Shuffle, skew, AQE, OOM debugging (8 scenarios), preemptible clusters, advanced PySpark |
| 14 | [14-dbt-Complete-Guide.md](14-dbt-Complete-Guide.md) | Models, tests, snapshots, incremental strategies, macros, BigQuery |
| 15 | [15-Behavioral-STAR-Examples.md](15-Behavioral-STAR-Examples.md) | 8 detailed STAR stories for behavioral interviews |
| 16 | [16-Hadoop-Hive-Migration-to-GCP.md](16-Hadoop-Hive-Migration-to-GCP.md) | Full Hadoop/Hive → GCP migration (historical load, incremental sync, validation, cutover) |

## How to Use This Guide

1. **If you have 14 days**: Follow the day-by-day roadmap in file 11
2. **If you have 7 days**: Use the compressed 7-day plan at the bottom of file 11
3. **If you have 3 days**: Focus on files 04 (BigQuery), 08 (Q&A), and 09 (Company strategy)
4. **For quick reference**: Print the cheat sheet from file 11

## Key Principles

- **Understand WHY, not just WHAT** — Interviewers ask "why did you choose X?"
- **Practice out loud** — Thinking silently ≠ explaining clearly
- **Build something** — The mini-project in file 10 gives you real talking points
- **Tailor to the company** — Startup wants speed, enterprise wants governance

Good luck!
