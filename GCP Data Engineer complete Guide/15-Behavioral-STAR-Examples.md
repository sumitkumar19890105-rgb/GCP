# Behavioral Interview — STAR Examples for Data Engineers

## Framework: STAR
- **S**ituation: Context and background
- **T**ask: Your responsibility
- **A**ction: What you specifically did (with technical detail)
- **R**esult: Quantifiable outcome

---

## Story 1: Production Pipeline Failure (Incident Response)

### "Tell me about a time a critical pipeline failed."

**Situation:**
Our daily loan reconciliation pipeline processes 15M records from Oracle to BigQuery, running at 3 AM with a 6 AM SLA for morning reports. One Monday at 3:45 AM, PagerDuty fired — the Dataflow job had failed with BigQuery streaming insert quota errors.

**Task:**
I was the on-call data engineer. I needed to identify root cause, restore data flow within the SLA, and prevent recurrence.

**Action:**
1. **Immediate triage (3:45-4:00 AM):** Checked Dataflow job logs — saw "rateLimitExceeded" errors from BigQuery streaming API. The job was retrying indefinitely, burning through quota.
2. **Root cause identification (4:00-4:15 AM):** Checked upstream Oracle extract — the extract job had failed on Friday, retried Saturday with backlog, sending Friday + Saturday + Sunday data in one shot (3x normal volume = 45M records instead of 15M).
3. **Immediate fix (4:15-5:00 AM):** 
   - Killed the failed Dataflow job
   - Modified the pipeline to use batch load (GCS → BQ load job) instead of streaming inserts for this run
   - Chunked the 45M records into 5 date-based partitions
   - Ran 5 parallel load jobs
4. **Validation (5:00-5:30 AM):** Ran reconciliation query — source count matched target. All 3 days' data loaded correctly.
5. **Long-term fix (next sprint):**
   - Added Pub/Sub as a buffer between Oracle extract and Dataflow (handles volume spikes)
   - Implemented volume anomaly detection: alert if input exceeds 2x average
   - Added circuit breaker: auto-switch from streaming to batch if volume exceeds threshold
   - Documented runbook for similar incidents

**Result:**
- Restored within SLA (5:30 AM, 30 min buffer)
- Volume spike alert caught 3 similar incidents in the following 2 months before they caused failures
- Pipeline resilience improved — zero streaming quota failures since

---

## Story 2: Performance Optimization (Scale Challenge)

### "Tell me about a time you optimized a slow system."

**Situation:**
Our Spark-based risk scoring pipeline on Dataproc took 4.5 hours to process daily portfolios (50M loan records). The business needed results by 7 AM for trader decisions, but the pipeline started at 4 AM and finished at 8:30 AM — missing the SLA by 1.5 hours.

**Task:**
Reduce the pipeline runtime from 4.5 hours to under 2.5 hours (to finish by 6:30 AM with buffer), without increasing infrastructure cost by more than 20%.

**Action:**
1. **Profiled the pipeline** using Spark UI:
   - Stage 3 (large join) took 2.5 hours alone
   - One executor was running 40 minutes longer than others (skew)
   - 200 shuffle partitions, but most were < 1MB while one was 8GB
   
2. **Identified root cause:** The join key was `portfolio_id`, and one portfolio ("ALL_CONSUMER") had 30M of the 50M records.

3. **Applied fixes:**
   - **Salted the hot key:** Split "ALL_CONSUMER" into 20 sub-keys, exploded the small dimension table
   - **Broadcast the lookup table** (500MB) instead of shuffle join — saved 50% of shuffle time
   - **Enabled AQE** for automatic coalescing of remaining small partitions
   - **Switched to Parquet** with predicate pushdown (was reading CSV — scanning all columns)
   - **Added pre-filtering:** Moved filter conditions before the join (reduced data by 40%)

4. **Infrastructure change:** Added 5 preemptible workers (cost: +15%) for parallelism during the join stage.

**Result:**
- Pipeline runtime: 4.5h → 1h 45min (61% reduction)
- Cost: +15% from preemptible workers, but -30% from reduced executor-hours. Net: 15% cheaper.
- Consistently finished by 5:45 AM — 75 min ahead of SLA
- Approach was documented and reused for 3 other skewed pipelines

---

## Story 3: Data Quality Issue (Detection and Prevention)

### "Describe a situation where you caught a data quality issue before it impacted the business."

**Situation:**
Our finance reporting pipeline feeds BigQuery data to Looker dashboards used by 200+ analysts. One Thursday morning, I noticed our automated reconciliation check showed a 0.3% discrepancy in total loan balances — small enough to not trigger the 1% alert threshold, but unusual (normally it's < 0.01%).

**Task:**
Investigate the discrepancy, determine if it's a real issue, and fix it before the Friday board report which uses these numbers.

**Action:**
1. **Drilled down:** The 0.3% discrepancy was concentrated in one product line (auto loans). Checked row counts — matched. So the issue was in specific record values, not missing records.

2. **Identified the pattern:** Found 2,400 auto loan records where `principal_balance` was exactly 100x the expected value (e.g., $25,000 became $2,500,000). These were all from one source system feed.

3. **Root cause:** The upstream Oracle system had been patched Wednesday night. The patch changed the currency field from dollars to cents for auto loans only. Our pipeline had no unit validation — it just loaded whatever came in.

4. **Immediate fix:**
   ```sql
   UPDATE curated.loans
   SET principal_balance = principal_balance / 100
   WHERE product_line = 'AUTO'
     AND load_date = '2026-06-19'
     AND principal_balance > (SELECT PERCENTILE_CONT(principal_balance, 0.99) 
                              FROM curated.loans WHERE product_line = 'AUTO' AND load_date < '2026-06-19');
   ```

5. **Prevention:**
   - Added **statistical anomaly detection**: flag records where value deviates > 3 standard deviations from rolling 30-day average
   - Added **unit test**: assert max(principal_balance) < 10M for any product line
   - Added **source-target reconciliation at SUM level** (not just COUNT)
   - Lowered alert threshold from 1% to 0.1%

**Result:**
- Caught and fixed before Friday board report — $3.2B reporting error avoided
- Zero analysts saw incorrect data
- Anomaly detection has since caught 5 similar upstream changes in 6 months
- Became a template for other team's quality frameworks

---

## Story 4: Cross-Team Collaboration (Stakeholder Management)

### "Tell me about a time you had to work with a difficult stakeholder or team."

**Situation:**
I was building a real-time CDC pipeline from the core banking system to our BigQuery analytics layer. The DBA team (who owned the Oracle source) was resistant to enabling CDC — they were concerned about performance impact on the production database, and they'd been burned before by a previous CDC tool that caused 20% query degradation.

**Task:**
Get the DBA team on board with CDC enablement while respecting their performance concerns and maintaining a productive working relationship.

**Action:**
1. **Understood their perspective:** Had a 1:1 with the lead DBA. Their concern was legitimate — LogMiner-based CDC had caused issues before. They weren't saying no — they were saying "prove it won't hurt us."

2. **Proposed a pilot approach:**
   - Start with ONE low-traffic table (reference data, 10K records/day)
   - Use GCP Datastream (which uses non-intrusive log-based CDC, not LogMiner)
   - Agreed on success criteria: < 1% CPU impact on source, measured over 2 weeks
   
3. **Ran the pilot transparently:**
   - Shared read-only access to our monitoring dashboard showing source DB metrics
   - Sent weekly summary reports: "CPU: no change. I/O: +0.2%. Replication lag: 3 seconds."
   - Invited their team to our sprint demo showing the real-time data flowing

4. **Scaled gradually:** After 2-week pilot success, expanded table by table over 4 sprints (not all at once). Let them veto any table that had concerns.

5. **Built trust:** When we accidentally misconfigured one table and saw a 2% I/O spike, I immediately paused it, reported to them, fixed the config, and resumed only after their approval.

**Result:**
- Full CDC enabled for 47 tables within 3 months (originally estimated 6 months)
- Zero production incidents attributed to CDC
- DBA team became advocates — they now recommend us as a model for other teams
- Eliminated 6-hour batch delay — analytics now has <5 minute freshness

---

## Story 5: Technical Design Decision (Trade-offs)

### "Tell me about a significant technical decision you made and the trade-offs."

**Situation:**
We were designing the data platform for a new product: real-time credit scoring. The business wanted sub-second scoring, but also needed full audit trail for regulatory compliance. Two conflicting requirements: speed vs. comprehensive logging.

**Task:**
Design an architecture that satisfies both the sub-second latency SLA and complete audit requirements for SOX compliance.

**Action:**
1. **Analyzed the options:**
   - Option A: Single synchronous pipeline (score + log in one call) → 800ms latency. Too slow.
   - Option B: Score first, log async → 100ms latency, but audit log may be lost if async queue fails. Compliance risk.
   - Option C: Dual-write pattern → Complex, consistency issues between score and log.

2. **Proposed a hybrid architecture:**
   ```
   Request → Pub/Sub Topic → Two Subscribers:
     ├── Sub 1 (Pull, High Priority): Score Engine (Vertex AI) → Response (100ms)
     └── Sub 2 (Push): Audit Logger → BigQuery (eventual, guaranteed delivery)
   ```
   
   - Scoring path: Ultra-low latency via pre-computed features in Bigtable
   - Audit path: Pub/Sub guarantees at-least-once delivery to BigQuery
   - Both share the same event ID for correlation
   - If audit write fails, Pub/Sub retries (dead-letter after 7 days)

3. **Addressed compliance concern:**
   - Demonstrated that Pub/Sub has 99.95% availability and 7-day retention
   - Added a daily reconciliation job: compare scores served vs. audit records
   - Any discrepancy → alert + automatic replay from Pub/Sub dead-letter topic

4. **Validated with POC:** Built a 1-week prototype, stress-tested with 10K req/sec. Scoring: P99 = 95ms. Audit lag: P99 = 2 seconds. Zero audit records lost.

**Result:**
- Architecture approved by compliance team and CTO
- Production scoring: P50 = 45ms, P99 = 120ms (well within 1s SLA)
- Audit completeness: 99.9997% (3 records lost in 6 months, all recovered from dead-letter)
- Pattern adopted by 2 other teams for similar score-and-log requirements

---

## Story 6: Ownership and Initiative (Going Beyond)

### "Tell me about a time you took initiative beyond your role."

**Situation:**
Our team had 47 Airflow DAGs running in production. There was no visibility into overall pipeline health — each engineer monitored their own DAGs. When I joined, I noticed we had 5-6 silent failures per week that were only caught when business users reported missing data (usually 24-48 hours late).

**Task:**
This wasn't assigned to me — I was working on a different feature. But the problem was causing trust issues with our stakeholders and repeat firefighting for the team.

**Action:**
1. **Built a "Pipeline Health Dashboard" in 3 days** (used my 20% time):
   - Cloud Monitoring custom metrics from Airflow
   - Dashboard showing: DAG success rate, average duration vs baseline, last successful run
   - Color-coded: Green (healthy), Yellow (slow), Red (failed/stale)

2. **Added proactive alerting:**
   - Alert if ANY DAG's last success is > 2x its schedule interval
   - Alert if duration exceeds 150% of rolling 7-day average
   - Alert if downstream BigQuery table hasn't been updated in expected timeframe

3. **Created a weekly "Data Platform Health Report"** (automated email to team + stakeholders):
   - SLA compliance rate (target: 99%)
   - Top 5 flakiest DAGs
   - Data freshness per domain

4. **Presented to team lead** with data: "We had 23 silent failures last month. With this system, 21 would have been caught within 15 minutes instead of 24-48 hours."

**Result:**
- Team adopted it within 1 week. Manager made it a standard practice.
- Silent failure detection: 24-48 hours → 15 minutes average
- SLA compliance improved from 91% to 98.5% in first month
- Won "Engineering Excellence" recognition that quarter
- Stakeholder satisfaction (measured via quarterly survey) improved 30%

---

## Story 7: Learning from Failure

### "Tell me about a time you made a mistake."

**Situation:**
Early in my role, I was tasked with backfilling 6 months of historical data for a new reporting table. The table was partitioned by date and contained financial transactions.

**Task:**
Load 6 months of data (approximately 500M records) into production BigQuery.

**Action (what went wrong):**
1. I wrote the backfill script and tested it on 1 day of data — worked perfectly.
2. I ran it for all 180 days in a single batch — also seemed to work (job completed in 2 hours).
3. **The mistake:** I didn't realize my script had a bug where it loaded each day's data into the WRONG partition (all data went to the run date's partition, not the original transaction date's partition). The table "looked" correct (right row count) but the partitioning was wrong.
4. An analyst noticed 2 days later when a date-filtered query returned 0 rows for last month.

**What I did to fix:**
1. **Immediately:** Acknowledged the mistake to the team, created an incident ticket.
2. **Fixed:** Deleted the bad data (single partition), re-ran with corrected script (explicit partition date from source).
3. **Validated:** Wrote reconciliation queries verifying each partition's date range matched expected.
4. **Prevented recurrence:**
   - Added a **post-load validation** to all my scripts: check that MAX(date) in each partition matches the partition date
   - Added a **CI test** for backfill scripts: run on 3 test dates, verify partition alignment
   - Wrote a team wiki doc: "Backfill Checklist" (since adopted by 4 other engineers)

**Result:**
- Total downtime: 2 days of incorrect data (fixed within 4 hours of discovery)
- No financial impact (reports were for a feature in soft-launch, not yet customer-facing)
- The checklist and validation pattern prevented at least 2 similar issues from other team members in the following months
- Learned: always validate partition alignment, not just row counts

---

## Story 8: Mentoring / Knowledge Sharing

### "How do you help others on your team?"

**Situation:**
A junior engineer joined our team and was assigned to build a CDC pipeline from PostgreSQL to BigQuery. They were struggling with Datastream configuration and had been stuck for 3 days, hesitant to ask for help.

**Task:**
Help them become productive without doing the work for them (building their confidence and independence).

**Action:**
1. **Noticed the blocker** in standup (their update was vague: "still working on Datastream setup"). I offered to pair for 30 minutes.
2. **Paired without taking over:** Asked them to share their screen and explain their approach. The issue was a VPC networking config (Datastream couldn't reach the PostgreSQL private IP).
3. **Guided discovery:** Instead of saying "add a VPC peering," I asked: "What network is Datastream running in? What network is your PostgreSQL in? Can you check if they can see each other?" They found the gap themselves.
4. **Created a "CDC Setup Playbook"** (1 page) documenting the common pitfalls we'd both encountered. Shared with the whole team.
5. **Set up regular 1:1s** (30 min/week) for their first month — covered architecture patterns, debugging approaches, and BigQuery optimization.

**Result:**
- They delivered the CDC pipeline on time (2 days after unblock, within sprint commitment)
- They independently built 3 more CDC pipelines without help in the following quarter
- The playbook reduced setup time for CDC from 5 days average to 2 days for the whole team
- They later gave a tech talk on CDC patterns to the broader org

---

## Tips for Behavioral Interviews

1. **Prepare 6-8 stories** — they can be remixed for different questions
2. **Be specific** — "15M records" is better than "lots of data"
3. **Quantify results** — "61% reduction" not "much faster"
4. **Show technical depth** — interviewers want to see you understand the WHY
5. **Own mistakes** — saying "I messed up and here's what I learned" is powerful
6. **Show growth** — each story should demonstrate you leveled up
7. **Time yourself** — 2-3 minutes per story, not 10 minutes

### Common Questions Mapped to Stories Above
| Question | Use Story # |
|----------|-------------|
| "Tell me about a failure" | 1 or 7 |
| "Optimization / performance" | 2 |
| "Data quality" | 3 |
| "Conflict / difficult stakeholder" | 4 |
| "Technical decision" | 5 |
| "Initiative / leadership" | 6 |
| "Mentoring" | 8 |
| "Working under pressure" | 1 |
| "Trade-offs" | 5 |
| "Process improvement" | 6 |
