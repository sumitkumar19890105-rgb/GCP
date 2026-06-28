# Advanced SQL Practice Problems (20 Problems)

## Difficulty Progression
- Problems 1-5: Warm-up (Window functions, CTEs)
- Problems 6-10: Medium (Sessionization, running totals with resets)
- Problems 11-15: Hard (Gaps-and-islands, recursive CTEs)
- Problems 16-20: Expert (Complex real-world finance scenarios)

---

## Problem 1: Second Highest Transaction Per Customer
```sql
-- Given: transactions(txn_id, customer_id, amount, txn_date)
-- Find: Second highest transaction amount per customer

-- SOLUTION:
WITH ranked AS (
    SELECT
        customer_id,
        amount,
        txn_date,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rnk
    FROM transactions
)
SELECT customer_id, amount, txn_date
FROM ranked
WHERE rnk = 2;
```

---

## Problem 2: Year-over-Year Growth
```sql
-- Given: monthly_revenue(month DATE, region STRING, revenue NUMERIC)
-- Find: YoY growth percentage per region

-- SOLUTION:
WITH current_and_prev AS (
    SELECT
        month,
        region,
        revenue,
        LAG(revenue, 12) OVER (PARTITION BY region ORDER BY month) as prev_year_revenue
    FROM monthly_revenue
)
SELECT
    month,
    region,
    revenue,
    prev_year_revenue,
    ROUND((revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0) * 100, 2) as yoy_growth_pct
FROM current_and_prev
WHERE prev_year_revenue IS NOT NULL
ORDER BY region, month;
```

---

## Problem 3: Consecutive Days Active
```sql
-- Given: user_logins(user_id, login_date)
-- Find: Maximum consecutive days each user was active

-- SOLUTION:
WITH login_groups AS (
    SELECT
        user_id,
        login_date,
        login_date - INTERVAL (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)) DAY as grp
    FROM (SELECT DISTINCT user_id, login_date FROM user_logins)
)
SELECT
    user_id,
    MIN(login_date) as streak_start,
    MAX(login_date) as streak_end,
    DATE_DIFF(MAX(login_date), MIN(login_date), DAY) + 1 as consecutive_days
FROM login_groups
GROUP BY user_id, grp
ORDER BY consecutive_days DESC;

-- To get MAX streak per user:
WITH login_groups AS (
    SELECT
        user_id,
        login_date,
        login_date - INTERVAL (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date)) DAY as grp
    FROM (SELECT DISTINCT user_id, login_date FROM user_logins)
),
streaks AS (
    SELECT
        user_id,
        DATE_DIFF(MAX(login_date), MIN(login_date), DAY) + 1 as streak_length
    FROM login_groups
    GROUP BY user_id, grp
)
SELECT user_id, MAX(streak_length) as max_consecutive_days
FROM streaks
GROUP BY user_id;
```

---

## Problem 4: Running Total with Reset on Condition
```sql
-- Given: account_transactions(account_id, txn_date, amount, txn_type)
-- Find: Running balance per account, resetting to 0 when balance goes negative

-- SOLUTION (BigQuery):
WITH ordered AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_date) as rn
    FROM account_transactions
),
recursive_balance AS (
    -- Base case
    SELECT account_id, txn_date, amount, txn_type, rn,
        GREATEST(amount, 0) as running_balance
    FROM ordered WHERE rn = 1
    
    UNION ALL
    
    -- Recursive case
    SELECT o.account_id, o.txn_date, o.amount, o.txn_type, o.rn,
        GREATEST(r.running_balance + o.amount, 0) as running_balance
    FROM ordered o
    JOIN recursive_balance r 
        ON o.account_id = r.account_id AND o.rn = r.rn + 1
)
SELECT * FROM recursive_balance
ORDER BY account_id, txn_date;

-- Alternative (non-recursive, using arrays in BigQuery):
SELECT
    account_id,
    txn_date,
    amount,
    (SELECT SUM(x) FROM UNNEST(
        ARRAY(SELECT amount FROM account_transactions t2 
              WHERE t2.account_id = t1.account_id AND t2.txn_date <= t1.txn_date)
    ) x) as running_balance
FROM account_transactions t1;
```

---

## Problem 5: Percentile Rank and Outlier Detection
```sql
-- Given: loan_applications(loan_id, amount, credit_score, region, app_date)
-- Find: Flag loans where amount is above 95th percentile for their region

-- SOLUTION:
WITH percentiles AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount) as pct_rank,
        PERCENTILE_CONT(amount, 0.95) OVER (PARTITION BY region) as p95_amount
    FROM loan_applications
)
SELECT
    loan_id,
    amount,
    region,
    p95_amount,
    CASE WHEN amount > p95_amount THEN 'OUTLIER' ELSE 'NORMAL' END as flag
FROM percentiles
WHERE pct_rank > 0.95
ORDER BY amount DESC;
```

---

## Problem 6: Sessionization (Gap-Based)
```sql
-- Given: clickstream(user_id, event_time TIMESTAMP, page STRING)
-- Find: Group events into sessions. New session starts if gap > 30 minutes.

-- SOLUTION:
WITH with_gap AS (
    SELECT
        user_id,
        event_time,
        page,
        TIMESTAMP_DIFF(
            event_time,
            LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time),
            MINUTE
        ) as minutes_since_last
    FROM clickstream
),
with_session_flag AS (
    SELECT
        *,
        CASE WHEN minutes_since_last > 30 OR minutes_since_last IS NULL THEN 1 ELSE 0 END as new_session
    FROM with_gap
),
with_session_id AS (
    SELECT
        *,
        SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) as session_id
    FROM with_session_flag
)
SELECT
    user_id,
    session_id,
    MIN(event_time) as session_start,
    MAX(event_time) as session_end,
    TIMESTAMP_DIFF(MAX(event_time), MIN(event_time), SECOND) as session_duration_sec,
    COUNT(*) as events_in_session,
    ARRAY_AGG(page ORDER BY event_time) as page_path
FROM with_session_id
GROUP BY user_id, session_id
ORDER BY user_id, session_start;
```

---

## Problem 7: Funnel Analysis
```sql
-- Given: events(user_id, event_time, event_type)
-- event_types: 'page_view', 'add_to_cart', 'checkout_start', 'payment', 'confirmation'
-- Find: Conversion funnel with drop-off at each step

-- SOLUTION:
WITH funnel_steps AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as step_1_view,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as step_2_cart,
        MAX(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) as step_3_checkout,
        MAX(CASE WHEN event_type = 'payment' THEN 1 ELSE 0 END) as step_4_payment,
        MAX(CASE WHEN event_type = 'confirmation' THEN 1 ELSE 0 END) as step_5_confirm
    FROM events
    WHERE DATE(event_time) = CURRENT_DATE() - 1
    GROUP BY user_id
)
SELECT
    COUNT(*) as total_users,
    SUM(step_1_view) as viewed,
    SUM(step_2_cart) as added_to_cart,
    SUM(step_3_checkout) as started_checkout,
    SUM(step_4_payment) as paid,
    SUM(step_5_confirm) as confirmed,
    -- Conversion rates
    ROUND(SUM(step_2_cart) / NULLIF(SUM(step_1_view), 0) * 100, 1) as view_to_cart_pct,
    ROUND(SUM(step_3_checkout) / NULLIF(SUM(step_2_cart), 0) * 100, 1) as cart_to_checkout_pct,
    ROUND(SUM(step_5_confirm) / NULLIF(SUM(step_1_view), 0) * 100, 1) as overall_conversion_pct
FROM funnel_steps;

-- With ordered funnel (user must do steps IN ORDER):
WITH ordered_events AS (
    SELECT
        user_id,
        event_type,
        event_time,
        LEAD(event_type) OVER (PARTITION BY user_id ORDER BY event_time) as next_event
    FROM events
)
-- Shows how many users proceed from each step to the next in sequence
SELECT
    event_type as current_step,
    next_event as next_step,
    COUNT(DISTINCT user_id) as users
FROM ordered_events
WHERE event_type IN ('page_view', 'add_to_cart', 'checkout_start', 'payment')
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

---

## Problem 8: Gaps and Islands — Finding Missing Dates
```sql
-- Given: daily_loads(source STRING, load_date DATE)
-- Find: Date ranges where data is MISSING for each source (gaps)

-- SOLUTION:
WITH date_range AS (
    SELECT date
    FROM UNNEST(GENERATE_DATE_ARRAY('2026-01-01', CURRENT_DATE())) as date
),
all_sources AS (
    SELECT DISTINCT source FROM daily_loads
),
expected AS (
    SELECT source, date as expected_date
    FROM all_sources CROSS JOIN date_range
),
with_actual AS (
    SELECT
        e.source,
        e.expected_date,
        CASE WHEN d.load_date IS NULL THEN 1 ELSE 0 END as is_missing
    FROM expected e
    LEFT JOIN daily_loads d ON e.source = d.source AND e.expected_date = d.load_date
),
islands AS (
    SELECT
        source,
        expected_date,
        is_missing,
        expected_date - INTERVAL (ROW_NUMBER() OVER (PARTITION BY source, is_missing ORDER BY expected_date)) DAY as grp
    FROM with_actual
    WHERE is_missing = 1
)
SELECT
    source,
    MIN(expected_date) as gap_start,
    MAX(expected_date) as gap_end,
    DATE_DIFF(MAX(expected_date), MIN(expected_date), DAY) + 1 as days_missing
FROM islands
GROUP BY source, grp
ORDER BY source, gap_start;
```

---

## Problem 9: Slowly Changing Dimension Type 2 — Build History
```sql
-- Given: customer_snapshots(customer_id, name, segment, snapshot_date)
-- Snapshots are taken daily. Build SCD Type 2 from snapshots.
-- Find: effective_from, effective_to, is_current for each version

-- SOLUTION:
WITH changes_detected AS (
    SELECT
        customer_id,
        name,
        segment,
        snapshot_date,
        LAG(name) OVER (PARTITION BY customer_id ORDER BY snapshot_date) as prev_name,
        LAG(segment) OVER (PARTITION BY customer_id ORDER BY snapshot_date) as prev_segment
    FROM customer_snapshots
),
change_points AS (
    SELECT *,
        CASE WHEN (name != prev_name OR segment != prev_segment OR prev_name IS NULL)
             THEN 1 ELSE 0 END as is_change
    FROM changes_detected
),
versioned AS (
    SELECT *,
        SUM(is_change) OVER (PARTITION BY customer_id ORDER BY snapshot_date) as version_num
    FROM change_points
)
SELECT
    customer_id,
    name,
    segment,
    MIN(snapshot_date) as effective_from,
    CASE 
        WHEN LEAD(MIN(snapshot_date)) OVER (PARTITION BY customer_id ORDER BY version_num) IS NULL 
        THEN DATE '9999-12-31'
        ELSE DATE_SUB(LEAD(MIN(snapshot_date)) OVER (PARTITION BY customer_id ORDER BY version_num), INTERVAL 1 DAY)
    END as effective_to,
    CASE 
        WHEN LEAD(MIN(snapshot_date)) OVER (PARTITION BY customer_id ORDER BY version_num) IS NULL 
        THEN TRUE ELSE FALSE
    END as is_current
FROM versioned
GROUP BY customer_id, name, segment, version_num
ORDER BY customer_id, effective_from;
```

---

## Problem 10: Median and Moving Averages
```sql
-- Given: daily_metrics(metric_date, region, revenue)
-- Find: 7-day moving average and median revenue per region

-- SOLUTION:
SELECT
    metric_date,
    region,
    revenue,
    -- 7-day moving average
    AVG(revenue) OVER (
        PARTITION BY region 
        ORDER BY metric_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d,
    -- Median (50th percentile) over last 30 days
    PERCENTILE_CONT(revenue, 0.5) OVER (
        PARTITION BY region 
        ORDER BY metric_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as rolling_median_30d,
    -- Deviation from moving average (anomaly detection)
    revenue - AVG(revenue) OVER (
        PARTITION BY region 
        ORDER BY metric_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as deviation_from_avg,
    -- Z-score
    SAFE_DIVIDE(
        revenue - AVG(revenue) OVER (PARTITION BY region ORDER BY metric_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),
        STDDEV(revenue) OVER (PARTITION BY region ORDER BY metric_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    ) as z_score
FROM daily_metrics
ORDER BY region, metric_date;
```

---

## Problem 11: Matching/Reconciliation Between Systems
```sql
-- Given: 
--   source_ledger(txn_id, amount, post_date, account)
--   target_warehouse(txn_id, amount, load_date, account)
-- Find: Mismatches between source and target (reconciliation report)

-- SOLUTION:
WITH reconciled AS (
    SELECT
        COALESCE(s.txn_id, t.txn_id) as txn_id,
        s.amount as source_amount,
        t.amount as target_amount,
        s.post_date as source_date,
        t.load_date as target_date,
        s.account as source_account,
        t.account as target_account,
        CASE
            WHEN s.txn_id IS NULL THEN 'MISSING_IN_SOURCE'
            WHEN t.txn_id IS NULL THEN 'MISSING_IN_TARGET'
            WHEN s.amount != t.amount THEN 'AMOUNT_MISMATCH'
            WHEN s.account != t.account THEN 'ACCOUNT_MISMATCH'
            ELSE 'MATCHED'
        END as reconciliation_status
    FROM source_ledger s
    FULL OUTER JOIN target_warehouse t ON s.txn_id = t.txn_id
    WHERE s.post_date = '2026-06-21' OR t.load_date = '2026-06-21'
)
SELECT
    reconciliation_status,
    COUNT(*) as record_count,
    SUM(ABS(COALESCE(source_amount, 0) - COALESCE(target_amount, 0))) as total_variance
FROM reconciled
GROUP BY reconciliation_status

UNION ALL

SELECT
    'SUMMARY' as reconciliation_status,
    COUNT(*) as record_count,
    SUM(CASE WHEN reconciliation_status != 'MATCHED' THEN 1 ELSE 0 END) as total_variance
FROM reconciled;
```

---

## Problem 12: Hierarchical Query — Org Chart / Category Tree
```sql
-- Given: categories(category_id, name, parent_id)
-- Find: Full path from root to each leaf category

-- SOLUTION (Recursive CTE):
WITH RECURSIVE category_tree AS (
    -- Base: root categories (no parent)
    SELECT
        category_id,
        name,
        parent_id,
        CAST(name AS STRING) as full_path,
        0 as depth
    FROM categories
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Recursive: children
    SELECT
        c.category_id,
        c.name,
        c.parent_id,
        CONCAT(ct.full_path, ' > ', c.name) as full_path,
        ct.depth + 1
    FROM categories c
    JOIN category_tree ct ON c.parent_id = ct.category_id
)
SELECT * FROM category_tree
ORDER BY full_path;

-- Find all descendants of a specific category:
WITH RECURSIVE descendants AS (
    SELECT category_id, name, parent_id
    FROM categories WHERE category_id = 'ROOT_001'
    
    UNION ALL
    
    SELECT c.category_id, c.name, c.parent_id
    FROM categories c
    JOIN descendants d ON c.parent_id = d.category_id
)
SELECT * FROM descendants;
```

---

## Problem 13: Time-Weighted Average
```sql
-- Given: interest_rates(account_id, rate FLOAT64, effective_date DATE)
-- Find: Time-weighted average rate for each account over a period

-- SOLUTION:
WITH rate_periods AS (
    SELECT
        account_id,
        rate,
        effective_date as period_start,
        COALESCE(
            LEAD(effective_date) OVER (PARTITION BY account_id ORDER BY effective_date) - INTERVAL 1 DAY,
            DATE '2026-06-30'
        ) as period_end
    FROM interest_rates
),
weighted AS (
    SELECT
        account_id,
        rate,
        period_start,
        period_end,
        DATE_DIFF(
            LEAST(period_end, DATE '2026-06-30'),
            GREATEST(period_start, DATE '2026-01-01'),
            DAY
        ) + 1 as days_active
    FROM rate_periods
    WHERE period_end >= DATE '2026-01-01'
      AND period_start <= DATE '2026-06-30'
)
SELECT
    account_id,
    SUM(rate * days_active) / SUM(days_active) as time_weighted_avg_rate,
    SUM(days_active) as total_days
FROM weighted
GROUP BY account_id;
```

---

## Problem 14: Event Sequence Pattern Matching
```sql
-- Given: user_events(user_id, event_type, event_time)
-- Find: Users who did: login → search → purchase within 1 hour (in order)

-- SOLUTION:
WITH event_sequence AS (
    SELECT
        user_id,
        event_type,
        event_time,
        LEAD(event_type, 1) OVER (PARTITION BY user_id ORDER BY event_time) as next_event,
        LEAD(event_type, 2) OVER (PARTITION BY user_id ORDER BY event_time) as event_after_next,
        LEAD(event_time, 2) OVER (PARTITION BY user_id ORDER BY event_time) as time_of_third
    FROM user_events
    WHERE event_type IN ('login', 'search', 'purchase')
)
SELECT DISTINCT
    user_id,
    event_time as login_time,
    time_of_third as purchase_time,
    TIMESTAMP_DIFF(time_of_third, event_time, MINUTE) as minutes_to_convert
FROM event_sequence
WHERE event_type = 'login'
  AND next_event = 'search'
  AND event_after_next = 'purchase'
  AND TIMESTAMP_DIFF(time_of_third, event_time, MINUTE) <= 60
ORDER BY minutes_to_convert;
```

---

## Problem 15: Cumulative Distinct Count
```sql
-- Given: orders(order_id, customer_id, order_date)
-- Find: Cumulative distinct customers over time (how many unique customers by each date)

-- SOLUTION:
WITH first_order AS (
    SELECT
        customer_id,
        MIN(order_date) as first_order_date
    FROM orders
    GROUP BY customer_id
),
daily_new AS (
    SELECT
        first_order_date as date,
        COUNT(*) as new_customers
    FROM first_order
    GROUP BY first_order_date
)
SELECT
    date,
    new_customers,
    SUM(new_customers) OVER (ORDER BY date) as cumulative_unique_customers
FROM daily_new
ORDER BY date;
```

---

## Problem 16: Loan Delinquency Bucketing (Finance-Specific)
```sql
-- Given: loan_payments(loan_id, due_date, paid_date, amount_due, amount_paid)
-- Find: Classify each loan into delinquency buckets as of today

-- SOLUTION:
WITH latest_status AS (
    SELECT
        loan_id,
        MAX(due_date) as latest_due_date,
        MAX(CASE WHEN paid_date IS NOT NULL THEN due_date END) as last_paid_due_date,
        SUM(amount_due) as total_due,
        SUM(COALESCE(amount_paid, 0)) as total_paid
    FROM loan_payments
    WHERE due_date <= CURRENT_DATE()
    GROUP BY loan_id
),
with_dpd AS (
    SELECT
        loan_id,
        total_due,
        total_paid,
        total_due - total_paid as outstanding,
        DATE_DIFF(CURRENT_DATE(), last_paid_due_date, DAY) as days_past_due
    FROM latest_status
    WHERE total_paid < total_due  -- Only delinquent loans
)
SELECT
    loan_id,
    outstanding,
    days_past_due,
    CASE
        WHEN days_past_due <= 0 THEN 'CURRENT'
        WHEN days_past_due BETWEEN 1 AND 30 THEN '1-30 DPD'
        WHEN days_past_due BETWEEN 31 AND 60 THEN '31-60 DPD'
        WHEN days_past_due BETWEEN 61 AND 90 THEN '61-90 DPD'
        WHEN days_past_due BETWEEN 91 AND 180 THEN '91-180 DPD'
        ELSE '180+ DPD (Write-off candidate)'
    END as delinquency_bucket
FROM with_dpd
ORDER BY days_past_due DESC;

-- Summary view:
SELECT
    delinquency_bucket,
    COUNT(*) as loan_count,
    SUM(outstanding) as total_outstanding,
    AVG(days_past_due) as avg_dpd
FROM (/* above query */) 
GROUP BY delinquency_bucket
ORDER BY MIN(days_past_due);
```

---

## Problem 17: Detecting Duplicate Payments
```sql
-- Given: payments(payment_id, customer_id, amount, payment_time, merchant_id)
-- Find: Potential duplicate payments (same customer, same amount, same merchant, within 5 minutes)

-- SOLUTION:
WITH potential_dupes AS (
    SELECT
        p1.payment_id as original_id,
        p2.payment_id as duplicate_id,
        p1.customer_id,
        p1.amount,
        p1.merchant_id,
        p1.payment_time as original_time,
        p2.payment_time as duplicate_time,
        TIMESTAMP_DIFF(p2.payment_time, p1.payment_time, SECOND) as seconds_apart
    FROM payments p1
    JOIN payments p2
        ON p1.customer_id = p2.customer_id
        AND p1.amount = p2.amount
        AND p1.merchant_id = p2.merchant_id
        AND p2.payment_time > p1.payment_time
        AND TIMESTAMP_DIFF(p2.payment_time, p1.payment_time, MINUTE) <= 5
        AND p1.payment_id != p2.payment_id
)
SELECT *,
    CASE 
        WHEN seconds_apart < 10 THEN 'HIGH_CONFIDENCE_DUPE'
        WHEN seconds_apart < 60 THEN 'LIKELY_DUPE'
        ELSE 'POSSIBLE_DUPE'
    END as confidence
FROM potential_dupes
ORDER BY customer_id, original_time;
```

---

## Problem 18: Pivot / Unpivot (Cross-Tab Reports)
```sql
-- Given: monthly_metrics(product, month, metric_name, value)
-- Find: Pivot metrics as columns for each product-month

-- PIVOT:
SELECT *
FROM monthly_metrics
PIVOT (
    SUM(value)
    FOR metric_name IN ('revenue', 'cost', 'users', 'churn_rate')
) AS pivoted
ORDER BY product, month;

-- UNPIVOT (reverse):
SELECT product, month, metric_name, metric_value
FROM product_summary
UNPIVOT (
    metric_value FOR metric_name IN (revenue, cost, users, churn_rate)
);

-- Manual pivot with CASE (more portable):
SELECT
    product,
    month,
    SUM(CASE WHEN metric_name = 'revenue' THEN value END) as revenue,
    SUM(CASE WHEN metric_name = 'cost' THEN value END) as cost,
    SUM(CASE WHEN metric_name = 'users' THEN value END) as users,
    SUM(CASE WHEN metric_name = 'churn_rate' THEN value END) as churn_rate,
    SUM(CASE WHEN metric_name = 'revenue' THEN value END) - 
        SUM(CASE WHEN metric_name = 'cost' THEN value END) as profit
FROM monthly_metrics
GROUP BY product, month;
```

---

## Problem 19: Data Lineage Query — Impact Analysis
```sql
-- Given: table_dependencies(source_table, target_table, transform_type)
-- Find: All downstream tables affected if 'raw.customers' changes (cascade impact)

-- SOLUTION (Recursive):
WITH RECURSIVE downstream AS (
    -- Direct dependents
    SELECT
        source_table,
        target_table,
        transform_type,
        1 as depth,
        ARRAY[source_table, target_table] as path
    FROM table_dependencies
    WHERE source_table = 'raw.customers'
    
    UNION ALL
    
    -- Indirect dependents
    SELECT
        d.target_table as source_table,
        td.target_table,
        td.transform_type,
        d.depth + 1,
        ARRAY_CONCAT(d.path, [td.target_table])
    FROM downstream d
    JOIN table_dependencies td ON d.target_table = td.source_table
    WHERE d.depth < 10  -- Prevent infinite loops
      AND td.target_table NOT IN UNNEST(d.path)  -- Prevent cycles
)
SELECT
    target_table as affected_table,
    MIN(depth) as hops_from_source,
    ANY_VALUE(path) as dependency_path,
    STRING_AGG(DISTINCT transform_type) as transform_types
FROM downstream
GROUP BY target_table
ORDER BY hops_from_source, affected_table;
```

---

## Problem 20: Complete Reconciliation Framework
```sql
-- COMPREHENSIVE RECONCILIATION: Source system → Landing → Raw → Curated → Serving
-- Run daily to validate entire pipeline

-- SOLUTION:
WITH layer_counts AS (
    SELECT 'source' as layer, COUNT(*) as row_count, SUM(amount) as total_amount
    FROM `project.external.source_extract` WHERE date = '{{ ds }}'
    UNION ALL
    SELECT 'landing', COUNT(*), SUM(CAST(amount AS NUMERIC))
    FROM `project.landing.transactions` WHERE _PARTITIONDATE = '{{ ds }}'
    UNION ALL
    SELECT 'raw', COUNT(*), SUM(amount)
    FROM `project.raw.transactions` WHERE load_date = '{{ ds }}'
    UNION ALL
    SELECT 'curated', COUNT(*), SUM(amount)
    FROM `project.curated.transactions` WHERE transaction_date = '{{ ds }}'
    UNION ALL
    SELECT 'serving', COUNT(*), SUM(amount)
    FROM `project.serving.transactions_daily` WHERE summary_date = '{{ ds }}'
),
validation AS (
    SELECT
        layer,
        row_count,
        total_amount,
        LAG(row_count) OVER (ORDER BY 
            CASE layer WHEN 'source' THEN 1 WHEN 'landing' THEN 2 
                       WHEN 'raw' THEN 3 WHEN 'curated' THEN 4 WHEN 'serving' THEN 5 END
        ) as prev_layer_count,
        LAG(total_amount) OVER (ORDER BY 
            CASE layer WHEN 'source' THEN 1 WHEN 'landing' THEN 2 
                       WHEN 'raw' THEN 3 WHEN 'curated' THEN 4 WHEN 'serving' THEN 5 END
        ) as prev_layer_amount
    FROM layer_counts
)
SELECT
    layer,
    row_count,
    total_amount,
    row_count - COALESCE(prev_layer_count, row_count) as row_diff,
    total_amount - COALESCE(prev_layer_amount, total_amount) as amount_diff,
    CASE
        WHEN row_count = 0 THEN 'CRITICAL: No data'
        WHEN ABS(row_count - COALESCE(prev_layer_count, row_count)) > 0 THEN 'WARNING: Row count mismatch'
        WHEN ABS(total_amount - COALESCE(prev_layer_amount, total_amount)) > 0.01 THEN 'WARNING: Amount mismatch'
        ELSE 'PASS'
    END as status
FROM validation
ORDER BY CASE layer WHEN 'source' THEN 1 WHEN 'landing' THEN 2 
                    WHEN 'raw' THEN 3 WHEN 'curated' THEN 4 WHEN 'serving' THEN 5 END;
```

---

## Tips for SQL Interviews

1. **Always clarify**: "Is there an index on X?", "What's the data volume?", "Are there duplicates?"
2. **Start simple, then optimize**: Write the correct answer first, then discuss optimization
3. **Talk through your approach**: "First I'll identify the groups, then rank within each..."
4. **Know your window functions cold**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM OVER, NTILE
5. **Practice the pattern**: Most hard problems are combinations of CTEs + Window Functions + CASE
6. **BigQuery-specific**: Know UNNEST, ARRAY_AGG, STRUCT, GENERATE_DATE_ARRAY, APPROX functions
