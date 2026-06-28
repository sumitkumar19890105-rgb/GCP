# Spark / Dataproc Deep Dive — Advanced

## 1. Spark Architecture Internals

### Problem: How does Spark actually execute your code under the hood?

**Definition:** Spark is a distributed computing engine that splits your data into **partitions** and processes them in parallel across a cluster. Understanding the execution model (Driver → Jobs → Stages → Tasks) is critical for debugging slow pipelines and optimizing resource usage.

**Why it matters:**
- Without understanding stages, you can't identify which part of your pipeline is slow
- Without understanding partitions, you'll either underutilize the cluster (too few) or waste overhead (too many)
- Without understanding shuffle boundaries, you can't minimize the most expensive operation in Spark

**Key Insight for Interviews:**
> "When I look at a slow Spark job, the first thing I check is the Spark UI's Stages tab. A job is broken into stages at shuffle boundaries. Within each stage, narrow transforms (map, filter) are pipelined together. I identify the bottleneck stage, then check if it's shuffle-heavy, skewed, or spilling to disk."

### Execution Model
```
Driver (SparkContext)
  │
  ├── Job (triggered by action: collect, write, count)
  │     ├── Stage 1 (narrow transforms: map, filter)
  │     │     ├── Task 1 (Partition 1)
  │     │     ├── Task 2 (Partition 2)
  │     │     └── Task N (Partition N)
  │     │
  │     └── Stage 2 (after shuffle: groupBy, join)
  │           ├── Task 1
  │           └── Task M
  │
  └── DAG Scheduler → Task Scheduler → Executors
```

### Key Concepts
| Concept | Definition | Impact |
|---------|-----------|--------|
| **Partition** | Logical chunk of data (default 200 for shuffle) | Too few = underutilization, too many = overhead |
| **Shuffle** | Data redistribution across nodes | Most expensive operation (disk I/O + network) |
| **Spill** | When partition exceeds memory, written to disk | Degrades performance significantly |
| **Skew** | Uneven data distribution across partitions | One task takes 100x longer than others |
| **Catalyst Optimizer** | SQL query plan optimizer | Predicate pushdown, constant folding, join reorder |
| **Tungsten** | Memory/CPU optimization engine | Off-heap memory, code generation |

---

## 2. Shuffle — The #1 Performance Killer

### Problem: Why is my Spark job slow even with a large cluster?

**Definition:** A **shuffle** is when Spark must redistribute data across the cluster — all records with the same key must end up on the same executor. This involves serializing data, writing to disk, transferring over the network, and deserializing. It's the most expensive operation in Spark.

**Real-world analogy:** Imagine 10 librarians sorting books. Each has a random pile. To group by genre, every librarian must send their sci-fi books to librarian #1, their romance books to librarian #2, etc. The "sending" step is the shuffle — slow and expensive.

**Why it matters:**
- A single unnecessary shuffle can turn a 5-minute job into a 2-hour job
- Shuffles write intermediate data to disk (even SSDs are 100x slower than RAM)
- Network transfer between nodes adds latency proportional to data size

**How to resolve:**
1. **Avoid shuffle entirely** — Use broadcast joins for small tables
2. **Reduce shuffle data** — Filter/project before joins (less data to move)
3. **Pre-partition once** — If you join on the same key multiple times, partition by that key upfront
4. **Tune partition count** — Too many = scheduling overhead; too few = OOM/skew
5. **Enable AQE** — Spark 3.x auto-optimizes partitions after shuffle

**Interview Perspective:**
> "Shuffle is the #1 thing I optimize. My approach: (1) Can I eliminate the shuffle? Broadcast joins for small tables. (2) Can I reduce it? Push filters/projections before the shuffle. (3) Can I do it smarter? Pre-partition by join key so subsequent joins are shuffle-free. I always check `explain()` to confirm my join strategy."

### What Causes Shuffle
```python
# These operations trigger shuffle:
df.groupBy("key").agg(...)        # GroupBy
df1.join(df2, "key")              # Join (sort-merge by default)
df.repartition(100)               # Explicit repartition
df.distinct()                     # Distinct
df.orderBy("col")                 # Global sort
df.rdd.reduceByKey(...)           # ReduceByKey
```

### Shuffle Optimization Strategies

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, spark_partition_id

spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# 1. REDUCE SHUFFLE PARTITIONS for small data
spark.conf.set("spark.sql.shuffle.partitions", "50")  # Default 200 is too many for < 10GB

# 2. PRE-PARTITION to avoid shuffle during join
df_partitioned = df.repartition(100, "join_key")  # Partition by join key ONCE
df_partitioned.write.partitionBy("join_key").parquet("gs://bucket/partitioned/")

# 3. Use COALESCE instead of REPARTITION to reduce partitions (no shuffle)
df_small = df.coalesce(10)  # Merges partitions locally, no network transfer

# 4. Monitor partition sizes
df.withColumn("partition_id", spark_partition_id()) \
    .groupBy("partition_id").count() \
    .orderBy("count", ascending=False) \
    .show(20)
```

---

## 3. Broadcast Joins — Eliminating Shuffle

### Problem: How do you join a huge table with a small lookup table without expensive shuffle?

**Definition:** A **broadcast join** (aka map-side join) sends the entire small table to every executor's memory. Each executor can then join its partition of the large table locally — no data movement needed for the large table.

**Why it matters:**
- A regular sort-merge join shuffles BOTH tables across the network
- For a 100GB fact table joining a 50MB dimension table: sort-merge moves ~100GB over network; broadcast moves only 50MB
- Performance improvement: typically **5-50x faster** for eligible joins

**How to resolve (when to use):**
| Scenario | Use Broadcast? | Why |
|----------|---------------|-----|
| 100GB ⨝ 50MB | ✅ Yes | Small table fits in executor memory |
| 100GB ⨝ 10GB | ⚠️ Maybe | Only if executors have enough memory (check `spark.driver.memory`) |
| 100GB ⨝ 100GB | ❌ No | Too large to broadcast — use sort-merge or bucket join |
| Stream ⨝ lookup | ✅ Yes | Dimension/lookup tables are classic broadcast candidates |

**Interview Perspective:**
> "Whenever I see a join in Spark, my first question is: 'Is one side small enough to broadcast?' If yes, I force a broadcast join to eliminate shuffle entirely. I've seen 10-minute stages drop to 30 seconds just by adding `broadcast()`. I also check `explain(True)` to confirm Spark chose BroadcastHashJoin and not SortMergeJoin."

### When to Use
- One side of join is small (< 10MB default, configurable up to 8GB)
- Avoids shuffle entirely — small table is sent to all executors

```python
from pyspark.sql.functions import broadcast

# Explicit broadcast (override threshold)
result = large_transactions.join(
    broadcast(small_lookup_table),  # Force broadcast
    "category_id"
)

# Configure threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")  # 100MB

# Check if broadcast was used (in explain plan)
result.explain(True)
# Look for: BroadcastHashJoin (good) vs SortMergeJoin (shuffle)
```

### Broadcast Variables (for non-DataFrame lookups)
```python
# For Python dicts, lists used in UDFs
country_map = {"US": "United States", "UK": "United Kingdom", ...}
country_broadcast = spark.sparkContext.broadcast(country_map)

@udf(StringType())
def get_country_name(code):
    return country_broadcast.value.get(code, "Unknown")

df_with_country = df.withColumn("country_name", get_country_name(col("country_code")))
```

---

## 4. Data Skew — Detection and Handling

### Problem: One Spark task takes 2 hours while the other 199 finish in 5 minutes. Why?

**Definition:** **Data skew** occurs when data is unevenly distributed across partitions. If one key (e.g., `customer_id = "AMAZON"`) has 50 million records while the average customer has 100 records, the partition handling Amazon gets overwhelmed while other executors sit idle.

**Real-world analogy:** 10 cashiers at a grocery store. 9 have 5 customers each. 1 has 500 customers. The store can't close until that last cashier finishes — everyone waits.

**Why it matters:**
- Your job is only as fast as the **slowest task** (the skewed one)
- The skewed executor may OOM (one partition too large for memory)
- Autoscaling won't help — adding more executors doesn't shrink the hot partition

**How to detect:**
1. **Spark UI → Stages → Tasks tab**: Look for one task with much longer duration than others
2. **Key distribution check**: `df.groupBy("key").count().orderBy(desc("count"))` — if top key is 1000x average, you have skew
3. **Shuffle read/write size**: One task reading 10GB while others read 50MB = skew

**How to resolve:**

| Technique | When to Use | How It Works |
|-----------|------------|---------------|
| **Salt keys** | Join skew on known hot keys | Add random suffix to spread hot key across N partitions |
| **Isolate hot keys** | Few hot keys, rest normal | Broadcast-join hot keys separately, regular-join the rest |
| **Enable AQE skew join** | Spark 3.x, unknown skew | Spark auto-detects and splits skewed partitions at runtime |
| **Increase parallelism** | Mild skew | More shuffle partitions = smaller max partition |
| **Pre-aggregate** | Aggregation skew | Partial aggregation before shuffle (CombinePerKey pattern) |

**Interview Perspective:**
> "Data skew is the #1 reason Spark jobs have 'straggler tasks.' My approach: first detect in Spark UI (one task 100x slower). Then check key distribution. If it's join skew, I use the salting technique — add a random suffix to the hot key, explode the small table with all suffixes, join on the salted key. For Spark 3.x, I also enable AQE's `skewJoin.enabled` which handles it automatically at runtime."

### Detecting Skew
```python
# Method 1: Check partition sizes
from pyspark.sql.functions import spark_partition_id, count

df.groupBy(spark_partition_id().alias("partition")) \
    .agg(count("*").alias("rows")) \
    .orderBy("rows", ascending=False) \
    .show(10)

# Method 2: Check key distribution
df.groupBy("join_key") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(20)
# If top key has 100M rows and average is 10K → severe skew

# Method 3: Check stage duration in Spark UI
# Long tail in one task while others finish fast = skew
```

### Fixing Skew — Salt Key Technique
```python
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
import random

SALT_BUCKETS = 10

# Step 1: Salt the large table's key
large_df_salted = large_df.withColumn(
    "salt", (F.rand() * SALT_BUCKETS).cast(IntegerType())
).withColumn(
    "salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt"))
)

# Step 2: Explode the small table with all salt values
from pyspark.sql.functions import explode, array, lit

salt_array = array([lit(i) for i in range(SALT_BUCKETS)])
small_df_exploded = small_df.withColumn("salt", explode(salt_array)) \
    .withColumn("salted_key", F.concat(F.col("join_key"), F.lit("_"), F.col("salt")))

# Step 3: Join on salted key (evenly distributed)
result = large_df_salted.join(small_df_exploded, "salted_key") \
    .drop("salt", "salted_key")
```

### Fixing Skew — Isolate Hot Keys
```python
# Identify hot keys
hot_keys = df.groupBy("key").count() \
    .filter(col("count") > 1000000) \
    .select("key").rdd.flatMap(lambda x: x).collect()

# Split: process hot keys with broadcast, rest with regular join
hot_df = large_df.filter(col("key").isin(hot_keys))
normal_df = large_df.filter(~col("key").isin(hot_keys))

# Broadcast join for hot keys (small lookup table is tiny)
hot_result = hot_df.join(broadcast(small_df), "key")

# Regular join for normal keys
normal_result = normal_df.join(small_df, "key")

# Combine
result = hot_result.unionByName(normal_result)
```

---

## 5. Adaptive Query Execution (AQE) — Spark 3.x

### Problem: How can Spark auto-optimize at runtime when we don't know data sizes/distribution in advance?

**Definition:** **AQE (Adaptive Query Execution)** is Spark 3.x's runtime optimization engine. Unlike the static Catalyst optimizer (which plans before execution), AQE collects real statistics DURING execution and re-optimizes the remaining plan on-the-fly.

**Why it matters (before AQE):**
- You set `shuffle.partitions = 200` but your data only needs 15 → 185 empty tasks wasting scheduling time
- A filter reduces 100GB to 5MB, but Spark still uses sort-merge join instead of switching to broadcast
- A skewed key overwhelms one executor, but Spark doesn't know until it's too late

**What AQE solves automatically:**

| Problem | AQE Solution | Manual Equivalent |
|---------|-------------|-------------------|
| Too many shuffle partitions | Auto-coalesces small partitions | Manually tuning `shuffle.partitions` |
| Wrong join strategy | Switches to broadcast if data shrinks after filter | Adding `broadcast()` hint manually |
| Data skew in joins | Splits skewed partition into sub-partitions | Manual salt-key technique |

**Interview Perspective:**
> "AQE is my first recommendation for any Spark 3.x job. It handles three things automatically: (1) coalesces empty/small shuffle partitions, (2) switches join strategies mid-execution if one side is small, (3) detects and splits skewed partitions. Before AQE, we manually tuned partition counts and applied salting. Now Spark adapts at runtime based on actual data sizes. I still keep AQE enabled alongside manual optimizations as a safety net."

### What AQE Does Automatically
1. **Coalesces shuffle partitions**: Merges small partitions after shuffle
2. **Switches join strategy**: Converts sort-merge to broadcast if one side is small after filter
3. **Handles skew**: Splits skewed partitions into smaller sub-partitions

### Configuration
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Default: true in Spark 3.2+
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")  # Min 1MB per partition
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")  # 5x median = skewed
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

# AQE dynamically optimizes at runtime based on actual data statistics
# Before AQE: 200 shuffle partitions (many empty/tiny)
# After AQE: Automatically coalesced to actual needed count
```

---

## 6. Partitioning Strategy for Data Lake

### Problem: How do you organize files so queries read only relevant data (not full table scans)?

**Definition:** **Partitioning** in a data lake means physically organizing files into directory hierarchies (e.g., `year=2026/month=06/region=US/`). When you query with a filter on the partition column, Spark reads ONLY the matching directories — this is called **partition pruning**.

**Why it matters:**
- Without partitioning: Query for June 2026 US data scans ALL files (TB of data) → slow + expensive
- With partitioning: Same query reads only `year=2026/month=06/region=US/` → fast + cheap
- Over-partitioning: Too many tiny files (e.g., partition by second) → file listing overhead kills performance

**How to resolve — choosing partition columns:**

| Guideline | Why |
|-----------|-----|
| Partition by columns used in WHERE/filter | Enables partition pruning |
| Choose low-cardinality columns (date, region) | Manageable number of directories |
| Avoid high-cardinality columns (user_id) | Millions of folders = small file problem |
| Target 100MB-1GB files per partition | Sweet spot for Parquet I/O |

**Partitioning vs Bucketing vs Clustering:**

| Technique | Use Case | How It Works |
|-----------|----------|---------------|
| **Partitioning** | Filter pruning (WHERE date = ...) | Separate directories per value |
| **Bucketing** | Shuffle-free joins on same key | Hash-distribute into N fixed files |
| **Clustering (BQ)** | Sort-based pruning within partition | Physically sort data within partition |

**Interview Perspective:**
> "I partition data lake tables by the most common filter columns — typically date and a low-cardinality business dimension like region. This enables partition pruning where Spark skips irrelevant directories entirely. For join-heavy workloads, I additionally bucket tables by the join key with the same bucket count — this eliminates shuffle on subsequent joins. I target 100MB-1GB per file to avoid the small files problem."

### File-Based Partitioning
```python
# Write with partitioning (creates directory structure)
df.write \
    .partitionBy("year", "month", "region") \
    .mode("overwrite") \
    .parquet("gs://data-lake/curated/transactions/")

# Result on GCS:
# gs://data-lake/curated/transactions/year=2026/month=06/region=US/part-00000.parquet
# gs://data-lake/curated/transactions/year=2026/month=06/region=APAC/part-00001.parquet

# Read with partition pruning (only reads relevant folders)
df = spark.read.parquet("gs://data-lake/curated/transactions/") \
    .filter((col("year") == 2026) & (col("month") == 6))
# Only reads year=2026/month=06/ folders!
```

### Bucketing (Hash Partitioning for Joins)
```python
# Bucket tables by join key — eliminates shuffle on subsequent joins
df.write \
    .bucketBy(100, "customer_id") \
    .sortBy("customer_id") \
    .saveAsTable("transactions_bucketed")

# When two tables are bucketed by same key and same # buckets:
# JOIN happens locally (no shuffle!)
t1 = spark.table("transactions_bucketed")
t2 = spark.table("customers_bucketed")  # Also bucketed by customer_id, 100 buckets
result = t1.join(t2, "customer_id")  # No shuffle! Bucket-to-bucket merge
```

### Repartition vs Coalesce
```python
# REPARTITION: Full shuffle, can increase OR decrease partitions
df.repartition(200)           # Even distribution, full shuffle
df.repartition("key")         # Partition by key, full shuffle
df.repartition(100, "key")    # 100 partitions, by key

# COALESCE: No shuffle, can only DECREASE partitions
df.coalesce(10)               # Merge locally, no network I/O
# Use when writing fewer output files

# RULE OF THUMB:
# - Input → many transforms → write: coalesce before write to reduce small files
# - Before join on key: repartition by that key
# - After filter that reduces data significantly: coalesce
```

---

## 7. Memory Management and Tuning

### Problem: Spark jobs failing with OutOfMemoryError or YARN killing containers — how to fix?

**Definition:** Each Spark executor has a fixed pool of memory divided into regions: **Execution memory** (shuffles, sorts, joins), **Storage memory** (cached DataFrames), and **User memory** (Python objects, UDFs). When any region is exhausted, data spills to disk (slow) or the job OOMs.

**Why it matters:**
- **OOM on executor** = one partition too large for available memory → task fails
- **Container killed by YARN** = total memory (JVM + Python + overhead) exceeds allocation → hard crash
- **Excessive disk spill** = data doesn't fit in memory → 10-100x slower reads from disk

**How to resolve:**

| Symptom | Root Cause | Fix |
|---------|-----------|-----|
| `java.lang.OutOfMemoryError: Java heap space` | Partition too large or accumulation in driver | Increase `executor.memory` OR increase partition count |
| `Container killed by YARN` | Python UDFs consuming extra memory beyond JVM heap | Increase `executor.memoryOverhead` (default 10% is too low for PySpark) |
| Disk spill (visible in Spark UI) | Shuffle/sort data exceeds execution memory | Increase memory OR reduce partition size |
| Driver OOM | `collect()` or `toPandas()` bringing too much data to driver | Never collect large DataFrames — use `take(N)` or write to storage |

**Rule of thumb for sizing:**
- `executor.memory` = 4-8GB (larger = more GC pauses)
- `executor.cores` = 4-5 (more cores share the memory pool)
- `executor.memoryOverhead` = 20-30% of executor.memory for PySpark
- Total executors = (cluster cores / executor.cores) - 1 (leave 1 core for YARN)

**Interview Perspective:**
> "When I see OOM errors, I first check: is it the executor or the driver? Executor OOM means partitions are too large — I increase `shuffle.partitions` to create smaller chunks, or increase `executor.memory`. For PySpark with Python UDFs, I also increase `memoryOverhead` since Python objects live off-heap. I use Kryo serialization for 10x better efficiency, and I never call `collect()` on large DataFrames."

### Memory Layout per Executor
```
Executor Memory (e.g., 8GB)
├── Reserved (300MB)
├── User Memory (40%): Python objects, UDFs, accumulators
├── Execution Memory (30%): Shuffles, sorts, aggregations
└── Storage Memory (30%): Cached DataFrames, broadcast variables

# Unified Memory Management (Spark 2+):
# Execution and Storage share a pool — execution can borrow from storage
```

### Key Configurations
```python
# Executor configuration
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")  # Off-heap (Python, network buffers)
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.executor.instances", "10")

# Memory fractions
spark.conf.set("spark.memory.fraction", "0.6")        # Execution + Storage share
spark.conf.set("spark.memory.storageFraction", "0.5")  # Initial storage share

# Serialization (Kryo is 10x faster than Java serialization)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# Avoid OOM with these:
spark.conf.set("spark.sql.shuffle.partitions", "400")  # More partitions = smaller per partition
spark.conf.set("spark.sql.files.maxPartitionBytes", "128m")  # Max bytes per read partition
```

### Handling OOM Errors — All Scenarios

#### Scenario 1: `java.lang.OutOfMemoryError: Java heap space`

**Where:** Executor JVM
**Root Cause:** A single partition is too large to fit in executor memory (e.g., skewed data, too few partitions, or a large accumulation during aggregation).

```python
# FIXES:

# Fix 1: Increase executor memory
spark.conf.set("spark.executor.memory", "16g")  # Was 8g, double it

# Fix 2: Increase partitions (each partition = smaller data chunk)
spark.conf.set("spark.sql.shuffle.partitions", "800")  # Was 200

# Fix 3: Repartition before heavy operation
df = df.repartition(500, "key_column")  # Spread data more evenly

# Fix 4: Reduce broadcast threshold (prevent broadcasting too-large tables)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")  # Lower from 100m
```

---

#### Scenario 2: `Container killed by YARN for exceeding memory limits`

**Where:** OS/YARN level (entire container killed, not just JVM)
**Root Cause:** Total memory (JVM heap + off-heap + Python processes) exceeds the YARN container allocation. Common with PySpark UDFs because Python objects live outside the JVM.

```python
# FIXES:

# Fix 1: Increase memory overhead (off-heap allocation for Python, network buffers)
spark.conf.set("spark.executor.memoryOverhead", "4g")  # Default is only 10% of executor.memory

# Fix 2: For PySpark with pandas UDFs — increase Python worker memory
spark.conf.set("spark.executor.pyspark.memory", "2g")  # Dedicated Python memory

# Fix 3: Ensure total doesn't exceed node capacity
# Total per executor = executor.memory + memoryOverhead + pyspark.memory
# Example: 8g + 4g + 2g = 14g → node must have at least 16g available
```

---

#### Scenario 3: `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Where:** Executor JVM
**Root Cause:** JVM spending >98% of time doing garbage collection and recovering <2% of heap. Usually caused by too many small objects (e.g., strings in a large group-by, or very wide rows).

```python
# FIXES:

# Fix 1: Increase memory so GC has more room
spark.conf.set("spark.executor.memory", "16g")

# Fix 2: Use Kryo serialization (more compact objects, less GC pressure)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# Fix 3: Reduce data held in memory (fewer cached DataFrames)
df.unpersist()  # Explicitly release cached data when no longer needed

# Fix 4: Use G1GC garbage collector (better for large heaps)
spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:G1HeapRegionSize=16m")
```

---

#### Scenario 4: `OutOfMemoryError` on Driver

**Where:** Driver (master) node
**Root Cause:** Calling `collect()`, `toPandas()`, or `show(1000000)` which pulls all data from executors to the single driver node. Also caused by too many partitions in the DAG plan.

```python
# FIXES:

# Fix 1: NEVER collect large DataFrames — use take() or write to storage
# ❌ BAD:
all_data = df.collect()  # Pulls 100M rows to driver → OOM

# ✅ GOOD:
sample = df.take(100)  # Only pulls 100 rows
df.write.parquet("gs://bucket/output/")  # Write to storage, not to driver

# Fix 2: Increase driver memory
spark.conf.set("spark.driver.memory", "8g")  # Default 1g is too low for large plans

# Fix 3: Avoid .toPandas() on large DataFrames
# ❌ BAD:
pandas_df = spark_df.toPandas()  # 50M rows → driver OOM

# ✅ GOOD:
pandas_df = spark_df.limit(100000).toPandas()  # Safe sample size
# Or use Spark natively without converting to Pandas
```

---

#### Scenario 5: `OutOfMemoryError` during Broadcast Join

**Where:** Executor or Driver (depending on broadcast mechanism)
**Root Cause:** Broadcasting a table that's larger than expected (e.g., after join/filter the "small" table is actually 5GB). The entire broadcast table must fit in EACH executor's memory.

```python
# FIXES:

# Fix 1: Lower the auto-broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable auto-broadcast
# Or set a conservative limit:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50m")  # Only broadcast if < 50MB

# Fix 2: Remove explicit broadcast() hint and let Spark choose sort-merge
# ❌ Was:
result = big_df.join(broadcast(medium_df), "key")  # medium_df is 3GB → OOM
# ✅ Fix:
result = big_df.join(medium_df, "key")  # Let Spark use sort-merge join

# Fix 3: If you MUST broadcast, increase driver memory (broadcast collects to driver first)
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.driver.maxResultSize", "4g")
```

---

#### Scenario 6: `OutOfMemoryError` during Shuffle/Sort (Disk Spill Exhausted)

**Where:** Executor
**Root Cause:** During shuffle, data that doesn't fit in memory spills to local disk. If local disk is also full (or spill is disabled), OOM occurs. Common with large aggregations or sorts on high-cardinality keys.

```python
# FIXES:

# Fix 1: Ensure enough local disk for spill
# On Dataproc: use SSDs with sufficient space
# gcloud dataproc clusters create ... --worker-boot-disk-size=500GB --worker-boot-disk-type=pd-ssd

# Fix 2: Increase execution memory fraction
spark.conf.set("spark.memory.fraction", "0.8")  # Default 0.6, give more to execution

# Fix 3: More partitions = smaller per-partition data = less spill
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# Fix 4: Enable external shuffle service (survives executor restarts)
spark.conf.set("spark.shuffle.service.enabled", "true")

# Fix 5: Pre-filter/aggregate before the shuffle
# ❌ BAD: Shuffle everything then filter
result = df.groupBy("key").agg(sum("amount")).filter(col("sum(amount)") > 1000)
# ✅ GOOD: Filter first to reduce shuffle data
result = df.filter(col("amount") > 0).groupBy("key").agg(sum("amount"))
```

---

#### Scenario 7: `OutOfMemoryError` with Window Functions

**Where:** Executor
**Root Cause:** Window functions with `unboundedPreceding` to `unboundedFollowing` on a skewed partition key load the entire partition into memory. If one partition (e.g., one customer with 10M rows) is huge → OOM.

```python
# FIXES:

# Fix 1: Use bounded windows instead of unbounded
from pyspark.sql.window import Window

# ❌ BAD: Unbounded window on skewed key
w = Window.partitionBy("customer_id").orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# ✅ GOOD: Bounded window (last 30 rows)
w = Window.partitionBy("customer_id").orderBy("date") \
    .rowsBetween(-30, Window.currentRow)

# Fix 2: Repartition to spread skewed keys across more partitions
# Add a salt column for window operations
from pyspark.sql.functions import ntile
df = df.withColumn("bucket", ntile(10).over(Window.partitionBy("customer_id").orderBy("date")))
w = Window.partitionBy("customer_id", "bucket").orderBy("date")

# Fix 3: Increase executor memory for heavy window operations
spark.conf.set("spark.executor.memory", "16g")
```

---

#### Scenario 8: `OutOfMemoryError` during `cache()` / `persist()`

**Where:** Executor Storage Memory
**Root Cause:** Caching a DataFrame that's larger than available storage memory. The cached data competes with execution memory for shuffle/sort operations.

```python
# FIXES:

# Fix 1: Use DISK_ONLY storage level (cache to disk, not memory)
from pyspark import StorageLevel
df.persist(StorageLevel.DISK_ONLY)  # Slower reads but won't OOM

# Fix 2: Use MEMORY_AND_DISK (spills to disk when memory is full)
df.persist(StorageLevel.MEMORY_AND_DISK)  # Best of both worlds

# Fix 3: Cache ONLY what you reuse multiple times
# ❌ BAD: Caching everything
df1.cache()
df2.cache()
df3.cache()  # Memory exhausted

# ✅ GOOD: Cache only the reused intermediate
filtered_df = df.filter(col("status") == "ACTIVE").cache()  # Used 3 times downstream
result1 = filtered_df.groupBy("region").count()
result2 = filtered_df.groupBy("type").avg("amount")
result3 = filtered_df.join(other_df, "key")
filtered_df.unpersist()  # Release when done

# Fix 4: Check how much memory your cached DF uses
# Spark UI → Storage tab → shows cached DF sizes
# If cached DF is 12GB and you only have 4GB storage memory → spill/eviction
```

---

### OOM Debugging Flowchart

```
Job fails with OOM
    │
    ├── Where does it fail?
    │     │
    │     ├── DRIVER → Are you calling collect()/toPandas()?
    │     │              Yes → Use take(N) or write to storage
    │     │              No  → Increase spark.driver.memory
    │     │
    │     └── EXECUTOR → What operation is running?
    │           │
    │           ├── SHUFFLE/JOIN → Data skew? Check partition sizes
    │           │                  Yes → Salt keys or enable AQE skew handling
    │           │                  No  → Increase shuffle.partitions
    │           │
    │           ├── BROADCAST → Table too large to broadcast?
    │           │               Yes → Remove broadcast hint, use sort-merge
    │           │
    │           ├── WINDOW → Unbounded window on skewed partition?
    │           │            Yes → Bound the window or add salt bucket
    │           │
    │           ├── CACHE → Caching too much data?
    │           │           Yes → Use MEMORY_AND_DISK or unpersist unused
    │           │
    │           └── UDF → Python UDF consuming excess memory?
    │                     Yes → Increase memoryOverhead + pyspark.memory
    │
    └── YARN kills container → Total memory exceeds allocation
                               Increase memoryOverhead
```

---

### Quick Reference: OOM Fix Commands

```python
# Copy-paste these to quickly address OOM in your Spark job:

# --- Executor OOM ---
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "4g")
spark.conf.set("spark.sql.shuffle.partitions", "800")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# --- Driver OOM ---
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.driver.maxResultSize", "4g")

# --- Broadcast OOM ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable

# --- PySpark UDF OOM ---
spark.conf.set("spark.executor.memoryOverhead", "6g")
spark.conf.set("spark.executor.pyspark.memory", "2g")

# --- GC pressure ---
spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")

# --- Check current memory usage in code ---
print(f"Partitions: {df.rdd.getNumPartitions()}")
df.explain(True)  # Shows physical plan — look for BroadcastHashJoin vs SortMergeJoin
```

---

## 8. Dataproc on GCP — Production Patterns

### Problem: How do you set up a Spark cluster that's cost-effective, auto-scales, and production-ready?

**Definition:** **Dataproc** is GCP's managed Hadoop/Spark service. It handles cluster provisioning, configuration, patching, and teardown. Key decisions: machine types, preemptible workers (70% cheaper but can be reclaimed), autoscaling, and idle timeout.

**Why it matters:**
- A misconfigured cluster wastes money (over-provisioned) or fails under load (under-provisioned)
- Without preemptible workers, you pay full price for workers that are idle 80% of the time
- Without `max-idle`, forgotten clusters run 24/7 burning budget
- Without autoscaling, you can't handle variable workloads (peak vs off-peak)

**How to resolve — production cluster decisions:**

| Decision | Recommendation | Why |
|----------|---------------|-----|
| Worker type | `n2-standard-8` (8 vCPU, 32GB) | Good balance for Spark memory needs |
| Preemptible workers | 60-70% of workers | 70% cheaper, Spark handles task re-execution on reclaim |
| Autoscaling | Scale on YARN pending containers | Add workers when demand exceeds capacity |
| Idle timeout | 30-60 min | Auto-delete when no jobs running (save money) |
| Serverless vs cluster | Serverless for scheduled jobs | No cluster management, pay per second |

**Dataproc vs Dataflow — which to use:**

| Factor | Dataproc (Spark) | Dataflow (Beam) |
|--------|-----------------|------------------|
| Best for | Complex transformations, ML, iterative algos | Simple ETL, streaming |
| Pricing | Per-cluster (VMs) | Per-job (autoscale) |
| Management | Semi-managed (you configure cluster) | Fully managed (no infra) |
| Language | Python, Scala, Java, R | Python, Java |
| Streaming | Structured Streaming | Native streaming |
| When to choose | Need Spark ecosystem (MLlib, GraphX) | Simple pipelines, Pub/Sub integration |

**Interview Perspective:**
> "For production Spark on GCP, I use Dataproc with a mix of standard and preemptible workers (70% preemptible for cost savings). I configure autoscaling based on YARN pending containers and set `max-idle=30m` so clusters auto-delete when unused. For scheduled ETL jobs without complex Spark logic, I prefer Dataproc Serverless — no cluster management, pay per second, and cold start is only ~60s."

### Cluster Configuration

### Preemptible vs Non-Preemptible Workers

**Definition:**
- **Non-preemptible (Standard) workers** — Guaranteed VMs that run until YOU stop them. They cost full price but will never be taken away mid-job.
- **Preemptible (Spot) workers** — Discounted VMs (60-91% cheaper) that Google can **reclaim at any time** with 30 seconds notice. Your running tasks on that VM get killed.

**Why use preemptible?**
- A `n2-standard-8` costs ~$0.39/hr standard vs ~$0.12/hr preemptible = **70% savings**
- For a 10-node cluster running 8 hours: $31.20 vs $9.60 (saves $21.60/day)

**Comparison:**

| Aspect | Non-Preemptible (Standard) | Preemptible (Spot) |
|--------|---------------------------|-------------------|
| **Price** | Full price ($0.39/hr for n2-standard-8) | 60-91% discount ($0.08-$0.12/hr) |
| **Availability** | Guaranteed — always running | Can be reclaimed anytime (30s notice) |
| **HDFS storage** | Yes — stores data blocks | **No HDFS** — only for compute tasks |
| **Best for** | Master node, critical tasks, HDFS storage | Map/shuffle tasks, stateless compute |
| **Max lifetime** | Unlimited | 24 hours (auto-deleted, then recreated) |
| **Data loss risk** | None | Task must be re-executed on another node |
| **Graceful shutdown** | N/A | 30 seconds to save state before termination |

**How Spark handles preemptible node loss:**
1. Preemptible VM is reclaimed by Google
2. Tasks running on that VM are marked as **failed**
3. Spark's task scheduler **re-runs the failed tasks** on remaining healthy nodes
4. Data in shuffle files on that node is lost → upstream tasks may need to re-compute
5. Job completes (slower, but doesn't crash)

**Best practices:**

```bash
# ✅ RECOMMENDED: Hybrid cluster (standard for stability, preemptible for burst)
# - Master: ALWAYS non-preemptible (if master dies, entire job fails)
# - Primary workers: Non-preemptible (store HDFS, provide baseline)
# - Secondary workers: Preemptible (cheap burst capacity)

# Example: 2 standard + 10 preemptible = good cost/reliability balance
gcloud dataproc clusters create my-cluster \
    --master-machine-type=n2-standard-4 \
    --num-workers=2 \                         # Non-preemptible (baseline)
    --worker-machine-type=n2-standard-8 \
    --num-secondary-workers=10 \              # Preemptible (burst capacity)
    --secondary-worker-type=preemptible \
    --region=us-central1
```

**When to use which:**

| Use Case | Recommended Config | Why |
|----------|-------------------|-----|
| Development/Testing | All preemptible + 1 standard master | Cheapest, OK if job restarts |
| Production (short jobs < 1hr) | 2 standard + 8 preemptible | Fast enough to retry if nodes are reclaimed |
| Production (long jobs > 4hr) | More standard, fewer preemptible | Long jobs have higher chance of node reclaim |
| Critical SLA jobs | All non-preemptible | Can't afford re-computation delays |
| ML Training (iterative) | All non-preemptible | Losing a node loses cached RDD/checkpoints |

**Enhanced Preemptible — Spot VMs (newer):**
```bash
# Spot VMs replace "preemptible" in newer Dataproc versions
# Same discounts but with more eviction transparency
gcloud dataproc clusters create my-cluster \
    --num-secondary-workers=10 \
    --secondary-worker-type=spot \    # "spot" instead of "preemptible"
    --region=us-central1
```

**Handling preemption gracefully:**
```python
# 1. Enable external shuffle service (shuffle data survives executor loss)
spark.conf.set("spark.shuffle.service.enabled", "true")

# 2. Set max task failures before job fails
spark.conf.set("spark.task.maxFailures", "8")  # Default 4, increase for preemptible

# 3. Enable speculation (restart slow/stuck tasks on other nodes)
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")  # Task 1.5x slower than median → restart

# 4. Checkpoint RDDs for iterative algorithms (ML training)
spark.sparkContext.setCheckpointDir("gs://bucket/checkpoints/")
rdd.checkpoint()  # Saves to GCS, survives node loss
```

**Interview Perspective:**
> "I use a hybrid cluster: 2-3 non-preemptible workers for baseline stability and HDFS, plus 5-15 preemptible workers for cost-effective burst capacity. The master is always non-preemptible. I enable external shuffle service so shuffle data survives executor loss, and I set `spark.task.maxFailures=8` to tolerate preemption retries. For critical SLA jobs, I go all non-preemptible. For dev/test workloads, I go mostly preemptible to save 70% on compute costs."

---

### Full Cluster Configuration Example
```bash
# Create optimized Dataproc cluster
gcloud dataproc clusters create loan-processing \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n2-standard-4 \
    --worker-machine-type=n2-standard-8 \
    --num-workers=5 \
    --num-secondary-workers=10 \    # Preemptible (70% cheaper)
    --secondary-worker-type=preemptible \
    --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/connectors/connectors.sh \
    --metadata=bigquery-connector-version=1.2.0 \
    --properties="spark:spark.sql.adaptive.enabled=true,spark:spark.sql.shuffle.partitions=400,spark:spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --optional-components=JUPYTER \
    --enable-component-gateway \
    --max-idle=30m \        # Auto-delete after 30min idle
    --autoscaling-policy=loan-autoscale
```

### Autoscaling Policy
```yaml
# autoscale-policy.yaml
workerConfig:
  minInstances: 2
  maxInstances: 20
  weight: 1
secondaryWorkerConfig:
  minInstances: 0
  maxInstances: 50
  weight: 1
basicAlgorithm:
  yarnConfig:
    scaleUpFactor: 1.0
    scaleDownFactor: 1.0
    scaleUpMinWorkerFraction: 0.0
    gracefulDecommissionTimeout: 1h
  cooldownPeriod: 2m
```

### Submit Jobs
```bash
# Submit PySpark job
gcloud dataproc jobs submit pyspark \
    gs://my-bucket/jobs/loan_processing.py \
    --cluster=loan-processing \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar \
    --properties="spark.sql.shuffle.partitions=400" \
    -- --input=gs://data-lake/raw/loans/ --output=project.dataset.loans_curated
```

### Serverless Spark (Dataproc Serverless)
```bash
# No cluster management — submit and forget
gcloud dataproc batches submit pyspark \
    gs://my-bucket/jobs/loan_processing.py \
    --region=us-central1 \
    --deps-bucket=gs://my-bucket/deps \
    --properties="spark.sql.adaptive.enabled=true" \
    --version=2.0 \
    -- --input=gs://data-lake/raw/ --date=2026-06-22
```

---

## 9. Advanced PySpark Patterns

### Problem: Python UDFs are 10-100x slower than built-in functions. How do you write performant PySpark code?

**Definition:** PySpark UDFs serialize data from the JVM to Python, process it row-by-row, then serialize back. This cross-process overhead makes them extremely slow. **Pandas UDFs** (vectorized) solve this by processing batches via Apache Arrow (zero-copy), and **built-in functions** avoid Python entirely.

**Why it matters:**
- A Python UDF on 100M rows: ~30 minutes
- Same logic with built-in `when()`: ~30 seconds (100x faster)
- Pandas UDF: ~2 minutes (15x faster than Python UDF)

**Performance hierarchy:**
```
Built-in functions (fastest) > Pandas UDF > Python UDF (slowest)
        ↑                         ↑                ↑
  Runs in JVM,            Uses Arrow for       Row-by-row
  no Python overhead      vectorized batch      serialization
```

**How to resolve — replace UDFs with built-in functions:**

| UDF Logic | Built-in Replacement |
|-----------|---------------------|
| if/else classification | `when().when().otherwise()` |
| String manipulation | `concat()`, `substring()`, `regexp_replace()` |
| Date arithmetic | `datediff()`, `date_add()`, `months_between()` |
| Null handling | `coalesce()`, `isNull()`, `fillna()` |
| Array operations | `explode()`, `array_contains()`, `size()` |

**When you MUST use a UDF** (no built-in equivalent):
- Complex business logic with external library calls
- ML model inference (use Pandas UDF for batch processing)
- Custom parsing that Spark functions can't handle

**Interview Perspective:**
> "I avoid Python UDFs in PySpark whenever possible — they're 10-100x slower due to serialization overhead. My first approach is always to use built-in Spark SQL functions (`when`, `concat`, `datediff`). If I need custom logic, I use Pandas UDFs which process data in vectorized batches via Apache Arrow. I only use regular Python UDFs as a last resort for logic that truly can't be expressed with built-in functions."

### Efficient UDFs
```python
# ❌ BAD: Python UDF (slow — serializes data to Python and back)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def classify_risk(score):
    if score >= 750: return "LOW"
    elif score >= 650: return "MEDIUM"
    else: return "HIGH"

# ✅ GOOD: Use built-in functions (10-100x faster)
from pyspark.sql.functions import when, col

df.withColumn("risk", 
    when(col("score") >= 750, "LOW")
    .when(col("score") >= 650, "MEDIUM")
    .otherwise("HIGH")
)

# ✅ GOOD: Pandas UDF (vectorized, uses Arrow for zero-copy transfer)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def classify_risk_vectorized(scores: pd.Series) -> pd.Series:
    return pd.cut(scores, bins=[0, 650, 750, 900], labels=["HIGH", "MEDIUM", "LOW"])

df.withColumn("risk", classify_risk_vectorized(col("score")))
```

### Window Functions in PySpark
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lag, sum as spark_sum, dense_rank

# Running balance
window_spec = Window.partitionBy("account_id").orderBy("transaction_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_with_balance = df.withColumn("running_balance", spark_sum("amount").over(window_spec))

# Previous value (for change detection)
df_with_prev = df.withColumn(
    "prev_amount",
    lag("amount", 1).over(Window.partitionBy("customer_id").orderBy("date"))
)

# Deduplication (keep latest per key)
dedup_window = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
df_deduped = df.withColumn("rn", row_number().over(dedup_window)) \
    .filter(col("rn") == 1) \
    .drop("rn")
```

### Handling Small Files (Compaction)

**Problem: Why does having thousands of small files kill performance?**

**Definition:** The **small files problem** occurs when a directory has thousands of tiny files (< 10MB each) instead of fewer large files (100MB-1GB). Each file requires a separate metadata lookup, file open/close, and task scheduling — the overhead overwhelms the actual data processing.

**Why it happens:**
- Streaming pipelines write frequently (one file per micro-batch)
- Over-partitioning creates many nearly-empty directories
- Append-mode writes add a new file per operation

**Impact:**
- File listing on GCS/S3 becomes slow (listing 100K files takes minutes)
- Each file = 1 Spark task → 100K tasks = massive scheduling overhead
- NameNode (HDFS) or metadata service overloaded

**How to resolve:**
1. **Periodic compaction** — Nightly job reads small files, coalesces, rewrites as larger files
2. **Repartition before write** — `df.coalesce(N)` to control output file count
3. **Target file size** — Calculate: `total_data_size / target_file_size = N partitions`
4. **Use Delta Lake/Iceberg** — Built-in `OPTIMIZE` command auto-compacts

**Interview Perspective:**
> "Small files are a common problem in streaming and frequently-appended data lakes. I run a nightly compaction job via Airflow that reads partitions with many small files, coalesces them into ~128MB Parquet files, and overwrites. For streaming, I configure `trigger(processingTime='5 minutes')` to batch micro-writes. Long-term, I recommend Delta Lake which has built-in `OPTIMIZE` for compaction."

```python
# Compaction implementation: Merge small files into optimal-sized files
# Run as a scheduled Airflow task (e.g., nightly)

def compact_partition(spark, input_path, output_path, target_file_size_mb=128):
    df = spark.read.parquet(input_path)
    
    # Calculate optimal partition count
    total_size = sum(f.length for f in spark._jvm.org.apache.hadoop.fs.FileSystem
                     .get(spark._jsc.hadoopConfiguration())
                     .listFiles(input_path, True))
    target_partitions = max(1, int(total_size / (target_file_size_mb * 1024 * 1024)))
    
    df.coalesce(target_partitions) \
        .write.mode("overwrite") \
        .parquet(output_path)

# Run as periodic Airflow task
compact_partition(spark, 
    "gs://lake/curated/transactions/date=2026-06-22/",
    "gs://lake/curated/transactions/date=2026-06-22/")
```

---

## 10. Interview Questions — Spark Advanced

**Q: A Spark job takes 4 hours. How do you reduce it to 30 minutes?**
> **Systematic approach:**
> 1. **Check Spark UI** — Which stages take longest? Are there stragglers?
> 2. **Check shuffle** — How much data is being shuffled? Can we reduce it?
>    - Pre-partition by join key
>    - Broadcast small tables
>    - Reduce shuffle partitions if data is small
> 3. **Check skew** — Is one partition 100x larger?
>    - Salt keys, or isolate hot keys with broadcast
>    - Enable AQE skew handling
> 4. **Check serialization** — Switch to Kryo
> 5. **Check UDFs** — Replace Python UDFs with built-in functions or Pandas UDFs
> 6. **Check I/O** — Reading unnecessary columns? Use columnar format (Parquet) + schema pruning
> 7. **Check cluster sizing** — More executors? More memory per executor?
>
> **Real example:** Job was slow because a single customer_id had 50M records (skew). Applied salting technique → reduced that stage from 3.5h to 20 minutes. Total job: 4h → 35min.

**Q: Explain the difference between narrow and wide transformations.**
> **Narrow** (no shuffle): map, filter, flatMap, union. Each input partition contributes to exactly one output partition. Can be pipelined within a single stage.
>
> **Wide** (requires shuffle): groupBy, join, distinct, repartition, orderBy. Data must be redistributed across the cluster. Creates a stage boundary.
>
> **Optimization insight:** Chain narrow transformations together (they'll be in one stage). Minimize the number of wide transformations. If you must shuffle, do it once on the right key rather than multiple times.

**Q: When would you use Dataproc Serverless vs a persistent cluster?**
> **Serverless:** Short-running jobs (< 2h), variable workloads, cost-sensitive (pay per second), no cluster management. Cold start ~60s.
>
> **Persistent cluster:** Long-running jobs, interactive development (Jupyter), jobs that reuse cached data, need specific libraries pre-installed, need custom init actions.
>
> **Hybrid:** Persistent cluster for development + Serverless for production scheduled jobs. This gives interactive development experience with cost-effective production runs.
