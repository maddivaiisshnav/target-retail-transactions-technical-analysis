# Concepts reference

Everything used in this project, explained from first principles. Read this
alongside the code — each section names the file that demonstrates it.

---

## The shape of any data pipeline

Data pipelines move data from where it is *created* to where it can be *asked
questions of*, transforming it along the way. Three distinct problems, three
tools:

| Problem | Tool | Job |
|---|---|---|
| How does data get from A to B reliably and continuously? | **Kafka** | durable pipe / buffer |
| How do I process more data than fits on one machine? | **Spark** | split work across machines |
| How do analysts query the result in SQL? | **Hive** | make files look like tables |

---

## Kafka

### The problem it solves

1,948 stores, each producing checkout events. If every store wrote *directly*
into the analytics database:

- the database is a single point of failure — it goes down, sales data is lost
- it is coupled to 1,948 systems; you cannot change it without breaking them
- adding a fraud-detection consumer means reconfiguring all 1,948 stores
- Black Friday traffic spikes hit the database directly

### What it is

A distributed, append-only **log**. An infinite file you can only append to,
replicated across machines so it survives failure.

### Vocabulary

- **Topic** — a named stream (`target_orders`). Like a table name, for events.
- **Partition** — a topic is split into N ordered logs so multiple machines
  work in parallel. **Ordering is guaranteed within a partition, never
  across.** This is the thing people most often get wrong.
- **Producer** — writes messages into a topic.
- **Consumer** — reads from a topic, tracking an **offset** ("I've read up to
  message #4,812").
- **Consumer group** — consumers cooperating; Kafka assigns each partition to
  exactly one member, so each message is processed once by the group. Add a
  consumer and Kafka rebalances — that's how you scale out. A 3-partition
  topic supports at most 3 useful consumers; a 4th sits idle.
- **Broker** — one Kafka server. A cluster is several.
- **Retention** — Kafka *keeps* messages for a configured period even after
  they are read.

### The superpower: replay

Kafka is not a queue that deletes on read; it is a log you can rewind. Your job
wrote garbage for three days? Fix the code, reset the offset, replay. With
RabbitMQ or SQS that data is gone forever.

### Keys and partitioning

Kafka hashes the message key to pick a partition, so same key → same partition
→ guaranteed ordering for that key. No key → round-robin → no ordering
guarantees at all.

We keyed by `customer_state`, which produced **213 / 668 / 119** across three
partitions — badly skewed, because SP is 42% of orders. That is **data skew**:
one consumer does 5× the work. Fixes: a higher-cardinality key, or salting the
hot key.

### acks — the durability/throughput trade-off

| setting | meaning | risk |
|---|---|---|
| `acks=0` | fire and forget | fastest, loses data freely |
| `acks=1` | leader confirms only | loses data if leader dies pre-replication |
| `acks=all` | every in-sync replica has it | slowest, safest — used here |

For sales data, `all` is the only defensible choice.

### `auto_offset_reset`

What to do when a consumer group has no stored offset (first run).
`earliest` = from the beginning; `latest` = only new messages.

The brief's "start the consumer before the producer" instruction exists *only*
because the default is `latest`. Setting `earliest` removes the race entirely
and enables replay. Understanding this flag is worth more than the exercise.

### Manual offset commits

With `enable_auto_commit=True`, Kafka commits on a timer — so a crash between
"offset committed" and "data actually written" loses messages permanently.
Committing manually after the data is safely handled gives **at-least-once**
delivery.

**Files:** `src/ingestion/producer.py`, `src/ingestion/consumer.py`

---

## PySpark

### The problem it solves

200 GB of orders. Pandas loads everything into one machine's RAM and dies.
Even if it fit, one CPU processing 200 GB serially takes hours.

### What it is

A framework that splits data into chunks, ships your *code* to many machines,
runs it in parallel, and combines the results.

### Vocabulary

- **Driver** — the process running your script; builds the plan, coordinates.
- **Executors** — worker processes doing the actual work.
- **Partition** (different meaning from Kafka!) — a chunk of the DataFrame on
  one executor. 200 GB might be 1,600 partitions of 128 MB.
- **DataFrame** — a distributed table with a schema. Looks like pandas; is not.
- **py4j** — the bridge letting Python call into the JVM. Your Python code
  never touches the data; it builds instructions the JVM executes.

### Lazy evaluation — the central idea

`filter`, `select`, `groupBy`, `withColumn` are **transformations**: lazy,
nothing runs, Spark just records intent as a DAG. `show`, `count`, `collect`,
`write` are **actions**: only these trigger execution.

Because Spark sees the *whole* plan before running it, the **Catalyst
optimizer** rewrites it. Write this:

```python
df.groupBy("customer_city").sum("order_products_value") \
  .filter(col("customer_city") == "Dallas")
```

and Spark filters *first*, then aggregates — orders of magnitude less work. In
pandas each line executes immediately and you pay for your own bad ordering.

### Shuffle — why jobs are slow

To `groupBy("customer_city")`, every "Dallas" row must reach the *same*
executor. That means moving data across the network.

- **Narrow** (no movement, fast): `filter`, `select`, `withColumn`, `map`
- **Wide** (shuffle, slow): `groupBy`, `join`, `distinct`, `orderBy`

When a Spark job is slow, it is almost always a shuffle.

`spark.sql.shuffle.partitions` defaults to **200**, tuned for terabytes. On
1,000 rows that spawns 200 near-empty tasks — pure overhead. We set 8. Tuning
this is among the most common real optimisations.

### `cache()`

Because transformations are lazy and nothing is stored by default, running
eight actions on the same DataFrame re-reads and re-cleans the source **eight
times**. `cache()` says "compute once, keep it in memory."

### Schema: contract, not convenience

CSV and Kafka both carry *text*, never types. `inferSchema=True` is convenient
but wrong for production:

- it costs an **extra full pass** over the data
- it **guesses** — one `"N/A"` deep in a numeric column kills the job in prod

An explicit `StructType` is a contract: faster, and it fails loudly when
upstream data changes shape.

Type choices that matter: `customer_zip_code_prefix` is a **string**, not an
integer. A zip code is an identifier, never a quantity, and integer storage
eats leading zeros (`01040` → `1040`). **If you'd never do arithmetic on it,
it's a string.**

### ANSI mode and `try_cast`

Spark 4 defaults to ANSI mode: a bad cast is a hard error that kills the job.
`try_cast` returns NULL instead, so one malformed record cannot destroy a batch
of a million good ones. The trade-off is silent failure — so **count** the
failures rather than letting them disappear.

### Normalise your grouping keys

`SAO PAULO` and `Sao Paulo` are different strings to a `groupBy`. In this
dataset that split the top city into two buckets and reported 143 orders
instead of 149 — a 4% error nobody would notice for months. Always
`trim` → `lower` → `initcap` before grouping.

### Structured Streaming

Treat the stream as an unbounded **table** that keeps growing. Write the same
DataFrame code as for batch; Spark runs it incrementally per micro-batch.
`readStream` instead of `read`, `writeStream` instead of `write`. That's it —
no separate API.

- **`checkpointLocation`** — where Spark records the offsets it has processed.
  Without it there is *no* fault tolerance. With it, a crashed job resumes at
  the exact record it stopped on. Verified in this project: re-running the
  stream processed zero rows and bronze stayed at 1,000 rows, not 2,000.
- **`trigger(availableNow=True)`** — process everything currently available,
  then stop. How you run a "streaming" job on a schedule, and increasingly the
  default in practice, because always-on clusters are expensive and few
  businesses truly need sub-second freshness.
- **`outputMode`** — `append` for plain rows; `complete` reprints the whole
  result table each batch (aggregations only, small result sets); `update`
  plus a watermark for unbounded key spaces.

**Files:** `src/processing/common.py`, `src/processing/03_analysis.py`,
`src/ingestion/streaming_consumer.py`

---

## Hive

### What it actually is in 2026

Hive began as a system compiling SQL into MapReduce jobs. **That engine is
dead — Spark replaced it.** What survived, and what every lakehouse still
runs, is the **Hive Metastore**: a small relational database holding
*metadata*.

```
table 'city_sales'  →  files at output/warehouse/target_retail.db/city_sales/
                    →  schema: customer_city STRING, total_sales DOUBLE
                    →  format: Parquet
                    →  partitions: customer_state=SP/ ...
```

### Why it matters: storage/compute separation

The data is just Parquet files in a filesystem or object store. The metastore
is the **catalog** saying "those files are a table called X." Any engine —
Spark, Trino, Presto, Flink, DuckDB — can read the same metastore and query
the same files. **Nobody owns the data.**

Contrast with Postgres, where data lives in a proprietary internal format only
Postgres can read. Separation is why you can spin up a 100-node cluster for an
hour, run a job, and shut it down — the data doesn't live in the cluster.

### `saveAsTable` vs `save`

```python
df.write.save(path)        # writes files. Nothing knows they exist.
df.write.saveAsTable(name) # writes files AND registers them in the metastore,
                           # so SELECT * FROM name works.
```

### Managed vs external tables

- **Managed** — Hive owns the data. `DROP TABLE` **deletes the files**.
- **External** — you point Hive at a path you control. `DROP TABLE` removes
  only metadata; files survive.

Real teams overwhelmingly use **external**, because `DROP TABLE` silently
nuking production data is a bad afternoon.

### Parquet

**Columnar** storage: instead of row-by-row, all of `customer_city` is stored
together, then all of `order_products_value`. Consequences:

1. `SELECT customer_city` reads only that column's bytes — 10× less I/O
2. similar values sit adjacent, so compression is far better
3. per-row-group min/max statistics let engines skip whole files that cannot
   match a filter (**predicate pushdown**)

CSV has none of this. Never store analytics data as CSV.

### Partitioning and pruning

`partitionBy("customer_state")` writes into subdirectories:

```
orders_clean/customer_state=SP/part-0000.parquet
orders_clean/customer_state=RJ/part-0000.parquet
```

A query filtering on state reads **only** that directory. This is **partition
pruning**, the biggest performance lever in a data lake. `EXPLAIN` proves it —
look for `PartitionFilters` and a `Location` naming a single directory.

**The counter-lesson:** partition by a high-cardinality column (like `Id`) and
you create millions of tiny files, which is far *slower*. That's the **small
files problem**. Pick a column queries actually filter on, with moderate
cardinality.

### Idempotency

`mode("overwrite")` means re-running produces the same end state rather than
duplicating rows. Non-negotiable — retries and backfills are normal operation,
not exceptions.

**File:** `src/processing/04_save_to_hive.py`

---

## Analytics concepts

### Pearson vs Spearman correlation

**Pearson** measures *linear* association, −1 to +1. It assumes interval data.
**Spearman** is Pearson computed on the *ranks*, so it detects any *monotonic*
relationship, linear or not, and is the correct choice for **ordinal** data
like a 1–5 review score. Neither implies causation. This project computes both.

### Mean vs median

Both delivery time and approval time are **right-skewed** — a few extreme
values drag the mean well above what a typical customer experiences. Approval
time is the stark case: median **21 minutes**, mean **10.4 hours**. Report the
median for skewed distributions, or you're describing nobody's reality.

### Minimum-volume guards

A city with one order and one 1-star review is noise. Without a `>= 5 orders`
filter, every "top/bottom N" ranking is dominated by tiny samples.

### Distribution beats the average

The review mean is 4.09, which sounds healthy. The distribution is **bimodal**:
58% five-star and 11.9% one-star. One customer in eight is actively unhappy —
invisible in the mean. Always look at the shape.

### Statistical power

At n=978, a correlation must exceed roughly |r| > 0.063 to be distinguishable
from zero. Our 0.022 is inside the noise. **Knowing when your data cannot
answer the question is part of the job** — and reporting a null result
honestly is better engineering than manufacturing a story.

---

## Data quality thinking

### Nulls are a business question

The brief says "remove null values if any." Taken literally that is
`df.dropna()` — a silent 2.2% data loss that hides a real defect.

The correct policy is **per-metric**:

- revenue and counts → keep every row (`avg()` already skips nulls)
- delivery-time metrics → filter to rows that *have* a delivery date
- contradictory records → quarantine and report upstream

### Null semantics do not survive serialization

pandas `NaN` is a **float**. `json.dumps(float('nan'))` emits the bare token
`NaN` — not legal JSON — and the consumer reads back the string `"nan"`.
Fix at the source (`NaN` → `None`), *and* defend in the consumer. A consumer
must never assume its upstream is well behaved.

### Look at your data before you trust your aggregation

The `groupBy` was never wrong. The assumption that the key was clean was
wrong. This is the single most important habit in the field.
