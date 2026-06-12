# Querying Iceberg with DuckDB

Start an interactive shell (the catalog is attached automatically by `duckdb/init.sql`):

```bash
docker compose run --rm duckdb
```

The catalog is attached as `lake`, so tables are addressed as `lake.lakehouse.<table>`.

## Basics

```sql
SHOW ALL TABLES;

SELECT count(*) FROM lake.lakehouse.orders;

SELECT * FROM lake.lakehouse.orders LIMIT 10;
```

Run the count twice with a few seconds in between — it grows while the producers stream. The sink commits every ~15 seconds.

## Aggregations

```sql
-- Revenue by product
SELECT product, count(*) AS orders, round(sum(price * quantity), 2) AS revenue
FROM lake.lakehouse.orders
GROUP BY product
ORDER BY revenue DESC;

-- Order volume per minute (a tiny streaming dashboard)
SELECT date_trunc('minute', order_ts) AS minute, count(*) AS orders
FROM lake.lakehouse.orders
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;
```

## Joins across tables

```sql
-- Payment success rate per order status
SELECT o.status AS order_status, p.status AS payment_status, count(*) AS n
FROM lake.lakehouse.orders o
JOIN lake.lakehouse.payments p ON o.order_id = p.order_id
GROUP BY 1, 2
ORDER BY n DESC;
```

## Deduplicating a CDC-style stream

The `customers` topic emits repeated profile updates per `customer_id`. The table is
append-only, so build the "current state" view with latest-record-wins:

```sql
SELECT customer_id, name, tier, city, updated_ts
FROM (
    SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY updated_ts DESC) AS rn
    FROM lake.lakehouse.customers
)
WHERE rn = 1
ORDER BY customer_id
LIMIT 20;

-- How many versions does each customer have?
SELECT customer_id, count(*) AS versions
FROM lake.lakehouse.customers
GROUP BY customer_id
ORDER BY versions DESC
LIMIT 10;
```

## Merge-on-Read in action (row-level writes from DuckDB)

`customers` was created with `write.update.mode=merge-on-read` and is unpartitioned,
so DuckDB can modify it. DuckDB always writes **positional delete files** (MoR):

```sql
-- GDPR-style erasure
DELETE FROM lake.lakehouse.customers WHERE customer_id = 7;

-- Anonymize a column
UPDATE lake.lakehouse.customers SET email = 'redacted@example.com' WHERE customer_id = 9;
```

Now look at the table internals — the deleted rows are *not* rewritten, Iceberg just
recorded delete files that are merged at read time:

```sql
-- Snapshot history: note operation = 'delete' / 'overwrite'
SELECT * FROM iceberg_snapshots(lake.lakehouse.customers);

-- Data files vs delete files
SELECT content, file_path, record_count
FROM iceberg_metadata(lake.lakehouse.customers)
ORDER BY content;
```

Rows with `content = EXISTING`/`ADDED` are Parquet data files; `POSITION_DELETES`
rows are the MoR delete files. You can also see them in the MinIO console under
`warehouse/lakehouse/lakehouse.db/customers/data/`.

## Time travel

```sql
-- List snapshots, then query an older one
SELECT snapshot_id, timestamp_ms FROM iceberg_snapshots(lake.lakehouse.orders);

SELECT count(*) FROM lake.lakehouse.orders AT (VERSION => <snapshot_id>);

-- Or by timestamp
SELECT count(*) FROM lake.lakehouse.orders AT (TIMESTAMP => now() - INTERVAL 5 minutes);
```

## Inspecting table metadata

```sql
-- Where does the data actually live?
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg_metadata(lake.lakehouse.orders)
LIMIT 10;

-- Table properties (note the CoW/MoR write modes)
SELECT * FROM iceberg_table_properties(lake.lakehouse.orders);
SELECT * FROM iceberg_table_properties(lake.lakehouse.customers);
```

## Gold-layer tables

After building the derived tables in [spark.md](spark.md) (section 4), they are
ordinary Iceberg tables in the same catalog — DuckDB sees them immediately:

```sql
SHOW ALL TABLES;

-- Current customer profiles, already deduplicated
SELECT tier, count(*) AS customers FROM lake.lakehouse.customer_dim GROUP BY tier;

-- The pre-joined fact table replaces the three-way join from earlier
SELECT payment_method, round(sum(price * quantity), 2) AS revenue
FROM lake.lakehouse.order_payment_fact
WHERE payment_status = 'captured'
GROUP BY payment_method
ORDER BY revenue DESC;

-- Daily rollup
SELECT order_date, sum(revenue) AS revenue, sum(orders) AS orders
FROM lake.lakehouse.daily_revenue
GROUP BY order_date
ORDER BY order_date DESC;
```

These are batch snapshots: the bronze tables keep growing while gold tables stand
still. Refresh them by re-running the `CREATE OR REPLACE` statements in Spark.

## Limits to be aware of

- DuckDB only writes merge-on-read (positional deletes). Copy-on-write updates,
  compaction, and `CREATE TABLE AS SELECT` are done with Spark in this demo — see
  [spark.md](spark.md).
- `UPDATE`/`DELETE` work on unpartitioned (or bucket/truncate-partitioned) tables;
  `orders` is day-partitioned, so modify it via Spark instead.
