# Querying Iceberg with Spark

Spark complements DuckDB in this demo: it talks thrift to the Hive Metastore
directly and supports things DuckDB cannot do yet — copy-on-write updates, Iceberg
maintenance procedures (compaction, snapshot expiry), and `CREATE TABLE AS SELECT`.

Start an interactive SQL shell:

```bash
docker compose exec -it spark spark-sql
```

The catalog `lake` is pre-configured (see `spark/spark-defaults.conf`), so tables
are addressed as `lake.lakehouse.<table>`. Exit the shell with `exit;` or Ctrl+D.

## 1. Inspect Merge-on-Read state

Run this *after* the DuckDB `DELETE`/`UPDATE` examples on `customers`
(see [duckdb.md](duckdb.md)) so there are delete files to look at.

```sql
SHOW TBLPROPERTIES lake.lakehouse.customers ('write.update.mode');
```

The `files` metadata table breaks the table down by file type. `content = 0` is a
data file, `1` a position-delete file, `2` an equality-delete file:

```sql
SELECT
  sum(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
  sum(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files,
  sum(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS equality_delete_files
FROM lake.lakehouse.customers.files;
```

```sql
SELECT committed_at, operation, summary['added-delete-files'] AS added_delete_files
FROM lake.lakehouse.customers.snapshots
ORDER BY committed_at DESC
LIMIT 5;
```

## 2. Compaction: turning logical deletes into physical deletes

MoR deletes are *logical* — the deleted rows still sit in the original Parquet
files, masked by delete files at read time. Compaction rewrites the data files with
the deletes applied, making the deletion *physical*. This matters for GDPR-style
erasure: until compaction runs, the "deleted" bytes are still on disk.

Check the state before (note the row count and file counts):

```sql
SELECT count(*) FROM lake.lakehouse.customers;

SELECT
  sum(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
  sum(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files
FROM lake.lakehouse.customers.files;
```

Compact the data files. `rewrite-all` makes the demo deterministic: by default the
procedure leaves small files alone, this forces every file (including the many tiny
streaming commits) into the rewrite, with deletes applied:

```sql
CALL lake.system.rewrite_data_files(
  table => 'lakehouse.customers',
  options => map('rewrite-all', 'true')
);
```

The output reports how many files were rewritten (likely into a single file). Check
again:

```sql
SELECT
  sum(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
  sum(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files
FROM lake.lakehouse.customers.files;

SELECT count(*) FROM lake.lakehouse.customers;  -- same logical rows (plus new streaming arrivals)
```

Data files were squashed, but the position-delete files are *still listed* — they
now point at data files that no longer exist ("dangling" deletes). A second
procedure drops them:

```sql
CALL lake.system.rewrite_position_delete_files(
  table => 'lakehouse.customers',
  options => map('rewrite-all', 'true')
);

SELECT
  sum(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
  sum(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_files
FROM lake.lakehouse.customers.files;
```

Now `position_delete_files` is **0**. The row count never changed — compaction
alters the physical layout, never the data. The compactions show up as `replace`
operations in the snapshot log, interleaved with the sink's ongoing `append`s:

```sql
SELECT committed_at, operation FROM lake.lakehouse.customers.snapshots
ORDER BY committed_at DESC LIMIT 5;
```

### Cleaning up old files

The pre-compaction files still exist on MinIO — older snapshots reference them
(that is what makes time travel work). To physically remove them, expire the old
snapshots, then delete unreferenced files:

```sql
CALL lake.system.expire_snapshots(
  table => 'lakehouse.customers',
  retain_last => 1,
  older_than => TIMESTAMP '2099-01-01 00:00:00'
);
```

The output counts the deleted files (data files, position-delete files, manifests).
After this, browse the MinIO console
([http://localhost:9001](http://localhost:9001)) under
`warehouse/lakehouse/lakehouse.db/customers/data/` — the delete files and the
pre-compaction Parquet are gone. The deleted customer's bytes no longer exist
anywhere. Trade-off: you also gave up time travel to the expired snapshots.

Note: the sink keeps streaming into `customers` while you do all of this —
compaction commits like any other writer, and concurrent appends are fine.

## 3. Copy-on-Write update

`orders` was created with `write.update.mode=copy-on-write`:

```sql
SHOW TBLPROPERTIES lake.lakehouse.orders ('write.update.mode');
```

File counts before — many small data files from streaming commits, zero delete
files:

```sql
SELECT
  sum(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
  sum(CASE WHEN content > 0 THEN 1 ELSE 0 END) AS delete_files
FROM lake.lakehouse.orders.files;

SELECT count(*) FROM lake.lakehouse.orders WHERE status = 'shipped';
```

The CoW operation — every data file containing a `shipped` row is rewritten in
place:

```sql
UPDATE lake.lakehouse.orders SET status = 'delivered' WHERE status = 'shipped';
```

After: far fewer data files (the rewrite coalesced them) and still **zero** delete
files — that's the difference from MoR:

```sql
SELECT
  sum(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_files,
  sum(CASE WHEN content > 0 THEN 1 ELSE 0 END) AS delete_files
FROM lake.lakehouse.orders.files;
```

The snapshot log records it as `overwrite`, and the summary shows how many files
were added vs removed:

```sql
SELECT committed_at, operation,
       summary['added-data-files'] AS added,
       summary['deleted-data-files'] AS removed
FROM lake.lakehouse.orders.snapshots
ORDER BY committed_at DESC
LIMIT 5;
```

## 4. Gold-layer tables: joins and aggregations as new Iceberg tables

A classic lakehouse pattern: the streamed tables are the *bronze* layer; batch jobs
derive cleaned/joined/aggregated *gold* tables from them. `CREATE OR REPLACE TABLE`
makes each statement idempotent — re-run it any time to refresh the table with the
latest streamed data.

### customer_dim — deduplicate the CDC stream

`customers` holds every profile update ever streamed. The dimension table keeps
only the latest version per customer:

```sql
CREATE OR REPLACE TABLE lake.lakehouse.customer_dim
USING iceberg AS
SELECT customer_id, name, email, tier, city, updated_ts
FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY updated_ts DESC) AS rn
  FROM lake.lakehouse.customers
)
WHERE rn = 1;

SELECT count(*) AS customers FROM lake.lakehouse.customer_dim;
SELECT * FROM lake.lakehouse.customer_dim ORDER BY customer_id LIMIT 5;
```

### order_payment_fact — join the three streams

```sql
CREATE OR REPLACE TABLE lake.lakehouse.order_payment_fact
USING iceberg AS
SELECT
  o.order_id, o.customer_id, c.name AS customer_name, c.tier,
  o.product, o.quantity, o.price, o.currency,
  o.status AS order_status, p.method AS payment_method,
  p.status AS payment_status, o.order_ts
FROM lake.lakehouse.orders o
LEFT JOIN lake.lakehouse.payments p ON o.order_id = p.order_id
LEFT JOIN lake.lakehouse.customer_dim c ON o.customer_id = c.customer_id;

SELECT order_status, payment_status, count(*) AS n
FROM lake.lakehouse.order_payment_fact
GROUP BY 1, 2 ORDER BY n DESC;
```

### daily_revenue — aggregate

```sql
CREATE OR REPLACE TABLE lake.lakehouse.daily_revenue
USING iceberg AS
SELECT
  date(order_ts) AS order_date, product, currency,
  count(*) AS orders,
  round(sum(price * quantity), 2) AS revenue
FROM lake.lakehouse.orders
GROUP BY 1, 2, 3;

SELECT * FROM lake.lakehouse.daily_revenue ORDER BY revenue DESC LIMIT 5;
```

### Check the result

```sql
SHOW TABLES IN lake.lakehouse;
```

All six tables are ordinary Iceberg tables in the same catalog, so the gold tables
are immediately queryable from DuckDB too — see the gold-layer section in
[duckdb.md](duckdb.md). Since they are batch snapshots, they go stale as streaming
continues; re-run the `CREATE OR REPLACE` statements to refresh them.

## 5. Schema evolution

Iceberg lets you add columns without rewriting existing files. This demo shows two
paths: streaming evolution via the Kafka Connect sink (`orders`) and engine evolution
via Spark DDL (`customers`).

### Orders — streaming evolution (add `channel`)

The sink has `iceberg.tables.evolve-schema-enabled=true`. Before triggering v2,
confirm the table has no `channel` column:

```sql
DESCRIBE TABLE lake.lakehouse.orders;
```

In another terminal, trigger schema v2 on the producer (adds nullable `channel` to
the Kafka Connect JSON schema):

```bash
docker compose run --rm evolve-orders-schema
docker compose logs -f producer-orders   # look for: schema v2 active (added channel)
```

Wait ~15 seconds for the next sink commit, then re-describe and query:

```sql
DESCRIBE TABLE lake.lakehouse.orders;

SELECT channel, count(*) AS n
FROM lake.lakehouse.orders
GROUP BY channel;

SELECT count(*) AS with_channel
FROM lake.lakehouse.orders
WHERE channel IS NOT NULL;
```

Rows written before evolution have `channel = NULL`; new rows carry `web`, `mobile`,
or `api`. DuckDB and ClickHouse see the same column — see [duckdb.md](duckdb.md) and
[clickhouse.md](clickhouse.md).

Time-travel to a snapshot from before the trigger: every row shows `channel` as null
(the current schema is applied to older files).

### Customers — engine evolution (add `referral_code`)

Spark can evolve the table directly:

```sql
DESCRIBE TABLE lake.lakehouse.customers;

ALTER TABLE lake.lakehouse.customers ADD COLUMN referral_code STRING;

DESCRIBE TABLE lake.lakehouse.customers;

SELECT customer_id, referral_code
FROM lake.lakehouse.customers
LIMIT 10;
```

`referral_code` is `NULL` for all rows written before the `ALTER`. Optionally set a
few values so the column is not all-null after compaction demos:

```sql
UPDATE lake.lakehouse.customers SET referral_code = 'FRIEND10' WHERE customer_id = 3;
UPDATE lake.lakehouse.customers SET referral_code = 'WELCOME' WHERE customer_id = 12;
```
