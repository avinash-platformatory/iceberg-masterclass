# Querying Iceberg with ClickHouse

ClickHouse is a columnar OLAP engine with native Iceberg read support. It reads the
same Parquet files on MinIO that the Kafka sink writes — no data copy required.

Start an interactive client:

```bash
docker compose exec -it clickhouse clickhouse-client
```

## 1. Register the tables

The `allow_database_iceberg` setting is pre-enabled in `clickhouse/users.d/iceberg.xml`.

ClickHouse's `DataLakeCatalog` engine always sends a `warehouse` query parameter to
the REST catalog. Gravitino treats that value as a *catalog name*, not a storage
path, so auto-discovery via REST does not work with our Gravitino facade ([upstream
issue](https://github.com/apache/gravitino/issues/10486)). Instead, register each
table by pointing at its Iceberg root on MinIO — the same files DuckDB and Spark use.

Run once per fresh volume (idempotent):

```sql
CREATE DATABASE IF NOT EXISTS lake ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS lake.orders
ENGINE = IcebergS3('http://minio:9000/warehouse/lakehouse/lakehouse.db/orders/', 'minioadmin', 'minioadmin');

CREATE TABLE IF NOT EXISTS lake.customers
ENGINE = IcebergS3('http://minio:9000/warehouse/lakehouse/lakehouse.db/customers/', 'minioadmin', 'minioadmin');

CREATE TABLE IF NOT EXISTS lake.payments
ENGINE = IcebergS3('http://minio:9000/warehouse/lakehouse/lakehouse.db/payments/', 'minioadmin', 'minioadmin');
```

After building gold-layer tables in [spark.md](spark.md) section 4, register those
too:

```sql
CREATE TABLE IF NOT EXISTS lake.customer_dim
ENGINE = IcebergS3('http://minio:9000/warehouse/lakehouse/lakehouse.db/customer_dim/', 'minioadmin', 'minioadmin');

CREATE TABLE IF NOT EXISTS lake.order_payment_fact
ENGINE = IcebergS3('http://minio:9000/warehouse/lakehouse/lakehouse.db/order_payment_fact/', 'minioadmin', 'minioadmin');

CREATE TABLE IF NOT EXISTS lake.daily_revenue
ENGINE = IcebergS3('http://minio:9000/warehouse/lakehouse/lakehouse.db/daily_revenue/', 'minioadmin', 'minioadmin');
```

List registered tables:

```sql
SHOW TABLES FROM lake;
```

## 2. Basic queries

```sql
SELECT count() FROM lake.orders;
```

Run the count twice with a few seconds in between — it grows while the sink streams.

```sql
-- Revenue by product
SELECT product, count() AS orders, round(sum(price * quantity), 2) AS revenue
FROM lake.orders
GROUP BY product
ORDER BY revenue DESC;

-- Orders joined to payments
SELECT o.status AS order_status, p.status AS payment_status, count() AS n
FROM lake.orders AS o
INNER JOIN lake.payments AS p ON o.order_id = p.order_id
GROUP BY order_status, payment_status
ORDER BY n DESC
LIMIT 10;
```

Partition pruning on the day-partitioned `orders` table:

```sql
SELECT count()
FROM lake.orders
SETTINGS use_iceberg_partition_pruning = 1;
```

### Gold-layer tables

```sql
SELECT tier, count() AS customers
FROM lake.customer_dim
GROUP BY tier;

SELECT order_date, sum(revenue) AS revenue, sum(orders) AS orders
FROM lake.daily_revenue
GROUP BY order_date
ORDER BY order_date DESC;
```

## 3. Time travel

Iceberg snapshots are created on every sink commit (~15 seconds), compaction, and
batch write. ClickHouse can query any historical snapshot by ID or timestamp.

### Discover snapshots

```sql
SELECT snapshot_id, made_current_at, is_current_ancestor
FROM system.iceberg_history
WHERE database = 'lake' AND table = 'orders'
ORDER BY made_current_at DESC
LIMIT 10;
```

Pick a `snapshot_id` from a few rows back (not the latest).

### By snapshot ID

```sql
SELECT count() FROM lake.orders;  -- current head

SELECT count() FROM lake.orders
SETTINGS iceberg_snapshot_id = <snapshot_id_from_above>;
```

The historical count is lower — only the rows that existed at that snapshot.

### By timestamp

`SETTINGS` values must be literals, so derive the millisecond timestamp from
`system.iceberg_history` first. Use a value slightly *after* `made_current_at`
(Iceberg picks the snapshot current at or before the timestamp):

```sql
SELECT
    snapshot_id,
    toUInt64(toUnixTimestamp(made_current_at) * 1000) + 1000 AS ts_ms
FROM system.iceberg_history
WHERE database = 'lake' AND table = 'orders'
ORDER BY made_current_at ASC
LIMIT 1;
```

Copy `ts_ms` into the time-travel query:

```sql
SELECT count() FROM lake.orders
SETTINGS iceberg_timestamp_ms = <ts_ms_from_above>;
```

On a stack that has been streaming for several minutes, this also works — compare
the result to a fresh `SELECT count() FROM lake.orders`:

```sql
SELECT toUInt64(toUnixTimestamp(now() - INTERVAL 5 MINUTE) * 1000) AS ts_ms;
-- paste ts_ms into:
SELECT count() FROM lake.orders SETTINGS iceberg_timestamp_ms = <ts_ms>;
```

You cannot set both `iceberg_snapshot_id` and `iceberg_timestamp_ms` in the same
query.

For the same feature in DuckDB syntax, see the time-travel section of
[duckdb.md](duckdb.md).

## 4. Notes

- **Read-only in this demo** — compaction, CTAS, and row-level writes are done with
  Spark; see [spark.md](spark.md).
- **Position deletes** on `customers` (MoR) are applied at read time. Equality
  deletes and deletion vectors (v3) are not supported.
- **Snapshot granularity** — time travel jumps between commits, not arbitrary
  millisecond precision. With a ~15s sink interval, counts change stepwise.
- **HTTP interface** — queries also work via `curl http://localhost:8123/` if you
  prefer not to use the client.
