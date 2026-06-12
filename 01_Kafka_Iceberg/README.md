# Kafka → Iceberg: Real-Time Sink

A self-contained lakehouse: fake e-commerce events stream through Kafka and land in
Apache Iceberg tables on MinIO, cataloged by a Hive Metastore, queryable with DuckDB
(and Spark). Everything runs in Docker Compose — no other dependencies.

```
producers (fake data) ──> Kafka ──> Kafka Connect (Iceberg sink)
                                          │
                                          v
        Hive Metastore (catalog) <── Iceberg REST facade
              │    ^                      ^
       Postgres    │                      │
                   │                   DuckDB
                 Spark
                                  data files live in MinIO
```

The Hive Metastore is the catalog (its metadata lives in Postgres). Because Kafka
Connect's Iceberg sink and DuckDB speak the Iceberg REST protocol rather than Hive
thrift, a thin REST facade ([Apache Gravitino](https://gravitino.apache.org/)) sits in
front of the metastore. Spark talks thrift to the metastore directly. All paths lead
to the same tables.

## Topics and tables

| Topic | Iceberg table | Pattern | Write mode |
|---|---|---|---|
| `orders` | `lakehouse.orders` | append-only facts, partitioned by `day(order_ts)` | copy-on-write |
| `customers` | `lakehouse.customers` | CDC-style profile updates, unpartitioned | merge-on-read |
| `payments` | `lakehouse.payments` | append-only facts, partitioned by `day(payment_ts)` | defaults |

In step 9 you'll derive three more *gold-layer* tables from these with batch SQL
(no topic behind them): `lakehouse.customer_dim` (deduplicated profiles),
`lakehouse.order_payment_fact` (three-way join), and `lakehouse.daily_revenue`
(aggregation).

## Prerequisites

- Docker with the Compose plugin (Docker Desktop on macOS/Windows, Docker Engine on Linux).
- ~6 GB of RAM available to Docker and ~5 GB of disk for images.

That's it. Helper scripts, producers, and query shells all run inside containers.

### Cross-OS notes

- **Windows**: use Docker Desktop with the WSL2 backend. Clone the repo *inside* WSL2
  for best performance, or anywhere if you only use the commands below. The
  `.gitattributes` file forces LF line endings on scripts so they work in containers
  even if Git is configured with `core.autocrlf=true`. If you cloned before that file
  existed, re-checkout: `git rm --cached -r . && git checkout .`
- **macOS (Apple Silicon)**: the `apache/hive:3.1.3` image is amd64-only; Docker
  Desktop runs it via Rosetta automatically (enabled by default). Everything else is
  multi-arch.
- **Linux**: use Docker Engine v23+ so `docker compose` (v2, no hyphen) is available.
- If a host port is taken on your machine, change it in [.env](.env) — nothing else
  references host ports.

## Step 1 — Start the stack

```bash
docker compose up -d --build
```

First start takes a few minutes (image pulls + two small image builds). Check that
everything is up:

```bash
docker compose ps
```

You should see `kafka`, `connect`, `minio`, `postgres`, `hive-metastore`,
`iceberg-rest`, `spark`, `kafka-ui` running, three `producer-*` containers streaming,
and `minio-init` exited with code 0.

## Step 2 — Watch the streams

The producers are already publishing fake events. Open **Kafka UI** at
[http://localhost:8088](http://localhost:8088) and look at the `orders`, `customers`
and `payments` topics — messages arrive every second or so.

Or tail a topic from the CLI:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic orders --max-messages 3
```

## Step 3 — Register the Iceberg sink connectors

```bash
docker compose run --rm register-connectors
```

This PUTs the three connector configs from
[connect/connectors/](connect/connectors/) to Kafka Connect. Each connector
auto-creates its Iceberg table (schema comes from the message schema) and commits new
data files roughly **every 15 seconds**.

Check connector health any time:

```bash
docker compose exec connect curl -s localhost:8083/connectors?expand=status
# or look at the Kafka Connect tab in Kafka UI
```

## Step 4 — See the lakehouse appear in MinIO

Open the MinIO console at [http://localhost:9001](http://localhost:9001)
(user `minioadmin`, password `minioadmin`) and browse
`warehouse/lakehouse/lakehouse.db/` — each table has a `metadata/` folder (Iceberg
manifests, metadata JSON) and a `data/` folder (Parquet, with
`order_ts_day=.../` partition folders under `orders`).

## Step 5 — Query with DuckDB

```bash
docker compose run --rm duckdb
```

The shell attaches the catalog automatically and lists the tables. Try:

```sql
SELECT count(*) FROM lake.lakehouse.orders;  -- run it twice, it grows
```

The full tour — aggregations, joins, CDC dedupe, time travel, metadata inspection —
is in **[queries/duckdb.md](queries/duckdb.md)**. Spark has its own guide in
**[queries/spark.md](queries/spark.md)** (used in steps 7–9); future chapters will
add `queries/trino.md`, `queries/clickhouse.md`, etc.

## Step 6 — Merge-on-Read (MoR)

`customers` was created with `write.update.mode=merge-on-read`. Row-level changes
write small *delete files* instead of rewriting data. Inside the DuckDB shell:

```sql
DELETE FROM lake.lakehouse.customers WHERE customer_id = 7;   -- GDPR erasure

SELECT content, file_path, record_count
FROM iceberg_metadata(lake.lakehouse.customers)
ORDER BY content;
```

Note the new `POSITION_DELETES` entries — the original Parquet files were *not*
rewritten; readers merge the deletes on the fly. Cheap writes, slightly costlier
reads: that's MoR.

## Step 7 — Compaction: making MoR deletes physical

The "deleted" rows from step 6 still physically exist in the Parquet files — they
are only masked at read time. Compaction rewrites the data files with the deletes
applied, which is what actually erases the bytes (important for GDPR). Open an
interactive Spark shell:

```bash
docker compose exec -it spark spark-sql
```

and follow sections 1–2 of **[queries/spark.md](queries/spark.md)**: inspect the
delete files, run the `rewrite_data_files` and `rewrite_position_delete_files`
procedures, watch the position-delete count drop to zero, then `expire_snapshots`
to remove the old files from MinIO for good.

## Step 8 — Copy-on-Write (CoW)

`orders` was created with `write.update.mode=copy-on-write`. Row-level changes
rewrite every affected data file — costlier writes, but reads stay pristine. In the
same Spark shell, follow section 3 of [queries/spark.md](queries/spark.md): update
`shipped` orders to `delivered` and compare file counts before and after. Data
files get replaced and the delete-file count stays **zero** — the opposite
trade-off from the MoR table in step 6.

| | Copy-on-Write | Merge-on-Read |
|---|---|---|
| Row-level write cost | high (rewrites files) | low (writes delete files) |
| Read cost | lowest | merge at read time |
| Good for | read-heavy, infrequent updates | frequent updates/deletes, CDC |

Note: streaming ingest via the sink connector is append-only in both cases; CoW/MoR
governs how *row-level* updates and deletes behave — typically done by batch engines
on top of the streamed tables, exactly as you just did.

## Step 9 — Build gold-layer tables (joins and aggregations)

Derive new Iceberg tables from the streamed ones — a deduplicated customer
dimension, an orders-payments-customers fact table, and a daily revenue rollup.
Still in the Spark shell, follow section 4 of
[queries/spark.md](queries/spark.md). The new tables land in the same catalog, so
you can immediately query them from DuckDB too (gold-layer section of
[queries/duckdb.md](queries/duckdb.md)).

## Step 10 — Tear down

```bash
docker compose down        # keep data volumes
docker compose down -v     # delete everything (Kafka, MinIO, metastore data)
```

## Endpoints

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8088 | — |
| Kafka (host clients) | `localhost:9094` | — |
| Kafka Connect REST | http://localhost:8083 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO S3 API | http://localhost:9000 | minioadmin / minioadmin |
| Iceberg REST catalog | http://localhost:9101/iceberg | — |

## Troubleshooting

- **Connector shows FAILED**: `docker compose logs connect --tail 100`. Most common
  cause is registering connectors before `iceberg-rest`/`hive-metastore` finished
  starting — re-run `docker compose run --rm register-connectors`.
- **No data files in MinIO yet**: the sink commits every ~15s *and* needs at least
  one record per topic; give it half a minute after registering.
- **DuckDB `ATTACH` fails**: the REST facade may still be starting;
  `docker compose logs iceberg-rest --tail 50`.
- **Ports already in use**: change the `*_PORT` values in [.env](.env) and
  `docker compose up -d` again.
- **Wiped state half-way**: `docker compose down -v && docker compose up -d --build`
  gives you a clean slate.
