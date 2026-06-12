# Catalog Migration: HMS → Polaris REST

A follow-on module to [01_Kafka_Iceberg](../01_Kafka_Iceberg/). It demonstrates
**metadata-only catalog migration**: register existing Iceberg tables from module 01
into [Apache Polaris](https://polaris.apache.org/) (REST catalog B) without copying
Parquet files on MinIO.

```
Module 01 (catalog A)                    Module 02 (catalog B)
─────────────────────                    ─────────────────────
Kafka Connect ──> Gravitino REST         register-tables ──> Polaris REST
Spark ──────────> Hive Metastore (HMS)         │
       │                                       │
       └──────────────> MinIO <─────────────────┘
                    (same metadata + data files)
```

**Read-only demo** — no Kafka Connect cutover. Both catalogs can **read** the same
tables; only one catalog should **write** in production.

## Prerequisites

1. Module 01 stack running (`cd ../01_Kafka_Iceberg && docker compose up -d`)
2. Connectors registered: `docker compose run --rm register-connectors`
3. Bronze tables populated (`orders`, `customers`, `payments`) — wait ~30s after
   registering connectors

## Step 1 — Start Polaris

From this directory:

```bash
docker compose up -d
```

Check health:

```bash
docker compose ps
curl -fs http://localhost:8182/q/health && echo OK
```

Polaris joins the `kafka-iceberg_default` Docker network and reaches MinIO at
`minio:9000`.

## Step 2 — Configure catalog B

```bash
docker compose run --rm polaris-setup
```

This creates:

- Polaris catalog `lake` with storage on `s3://warehouse/lakehouse` (MinIO)
- Namespace `lakehouse`
- Principal `migration_user` with manage privileges

Save the printed **Client ID** and **Client secret** — Spark and PyIceberg need them.

## Step 3 — Baseline on catalog A

In module 01's Spark shell:

```bash
cd ../01_Kafka_Iceberg
docker compose exec -it spark spark-sql
```

```sql
SELECT count(*) FROM lake.lakehouse.orders;
```

## Step 4 — Register tables in Polaris

```bash
cd ../02_Catalog_Migration
docker compose run --rm register-tables
```

The script reads each table's `metadata/metadata.json` pointer from MinIO and calls
Polaris `register` — **no data copy**.

## Step 5 — Query catalog B

Follow **[queries/spark.md](queries/spark.md)** — configure the `polaris` Spark
catalog with credentials from step 2, then compare counts:

```sql
SELECT count(*) FROM lake.lakehouse.orders;      -- HMS (catalog A)
SELECT count(*) FROM polaris.lakehouse.orders;   -- Polaris (catalog B)
```

Counts match right after registration. While Kafka still writes to catalog A only,
HMS row counts grow and Polaris stays at the registered snapshot — that gap is
intentional for this read-only module.

Optional DuckDB comparison: **[queries/duckdb.md](queries/duckdb.md)**.

## Step 5b — Query with PyIceberg

Native Python client for the Polaris REST catalog:

```bash
docker compose run --rm pyiceberg
```

Lists registered tables, prints row counts, and shows the latest snapshot ID on
`orders`. For interactive exploration and `register_table` examples, see
**[queries/pyiceberg.md](queries/pyiceberg.md)**.

## Step 6 — Inspect MinIO

Open [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin`).
Browse `warehouse/lakehouse/lakehouse.db/orders/` — paths are **unchanged** after
registration. Migration moved catalog pointers, not bytes.

## Step 7 — Concept recap

| Layer | What migrated? |
|-------|----------------|
| Parquet `data/` files | Nothing — same objects |
| Iceberg `metadata/` files | Nothing — same JSON |
| Catalog entry | **New** pointer in Polaris |
| Kafka Connect writer | Unchanged (still Gravitino REST) |

Production cutover would repoint writers to Polaris and retire HMS entries — out of
scope for this read-only module.

## Tear down

```bash
docker compose down        # keeps polaris-config volume
docker compose down -v     # remove polaris credentials volume
```

Module 01 keeps running independently.

## Endpoints

| Service | URL |
|---------|-----|
| Polaris Iceberg REST | http://localhost:8181/api/catalog |
| Polaris health | http://localhost:8182/q/health |

## Troubleshooting

- **`network kafka-iceberg_default not found`** — start module 01 first
  (`docker compose up -d` in `01_Kafka_Iceberg`).
- **`SKIP orders: no metadata`** — register connectors in module 01 and wait for sink
  commits (~15s).
- **`polaris-setup` principal already exists** — wipe volume:
  `docker compose down -v` and re-run setup.
- **Spark `polaris` catalog auth errors** — verify `credential=clientId:clientSecret`
  matches `polaris-setup` output, set `oauth2-server-uri`, and use explicit MinIO
  `s3.*` properties (see [spark/polaris-catalog.conf](spark/polaris-catalog.conf)).
- **`register-tables` 403 on s3a locations** — re-run `docker compose down -v` and
  `polaris-setup` (catalog `allowedLocations` must include `s3a://warehouse`).
- **Polaris port conflict** — change `POLARIS_API_PORT` / `POLARIS_ADMIN_PORT` in
  [.env](.env).
- **PyIceberg `Missing Polaris credentials`** — run `polaris-setup` before
  `pyiceberg`; credentials live in the `polaris-config` volume.
- **PyIceberg S3 errors** — ensure MinIO is reachable at `minio:9000` from the
  container network (module 01 must be running).
