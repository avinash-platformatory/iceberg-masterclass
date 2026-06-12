# Dual-catalog queries with Spark

Use the **Spark container from module 01** with an extra catalog config for Polaris
(catalog B). Module 01's `lake` catalog (HMS) is unchanged.

## 1. Configure the Polaris catalog

After `docker compose run --rm polaris-setup`, copy the printed **Client ID** and
**Client secret** into [`spark/polaris-catalog.conf`](../spark/polaris-catalog.conf)
(replace `CLIENT_ID` and `CLIENT_SECRET`), then start Spark:

```bash
cd ../01_Kafka_Iceberg
docker compose exec -it spark spark-sql \
  --properties-file /path/on/host/../02_Catalog_Migration/spark/polaris-catalog.conf
```

Or paste conf flags inline (substitute credentials from `polaris-setup` output):

```bash
docker compose exec -it spark spark-sql \
  --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.polaris.type=rest \
  --conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog \
  --conf spark.sql.catalog.polaris.oauth2-server-uri=http://polaris:8181/api/catalog/v1/oauth/tokens \
  --conf spark.sql.catalog.polaris.warehouse=lake \
  --conf spark.sql.catalog.polaris.credential='<clientId>:<clientSecret>' \
  --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.polaris.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.polaris.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.polaris.s3.path-style-access=true \
  --conf spark.sql.catalog.polaris.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.polaris.s3.secret-access-key=minioadmin
```

Explicit MinIO credentials are used here because credential vending to MinIO is not
configured in this demo stack.

> **Tip:** mount `02_Catalog_Migration/spark/polaris-catalog.conf` into the Spark
> container for convenience, or copy credentials once into a local file.

## 2. Baseline — catalog A (HMS)

Before registration, only the HMS path sees tables via `lake`:

```sql
SELECT count(*) AS orders_hms FROM lake.lakehouse.orders;
SELECT count(*) AS customers_hms FROM lake.lakehouse.customers;
SELECT count(*) AS payments_hms FROM lake.lakehouse.payments;
```

## 3. After register-tables — catalog B (Polaris)

Run from module 02:

```bash
cd ../02_Catalog_Migration
docker compose run --rm register-tables
```

Then in Spark:

```sql
SELECT count(*) AS orders_polaris FROM polaris.lakehouse.orders;
SELECT count(*) AS customers_polaris FROM polaris.lakehouse.customers;
SELECT count(*) AS payments_polaris FROM polaris.lakehouse.payments;
```

Counts should match **immediately after** `register-tables`. If module 01 is still
streaming, catalog A (HMS) receives new commits while Polaris stays at the registered
snapshot — row counts diverge. That is expected in this read-only migration demo.

## 4. Prove same table history

```sql
SELECT snapshot_id FROM lake.lakehouse.orders.snapshots
INTERSECT
SELECT snapshot_id FROM polaris.lakehouse.orders.snapshots;
```

Non-empty result means Polaris registered the **same** metadata lineage.

## 5. Time travel on Polaris

Pick a snapshot ID from the intersect query above:

```sql
SELECT count(*) FROM polaris.lakehouse.orders VERSION AS OF <snapshot_id>;
```

Compare to the same snapshot on HMS:

```sql
SELECT count(*) FROM lake.lakehouse.orders VERSION AS OF <snapshot_id>;
```

## 6. What did not move

No new Parquet copies were created. In the MinIO console
(`warehouse/lakehouse/lakehouse.db/orders/data/`) the file paths are identical —
only Polaris's catalog database gained entries pointing at existing
`metadata/*.metadata.json` files.
