# Querying Polaris with PyIceberg

Native Python client for the Iceberg REST catalog API. Uses the same OAuth
credentials and MinIO storage settings as [spark.md](spark.md).

## Prerequisites

1. `docker compose run --rm polaris-setup`
2. `docker compose run --rm register-tables`

## One-shot demo

```bash
docker compose run --rm pyiceberg
```

Expected output:

- Lists `orders`, `customers`, `payments` in namespace `lakehouse`
- Prints row counts and latest `snapshot_id` per table
- If `channel` exists on `orders`, shows a filtered count

## Interactive REPL

```bash
docker compose run --rm --entrypoint python pyiceberg -i
```

```python
from catalog_config import load_polaris_catalog

catalog = load_polaris_catalog()
catalog.list_tables("lakehouse")

orders = catalog.load_table("lakehouse.orders")
len(orders.scan().to_arrow())

# Projection + filter
orders.scan(
    row_filter="status = 'paid'",
    selected_fields=("order_id", "product", "price"),
).to_arrow()

# Snapshot history
[s.snapshot_id for s in orders.metadata.snapshots][-5:]
```

## Optional: register a table via PyIceberg

The primary registration path is `docker compose run --rm register-tables` (shell +
REST API). The same operation in Python:

```python
from catalog_config import load_polaris_catalog

catalog = load_polaris_catalog()
catalog.register_table(
    ("lakehouse", "orders"),
    "s3://warehouse/lakehouse/lakehouse.db/orders/metadata/00001-....metadata.json",
)
```

Use the current metadata file path from MinIO — no Parquet copy required.

## Compare to Spark

After registration, PyIceberg row counts match Spark's `polaris.lakehouse.*` counts.
While module 01 keeps streaming to catalog A (HMS), Polaris counts stay at the
registered snapshot — see [spark.md](spark.md) section 3.

Shared snapshot IDs can be compared:

```sql
-- Spark (module 01 container)
SELECT snapshot_id FROM polaris.lakehouse.orders.snapshots ORDER BY committed_at DESC LIMIT 1;
```

```python
# PyIceberg
orders.metadata.snapshots[-1].snapshot_id
```

## Troubleshooting

- **`Missing Polaris credentials`** — run `polaris-setup` first.
- **401 / Not authorized** — credentials expired or wrong; re-run `polaris-setup`
  and use the printed client ID / secret.
- **S3 / file not found** — verify `MINIO_ENDPOINT` is `http://minio:9000` inside
  the Docker network (not `localhost`).
- **Empty table list** — run `register-tables` after module 01 connectors have
  written metadata to MinIO.
