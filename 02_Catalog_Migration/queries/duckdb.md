# Dual-catalog queries with DuckDB

Module 02's DuckDB shell attaches **catalog A** (Gravitino → HMS) automatically.
Catalog B (Polaris) requires a bearer token from OAuth — Spark is the primary demo
path; this section is optional.

## 1. Start the shell

Prerequisite: `polaris-setup` and `register-tables` completed.

```bash
docker compose run --rm duckdb
```

`init-polaris.sql` attaches `lake` (Gravitino). Compare counts on catalog A:

```sql
SELECT count(*) AS orders_gravitino FROM lake.lakehouse.orders;
```

## 2. Catalog B — Polaris via bearer token (optional)

Obtain a token (run from host, substitute credentials from `polaris-setup`):

```bash
curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -d 'grant_type=client_credentials' \
  -d 'client_id=<clientId>' \
  -d 'client_secret=<clientSecret>' \
  -d 'scope=PRINCIPAL_ROLE:ALL' | jq -r '.access_token'
```

In DuckDB, attach Polaris with the token (DuckDB REST bearer support varies by
version — if this fails, use [spark.md](spark.md) for the dual-catalog demo):

```sql
ATTACH '' AS polaris (
    TYPE iceberg,
    ENDPOINT 'http://polaris:8181/api/catalog',
    AUTHORIZATION_TYPE 'bearer',
    BEARER_TOKEN '<paste_token_here>'
);

SHOW ALL TABLES;
SELECT count(*) AS orders_polaris FROM polaris.lakehouse.orders;
```

Counts should match `lake.lakehouse.orders`.

## 3. ClickHouse footnote

ClickHouse in module 01 uses `IcebergS3` paths directly — it never depended on HMS
or Gravitino. After catalog migration, **no ClickHouse changes are needed**; counts
stay valid. The lesson: storage paths are decoupled from catalog implementations.
