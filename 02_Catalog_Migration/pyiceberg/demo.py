#!/usr/bin/env python3
"""Read-only Polaris catalog demo — list tables, scan orders, print counts."""

from __future__ import annotations

import os
import sys

from catalog_config import load_polaris_catalog

NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "lakehouse")
BRONZE_TABLES = ("orders", "customers", "payments")


def main() -> int:
    print("Connecting to Polaris via PyIceberg...")
    catalog = load_polaris_catalog()

    tables = catalog.list_tables(NAMESPACE)
    table_names = sorted(identifier[1] if isinstance(identifier, tuple) else str(identifier) for identifier in tables)
    print(f"\nTables in {NAMESPACE}: {table_names}")

    missing = [t for t in BRONZE_TABLES if t not in table_names]
    if missing:
        print(f"WARNING: expected tables not registered: {missing}", file=sys.stderr)
        print("Run: docker compose run --rm register-tables", file=sys.stderr)

    print("\nRow counts (Polaris catalog):")
    for name in BRONZE_TABLES:
        if name not in table_names:
            continue
        table = catalog.load_table(f"{NAMESPACE}.{name}")
        count = len(table.scan().to_arrow())
        snapshots = table.metadata.snapshots
        latest_snapshot = snapshots[-1].snapshot_id if snapshots else None
        print(f"  {name}: {count} rows, latest snapshot_id={latest_snapshot}")

    orders = catalog.load_table(f"{NAMESPACE}.orders")
    schema_fields = [f.name for f in orders.schema().fields]
    print(f"\norders schema columns: {schema_fields}")

    if "channel" in schema_fields:
        web_count = len(orders.scan(row_filter="channel = 'web'").to_arrow())
        print(f"orders where channel='web': {web_count}")

    print("\nPyIceberg + Polaris demo complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
