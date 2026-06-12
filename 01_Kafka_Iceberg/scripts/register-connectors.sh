#!/bin/sh
# Registers every connector config in /connectors with Kafka Connect.
# Idempotent: PUT creates the connector or updates it if it already exists.
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"

CATALOG_URL="${CATALOG_URL:-http://iceberg-rest:9001/iceberg}"

echo "Waiting for Kafka Connect at $CONNECT_URL ..."
until curl -fs "$CONNECT_URL/connectors" > /dev/null; do
  sleep 3
done

echo "Ensuring 'lakehouse' namespace exists in the catalog"
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"namespace":["lakehouse"],"properties":{}}' \
  "$CATALOG_URL/v1/namespaces" > /dev/null || true

for file in /connectors/*.json; do
  name="iceberg-sink-$(basename "$file" .json)"
  echo "Registering $name"
  curl -fs -X PUT -H "Content-Type: application/json" \
    --data @"$file" "$CONNECT_URL/connectors/$name/config" > /dev/null
done

echo
echo "Connector status:"
sleep 5
for file in /connectors/*.json; do
  name="iceberg-sink-$(basename "$file" .json)"
  echo "  $name: $(curl -fs "$CONNECT_URL/connectors/$name/status" | grep -o '"state":"[A-Z]*"' | head -1)"
done
