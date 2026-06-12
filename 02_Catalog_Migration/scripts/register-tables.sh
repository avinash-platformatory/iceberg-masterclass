#!/bin/sh
# Register existing Iceberg tables from MinIO into Polaris (metadata-only migration).
set -eu

if [ ! -f /polaris-config/credentials.env ]; then
  echo "Missing /polaris-config/credentials.env — run: docker compose run --rm polaris-setup" >&2
  exit 1
fi
# shellcheck disable=SC1091
. /polaris-config/credentials.env

POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
CATALOG_NAME="${POLARIS_CATALOG_NAME:-lake}"
NAMESPACE="${ICEBERG_NAMESPACE:-lakehouse}"
BUCKET="${WAREHOUSE_BUCKET:-warehouse}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"

TABLES="orders customers payments"

mc alias set local "$MINIO_ENDPOINT" "${MINIO_ACCESS_KEY:-minioadmin}" "${MINIO_SECRET_KEY:-minioadmin}" >/dev/null

resolve_metadata_location() {
  table=$1
  meta_prefix="${BUCKET}/lakehouse/lakehouse.db/${table}/metadata"
  pointer_key="${meta_prefix}/metadata.json"

  if mc stat "local/${pointer_key}" >/dev/null 2>&1; then
    mc cat "local/${pointer_key}" | tr -d '\n\r'
    return
  fi

  latest=$(mc ls --json "local/${meta_prefix}/" \
    | jq -r 'select(.key != null) | select(.key | endswith(".metadata.json")) | .key' \
    | sort -V | tail -1)

  if [ -z "$latest" ]; then
    echo ""
    return
  fi
  echo "s3://${BUCKET}/lakehouse/lakehouse.db/${table}/metadata/${latest}"
}

normalize_s3_path() {
  path=$1
  table=$2
  case "$path" in
    s3://*) echo "$path" ;;
    s3a://*) echo "s3://${path#s3a://}" ;;
    /*) echo "s3://${BUCKET}${path}" ;;
    *)
      if echo "$path" | grep -q '\.metadata\.json$'; then
        echo "s3://${BUCKET}/lakehouse/lakehouse.db/${table}/metadata/${path}"
      else
        echo ""
      fi
      ;;
  esac
}

echo "Obtaining Polaris access token..."
TOKEN_RESPONSE=$(curl --fail-with-body -s -S -X POST "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=${POLARIS_USER_CLIENT_ID}&client_secret=${POLARIS_USER_CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL")
TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "Failed to obtain token: $TOKEN_RESPONSE" >&2
  exit 1
fi

for table in $TABLES; do
  raw=$(resolve_metadata_location "$table")
  if [ -z "$raw" ]; then
    echo "SKIP ${table}: no metadata under lakehouse/lakehouse.db/${table}/metadata/" >&2
    echo "       (is module 01 running with connectors registered?)" >&2
    continue
  fi

  METADATA_LOCATION=$(normalize_s3_path "$raw" "$table")
  if [ -z "$METADATA_LOCATION" ]; then
    echo "SKIP ${table}: could not resolve metadata location from: ${raw}" >&2
    continue
  fi

  echo "Registering ${NAMESPACE}.${table} -> ${METADATA_LOCATION}"

  RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    "${POLARIS_URL}/api/catalog/v1/${CATALOG_NAME}/namespaces/${NAMESPACE}/register" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"${table}\", \"metadata-location\": \"${METADATA_LOCATION}\"}")

  HTTP_CODE=$(echo "$RESPONSE" | tail -1)
  BODY=$(echo "$RESPONSE" | sed '$d')

  case "$HTTP_CODE" in
    200|201) echo "  OK" ;;
    409) echo "  Already registered — skipping." ;;
    *)
      if echo "$BODY" | grep -qi "already exists"; then
        echo "  Already registered — skipping."
      else
        echo "  FAILED (HTTP ${HTTP_CODE}): ${BODY}" >&2
        exit 1
      fi
      ;;
  esac
done

echo
echo "Registered tables in Polaris catalog '${CATALOG_NAME}'."
echo "Compare counts: see queries/spark.md"
