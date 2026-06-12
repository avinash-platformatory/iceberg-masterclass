#!/bin/sh
# One-shot: create Polaris catalog scoped to module-01 MinIO, principal, and namespace.
# Writes query-engine credentials to /polaris-config/credentials.env
set -eu

POLARIS_URL="${POLARIS_URL:-http://polaris:8181}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
CLIENT_ID="${POLARIS_ROOT_CLIENT_ID:-root}"
CLIENT_SECRET="${POLARIS_ROOT_CLIENT_SECRET:-s3cr3t}"
CATALOG_NAME="${POLARIS_CATALOG_NAME:-lake}"
NAMESPACE="${ICEBERG_NAMESPACE:-lakehouse}"
BUCKET="${WAREHOUSE_BUCKET:-warehouse}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"

apk add --no-cache curl jq >/dev/null

echo "Waiting for Polaris health..."
until curl -fs "http://polaris:8182/q/health" >/dev/null 2>&1; do
  sleep 2
done

echo "Obtaining root access token..."
TOKEN_RESPONSE=$(curl --fail-with-body -s -S -X POST "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL")
TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "Failed to obtain access token: $TOKEN_RESPONSE" >&2
  exit 1
fi

# Warehouse root — must cover s3a:// paths written by HMS/Spark in Iceberg metadata.
BASE_LOCATION="s3://${BUCKET}"

echo "Creating catalog '${CATALOG_NAME}'..."
PAYLOAD=$(jq -n \
  --arg name "$CATALOG_NAME" \
  --arg base "$BASE_LOCATION" \
  --arg bucket "$BUCKET" \
  --arg endpoint "$MINIO_ENDPOINT" \
  '{
    catalog: {
      name: $name,
      type: "INTERNAL",
      readOnly: false,
      properties: { "default-base-location": $base },
      storageConfigInfo: {
        storageType: "S3",
        allowedLocations: [
          ("s3://" + $bucket),
          ("s3a://" + $bucket),
          ("s3://" + $bucket + "/lakehouse"),
          ("s3a://" + $bucket + "/lakehouse")
        ],
        endpoint: $endpoint,
        endpointInternal: $endpoint,
        pathStyleAccess: true,
        region: "us-east-1"
      }
    }
  }')

if ! curl --fail-with-body -s -S -X POST "${POLARIS_URL}/api/management/v1/catalogs" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" >/dev/null 2>&1; then
  echo "Catalog may already exist — continuing."
fi

echo "Creating principal 'migration_user'..."
PRINCIPAL_RESPONSE=$(curl --fail-with-body -s -S -X POST "${POLARIS_URL}/api/management/v1/principals" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -H "Content-Type: application/json" \
  -d '{"principal": {"name": "migration_user", "properties": {}}}')

USER_CLIENT_ID=$(echo "$PRINCIPAL_RESPONSE" | jq -r '.credentials.clientId')
USER_CLIENT_SECRET=$(echo "$PRINCIPAL_RESPONSE" | jq -r '.credentials.clientSecret')
if [ -z "$USER_CLIENT_ID" ] || [ "$USER_CLIENT_ID" = "null" ]; then
  echo "Failed to create principal: $PRINCIPAL_RESPONSE" >&2
  exit 1
fi

curl --fail-with-body -s -S -X POST "${POLARIS_URL}/api/management/v1/principal-roles" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "migration_user_role", "properties": {}}}' >/dev/null 2>&1 || true

curl --fail-with-body -s -S -X POST "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "migration_catalog_role", "properties": {}}}' >/dev/null 2>&1 || true

curl --fail-with-body -s -S -X PUT "${POLARIS_URL}/api/management/v1/principals/migration_user/principal-roles" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -H "Content-Type: application/json" \
  -d '{"principalRole": {"name": "migration_user_role"}}' >/dev/null 2>&1 || true

curl --fail-with-body -s -S -X PUT "${POLARIS_URL}/api/management/v1/principal-roles/migration_user_role/catalog-roles/${CATALOG_NAME}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole": {"name": "migration_catalog_role"}}' >/dev/null 2>&1 || true

curl --fail-with-body -s -S -X PUT "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/migration_catalog_role/grants" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Polaris-Realm: ${POLARIS_REALM}" \
  -H "Content-Type: application/json" \
  -d '{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}' >/dev/null 2>&1 || true

echo "Obtaining user access token..."
USER_TOKEN_RESPONSE=$(curl --fail-with-body -s -S -X POST "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=${USER_CLIENT_ID}&client_secret=${USER_CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL")
USER_TOKEN=$(echo "$USER_TOKEN_RESPONSE" | jq -r '.access_token')

echo "Creating namespace '${NAMESPACE}'..."
curl --fail-with-body -s -S -X POST "${POLARIS_URL}/api/catalog/v1/${CATALOG_NAME}/namespaces" \
  -H "Authorization: Bearer ${USER_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"namespace\": [\"${NAMESPACE}\"], \"properties\": {}}" >/dev/null 2>&1 || \
  echo "Namespace may already exist — continuing."

mkdir -p /polaris-config
cat > /polaris-config/credentials.env <<EOF
POLARIS_USER_CLIENT_ID=${USER_CLIENT_ID}
POLARIS_USER_CLIENT_SECRET=${USER_CLIENT_SECRET}
POLARIS_CATALOG_NAME=${CATALOG_NAME}
POLARIS_REALM=${POLARIS_REALM}
POLARIS_URL=${POLARIS_URL}
ICEBERG_NAMESPACE=${NAMESPACE}
EOF

echo
echo "=========================================="
echo "Polaris setup complete"
echo "=========================================="
echo "Catalog:       ${CATALOG_NAME}"
echo "Namespace:     ${NAMESPACE}"
echo "Storage:       ${BASE_LOCATION} on ${MINIO_ENDPOINT}"
echo "Client ID:     ${USER_CLIENT_ID}"
echo "Client secret: ${USER_CLIENT_SECRET}"
echo
echo "Next: docker compose run --rm register-tables"
