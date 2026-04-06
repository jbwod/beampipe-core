#!/bin/bash

# Test script to query metadata for sources added in test_add_sources.sh

set -e

BASE_URL="http://localhost:8000/api/v1"
ADMIN_USERNAME="admin"
ADMIN_PASSWORD="Str1ngst!"

echo "POST /login"
TOKEN_RESPONSE=$(curl -s -X POST "${BASE_URL}/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=${ADMIN_USERNAME}&password=${ADMIN_PASSWORD}")

TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token // empty')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "Login failed!"
  echo "Response: $TOKEN_RESPONSE"
  exit 1
fi

echo "Login successful"
echo "Token: ${TOKEN:0:50}..."
echo ""

echo "GET /sources?page=1&items_per_page=10"
SOURCES_RESPONSE=$(curl -s -X GET "${BASE_URL}/sources?page=1&items_per_page=10" \
  -H "Authorization: Bearer ${TOKEN}")

echo "Found sources:"
echo "$SOURCES_RESPONSE" | jq '.data[] | {uuid, project_module, source_identifier, enabled}'
echo ""

SOURCE_UUIDS=$(echo "$SOURCES_RESPONSE" | jq -r '.data[].uuid')

if [ -z "$SOURCE_UUIDS" ]; then
  echo "No sources found to query metadata for."
  exit 0
fi

METADATA_FOUND_COUNT=0
METADATA_EMPTY_COUNT=0

for SOURCE_UUID in $SOURCE_UUIDS; do
  echo "GET /sources/${SOURCE_UUID}/metadata"
  METADATA_RESPONSE=$(curl -s -X GET "${BASE_URL}/sources/${SOURCE_UUID}/metadata" \
    -H "Authorization: Bearer ${TOKEN}")
  
  # Debug: show raw response if parsing fails
  if ! echo "$METADATA_RESPONSE" | jq -e '.source' > /dev/null 2>&1; then
    echo "  Raw response: $METADATA_RESPONSE" | head -c 200
    echo ""
    continue
  fi
  
  SOURCE_IDENTIFIER=$(echo "$METADATA_RESPONSE" | jq -r '.source.source_identifier // empty')
  if [ -z "$SOURCE_IDENTIFIER" ] || [ "$SOURCE_IDENTIFIER" = "null" ]; then
    SOURCE_IDENTIFIER="unknown"
  fi
  METADATA_COUNT=$(echo "$METADATA_RESPONSE" | jq -r '.metadata_count // 0')
  
  echo "  Source: $SOURCE_IDENTIFIER"
  echo "  Metadata entries: $METADATA_COUNT"
  
  if [ "$METADATA_COUNT" -gt 0 ]; then
    echo "  Metadata datasets (full objects):"
    echo "$METADATA_RESPONSE" | jq '
      .metadata[]
      | {sbid, created_at, updated_at}
        + ( .metadata_json.datasets[] )
    '
    METADATA_FOUND_COUNT=$((METADATA_FOUND_COUNT + 1))
  else
    echo "  No metadata found for this source yet."
    METADATA_EMPTY_COUNT=$((METADATA_EMPTY_COUNT + 1))
  fi
  echo ""
done

echo "  - Sources with metadata: $METADATA_FOUND_COUNT"
echo "  - Sources without metadata: $METADATA_EMPTY_COUNT"