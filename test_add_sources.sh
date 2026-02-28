#!/bin/bash

# Test script to add 5 sources to the registry

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

SOURCES=(
  "HIPASSJ0949-47b"
  "HIPASSJ2130-43b"
  "HIPASSJ1318-21"
)
)
CREATED_COUNT=0
EXISTING_COUNT=0

for SOURCE_ID in "${SOURCES[@]}"; do
  echo "POST /sources (adding ${SOURCE_ID})"
  ADD_RESPONSE=$(curl -s -X POST "${BASE_URL}/sources" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{
      \"project_module\": \"wallaby_hires\",
      \"source_identifier\": \"${SOURCE_ID}\",
      \"enabled\": true
    }")
  
  SOURCE_UUID=$(echo "$ADD_RESPONSE" | jq -r '.uuid // empty')
  if [ -z "$SOURCE_UUID" ] || [ "$SOURCE_UUID" = "null" ]; then
    echo "  Failed to add source ${SOURCE_ID}"
    echo "  Response: $ADD_RESPONSE"
  else
    echo "  Source created: $SOURCE_UUID"
    echo "$ADD_RESPONSE" | jq '{uuid, project_module, source_identifier, enabled}'
    CREATED_COUNT=$((CREATED_COUNT + 1))
  fi
  echo ""
done

echo "  - Created: $CREATED_COUNT sources"
echo "  - Failed: $((5 - CREATED_COUNT)) sources"
echo ""

echo "GET /sources?page=1&items_per_page=10"
SOURCES_RESPONSE=$(curl -s -X GET "${BASE_URL}/sources?page=1&items_per_page=10" \
  -H "Authorization: Bearer ${TOKEN}")

echo "All sources:"
echo "$SOURCES_RESPONSE" | jq '.data | length as $count | "Total sources: \($count)"'
echo "$SOURCES_RESPONSE" | jq '.data[] | {uuid, project_module, source_identifier, enabled}'
echo ""
