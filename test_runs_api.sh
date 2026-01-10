#!/bin/bash

# Test script

set -e

BASE_URL="http://localhost:8000/api/v1"
ADMIN_USERNAME="admin"
ADMIN_PASSWORD="Str1ngst!"

echo "=========================================="
echo "beampipe-core Testing Script"
echo "=========================================="
echo ""

# Step 1: Login and get token
echo "POST /login"
TOKEN_RESPONSE=$(curl -s -X POST "${BASE_URL}/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=${ADMIN_USERNAME}&password=${ADMIN_PASSWORD}")

TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token // empty')

if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "Login fail!"
  echo "Response: $TOKEN_RESPONSE"
  exit 1
fi

echo "Login success"
echo "Token: ${TOKEN:0:50}..."
echo ""

# Step 2: Create a run
echo "POST /runs."
CREATE_RESPONSE=$(curl -s -X POST "${BASE_URL}/runs" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "project_module": "wallaby",
    "source_identifier": "HIPASSJ1303+07",
    "archive_name": "casda",
    "dataset_id": "SB12345_visibilities.ms.tar",
    "dataset_metadata": {
      "sbid": 12345,
      "visibility_filename": "SB12345_visibilities.ms.tar",
      "observation_date": "2000-01-00"
    }
  }')

RUN_ID=$(echo "$CREATE_RESPONSE" | jq -r '.uuid // empty')

if [ -z "$RUN_ID" ] || [ "$RUN_ID" = "null" ]; then
  echo "fail!"
  echo "Response: $CREATE_RESPONSE"
  exit 1
fi

echo "success"
echo "Run ID: $RUN_ID"
echo "$CREATE_RESPONSE" | jq '.'
echo ""

# Step 3: Get the created run
echo "GET /runs/${RUN_ID}"
GET_RESPONSE=$(curl -s -X GET "${BASE_URL}/runs/${RUN_ID}" \
  -H "Authorization: Bearer ${TOKEN}")

echo "retrieved"
echo "$GET_RESPONSE" | jq '.'
echo ""

# Step 4: List all runs
echo "GET /runs?page=1&items_per_page=10"
LIST_RESPONSE=$(curl -s -X GET "${BASE_URL}/runs?page=1&items_per_page=10" \
  -H "Authorization: Bearer ${TOKEN}")

echo "listed"
echo "$LIST_RESPONSE" | jq '.data | length as $count | "Total runs: \($count)"'
echo "$LIST_RESPONSE" | jq '.data[0] | {uuid, status, project_module, source_identifier}'
echo ""

# Step 5: List runs with filters
echo "Step 5: Listing runs with filters (project_module=wallaby, status=pending)..."
FILTERED_RESPONSE=$(curl -s -X GET "${BASE_URL}/runs?project_module=wallaby&status=pending&page=1&items_per_page=5" \
  -H "Authorization: Bearer ${TOKEN}")

echo "runs retrieved"
echo "$FILTERED_RESPONSE" | jq '.data | length as $count | "Filtered runs: \($count)"'
echo ""

# Step 6: Update run status to RUNNING
echo "PATCH /runs/${RUN_ID}"
UPDATE_RESPONSE=$(curl -s -X PATCH "${BASE_URL}/runs/${RUN_ID}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "running",
    "scheduler_name": "slurm",
    "scheduler_job_id": "12345",
    "workflow_type": "daluge",
    "workflow_manifest": {
      "workflow_name": "wallaby_12",
      "parameters": {
        "source": "HIPASSJ1303+07",
        "dataset": "SB12345_visibilities.ms.tar"
      }
    }
  }')

echo "updated"
echo "$UPDATE_RESPONSE" | jq '{uuid, status, scheduler_job_id, workflow_type, started_at}'
echo ""

# Step 7: Update run status to COMPLETED
echo "PATCH /runs/${RUN_ID}"
COMPLETE_RESPONSE=$(curl -s -X PATCH "${BASE_URL}/runs/${RUN_ID}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "completed"
  }')

echo "completed"
echo "$COMPLETE_RESPONSE" | jq '{uuid, status, started_at, completed_at}'
echo ""

# Step 7: Create another run for testing
echo "POST /runs]"
CREATE_RESPONSE2=$(curl -s -X POST "${BASE_URL}/runs" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "project_module": "wallaby",
    "source_identifier": "HIPASSJ1304+08",
    "archive_name": "casda",
    "dataset_id": "SB54321_visibilities.ms.tar",
    "dataset_metadata": {
      "sbid": 54321,
      "visibility_filename": "SB54321_visibilities.ms.tar"
    }
  }')

RUN_ID2=$(echo "$CREATE_RESPONSE2" | jq -r '.uuid // empty')
echo "run created: $RUN_ID2"
echo ""

# Step 4: List all runs
echo "GET /runs?page=1&items_per_page=10"
LIST_RESPONSE=$(curl -s -X GET "${BASE_URL}/runs?page=1&items_per_page=10" \
  -H "Authorization: Bearer ${TOKEN}")

echo "listed"
echo "$LIST_RESPONSE" | jq '.data | length as $count | "Total runs: \($count)"'
echo "$LIST_RESPONSE" | jq '.data[0] | {uuid, status, project_module, source_identifier}'
echo ""

# Step 9: Test idempotency - try to create the same run again
echo "POST /runs (creating duplicate run)..."
IDEMPOTENT_RESPONSE=$(curl -s -X POST "${BASE_URL}/runs" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "project_module": "wallaby",
    "source_identifier": "HIPASSJ1303+07",
    "archive_name": "casda",
    "dataset_id": "SB12345_visibilities.ms.tar"
  }')

IDEMPOTENT_RUN_ID=$(echo "$IDEMPOTENT_RESPONSE" | jq -r '.uuid // empty')

if [ "$IDEMPOTENT_RUN_ID" = "$RUN_ID" ]; then
  echo "test passed - returned existing run"
else
  echo "test - got different run ID"
fi
echo ""

# Step 10: Test invalid status transition
echo "Step 10: Testing invalid status transition (COMPLETED -> PENDING)..."
INVALID_TRANSITION=$(curl -s -X PATCH "${BASE_URL}/runs/${RUN_ID}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "pending"
  }')

ERROR_MESSAGE=$(echo "$INVALID_TRANSITION" | jq -r '.detail // empty')

if [ -n "$ERROR_MESSAGE" ]; then
  echo "transition correctly rejected"
  echo "Error: $ERROR_MESSAGE"
else
  echo "transition was not rejected"
fi
echo ""

# Step 11: List runs by status
echo "Step 11: Listing completed runs..."
COMPLETED_RESPONSE=$(curl -s -X GET "${BASE_URL}/runs?status=completed&page=1&items_per_page=10" \
  -H "Authorization: Bearer ${TOKEN}")

echo "runs retrieved"
echo "$COMPLETED_RESPONSE" | jq '.data | length as $count | "Completed runs: \($count)"'
echo ""

echo "=========================================="
echo "tests completed!"
echo "=========================================="
echo ""
echo "Summary:"
echo "  - Created runs: $RUN_ID, $RUN_ID2"
echo "  - Tested: Create, Read, Update, List, Filter, Idempotency, Status Validation"
echo ""

