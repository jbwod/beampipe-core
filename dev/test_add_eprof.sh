
API_BASE="${BEAMPIPE_API_BASE:-http://127.0.0.1:8000}"
ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-Str1ngst!}"

TM_URL="${E2E_TM_URL:-http://dlg-tm.desk}"
DIM_HOST_FOR_TM="${E2E_DIM_HOST_FOR_TM:-dlg-dim}"
DIM_PORT_FOR_TM="${E2E_DIM_PORT_FOR_TM:-8001}"
DEPLOY_HOST="${E2E_DEPLOY_HOST:-dlg-dim.desk}"
DEPLOY_PORT="${E2E_DEPLOY_PORT:-80}"

PROFILE_NAME="${E2E_DEPLOYMENT_PROFILE_NAME:-test-staging-e2e-rest-dim}"
PROJECT_MODULE="${E2E_PROJECT_MODULE:-wallaby_hires}"
VERIFY_SSL="${E2E_VERIFY_SSL:-false}"

login_json="$(
  curl -sS -X POST "${API_BASE}/api/v1/login" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode "username=${ADMIN_USERNAME}" \
    --data-urlencode "password=${ADMIN_PASSWORD}"
)"

token="$(
  python3 -c 'import json,sys; print(json.load(sys.stdin).get("access_token") or "")' <<<"${login_json}"
)"

if [[ -z "${token}" ]]; then
  echo "Login failed (no access_token). Response:" >&2
  echo "${login_json}" >&2
  exit 1
fi

export PROFILE_NAME PROJECT_MODULE TM_URL DIM_HOST_FOR_TM DIM_PORT_FOR_TM DEPLOY_HOST DEPLOY_PORT VERIFY_SSL
body="$(
  python3 -c "
import json
import os
p = {
    'name': os.environ['PROFILE_NAME'],
    'description': 'rest_dim',
    'project_module': os.environ['PROJECT_MODULE'],
    'is_default': True,
    'deployment_backend': 'rest_dim',
    'deployment_config': {'kind': 'rest_dim'},
    'algo': 'metis',
    'num_par': 1,
    'num_islands': 0,
    'tm_url': os.environ['TM_URL'],
    'dim_host_for_tm': os.environ['DIM_HOST_FOR_TM'],
    'dim_port_for_tm': int(os.environ['DIM_PORT_FOR_TM']),
    'deploy_host': os.environ['DEPLOY_HOST'],
    'deploy_port': int(os.environ['DEPLOY_PORT']),
    'verify_ssl': os.environ['VERIFY_SSL'].lower() in ('1', 'true', 'yes'),
}
print(json.dumps(p))
"
)"

echo "POST ${API_BASE}/api/v1/deployment-profiles (name=${PROFILE_NAME}, project_module=${PROJECT_MODULE}, dim_host_for_tm=${DIM_HOST_FOR_TM}:${DIM_PORT_FOR_TM}, deploy=${DEPLOY_HOST}:${DEPLOY_PORT})" >&2

resp="$(
  curl -sS -w '\n%{http_code}' -X POST "${API_BASE}/api/v1/deployment-profiles" \
    -H "Authorization: Bearer ${token}" \
    -H 'Content-Type: application/json' \
    -d "${body}"
)"

http_code="$(echo "${resp}" | tail -n1)"
payload="$(echo "${resp}" | sed '$d')"

echo "${payload}" | python3 -m json.tool 2>/dev/null || echo "${payload}"

if [[ "${http_code}" != "201" ]]; then
  echo "HTTP ${http_code} (expected 201). If profile already exists, pick another E2E_EXECUTION_PROFILE_NAME or delete the row." >&2
  exit 1
fi

echo "Created execution profile. Set wallaby_hires WORKFLOW_RUN_AUTOMATION['execution_profile_id'] to the returned uuid if needed." >&2