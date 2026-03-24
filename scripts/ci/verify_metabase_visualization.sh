#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:3000}"
METABASE_USER_EMAIL="${METABASE_USER_EMAIL:-ci@test.com}"
ANALYTICS_DB_NAME="${ANALYTICS_DB_NAME:-analytics}"

required_vars=(METABASE_DB_PASSWORD ANALYTICS_DB_USER ANALYTICS_DB_PASSWORD)
for var_name in "${required_vars[@]}"; do
  if [ -z "${!var_name:-}" ]; then
    echo "Missing required environment variable: ${var_name}" >&2
    exit 1
  fi
done

curl_json() {
  curl -sS --fail --connect-timeout 5 --max-time 30 --retry 5 --retry-delay 2 --retry-connrefused "$@"
}

echo "Waiting for Metabase API session properties..."
PROPS=""
for i in $(seq 1 40); do
  PROPS=$(curl_json "$API_BASE/api/session/properties" 2>/dev/null || true)
  if [ -n "$PROPS" ]; then
    break
  fi
  echo "  session properties not ready (attempt $i/40)"
  sleep 3
done

if [ -z "$PROPS" ]; then
  echo "Failed to retrieve Metabase session properties"
  docker compose logs metabase --tail 100 || true
  exit 1
fi

echo "Setting up Metabase admin account..."
SETUP_TOKEN=$(echo "$PROPS" | python3 -c 'import json,sys; props=json.loads(sys.stdin.read() or "{}"); token=props.get("setup-token") if isinstance(props, dict) else ""; print(token if token else "")' 2>/dev/null || true)

if [ -n "$SETUP_TOKEN" ] && [ "$SETUP_TOKEN" != "None" ] && [ "$SETUP_TOKEN" != "null" ]; then
  echo "Running first-time Metabase setup..."
  SETUP_RESULT=$(curl -sS -X POST "$API_BASE/api/setup" \
    -H "Content-Type: application/json" \
    -d "{
      \"token\": \"$SETUP_TOKEN\",
      \"user\": {
        \"first_name\": \"CI\",
        \"last_name\": \"Test\",
        \"email\": \"$METABASE_USER_EMAIL\",
        \"password\": \"$METABASE_DB_PASSWORD\"
      },
      \"database\": {
        \"engine\": \"postgres\",
        \"name\": \"Analytics\",
        \"details\": {
          \"host\": \"analytics-db\",
          \"port\": 5432,
          \"dbname\": \"$ANALYTICS_DB_NAME\",
          \"user\": \"$ANALYTICS_DB_USER\",
          \"password\": \"$ANALYTICS_DB_PASSWORD\"
        }
      },
      \"prefs\": {
        \"site_name\": \"CI Test\",
        \"allow_tracking\": false
      }
    }")

  echo "$SETUP_RESULT" | python3 -c 'import json,sys; result=json.loads(sys.stdin.read() or "{}"); ok=(result.get("status")=="ok" or bool(result.get("id"))); ok and print("Metabase setup complete"); (not ok and "errors" in result) and print("Metabase setup validation errors: {}".format(result.get("errors"))); (not ok and "error" in result) and print("Metabase setup error: {}".format(result.get("error"))); sys.exit(0 if ok else 1)'
else
  echo "Metabase already set up"
fi

echo "Authenticating with Metabase..."
SESSION_TOKEN=""
SESSION_RESULT=""
for i in $(seq 1 30); do
  SESSION_RESULT=$(curl_json -X POST "$API_BASE/api/session" \
    -H "Content-Type: application/json" \
    -d "{\"username\": \"$METABASE_USER_EMAIL\", \"password\": \"$METABASE_DB_PASSWORD\"}" 2>/dev/null || true)

  SESSION_TOKEN=$(echo "$SESSION_RESULT" | python3 -c 'import json,sys; payload=json.loads(sys.stdin.read() or "{}"); print(payload.get("id", "") if isinstance(payload, dict) else "")' 2>/dev/null || true)

  if [ -n "$SESSION_TOKEN" ]; then
    break
  fi

  echo "  login not ready (attempt $i/30)"
  sleep 2
done

if [ -z "$SESSION_TOKEN" ]; then
  echo "Failed to authenticate with Metabase"
  echo "Last login response: ${SESSION_RESULT:-<empty>}"
  docker compose logs metabase --tail 100 || true
  exit 1
fi
echo "Session token acquired"

echo "Checking for analytics database..."
DATABASES_JSON=$(curl_json "$API_BASE/api/database" \
  -H "X-Metabase-Session: $SESSION_TOKEN")

HAS_ANALYTICS_DB=$(echo "$DATABASES_JSON" | python3 -c 'import json,sys; raw=json.loads(sys.stdin.read() or "[]"); dbs=raw if isinstance(raw, list) else raw.get("data", raw.get("databases", [])); dbs=[dbs] if isinstance(dbs, dict) else dbs; print("yes" if any(isinstance(d, dict) and d.get("engine")=="postgres" and d.get("name")=="Analytics" for d in dbs) else "no")')

if [ "$HAS_ANALYTICS_DB" != "yes" ]; then
  echo "Adding analytics database to Metabase..."
  curl_json -X POST "$API_BASE/api/database" \
    -H "Content-Type: application/json" \
    -H "X-Metabase-Session: $SESSION_TOKEN" \
    -d "{
      \"engine\": \"postgres\",
      \"name\": \"Analytics\",
      \"details\": {
        \"host\": \"analytics-db\",
        \"port\": 5432,
        \"dbname\": \"$ANALYTICS_DB_NAME\",
        \"user\": \"$ANALYTICS_DB_USER\",
        \"password\": \"$ANALYTICS_DB_PASSWORD\",
        \"ssl\": false
      }
    }" > /dev/null
  echo "Analytics database added"
else
  echo "Analytics database already registered"
fi

echo "Waiting for Metabase to sync database..."
sleep 20

DB_ID=$(curl_json "$API_BASE/api/database" \
  -H "X-Metabase-Session: $SESSION_TOKEN" | python3 -c 'import json,sys; raw=json.loads(sys.stdin.read() or "[]"); dbs=raw if isinstance(raw, list) else raw.get("data", raw.get("databases", [])); dbs=[dbs] if isinstance(dbs, dict) else dbs; print(next((str(d.get("id", "")) for d in dbs if isinstance(d, dict) and d.get("engine")=="postgres" and d.get("name")=="Analytics"), ""))')

if [ -z "$DB_ID" ]; then
  echo "Failed to resolve Analytics database ID in Metabase"
  exit 1
fi

echo "Using database ID: $DB_ID"
QUERY_RESULT=$(curl_json -X POST "$API_BASE/api/dataset" \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION_TOKEN" \
  -d "{
    \"database\": $DB_ID,
    \"type\": \"native\",
    \"native\": {
      \"query\": \"SELECT COUNT(*) as total_rows, SUM(total_amount) as total_revenue, COUNT(DISTINCT country) as countries FROM sales\"
    },
    \"parameters\": []
  }")

echo "$QUERY_RESULT" | python3 -c 'import json,sys; result=json.loads(sys.stdin.read() or "{}"); "error" in result and (print("Metabase query error: {}".format(result.get("error"))), sys.exit(1)); rows=result.get("data", {}).get("rows", []); rows or (print("VISUALIZATION CHECK FAILED: no data returned from Metabase"), sys.exit(1)); total_rows,total_revenue,countries=rows[0]; print("  Total rows:    {}".format(total_rows)); print("  Total revenue: {}".format(total_revenue)); print("  Countries:     {}".format(countries)); ok=int(total_rows)>0; print("VISUALIZATION CHECK PASSED: Metabase can query sales data" if ok else "VISUALIZATION CHECK FAILED: Metabase returned 0 rows"); sys.exit(0 if ok else 1)'
