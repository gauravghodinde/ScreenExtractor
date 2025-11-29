#!/usr/bin/env bash
set -euo pipefail

# Integration test: POST /jobs then poll Postgres until status == done (or failed)
# Usage: bash infra/tests/integration_test_wait_done.sh [timeout_seconds]

TIMEOUT=${1:-300}
SLEEP=2
ORCH_URL=${ORCH_URL:-http://localhost:8080}
PG_CONTAINER=${PG_CONTAINER:-infra-postgres-1}
PG_USER=${PG_USER:-scene_user}
PG_DB=${PG_DB:-scene_seeker}

BODY='{"video_path":"s3://scene-uploads/video.mkv","start":1,"end":3,"query":"integration done test"}'

echo "Posting job to $ORCH_URL/jobs"
RESP=$(curl -sS -X POST "$ORCH_URL/jobs" -H "Content-Type: application/json" -d "$BODY" )
if [[ -z "$RESP" ]]; then
  echo "Empty response from orchestrator"
  exit 2
fi

# extract id from JSON like {"id":"..."}
JOB_ID=$(echo "$RESP" | sed -E 's/.*"id"\s*:\s*"([^\"]+)".*/\1/')
if [[ -z "$JOB_ID" ]]; then
  echo "Failed to parse job id from response: $RESP"
  exit 2
fi

echo "Enqueued job id: $JOB_ID"

START_TS=$(date +%s)
while true; do
  NOW=$(date +%s)
  ELAPSED=$((NOW-START_TS))
  if (( ELAPSED > TIMEOUT )); then
    echo "Timed out after ${TIMEOUT}s waiting for job to complete"
    echo "--- worker logs (last 200 lines) ---"
    docker logs --tail 200 infra-worker-1 || true
    exit 1
  fi

  STATUS=$(docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -t -A -c "select status from jobs where id='$JOB_ID';" || echo "")
  STATUS=$(echo "$STATUS" | tr -d '\r' | tr -d '\n' | xargs || true)
  if [[ -z "$STATUS" ]]; then
    echo "Job $JOB_ID not yet visible in Postgres (elapsed ${ELAPSED}s)"
  else
    echo "Job $JOB_ID status: $STATUS"
    if [[ "$STATUS" == "done" ]]; then
      echo "Job completed: $STATUS"
      echo "--- worker logs (last 200 lines) ---"
      docker logs --tail 200 infra-worker-1 || true
      exit 0
    elif [[ "$STATUS" == "failed" ]]; then
      echo "Job failed"
      echo "--- worker logs (last 200 lines) ---"
      docker logs --tail 200 infra-worker-1 || true
      exit 2
    fi
  fi

  sleep $SLEEP
done
