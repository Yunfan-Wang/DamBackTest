#!/usr/bin/env bash
set -euo pipefail

COORD="http://127.0.0.1:9000"
DATANODE="http://127.0.0.1:9101"

echo "1. Put chunk"
curl -s -X POST "$DATANODE/put_chunk" \
  -H "Content-Type: application/json" \
  -d @chunk.json
echo

echo "2. Register chunk"
curl -s -X POST "$COORD/register_chunk" \
  -H "Content-Type: application/json" \
  -d @register.json
echo

echo "3. Submit job"
SUBMIT_RESP=$(curl -s -X POST "$COORD/submit_job" \
  -H "Content-Type: application/json" \
  -d '{
    "instrument": "AAPL",
    "start_ts": 1,
    "end_ts": 3,
    "strategy": "momentum"
  }')

echo "$SUBMIT_RESP"

JOB_ID=$(echo "$SUBMIT_RESP" | sed -E 's/.*"job_id":"([^"]+)".*/\1/')

echo "JOB_ID=$JOB_ID"
echo

sleep 1

echo "4. Check status"
curl -s "$COORD/job_status?id=$JOB_ID"
echo

echo "5. Get result"
curl -s "$COORD/job_result?id=$JOB_ID"
echo

echo
echo "Now manually kill coordinator, restart it, then press Enter to test recovery..."
read -r

echo "6. Check status after coordinator restart"
curl -s "$COORD/job_status?id=$JOB_ID"
echo

echo "7. Get result after coordinator restart"
curl -s "$COORD/job_result?id=$JOB_ID"
echo

echo
echo "Manual test complete."