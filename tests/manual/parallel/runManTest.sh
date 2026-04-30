#!/usr/bin/env bash
set -euo pipefail

COORD="http://127.0.0.1:9000"
DATANODE="http://127.0.0.1:9101"

echo "1. Send chunk1"
curl -s -X POST "$DATANODE/put_chunk" \
  -H "Content-Type: application/json" \
  -d @chunk1.json
echo

echo "2. Send chunk2"
curl -s -X POST "$DATANODE/put_chunk" \
  -H "Content-Type: application/json" \
  -d @chunk2.json
echo

echo "3. Register chunk1"
curl -s -X POST "$COORD/register_chunk" \
  -H "Content-Type: application/json" \
  -d @register1.json
echo

echo "4. Register chunk2"
curl -s -X POST "$COORD/register_chunk" \
  -H "Content-Type: application/json" \
  -d @register2.json
echo

echo "5. Submit parallel job"
SUBMIT_RESP=$(curl -s -X POST "$COORD/submit_job" \
  -H "Content-Type: application/json" \
  -d '{
    "instrument": "AAPL",
    "start_ts": 1,
    "end_ts": 4,
    "strategy": "momentum"
  }')

echo "$SUBMIT_RESP"

JOB_ID=$(echo "$SUBMIT_RESP" | sed -E 's/.*"job_id":"([^"]+)".*/\1/')
echo "JOB_ID=$JOB_ID"
echo

sleep 1

echo "6. Check status"
curl -s "$COORD/job_status?id=$JOB_ID"
echo

echo "7. Get result"
curl -s "$COORD/job_result?id=$JOB_ID"
echo