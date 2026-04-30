SendChunk
curl -X POST http://127.0.0.1:9101/put_chunk   -H "Content-Type: application/json"   -d @chunk1.json

curl -X POST http://127.0.0.1:9101/put_chunk   -H "Content-Type: application/json"   -d @chunk2.json

Register
curl -X POST http://127.0.0.1:9000/register_chunk \
  -H "Content-Type: application/json" \
  -d @register1.json

curl -X POST http://127.0.0.1:9000/register_chunk \
  -H "Content-Type: application/json" \
  -d @register2.json


submit:

curl -X POST http://127.0.0.1:9000/submit_job \
  -H "Content-Type: application/json" \
  -d '{
    "instrument": "AAPL",
    "start_ts": 1,
    "end_ts": 4,
    "strategy": "momentum"
  }'