SendChunk
go run ../../cmd/put_chunk_rpc --addr 127.0.0.1:9401 --file chunk1.json
go run ../../cmd/put_chunk_rpc --addr 127.0.0.1:9401 --file chunk2.json

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