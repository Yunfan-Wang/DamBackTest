1. send chunk
curl -X POST http://127.0.0.1:9101/put_chunk \
  -H "Content-Type: application/json" \
  -d @chunk.json


{"chunk_id":"chunk1","success":true}

2. register chunk to coordinator
curl -X POST http://127.0.0.1:9000/register_chunk   -H "Content-Type: application/json"   -d @register.json

{"chunk_id":"chunk1","success":true}

3. Submit a job
$ curl -X POST http://127.0.0.1:9000/submit_job \
  -H "Content-Type: application/json" \
  -d '{
    "instrument": "AAPL",
    "start_ts": 1,
    "end_ts": 3,
    "strategy": "momentum"
  }'

{"job_id":"job-1777532176074640700"}

4. Check status
curl "http://127.0.0.1:9000/job_status?id=job-xxxxx"

5. Get Result:
curl "http://127.0.0.1:9000/job_result?id=job-xxxxx"

Testing failure recovery:
Having done above-> kill coordinator then restart-> check status, result again