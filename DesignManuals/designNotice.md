1. Wal order v state update:
coordinator.go has:
schedulerLoop:
    _ = c.wal.Append(... JobRunning)
    c.mu.Lock()
    c.status[job.JobID] = JobRunning
Always follow: WAL -> state -> checkpoint
Keep it consistent.

2. RPC DataNode address:
"127.0.0.1:9401"
And:
Coordinator HTTP:
9000, 9010, 9020

Raft RPC:
8001, 8002, 8003

Raft control:
8101, 8102, 8103

3. 

