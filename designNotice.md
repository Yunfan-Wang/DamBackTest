1. Wal order v state update:
coordinator.go has:
schedulerLoop:
    _ = c.wal.Append(... JobRunning)
    c.mu.Lock()
    c.status[job.JobID] = JobRunning
Always follow: WAL -> state -> checkpoint
Keep it consistent.

