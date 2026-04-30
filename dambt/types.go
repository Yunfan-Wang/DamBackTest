package dambt

type ChunkID string
type JobID string

type MarketEvent struct {
	Timestamp int64   `json:"timestamp"`
	Instrument string `json:"instrument"`
	Price     float64 `json:"price"`
	Volume    int64   `json:"volume"`
}

type ChunkMeta struct {
	ChunkID    ChunkID  `json:"chunk_id"`
	Instrument string   `json:"instrument"`
	StartTs    int64    `json:"start_ts"`
	EndTs      int64    `json:"end_ts"`
	Replicas   []string `json:"replicas"`
	Version    uint64   `json:"version"`
	Sealed     bool     `json:"sealed"`
}

type JobSpec struct {
	JobID      JobID  `json:"job_id"`
	Instrument string `json:"instrument"`
	StartTs    int64  `json:"start_ts"`
	EndTs      int64  `json:"end_ts"`
	Strategy   string `json:"strategy"`
}

type JobStatus string

const (
	JobQueued  JobStatus = "queued"
	JobRunning JobStatus = "running"
	JobDone    JobStatus = "done"
	JobFailed  JobStatus = "failed"
)

type JobResult struct {
	JobID      JobID    `json:"job_id"`
	EventsRead int     `json:"events_read"`
	PnL        float64 `json:"pnl"`
	Logs       []string `json:"logs"`
}

type RegisterChunkRequest struct {
	Chunk ChunkMeta `json:"chunk"`
}

type SubmitJobRequest struct {
	Instrument string `json:"instrument"`
	StartTs    int64  `json:"start_ts"`
	EndTs      int64  `json:"end_ts"`
	Strategy   string `json:"strategy"`
}

type SubmitJobResponse struct {
	JobID JobID `json:"job_id"`
}

type JobStatusResponse struct {
	JobID  JobID     `json:"job_id"`
	Status JobStatus `json:"status"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}