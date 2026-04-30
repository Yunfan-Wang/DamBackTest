package dambt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"log"
)

type Coordinator struct {
	Addr string

	mu      sync.Mutex
	chunks  map[ChunkID]ChunkMeta
	jobs    map[JobID]JobSpec
	status  map[JobID]JobStatus
	results map[JobID]JobResult

	workers []string
	jobQueue chan JobSpec
	// failure recovery module
	wal            *WAL
	checkpointPath string
}

func NewCoordinator(addr string, workers []string) *Coordinator {
	c := &Coordinator{
		Addr:     addr,
		chunks:   make(map[ChunkID]ChunkMeta),
		jobs:     make(map[JobID]JobSpec),
		status:   make(map[JobID]JobStatus),
		results:  make(map[JobID]JobResult),
		workers:  workers,
		jobQueue: make(chan JobSpec, 1024),
		wal:            NewWAL("./dambt.wal"),
		checkpointPath: "./dambt.checkpoint.json",
	}
	_ = c.recoverState()
	return c

}

func (c *Coordinator) Start() error {
	go c.schedulerLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/register_chunk", c.handleRegisterChunk)
	mux.HandleFunc("/submit_job", c.handleSubmitJob)
	mux.HandleFunc("/job_status", c.handleJobStatus)
	mux.HandleFunc("/job_result", c.handleJobResult)

	return http.ListenAndServe(c.Addr, mux)
}

func (c *Coordinator) handleRegisterChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req RegisterChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	chunk := req.Chunk
	if chunk.ChunkID == "" {
		WriteError(w, http.StatusBadRequest, "missing chunk_id")
		return
	}
	if len(chunk.Replicas) == 0 {
		WriteError(w, http.StatusBadRequest, "chunk must have at least one replica")
		return
	}

	if err := c.wal.Append(WALEntry{
		Type:  "REGISTER_CHUNK",
		Chunk: &chunk,
	}); err != nil {
		WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	c.mu.Lock()
	c.chunks[chunk.ChunkID] = chunk
	_ = c.checkpointLocked()
	c.mu.Unlock()
	
	WriteJSON(w, http.StatusOK, map[string]any{
		"success":  true,
		"chunk_id": chunk.ChunkID,
	})
}

func (c *Coordinator) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	if req.Instrument == "" {
		WriteError(w, http.StatusBadRequest, "missing instrument")
		return
	}
	if req.EndTs < req.StartTs {
		WriteError(w, http.StatusBadRequest, "invalid time range")
		return
	}

	job := JobSpec{
		JobID:      NewJobID(),
		Instrument: req.Instrument,
		StartTs:    req.StartTs,
		EndTs:      req.EndTs,
		Strategy:   req.Strategy,
	}

	if err := c.wal.Append(WALEntry{
		Type: "SUBMIT_JOB",
		Job:  &job,
	}); err != nil {
		WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	c.mu.Lock()
	c.jobs[job.JobID] = job
	c.status[job.JobID] = JobQueued
	_ = c.checkpointLocked()
	c.mu.Unlock()

	c.jobQueue <- job

	WriteJSON(w, http.StatusOK, SubmitJobResponse{
		JobID: job.JobID,
	})
}

func (c *Coordinator) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := JobID(r.URL.Query().Get("id"))
	if jobID == "" {
		WriteError(w, http.StatusBadRequest, "missing job id")
		return
	}

	c.mu.Lock()
	status, ok := c.status[jobID]
	c.mu.Unlock()

	if !ok {
		WriteError(w, http.StatusNotFound, "job not found")
		return
	}

	WriteJSON(w, http.StatusOK, JobStatusResponse{
		JobID:  jobID,
		Status: status,
	})
}

func (c *Coordinator) handleJobResult(w http.ResponseWriter, r *http.Request) {
	jobID := JobID(r.URL.Query().Get("id"))
	if jobID == "" {
		WriteError(w, http.StatusBadRequest, "missing job id")
		return
	}

	c.mu.Lock()
	result, ok := c.results[jobID]
	status := c.status[jobID]
	c.mu.Unlock()

	if !ok {
		if status == JobQueued || status == JobRunning {
			WriteError(w, http.StatusAccepted, "job not finished")
			return
		}
		WriteError(w, http.StatusNotFound, "result not found")
		return
	}

	WriteJSON(w, http.StatusOK, result)
}

func (c *Coordinator) schedulerLoop() {
	// workerIndex := 0

	for job := range c.jobQueue {
		running := JobRunning
		_ = c.wal.Append(WALEntry{
			Type:   "JOB_STATUS",
			JobID:  job.JobID,
			Status: &running,
		})
		log.Printf("[coordinator] picked job=%s", job.JobID)
		c.mu.Lock()
		c.status[job.JobID] = JobRunning
		chunks := c.findChunksLocked(job)
		c.mu.Unlock()
		log.Printf("[coordinator] job=%s matched chunks=%d", job.JobID, len(chunks))
		if len(chunks) == 0 {
			c.markJobFailed(job.JobID)
			continue
		}

		if len(c.workers) == 0 {
			c.markJobFailed(job.JobID)
			continue
		}

		// worker := c.workers[workerIndex%len(c.workers)]
		// workerIndex++

		// result, err := runJobOnWorker(worker, job, chunks)
		// if err != nil {
		// 	c.markJobFailed(job.JobID)
		// 	continue
		// }

		//
		// result, nextWorkerIndex, err := c.runJobWithRetry(workerIndex, job, chunks)
		// workerIndex = nextWorkerIndex

		// if err != nil {
		// 	c.markJobFailed(job.JobID)
		// 	continue
		// }
		log.Printf("[coordinator] dispatching job=%s to workers=%v", job.JobID, c.workers)
		result, err := c.runJobParallel(job, chunks)
		if err != nil {
			c.markJobFailed(job.JobID)
			continue
		}
		
		_ = c.wal.Append(WALEntry{
			Type:   "JOB_DONE",
			Result: &result,
		})
		c.mu.Lock()
		c.results[job.JobID] = result
		c.status[job.JobID] = JobDone
		_ = c.checkpointLocked()
		c.mu.Unlock()
		log.Printf("[coordinator] job=%s done events=%d pnl=%.4f", job.JobID, result.EventsRead, result.PnL)
	}
}

func (c *Coordinator) findChunksLocked(job JobSpec) []ChunkMeta {
	out := []ChunkMeta{}

	for _, chunk := range c.chunks {
		if ChunkOverlaps(chunk, job.Instrument, job.StartTs, job.EndTs) {
			out = append(out, chunk)
		}
	}

	return out
}

func (c *Coordinator) markJobFailed(jobID JobID) {
	failed := JobFailed
	_ = c.wal.Append(WALEntry{
		Type:   "JOB_STATUS",
		JobID:  jobID,
		Status: &failed,
	})

	c.mu.Lock()
	c.status[jobID] = JobFailed
	_ = c.checkpointLocked()
	c.mu.Unlock()
	log.Printf("[coordinator] job=%s FAILED", jobID)
}

func runJobOnWorker(workerAddr string, job JobSpec, chunks []ChunkMeta) (JobResult, error) {
	req := RunJobRequest{
		Job:    job,
		Chunks: chunks,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return JobResult{}, err
	}

	url := fmt.Sprintf("http://%s/run_job", workerAddr)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return JobResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return JobResult{}, fmt.Errorf("worker returned status %d", resp.StatusCode)
	}

	var out RunJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return JobResult{}, err
	}

	return out.Result, nil
}


// failure recovery functions
func (c *Coordinator) recoverState() error {
	cp, err := LoadCheckpoint(c.checkpointPath)
	if err != nil {
		return err
	}

	if cp.Chunks != nil {
		c.chunks = cp.Chunks
	}
	if cp.Jobs != nil {
		c.jobs = cp.Jobs
	}
	if cp.Status != nil {
		c.status = cp.Status
	}
	if cp.Results != nil {
		c.results = cp.Results
	}

	entries, err := c.wal.Load()
	if err != nil {
		return err
	}

	for _, e := range entries {
		c.applyWALEntry(e)
	}

	for jobID, st := range c.status {
		if st == JobRunning || st == JobQueued {
			job := c.jobs[jobID]
			c.status[jobID] = JobQueued
			c.jobQueue <- job
		}
	}

	return nil
}

func (c *Coordinator) applyWALEntry(e WALEntry) {
	switch e.Type {
	case "REGISTER_CHUNK":
		if e.Chunk != nil {
			c.chunks[e.Chunk.ChunkID] = *e.Chunk
		}
	case "SUBMIT_JOB":
		if e.Job != nil {
			c.jobs[e.Job.JobID] = *e.Job
			c.status[e.Job.JobID] = JobQueued
		}
	case "JOB_STATUS":
		if e.Status != nil {
			c.status[e.JobID] = *e.Status
		}
	case "JOB_DONE":
		if e.Result != nil {
			c.results[e.Result.JobID] = *e.Result
			c.status[e.Result.JobID] = JobDone
		}
	}
}

func (c *Coordinator) checkpointLocked() error {
	cp := CoordinatorCheckpoint{
		Chunks:  c.chunks,
		Jobs:    c.jobs,
		Status:  c.status,
		Results: c.results,
	}
	return SaveCheckpoint(c.checkpointPath, cp)
}


// func (w *WAL) Reset() error {
// 	return os.WriteFile(w.path, []byte{}, 0644)
// }


// workfailure failure n retry helpers

func (c *Coordinator) runJobWithRetry(startIndex int, job JobSpec, chunks []ChunkMeta) (JobResult, int, error) {
	var lastErr error
	for attempt := 0; attempt < len(c.workers); attempt++ {
		idx := (startIndex + attempt) % len(c.workers)
		worker := c.workers[idx]

		result, err := runJobOnWorker(worker, job, chunks)
		if err == nil {
			return result, idx + 1, nil
		}
		lastErr = err
	}
	return JobResult{}, startIndex, fmt.Errorf("all workers failed, last error: %v", lastErr)
}


/**
Parallel execution helpers
*/

func splitChunks(chunks []ChunkMeta, n int) [][]ChunkMeta {
	if n <= 0 {
		return nil
	}

	out := make([][]ChunkMeta, n)

	for i, chunk := range chunks {
		idx := i % n
		out[idx] = append(out[idx], chunk)
	}

	nonEmpty := [][]ChunkMeta{}
	for _, group := range out {
		if len(group) > 0 {
			nonEmpty = append(nonEmpty, group)
		}
	}

	return nonEmpty
}

func (c *Coordinator) runJobParallel(job JobSpec, chunks []ChunkMeta) (JobResult, error) {
	if len(c.workers) == 0 {
		return JobResult{}, fmt.Errorf("no workers available")
	}

	parallelism := len(c.workers)
	if len(chunks) < parallelism {
		parallelism = len(chunks)
	}

	groups := splitChunks(chunks, parallelism)
	log.Printf("[coordinator] job=%s parallel groups=%d", job.JobID, len(groups))
	var wg sync.WaitGroup
	resultCh := make(chan JobResult, len(groups))
	errCh := make(chan error, len(groups))

	for i, group := range groups {
		startIndex := (i + 1) % len(c.workers)

		wg.Add(1)
		go func(chunkGroup []ChunkMeta, start int) {
			defer wg.Done()

			result, _, err := c.runJobWithRetry(start, job, chunkGroup)
			if err != nil {
				errCh <- err
				return
			}

			resultCh <- result
		}(group, startIndex)
	}

	wg.Wait()
	close(resultCh)
	close(errCh)

	if len(errCh) > 0 {
		return JobResult{}, <-errCh
	}

	parts := []JobResult{}
	for r := range resultCh {
		parts = append(parts, r)
	}

	return MergeResults(job, parts), nil
}
