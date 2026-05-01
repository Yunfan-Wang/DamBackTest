package dambt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"log"
	"remote"
	"raft"
	"time"
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
	// wal            *WAL
	// checkpointPath string
	// raft
	rf      *raft.RaftPeer
	applyCh <-chan raft.ApplyMsg
	pending map[int]chan struct{}
	applied map[int]bool

}

// * NewCoordinator -- initializes a Coordinator instance with Raft-backed metadata replication.
//
// This sets up:
//   in memory metadata state (chunks, jobs, status, results)
//   Raft peer for replicated state machine
//   Then apply channel for committed log entries and do
//   
// Behavior:
//   If raftPeers / ctrlPeers are not provided, defaults are used.
//   Raft is immediately activated before returning.
//
// Limitations / potential failure scenarios:
//   1. Assumes peer lists are correctly aligned (same length); panics otherwise.
//   2. Does not validate network reachability of peers at initialization.
//   3. Coordinator state is in-memory; durability depends entirely on Raft.
func NewCoordinator(addr string, workers []string, id int, raftPeers []string, ctrlPeers []string) *Coordinator {
	if len(raftPeers) == 0 {
		raftPeers = []string{
			"127.0.0.1:8001",
			"127.0.0.1:8002",
			"127.0.0.1:8003",
		}
	}

	if len(ctrlPeers) == 0 {
		ctrlPeers = []string{
			"127.0.0.1:8101",
			"127.0.0.1:8102",
			"127.0.0.1:8103",
		}
	}

	if len(raftPeers) != len(ctrlPeers) {
		panic("raftPeers and ctrlPeers must have same len")
	}

	peerInfo := make([]raft.RaftSetupInfo, 0, len(raftPeers))
	for i := range raftPeers {
		peerInfo = append(peerInfo, raft.RaftSetupInfo{
			Id:    i,
			Addr:  raftPeers[i],
			Caddr: ctrlPeers[i],
		})
	}

	rf := raft.NewRaftPeer(peerInfo, id)
	_ = rf.Activate()

	applyCh := rf.ApplyChan()

	return &Coordinator{
		Addr:     addr,
		chunks:   make(map[ChunkID]ChunkMeta),
		jobs:     make(map[JobID]JobSpec),
		status:   make(map[JobID]JobStatus),
		results:  make(map[JobID]JobResult),
		workers:  workers,
		jobQueue: make(chan JobSpec, 1024),

		applyCh: applyCh,
		rf:      rf,

		pending: make(map[int]chan struct{}),
		applied: make(map[int]bool),
	}
}

// * Start -- starts background loops and exposes HTTP endpoints for the coordinator.
//
// Behaviors:
// Launches the Raft apply loop and scheduler loop, then starts an HTTP server
// providing endpoints for chunk registration, job submission, and result queries.
//
// Limitations / potential failure scenarios:
// 1. Blocking call; does not support graceful shutdown.
// 2. Background goroutines are not terminated if server fails.
// 3. There are no health checks or restart mechanisms.
func (c *Coordinator) Start() error {
	go c.raftApplyLoop()
	go c.schedulerLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/register_chunk", c.handleRegisterChunk)
	mux.HandleFunc("/submit_job", c.handleSubmitJob)
	mux.HandleFunc("/job_status", c.handleJobStatus)
	mux.HandleFunc("/job_result", c.handleJobResult)

	return http.ListenAndServe(c.Addr, mux)
}

// * handleRegisterChunk - registers chunk metadata via a replicated command.
//
// Behaviors:
// Validates incoming chunk metadata and submits a Raft command to ensure
// consistent replication of chunk state across coordinators.
//
// Limitations / potential failure scenarios:
// 1. Rejects invalid chunk definitions (missing ID or replicas).
// 2. Fails if current node is not Raft leader.
// 3. No deduplication or version control for repeated registrations.
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

	// if err := c.wal.Append(WALEntry{
	// 	Type:  "REGISTER_CHUNK",
	// 	Chunk: &chunk,
	// }); err != nil {
	// 	WriteError(w, http.StatusInternalServerError, err.Error())
	// 	return
	// }
	//obsolete direct mutation
	// c.mu.Lock()
	// c.chunks[chunk.ChunkID] = chunk
	// _ = c.checkpointLocked()
	// c.mu.Unlock()

	if err := c.submitMetadataCommand(DAMBTCommand{
		Op:    CmdRegisterChunk,
		Chunk: &chunk,
	}); err != nil {
		WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	
	WriteJSON(w, http.StatusOK, map[string]any{
		"success":  true,
		"chunk_id": chunk.ChunkID,
	})
}

// * handleSubmitJob - submits a new backtesting job into the system.
//
// Behaviors:
// Validates request parameters, constructs a JobSpec, and submits it
// through Raft so that job creation is replicated and consistent.
//
// Limitations / potential failure scenarios:
// 1. Rejects invalid time ranges or missing fields.
// 2. Fails if current node is not Raft leader.
// 3. Does not validate correctness of strategy logic.
func (c *Coordinator) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, http.StatusMethodNotAllowed, "method is not allowed")
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

	// if err := c.wal.Append(WALEntry{
	// 	Type: "SUBMIT_JOB",
	// 	Job:  &job,
	// }); err != nil {
	// 	WriteError(w, http.StatusInternalServerError, err.Error())
	// 	return
	// }
	if err := c.submitMetadataCommand(DAMBTCommand{
		Op:  CmdSubmitJob,
		Job: &job,
	}); err != nil {
		WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// obsolete direct mutation + queue
	// c.mu.Lock()
	// c.jobs[job.JobID] = job
	// c.status[job.JobID] = JobQueued
	// _ = c.checkpointLocked()
	// c.mu.Unlock()

	// c.jobQueue <- job

	WriteJSON(w, http.StatusOK, SubmitJobResponse{
		JobID: job.JobID,
	})
}

// * handleJobStatus - returns the current status of a job.
//
// Behaviors:
// Reads replicated state to return the latest job status,
// including queued, running, completed, or failed states.
//
// Limitations / potential failure scenarios:
// 1. Returns 404 if job does not exist.
// 2. No progress granularity beyond coarse status.
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
// * handleJobResult - retrieves the final result of a completed job.
//
// Behaviors:
// Returns stored job results if available, or indicates if the job
// is still running or has not produced output.
//
// Limitations / potential failure scenarios:
// 1. Returns 202 if job is not finished.
// 2. Returns 404 if result is missing after completion.
// 3. Cannot stream partial or intermediate results.
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
// * schedulerLoop - continuously schedules and dispatches jobs to workers.
//
// Behaviors:
// Consumes queued jobs, ensures leader only execution, updates job status,
// assigns chunks, and executes jobs in parallel across workers.
//
// Limitations / potential failure scenarios:
// 1. Only leader schedules jobs; followers remain idle.
// 2. No load balancing or prioritization strategy.
// 3. Jobs fail entirely if worker execution fails.
func (c *Coordinator) schedulerLoop() {
	// workerIndex := 0

	for job := range c.jobQueue {
		if !c.rf.IsLeader() {
			log.Printf("[coordinator] skip job=%s because not leader", job.JobID)
			continue
		}
		// running := JobRunning
		// _ = c.wal.Append(WALEntry{
		// 	Type:   "JOB_STATUS",
		// 	JobID:  job.JobID,
		// 	Status: &running,
		// })
		c.mu.Lock()
		st := c.status[job.JobID]
		c.mu.Unlock()
		if st == JobRunning || st == JobDone || st == JobFailed {
			log.Printf("[coordinator] skip duplicate job=%s status=%s", job.JobID, st)
			continue
		}
		log.Printf("[coordinator] picked job=%s", job.JobID)
		// obsolete scheduler status updates
		// c.mu.Lock()
		// c.status[job.JobID] = JobRunning
		// chunks := c.findChunksLocked(job)
		// c.mu.Unlock()
		_ = c.submitMetadataCommand(DAMBTCommand{
			Op:     CmdJobStatus,
			JobID:  job.JobID,
			Status: JobRunning,
		})

		c.mu.Lock()
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
		
		// _ = c.wal.Append(WALEntry{
		// 	Type:   "JOB_DONE",
		// 	Result: &result,
		// })
		// Obsolete
		// c.mu.Lock()
		// c.results[job.JobID] = result
		// c.status[job.JobID] = JobDone
		// _ = c.checkpointLocked()
		// c.mu.Unlock()

		_ = c.submitMetadataCommand(DAMBTCommand{
			Op:     CmdJobDone,
			Result: &result,
		})

		log.Printf("[coordinator] job=%s done events=%d pnl=%.4f", job.JobID, result.EventsRead, result.PnL)
	}
}

// * findChunksLocked - finds all chunks overlapping a job query.
//
// Behaviors:
// Iterates over chunk metadata and returns all chunks matching
// the instrument and time range of the job.
//
// Limitations / potential failure scenarios:
// 1. Must be called with mutex held (not thread-safe).
// 2. Linear scan; inefficient for large metadata sets.
// 3. No indexing or caching optimization.
func (c *Coordinator) findChunksLocked(job JobSpec) []ChunkMeta {
	out := []ChunkMeta{}

	for _, chunk := range c.chunks {
		if ChunkOverlaps(chunk, job.Instrument, job.StartTs, job.EndTs) {
			out = append(out, chunk)
		}
	}

	return out
}

// func (c *Coordinator) markJobFailed(jobID JobID) {
// 	failed := JobFailed
// 	_ = c.wal.Append(WALEntry{
// 		Type:   "JOB_STATUS",
// 		JobID:  jobID,
// 		Status: &failed,
// 	})

// 	c.mu.Lock()
// 	c.status[jobID] = JobFailed
// 	_ = c.checkpointLocked()
// 	c.mu.Unlock()
// 	log.Printf("[coordinator] job=%s FAILED", jobID)
// }

// Raft specialized 

// * markJobFailed -- marks a job as failed via Raft replication.
//
// Behaviors:
// Submits a job status update command to ensure failure state
// is consistently replicated across all coordinators.
//
// Limitations / potential failure scenarios:
// 1. Assumes job exists in metadata.
// 2. Relies on Raft commit success.
func (c *Coordinator) markJobFailed(jobID JobID) {
	_ = c.submitMetadataCommand(DAMBTCommand{
		Op:     CmdJobStatus,
		JobID:  jobID,
		Status: JobFailed,
	})
	log.Printf("[coordinator] job=%s FAILED", jobID)
}

// * runJobOnWorker - executes a job on a worker via HTTP.
//
// Behaviors:
// Sends job and chunk data to a worker endpoint and waits
// for a synchronous result response.
//
// Limitations / potential failure scenarios:
// 1. No retry logic on failure.
// 2. Network errors directly propagate.
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
// func (c *Coordinator) recoverState() error {
// 	cp, err := LoadCheckpoint(c.checkpointPath)
// 	if err != nil {
// 		return err
// 	}

// 	if cp.Chunks != nil {
// 		c.chunks = cp.Chunks
// 	}
// 	if cp.Jobs != nil {
// 		c.jobs = cp.Jobs
// 	}
// 	if cp.Status != nil {
// 		c.status = cp.Status
// 	}
// 	if cp.Results != nil {
// 		c.results = cp.Results
// 	}

// 	entries, err := c.wal.Load()
// 	if err != nil {
// 		return err
// 	}

// 	for _, e := range entries {
// 		c.applyWALEntry(e)
// 	}

// 	for jobID, st := range c.status {
// 		if st == JobRunning || st == JobQueued {
// 			job := c.jobs[jobID]
// 			c.status[jobID] = JobQueued
// 			c.jobQueue <- job
// 		}
// 	}

// 	return nil
// }

// func (c *Coordinator) applyWALEntry(e WALEntry) {
// 	switch e.Type {
// 	case "REGISTER_CHUNK":
// 		if e.Chunk != nil {
// 			c.chunks[e.Chunk.ChunkID] = *e.Chunk
// 		}
// 	case "SUBMIT_JOB":
// 		if e.Job != nil {
// 			c.jobs[e.Job.JobID] = *e.Job
// 			c.status[e.Job.JobID] = JobQueued
// 		}
// 	case "JOB_STATUS":
// 		if e.Status != nil {
// 			c.status[e.JobID] = *e.Status
// 		}
// 	case "JOB_DONE":
// 		if e.Result != nil {
// 			c.results[e.Result.JobID] = *e.Result
// 			c.status[e.Result.JobID] = JobDone
// 		}
// 	}
// }

// func (c *Coordinator) checkpointLocked() error {
// 	cp := CoordinatorCheckpoint{
// 		Chunks:  c.chunks,
// 		Jobs:    c.jobs,
// 		Status:  c.status,
// 		Results: c.results,
// 	}
// 	return SaveCheckpoint(c.checkpointPath, cp)
// }


// func (w *WAL) Reset() error {
// 	return os.WriteFile(w.path, []byte{}, 0644)
// }


// workfailure failure n retry helpers

// * runJobWithRetry - retries job execution across workers.
//
// Behaviors:
// Attempts execution on multiple workers in sequence until
// one succeeds or all fail.
//
// Limitations / potential failure scenarios:
// 1. Sequential retries increase latency.
// 2. Returns only last encountered error.
func (c *Coordinator) runJobWithRetry(startIndex int, job JobSpec, chunks []ChunkMeta) (JobResult, int, error) {
	var lastErr error
	for attempt := 0; attempt < len(c.workers); attempt++ {
		idx := (startIndex + attempt) % len(c.workers)
		worker := c.workers[idx]
		// use rpc instead
		// result, err := runJobOnWorker(worker, job, chunks)
		result, err := runJobOnWorkerRPC(worker, job, chunks)

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

// * splitChunks - partitions chunks into groups for parallel execution.
//
// Behaviors:
// Distributes chunks evenly across N groups and removes empty groups.
//
// Limitations / potential failure scenarios:
// 1. Does not consider chunk size imbalance.
// 2. Ignores data locality.
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

// * runJobParallel - executes a job across multiple workers concurrently.
//
// Behaviors:
// Splits chunks into groups, runs them in parallel using goroutines,
// and merges partial results into a final output.
//
// Limitations / potential failure scenarios:
// 1. Fails if any sub-task fails.
// 2. No partial recovery or retry at group level.
// 3. Numerical faults do carry on.
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

// RPC upgrade

// * runJobOnWorkerRPC - executes a job using RPC abstraction.
//
// Behaviors:
// Dynamically binds RPC client interface and invokes the
// RunBacktest method on the worker.
//
// Limitations / potential failure scenarios:
// 1. RPC binding may fail due to interface mismatch.
// 2. Errors are string-based and not strongly typed.
func runJobOnWorkerRPC(workerAddr string, job JobSpec, chunks []ChunkMeta) (JobResult, error) {
	client := &WorkerRPCInterface{}

	if err := remote.CallerStubCreator(client, workerAddr, false, false); err != nil {
		return JobResult{}, err
	}

	resp, rerr := client.RunBacktest(RunJobRequest{
		Job:    job,
		Chunks: chunks,
	})

	if rerr.Err != "" {
		return JobResult{}, fmt.Errorf("%s", rerr.Err)
	}

	return resp.Result, nil
}

// raft coordinator
// This mirrors HKVC pattern: external request becomes a command, 
// Raft commits it, then applyCommand mutates local service state. 
// HKVC already used that model with submitAndWait and consumeApplies. refer to my lab3 HKVC implementation

// * applyDAMBTCommand - applies a committed Raft command to local state.
//
// Behaviors:
// Updates metadata state (chunks, jobs, status, results) according
// to the operation encoded in the command.
//
// Limitations / potential failure scenarios:
// 1. Assumes commands are valid and well-formed.
// 2. No schema/version control for command evolution.
func (c *Coordinator) applyDAMBTCommand(cmd DAMBTCommand) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch cmd.Op {
	case CmdRegisterChunk:
		if cmd.Chunk != nil {
			c.chunks[cmd.Chunk.ChunkID] = *cmd.Chunk
		}

	case CmdSubmitJob:
		if cmd.Job != nil {
			if _, exists := c.jobs[cmd.Job.JobID]; exists {
				return
			}

			c.jobs[cmd.Job.JobID] = *cmd.Job
			c.status[cmd.Job.JobID] = JobQueued

			if c.rf.IsLeader() {
				c.jobQueue <- *cmd.Job
			}
		}
	case CmdJobStatus:
		c.status[cmd.JobID] = cmd.Status

	case CmdJobDone:
		if cmd.Result != nil {
			c.results[cmd.Result.JobID] = *cmd.Result
			c.status[cmd.Result.JobID] = JobDone
		}
	}

	// _ = c.checkpointLocked()
}

// * submitMetadataCommand - submits a command through Raft and waits for commit.
//
// Behaviors:
// Encodes and submits a command to Raft, blocks until the command is
// applied, and synchronizes using per-index channels.
//
// Limitations / potential failure scenarios:
// 1. Returns error if node is not leader.
// 2. Fixed timeout may fail under slow consensus.
func (c *Coordinator) submitMetadataCommand(cmd DAMBTCommand) error {
	data := EncodeCommand(cmd)

	sub := c.rf.Submit(data)
	if !sub.IsLeader {
		return fmt.Errorf("not leader")
	}

	log.Printf("[raft] submit index=%d term=%d op=%s", sub.Index, sub.Term, cmd.Op)
	ch := make(chan struct{}, 1)

	c.mu.Lock()
	if c.applied[sub.Index] {
		c.mu.Unlock()
		return nil
	}
	c.pending[sub.Index] = ch
	c.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-time.After(2 * time.Second):
		c.mu.Lock()
		delete(c.pending, sub.Index)
		c.mu.Unlock()
		return fmt.Errorf("raft commit timeout index=%d op=%s", sub.Index, cmd.Op)
	}
}

// * raftApplyLoop - processes committed Raft log entries continuously.
//
// Behaviors:
// Reads from apply channel, decodes commands, applies them to local state,
// and signals waiting goroutines for completion.
//
// Limitations / potential failure scenarios:
// 1. Decode errors are logged but not retried.
// 2. Silent skip of empty commands, which could be an UB.
func (c *Coordinator) raftApplyLoop() {
	for msg := range c.applyCh {
		if len(msg.Command) == 0 {
			continue
		}

		cmd, err := DecodeCommand(msg.Command)
		if err != nil {
			log.Printf("[raft] decode error: %v", err)
			continue
		}

		c.applyDAMBTCommand(cmd)

		c.mu.Lock()
		c.applied[msg.Index] = true
		if ch, ok := c.pending[msg.Index]; ok {
			ch <- struct{}{}
			delete(c.pending, msg.Index)
		}
		c.mu.Unlock()
	}
}