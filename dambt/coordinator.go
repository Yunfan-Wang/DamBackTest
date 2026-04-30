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
		panic("raftPeers and ctrlPeers must have same length")
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
func (c *Coordinator) markJobFailed(jobID JobID) {
	_ = c.submitMetadataCommand(DAMBTCommand{
		Op:     CmdJobStatus,
		JobID:  jobID,
		Status: JobFailed,
	})
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

// RPC upgrade

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
//This mirrors HKVC pattern: external request becomes a command, 
// Raft commits it, then applyCommand mutates local service state. 
// HKVC already used that model with submitAndWait and consumeApplies. refer to my lab3 HKVC implementation
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