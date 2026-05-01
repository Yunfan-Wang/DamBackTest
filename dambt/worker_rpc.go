package dambt

import (
	"encoding/gob"
	"remote"
)

type WorkerRPCInterface struct {
	RunBacktest func(RunJobRequest) (RunJobResponse, remote.RemoteError)
}

type WorkerRPCServer struct {
	Addr string
}

func init() {
	gob.Register(RunJobRequest{})
	gob.Register(RunJobResponse{})
	gob.Register(JobSpec{})
	gob.Register(JobResult{})
	gob.Register(ChunkMeta{})
	gob.Register(MarketEvent{})
	gob.Register([]ChunkMeta{})
	gob.Register([]MarketEvent{})
}

func NewWorkerRPCServer(addr string) *WorkerRPCServer {
	return &WorkerRPCServer{Addr: addr}
}

// * RunBacktest - executes a backtesting request on the worker.
//
// Behaviors:
// Fetches all required chunks, filters events by instrument and time range,
// runs the configured strategy, and returns the computed job result.
//
// Limitations / potential failure scenarios:
// 1. Fails if any required chunk cannot be fetched.
// 2. Loads all events into memory before filtering.
// 3. Performs no caching, prefetching, or partial retry inside the worker.
// 4. No advanced features like charting and validating.
func (w *WorkerRPCServer) RunBacktest(req RunJobRequest) (RunJobResponse, remote.RemoteError) {
	events, err := fetchAllChunks(req.Chunks)
	if err != nil {
		return RunJobResponse{}, remote.RemoteError{Err: err.Error()}
	}

	filtered := make([]MarketEvent, 0, len(events))
	for _, e := range events {
		if e.Instrument == req.Job.Instrument &&
			e.Timestamp >= req.Job.StartTs &&
			e.Timestamp <= req.Job.EndTs {
			filtered = append(filtered, e)
		}
	}

	result := RunStrategy(req.Job, filtered)

	return RunJobResponse{Result: result}, remote.RemoteError{}
}