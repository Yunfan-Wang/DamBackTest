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