package dambt

import (
	"encoding/json"
	"fmt"
	"net/http"
	"log"
	"remote"
)

type Worker struct {
	Addr string
}

func NewWorker(addr string) *Worker {
	return &Worker{
		Addr: addr,
	}
}

func (w *Worker) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/run_job", w.handleRunJob)

	return http.ListenAndServe(w.Addr, mux)
}

type RunJobRequest struct {
	Job    JobSpec     `json:"job"`
	Chunks []ChunkMeta `json:"chunks"`
}

type RunJobResponse struct {
	Result JobResult `json:"result"`
}

func (w *Worker) handleRunJob(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(rw, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req RunJobRequest
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(rw, http.StatusBadRequest, "invalid json body")
		return
	}
	log.Printf("[worker %s] received job=%s chunks=%d", w.Addr, req.Job.JobID, len(req.Chunks))
	events, err := fetchAllChunks(req.Chunks)
	if err != nil {
		WriteError(rw, http.StatusInternalServerError, err.Error())
		return
	}
	
	filtered := make([]MarketEvent, 0, len(events))
	for _, e := range events {
		if e.Instrument == req.Job.Instrument &&
			e.Timestamp >= req.Job.StartTs &&
			e.Timestamp <= req.Job.EndTs {
			filtered = append(filtered, e)
		}
	}
	log.Printf("[worker %s] fetched raw_events=%d filtered_events=%d job=%s", w.Addr, len(events), len(filtered), req.Job.JobID)

	
	result := RunStrategy(req.Job, filtered)
	log.Printf("[worker %s] finished job=%s events=%d pnl=%.4f", 	w.Addr, result.JobID, result.EventsRead, result.PnL)

	WriteJSON(rw, http.StatusOK, RunJobResponse{
		Result: result,
	})
}

func fetchAllChunks(chunks []ChunkMeta) ([]MarketEvent, error) {
	var all []MarketEvent

	for _, c := range chunks {
		events, err := fetchChunkFromReplicas(c)
		if err != nil {
			return nil, err
		}
		all = append(all, events...)
	}

	return all, nil
}
// legacy naiive fetch
// func fetchChunkFromReplicas(c ChunkMeta) ([]MarketEvent, error) {
// 	var lastErr error

// 	for _, replica := range c.Replicas {
// 		url := fmt.Sprintf("http://%s/get_chunk?id=%s", replica, c.ChunkID)
// 		log.Printf("[worker] fetching chunk=%s from %s", c.ChunkID, replica)
		
// 		resp, err := http.Get(url)
// 		if err != nil {
// 			lastErr = err
// 			continue
// 		}

// 		var out GetChunkResponse
// 		err = json.NewDecoder(resp.Body).Decode(&out)
// 		resp.Body.Close()

// 		if err != nil || resp.StatusCode != http.StatusOK {
// 			log.Printf("[worker %s] failed replica=%s chunk=%s err=%v",	c.ChunkID, replica, c.ChunkID, err)

// 			lastErr = fmt.Errorf("bad response from replica %s", replica)
// 			continue
// 		}
// 		log.Printf("[worker %s] success replica=%s chunk=%s events=%d", c.ChunkID, replica, c.ChunkID, len(out.Events))
// 		return out.Events, nil
// 	}

// 	if lastErr == nil {
// 		lastErr = fmt.Errorf("no replicas available for chunk %s", c.ChunkID)
// 	}
	
	
// 	return nil, lastErr
// }

// remote upgrade
func fetchChunkFromReplicas(c ChunkMeta) ([]MarketEvent, error) {
	var lastErr error

	for _, replica := range c.Replicas {
		log.Printf("[worker] rpc fetching chunk=%s from replica=%s", c.ChunkID, replica)

		client := &DataNodeRPCInterface{}
		if err := remote.CallerStubCreator(client, replica, false, false); err != nil {
			lastErr = err
			log.Printf("[worker] rpc stub failed chunk=%s replica=%s err=%v", c.ChunkID, replica, err)
			continue
		}

		out, rerr := client.GetChunk(c.ChunkID)
		if rerr.Err != "" {
			lastErr = fmt.Errorf(rerr.Err)
			log.Printf("[worker] rpc get failed chunk=%s replica=%s err=%v", c.ChunkID, replica, lastErr)
			continue
		}

		log.Printf("[worker] rpc success chunk=%s replica=%s events=%d",
			c.ChunkID, replica, len(out.Events))

		return out.Events, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no replicas available for chunk %s", c.ChunkID)
	}

	return nil, lastErr
}