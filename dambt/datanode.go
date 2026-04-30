package dambt

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"log"
)

type DataNode struct {
	Addr    string
	DataDir string
}

func NewDataNode(addr string, dataDir string) *DataNode {
	return &DataNode{
		Addr:    addr,
		DataDir: dataDir,
	}
}

func (d *DataNode) Start() error {
	if err := os.MkdirAll(d.DataDir, 0755); err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/put_chunk", d.handlePutChunk)
	mux.HandleFunc("/get_chunk", d.handleGetChunk)

	return http.ListenAndServe(d.Addr, mux)
}

type PutChunkRequest struct {
	ChunkID ChunkID       `json:"chunk_id"`
	Events  []MarketEvent `json:"events"`
}

type GetChunkResponse struct {
	ChunkID ChunkID       `json:"chunk_id"`
	Events  []MarketEvent `json:"events"`
}

func (d *DataNode) handlePutChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req PutChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	if req.ChunkID == "" {
		WriteError(w, http.StatusBadRequest, "missing chunk_id")
		return
	}

	path := d.chunkPath(req.ChunkID)
	if err := writeEventsCSV(path, req.Events); err != nil {
		WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("[datanode %s] stored chunk=%s events=%d", d.Addr, req.ChunkID, len(req.Events))
	WriteJSON(w, http.StatusOK, map[string]any{
		"success":  true,
		"chunk_id": req.ChunkID,
	})
}

func (d *DataNode) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	chunkID := ChunkID(r.URL.Query().Get("id"))
	if chunkID == "" {
		WriteError(w, http.StatusBadRequest, "missing chunk id")
		return
	}

	events, err := readEventsCSV(d.chunkPath(chunkID))
	if err != nil {
		WriteError(w, http.StatusNotFound, fmt.Sprintf("chunk not found: %s", chunkID))
		return
	}
	log.Printf("[datanode %s] served chunk=%s events=%d", d.Addr, chunkID, len(events))
	WriteJSON(w, http.StatusOK, GetChunkResponse{
		ChunkID: chunkID,
		Events:  events,
	})
}

func (d *DataNode) chunkPath(id ChunkID) string {
	return filepath.Join(d.DataDir, string(id)+".csv")
}

func writeEventsCSV(path string, events []MarketEvent) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	for _, e := range events {
		row := []string{
			strconv.FormatInt(e.Timestamp, 10),
			e.Instrument,
			strconv.FormatFloat(e.Price, 'f', -1, 64),
			strconv.FormatInt(e.Volume, 10),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return writer.Error()
}

func readEventsCSV(path string) ([]MarketEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	events := make([]MarketEvent, 0, len(rows))

	for _, row := range rows {
		if len(row) != 4 {
			continue
		}

		ts, err1 := strconv.ParseInt(row[0], 10, 64)
		price, err2 := strconv.ParseFloat(row[2], 64)
		volume, err3 := strconv.ParseInt(row[3], 10, 64)

		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}

		events = append(events, MarketEvent{
			Timestamp:  ts,
			Instrument: row[1],
			Price:      price,
			Volume:     volume,
		})
	}

	return events, nil
}