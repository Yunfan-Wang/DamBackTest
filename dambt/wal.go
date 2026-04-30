package dambt

import (
	"encoding/json"
	"os"
	"sync"
)

type WALEntry struct {
	Type   string          `json:"type"`
	Chunk  *ChunkMeta      `json:"chunk,omitempty"`
	Job    *JobSpec        `json:"job,omitempty"`
	Result *JobResult      `json:"result,omitempty"`
	Status *JobStatus      `json:"status,omitempty"`
	JobID  JobID           `json:"job_id,omitempty"`
}

type WAL struct {
	mu   sync.Mutex
	path string
}

func NewWAL(path string) *WAL {
	return &WAL{path: path}
}

func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}

	return f.Sync()
}

func (w *WAL) Load() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.Open(w.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	entries := []WALEntry{}

	for decoder.More() {
		var entry WALEntry
		if err := decoder.Decode(&entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}