package dambt

import (
	"encoding/json"
	"os"
)

type CoordinatorCheckpoint struct {
	Chunks  map[ChunkID]ChunkMeta `json:"chunks"`
	Jobs    map[JobID]JobSpec     `json:"jobs"`
	Status  map[JobID]JobStatus   `json:"status"`
	Results map[JobID]JobResult   `json:"results"`
}

func SaveCheckpoint(path string, cp CoordinatorCheckpoint) error {
	tmp := path + ".tmp"

	b, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

func LoadCheckpoint(path string) (CoordinatorCheckpoint, error) {
	var cp CoordinatorCheckpoint

	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return cp, nil
	}
	if err != nil {
		return cp, err
	}

	err = json.Unmarshal(b, &cp)
	return cp, err
}