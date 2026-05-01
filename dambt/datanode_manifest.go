package dambt

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type ChunkManifestEntry struct {
	ChunkID  ChunkID `json:"chunk_id"`
	Path     string  `json:"path"`
	Events   int     `json:"events"`
	Checksum string  `json:"checksum"`
	Version  int     `json:"version"`
	Sealed   bool    `json:"sealed"`
}

type DataNodeManifest struct {
	mu      sync.Mutex
	Path    string                          `json:"-"`
	Chunks  map[ChunkID]ChunkManifestEntry `json:"chunks"`
}


// * LoadDataNodeManifest -- loads a data node manifest from disk.
//
// Behaviors:
// Reads the manifest JSON file, initializes an empty manifest if the file
// does not exist, and restores the chunk metadata map for future storage lookup.
//
// Limitations / potential failure scenarios:
// 1. Returns an error if the manifest file exists but cannot be read.
// 2. Returns an error if JSON decoding fails.
func LoadDataNodeManifest(path string) (*DataNodeManifest, error) {
	m := &DataNodeManifest{
		Path:   path,
		Chunks: make(map[ChunkID]ChunkManifestEntry),
	}

	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}

	if len(b) == 0 {
		return m, nil
	}

	if err := json.Unmarshal(b, m); err != nil {
		return nil, err
	}

	if m.Chunks == nil {
		m.Chunks = make(map[ChunkID]ChunkManifestEntry)
	}

	m.Path = path
	return m, nil
}


// * Save -- persists the data node manifest atomically.
//
// Behaviors:
// Serializes the manifest as indented JSON, writes it to a temporary file,
// and renames the temporary file into the final manifest path.
//
// Limitations / potential failure scenarios:
// 1. Fails if the manifest directory cannot be created.
// 2. Fails if JSON serialization or file writing fails.
// 3. Atomicity depends on os.Rename behavior of the underlying filesystem.
func (m *DataNodeManifest) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(m.Path), 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	tmp := m.Path + ".tmp"
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmp, m.Path)
}

func (m *DataNodeManifest) Put(e ChunkManifestEntry) error {
	m.mu.Lock()
	m.Chunks[e.ChunkID] = e
	m.mu.Unlock()

	return m.Save()
}

func (m *DataNodeManifest) Get(id ChunkID) (ChunkManifestEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.Chunks[id]
	return e, ok
}

func FileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}