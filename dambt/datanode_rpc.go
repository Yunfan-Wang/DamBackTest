package dambt

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"remote"
)

type DataNodeRPCInterface struct {
	PutChunk func(PutChunkRequest) (bool, remote.RemoteError)
	GetChunk func(ChunkID) (GetChunkResponse, remote.RemoteError)
}

type DataNodeRPCServer struct {
	Addr    string
	DataDir string
	Manifest *DataNodeManifest
}

func init() {
	gob.Register(PutChunkRequest{})
	gob.Register(GetChunkResponse{})
	gob.Register(ChunkID(""))
	gob.Register(MarketEvent{})
	gob.Register([]MarketEvent{})
}

// * NewDataNodeRPCServer - initializes a data node RPC server.
//
// Behaviors:
// Creates the data directory if needed, loads the persistent chunk manifest,
// and prepares the server for PutChunk and GetChunk RPC operations.
//
// Limitations / potential failure scenarios:
// 1. Panics if the manifest cannot be loaded.
// 2. Does not validate whether existing manifest entries point to valid files.
func NewDataNodeRPCServer(addr string, dataDir string) *DataNodeRPCServer {
	_ = os.MkdirAll(dataDir, 0755)

	manifestPath := filepath.Join(dataDir, "manifest.json")
	manifest, err := LoadDataNodeManifest(manifestPath)
	if err != nil {
		panic(err)
	}

	return &DataNodeRPCServer{
		Addr:     addr,
		DataDir:  dataDir,
		Manifest: manifest,
	}
}

// * PutChunk - stores a chunk and records it in the data node manifest.
//
// Behaviors:
// Validates the chunk ID, writes market events to disk, computes a checksum,
// marks the chunk as sealed, and persists its metadata in the manifest.
//
// Limitations / potential failure scenarios:
// 1. Fails if chunk ID is missing or storage directory cannot be created.
// 2. Fails if writing events, computing checksum, or saving manifest fails.
func (d *DataNodeRPCServer) PutChunk(req PutChunkRequest) (bool, remote.RemoteError) {
	if req.ChunkID == "" {
		return false, remote.RemoteError{Err: "missing chunk_id"}
	}

	if err := os.MkdirAll(d.DataDir, 0755); err != nil {
		return false, remote.RemoteError{Err: err.Error()}
	}

	path := (&DataNode{Addr: d.Addr, DataDir: d.DataDir}).chunkPath(req.ChunkID)

	if err := writeEventsCSV(path, req.Events); err != nil {
		return false, remote.RemoteError{Err: err.Error()}
	}

	checksum, err := FileSHA256(path)
	if err != nil {
		return false, remote.RemoteError{Err: err.Error()}
	}

	entry := ChunkManifestEntry{
		ChunkID:  req.ChunkID,
		Path:     path,
		Events:   len(req.Events),
		Checksum: checksum,
		Version:  1,
		Sealed:   true,
	}

	if err := d.Manifest.Put(entry); err != nil {
		return false, remote.RemoteError{Err: err.Error()}
	}

	log.Printf("[datanode-rpc %s] stored chunk=%s events=%d checksum=%s",
		d.Addr, req.ChunkID, len(req.Events), checksum[:12])

	return true, remote.RemoteError{}
}

// * GetChunk - retrieves a stored chunk after checksum verification.
//
// Behaviors:
// Looks up chunk metadata in the manifest, verifies the file checksum,
// reads events from disk, and returns them to the caller.
//
// Limitations / potential failure scenarios:
// 1. Fails if the chunk ID is missing or not registered.
func (d *DataNodeRPCServer) GetChunk(id ChunkID) (GetChunkResponse, remote.RemoteError) {
	if id == "" {
		return GetChunkResponse{}, remote.RemoteError{Err: "missing chunk_id"}
	}

	entry, ok := d.Manifest.Get(id)
	if !ok {
		return GetChunkResponse{}, remote.RemoteError{
			Err: fmt.Sprintf("chunk not found in manifest: %s", id),
		}
	}

	checksum, err := FileSHA256(entry.Path)
	if err != nil {
		return GetChunkResponse{}, remote.RemoteError{Err: err.Error()}
	}

	if checksum != entry.Checksum {
		return GetChunkResponse{}, remote.RemoteError{
			Err: fmt.Sprintf("checksum mismatch for chunk %s", id),
		}
	}

	events, err := readEventsCSV(entry.Path)
	if err != nil {
		return GetChunkResponse{}, remote.RemoteError{
			Err: fmt.Sprintf("chunk read failed: %s", id),
		}
	}

	log.Printf("[datanode-rpc %s] served chunk=%s events=%d checksum=%s",
		d.Addr, id, len(events), checksum[:12])

	return GetChunkResponse{
		ChunkID: id,
		Events:  events,
	}, remote.RemoteError{}
}