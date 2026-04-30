package dambt

import (
	"encoding/gob"
	"fmt"
	"log"

	"remote"
)

type DataNodeRPCInterface struct {
	PutChunk func(PutChunkRequest) (bool, remote.RemoteError)
	GetChunk func(ChunkID) (GetChunkResponse, remote.RemoteError)
}

type DataNodeRPCServer struct {
	Addr    string
	DataDir string
}

func init() {
	gob.Register(PutChunkRequest{})
	gob.Register(GetChunkResponse{})
	gob.Register(ChunkID(""))
	gob.Register(MarketEvent{})
	gob.Register([]MarketEvent{})
}

func NewDataNodeRPCServer(addr string, dataDir string) *DataNodeRPCServer {
	return &DataNodeRPCServer{
		Addr:    addr,
		DataDir: dataDir,
	}
}

func (d *DataNodeRPCServer) PutChunk(req PutChunkRequest) (bool, remote.RemoteError) {
	if req.ChunkID == "" {
		return false, remote.RemoteError{Err: "missing chunk_id"}
	}

	path := (&DataNode{Addr: d.Addr, DataDir: d.DataDir}).chunkPath(req.ChunkID)

	if err := writeEventsCSV(path, req.Events); err != nil {
		return false, remote.RemoteError{Err: err.Error()}
	}

	log.Printf("[datanode-rpc %s] stored chunk=%s events=%d", d.Addr, req.ChunkID, len(req.Events))
	return true, remote.RemoteError{}
}

func (d *DataNodeRPCServer) GetChunk(id ChunkID) (GetChunkResponse, remote.RemoteError) {
	if id == "" {
		return GetChunkResponse{}, remote.RemoteError{Err: "missing chunk_id"}
	}

	path := (&DataNode{Addr: d.Addr, DataDir: d.DataDir}).chunkPath(id)

	events, err := readEventsCSV(path)
	if err != nil {
		return GetChunkResponse{}, remote.RemoteError{
			Err: fmt.Sprintf("chunk not found: %s", id),
		}
	}

	log.Printf("[datanode-rpc %s] served chunk=%s events=%d", d.Addr, id, len(events))

	return GetChunkResponse{
		ChunkID: id,
		Events:  events,
	}, remote.RemoteError{}
}