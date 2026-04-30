package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	"dambt"
	"remote"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9401", "datanode RPC address")
	file := flag.String("file", "chunk1.json", "chunk json file")
	flag.Parse()

	b, err := os.ReadFile(*file)
	if err != nil {
		log.Fatal(err)
	}

	var req dambt.PutChunkRequest
	if err := json.Unmarshal(b, &req); err != nil {
		log.Fatal(err)
	}

	client := &dambt.DataNodeRPCInterface{}
	if err := remote.CallerStubCreator(client, *addr, false, false); err != nil {
		log.Fatal(err)
	}

	ok, rerr := client.PutChunk(req)
	if rerr.Err != "" {
		log.Fatal(rerr.Err)
	}

	log.Printf("put_chunk_rpc success=%v chunk=%s", ok, req.ChunkID)
}