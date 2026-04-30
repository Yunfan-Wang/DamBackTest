package main

import (
	"flag"
	"log"
	"os"

	"dambt"
	"remote"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9401", "datanode RPC listen address")
	dataDir := flag.String("data", "./data/dn_rpc1", "data directory")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatal(err)
	}

	server := dambt.NewDataNodeRPCServer(*addr, *dataDir)

	iface := &dambt.DataNodeRPCInterface{
		PutChunk: server.PutChunk,
		GetChunk: server.GetChunk,
	}

	stub, err := remote.NewCalleeStub(iface, server, *addr, false, false)
	if err != nil {
		log.Fatal(err)
	}

	if err := stub.Start(); err != nil {
		log.Fatal(err)
	}

	log.Printf("DataNode RPC listening on %s, data=%s\n", *addr, *dataDir)

	select {}
}