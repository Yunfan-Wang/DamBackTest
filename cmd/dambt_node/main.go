package main

import (
	"flag"
	"log"
	"strings"

	"dambt"
	"remote"
)

func splitCSV(s string) []string {
	out := []string{}
	for _, x := range strings.Split(s, ",") {
		x = strings.TrimSpace(x)
		if x != "" {
			out = append(out, x)
		}
	}
	return out
}

func main() {
	role := flag.String("role", "", "node role: coordinator | worker | datanode")

	addr := flag.String("addr", "0.0.0.0:9000", "listen address")
	id := flag.Int("id", 0, "coordinator raft node id")

	workersRaw := flag.String("workers", "", "comma-separated worker addresses")
	raftPeersRaw := flag.String("raft-peers", "", "comma-separated raft peer addresses")
	ctrlPeersRaw := flag.String("ctrl-peers", "", "comma-separated raft control peer addresses")

	dataDir := flag.String("data", "./data/dn1", "datanode data directory")

	flag.Parse()

	switch *role {
	case "coordinator":
		workers := splitCSV(*workersRaw)
		raftPeers := splitCSV(*raftPeersRaw)
		ctrlPeers := splitCSV(*ctrlPeersRaw)

		node := dambt.NewCoordinator(*addr, workers, *id, raftPeers, ctrlPeers)

		log.Printf("[dambt-node] coordinator id=%d addr=%s workers=%v", *id, *addr, workers)
		log.Fatal(node.Start())

	case "worker":
		server := dambt.NewWorkerRPCServer(*addr)

		iface := &dambt.WorkerRPCInterface{
			RunBacktest: server.RunBacktest,
		}

		stub, err := remote.NewCalleeStub(iface, server, *addr, false, false)
		if err != nil {
			log.Fatal(err)
		}

		if err := stub.Start(); err != nil {
			log.Fatal(err)
		}

		log.Printf("[dambt-node] worker addr=%s", *addr)
		select {}

	case "datanode":
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

		log.Printf("[dambt-node] datanode addr=%s data=%s", *addr, *dataDir)
		select {}

	default:
		log.Fatalf("unknown or missing role: %s", *role)
	}
}