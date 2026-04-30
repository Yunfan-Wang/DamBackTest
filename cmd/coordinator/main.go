package main

import (
	"flag"
	"log"
	"strings"

	"dambt"
)

func main() {
	raftPeersRaw := flag.String("raft-peers", "", "comma-separated raft peer addresses")
	id := flag.Int("id", 0, "raft node id")
	addr := flag.String("addr", "127.0.0.1:9000", "coordinator listen address")
	workersRaw := flag.String("workers", "127.0.0.1:9201", "comma-separated worker addresses")
	ctrlPeersRaw := flag.String("ctrl-peers", "", "comma-separated raft control peer addresses")
	flag.Parse()
	
	workers := []string{}
	var raftPeers []string
	var ctrlPeers []string

	for _, w := range strings.Split(*workersRaw, ",") {
		w = strings.TrimSpace(w)
		if w != "" {
			workers = append(workers, w)
		}
	}

	if *raftPeersRaw != "" {
		for _, p := range strings.Split(*raftPeersRaw, ",") {
			raftPeers = append(raftPeers, strings.TrimSpace(p))
		}
	}

	if *ctrlPeersRaw != "" {
		for _, p := range strings.Split(*ctrlPeersRaw, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				ctrlPeers = append(ctrlPeers, p)
			}
		}
	}
	coord := dambt.NewCoordinator(*addr, workers, *id, raftPeers, ctrlPeers)

	log.Printf("Coordinator id=%d listening on %s, workers=%v\n", *id, *addr, workers)
	log.Fatal(coord.Start())
}