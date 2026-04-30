package main

import (
	"flag"
	"log"
	"strings"

	"dambt"
)

func main() {
	id := flag.Int("id", 0, "raft node id")
	addr := flag.String("addr", "127.0.0.1:9000", "coordinator listen address")
	workersRaw := flag.String("workers", "127.0.0.1:9201", "comma-separated worker addresses")
	flag.Parse()

	workers := []string{}
	for _, w := range strings.Split(*workersRaw, ",") {
		w = strings.TrimSpace(w)
		if w != "" {
			workers = append(workers, w)
		}
	}
	
	coord := dambt.NewCoordinator(*addr, workers, *id)

	log.Printf("Coordinator id=%d listening on %s, workers=%v\n", *id, *addr, workers)
	log.Fatal(coord.Start())
}