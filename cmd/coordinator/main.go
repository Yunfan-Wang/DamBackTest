package main

import (
	"flag"
	"log"
	"strings"

	"dambt"
)

func main() {
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

	coord := dambt.NewCoordinator(*addr, workers)

	log.Printf("Coordinator listening on %s, workers=%v\n", *addr, workers)
	log.Fatal(coord.Start())
}