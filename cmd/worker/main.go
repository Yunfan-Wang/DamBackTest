package main

import (
	"flag"
	"log"

	"dambt"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9201", "worker listen address")
	flag.Parse()

	worker := dambt.NewWorker(*addr)

	log.Printf("Worker listening on %s\n", *addr)
	log.Fatal(worker.Start())
}