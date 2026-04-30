package main

import (
	"flag"
	"log"

	"dambt"
	"remote"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9301", "worker RPC listen address")
	flag.Parse()

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
	log.Printf("Worker RPC listening on %s\n", *addr)
	select {}

}