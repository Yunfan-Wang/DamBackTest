package main

import (
	"flag"
	"log"

	"dambt"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9101", "datanode listen address")
	dataDir := flag.String("data", "./data/datanode1", "data directory")
	flag.Parse()

	node := dambt.NewDataNode(*addr, *dataDir)

	log.Printf("DataNode listening on %s, data=%s\n", *addr, *dataDir)
	log.Fatal(node.Start())
}