go run ./cmd/datanode_rpc --addr 127.0.0.1:9401 --data ./data/dn_rpc1

go run ./cmd/worker_rpc --addr 127.0.0.1:9301

go run ./cmd/worker_rpc --addr 127.0.0.1:9302
and more
worker...

go run ./cmd/coordinator \
  --addr 127.0.0.1:9000 \
  --workers 127.0.0.1:9301,127.0.0.1:9302,127.0.0.1:9999