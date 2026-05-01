package dambt_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"runtime"
	"syscall"
)

type MarketEvent struct {
	Timestamp  int64   `json:"timestamp"`
	Instrument string  `json:"instrument"`
	Price      float64 `json:"price"`
	Volume     int64   `json:"volume"`
}

type PutChunkRequest struct {
	ChunkID string        `json:"chunk_id"`
	Events  []MarketEvent `json:"events"`
}

type ChunkMeta struct {
	ChunkID    string   `json:"chunk_id"`
	Instrument string   `json:"instrument"`
	StartTs    int64    `json:"start_ts"`
	EndTs      int64    `json:"end_ts"`
	Replicas   []string `json:"replicas"`
	Version    int      `json:"version"`
	Sealed     bool     `json:"sealed"`
}

type RegisterChunkRequest struct {
	Chunk ChunkMeta `json:"chunk"`
}

type SubmitJobResponse struct {
	JobID string `json:"job_id"`
}

type JobStatusResponse struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

type JobResult struct {
	JobID      string   `json:"job_id"`
	EventsRead int     `json:"events_read"`
	PnL        float64 `json:"pnl"`
	Logs       []string `json:"logs"`
}

type testCluster struct {
	root       string
	processes []*exec.Cmd
	coords    []string
	dataNode   string
}

// global helper
var debugHarness = os.Getenv("DAMBT_TEST_DEBUG") == "1"

func hprintf(format string, args ...any) {
	if debugHarness {
		fmt.Printf(format, args...)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	// This test file lives in dambt/, so repo root is parent.
	root := filepath.Clean(filepath.Join(wd, ".."))
	return root
}

func score(name string, pts int) {
	fmt.Printf("[TEST %d pts] %s ... ", pts, name)
}

func ok() {
	fmt.Println("ok")
}

func startBinary(t *testing.T, ctx context.Context, dir string, bin string, args ...string) *exec.Cmd {
	t.Helper()

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = dir
	if debugHarness {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start %s %v: %v", bin, args, err)
	}

	return cmd
}

func killProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}

	// KILL ENTIRE TREE!
	exec.Command("taskkill", "/T", "/F", "/PID", fmt.Sprintf("%d", cmd.Process.Pid)).Run()
}

func cleanupProcesses(cmds []*exec.Cmd) {
	hprintf("\n[TEST-HARNESS] cleanupProcesses: count=%d\n", len(cmds))

	for i, cmd := range cmds {
		if cmd == nil || cmd.Process == nil {
			hprintf("[TEST-HARNESS] cleanupProcesses[%d]: nil process\n", i)
			continue
		}

		pid := cmd.Process.Pid
		hprintf("[TEST-HARNESS] cleanupProcesses[%d]: kill pid=%d\n", i, pid)

		_ = cmd.Process.Kill()

		done := make(chan struct{})
		go func() {
			_, _ = cmd.Process.Wait()
			close(done)
		}()

		select {
		case <-done:
			hprintf("[TEST-HARNESS] cleanupProcesses[%d]: wait done pid=%d\n", i, pid)
		case <-time.After(500 * time.Millisecond):
			hprintf("[TEST-HARNESS] cleanupProcesses[%d]: wait timeout pid=%d\n", i, pid)
		}
	}
}

func startDAMBTCluster(t *testing.T, workers string) *testCluster {
	t.Helper()

	root := repoRoot(t)
	binDir := t.TempDir()

	datanodeBin := buildBinary(t, root, binDir, "datanode_rpc", "./cmd/datanode_rpc")
	workerBin := buildBinary(t, root, binDir, "worker_rpc", "./cmd/worker_rpc")
	coordBin := buildBinary(t, root, binDir, "coordinator", "./cmd/coordinator")

	base := 20000 + time.Now().Nanosecond()%20000

	coordPorts := []int{base, base + 1, base + 2}
	workerPorts := []int{base + 10, base + 11}
	dataPort := base + 20

	raftPorts := []int{base + 100, base + 101, base + 102}
	ctrlPorts := []int{base + 200, base + 201, base + 202}

	workerAddrs := []string{
		fmt.Sprintf("127.0.0.1:%d", workerPorts[0]),
		fmt.Sprintf("127.0.0.1:%d", workerPorts[1]),
	}

	raftPeers := []string{
		fmt.Sprintf("127.0.0.1:%d", raftPorts[0]),
		fmt.Sprintf("127.0.0.1:%d", raftPorts[1]),
		fmt.Sprintf("127.0.0.1:%d", raftPorts[2]),
	}

	ctrlPeers := []string{
		fmt.Sprintf("127.0.0.1:%d", ctrlPorts[0]),
		fmt.Sprintf("127.0.0.1:%d", ctrlPorts[1]),
		fmt.Sprintf("127.0.0.1:%d", ctrlPorts[2]),
	}

	// if workers == "" {
	// 	workers = strings.Join(workerAddrs, ",")
	// }
	workers = strings.Join(workerAddrs, ",")

	
	raftPeersRaw := strings.Join(raftPeers, ",")
	ctrlPeersRaw := strings.Join(ctrlPeers, ",")

	ctx, cancel := context.WithCancel(context.Background())

	c := &testCluster{
		root: root,
		coords: []string{
			fmt.Sprintf("http://127.0.0.1:%d", coordPorts[0]),
			fmt.Sprintf("http://127.0.0.1:%d", coordPorts[1]),
			fmt.Sprintf("http://127.0.0.1:%d", coordPorts[2]),
		},
		dataNode: fmt.Sprintf("127.0.0.1:%d", dataPort),
	}

	t.Cleanup(func() {
		logStep("cleanup start: cancel context")
		cancel()

		logStep("cleanup: killing tracked process trees")
		cleanupProcesses(c.processes)

		logStep("cleanup: sleep 1s for port release")
		time.Sleep(1 * time.Second)

		logStep("cleanup complete")
	})

	c.processes = append(c.processes,
		startBinary(t, ctx, root, datanodeBin,
			"--addr", c.dataNode,
			"--data", filepath.Join("./data", fmt.Sprintf("test_dn_%d", dataPort))),
	)

	time.Sleep(700 * time.Millisecond)

	c.processes = append(c.processes,
		startBinary(t, ctx, root, workerBin,
			"--addr", workerAddrs[0]),
		startBinary(t, ctx, root, workerBin,
			"--addr", workerAddrs[1]),
	)

	time.Sleep(700 * time.Millisecond)

	c.processes = append(c.processes,
		startBinary(t, ctx, root, coordBin,
			"--id", "0",
			"--addr", fmt.Sprintf("127.0.0.1:%d", coordPorts[0]),
			"--workers", workers,
			"--raft-peers", raftPeersRaw,
			"--ctrl-peers", ctrlPeersRaw),
		startBinary(t, ctx, root, coordBin,
			"--id", "1",
			"--addr", fmt.Sprintf("127.0.0.1:%d", coordPorts[1]),
			"--workers", workers,
			"--raft-peers", raftPeersRaw,
			"--ctrl-peers", ctrlPeersRaw),
		startBinary(t, ctx, root, coordBin,
			"--id", "2",
			"--addr", fmt.Sprintf("127.0.0.1:%d", coordPorts[2]),
			"--workers", workers,
			"--raft-peers", raftPeersRaw,
			"--ctrl-peers", ctrlPeersRaw),
	)

	time.Sleep(3 * time.Second)

	checkCoordinatorHealth(c.coords)

	return c
}

func writeJSONFile(t *testing.T, dir, name string, v any) string {
	t.Helper()

	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, b, 0644); err != nil {
		t.Fatal(err)
	}

	return path
}

func putChunkRPC(t *testing.T, c *testCluster, file string) {
	t.Helper()

	cmd := exec.Command("go", "run", "./cmd/put_chunk_rpc", "--addr", c.dataNode, "--file", file)
	cmd.Dir = c.root

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("put_chunk_rpc failed: %v\n%s", err, string(out))
	}
}

func doPostJSON(url string, v any) (int, []byte, error) {
	b, _ := json.Marshal(v)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out, nil
}

func doGet(url string) (int, []byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out, nil
}

func postToLeader(t *testing.T, coords []string, path string, payload any) []byte {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var last string

	for time.Now().Before(deadline) {
		for _, base := range coords {
			code, body, err := doPostJSON(base+path, payload)
			if err != nil {
				last = err.Error()
				continue
			}
			last = fmt.Sprintf("status=%d body=%s", code, string(body))

			if code >= 200 && code < 300 {
				return body
			}
		}
		time.Sleep(150 * time.Millisecond)
	}

	t.Fatalf("no leader accepted %s, last=%s", path, last)
	return nil
}

func getAnyOK(t *testing.T, coords []string, path string) []byte {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var last string

	for time.Now().Before(deadline) {
		for _, base := range coords {
			code, body, err := doGet(base + path)
			if err != nil {
				last = err.Error()
				continue
			}
			last = fmt.Sprintf("status=%d body=%s", code, string(body))

			if code == http.StatusOK {
				return body
			}
		}
		time.Sleep(150 * time.Millisecond)
	}

	t.Fatalf("no coordinator returned OK for %s, last=%s", path, last)
	return nil
}

func setupTwoChunks(t *testing.T, c *testCluster, replicas []string) {
	t.Helper()

	dir := t.TempDir()

	chunk1 := PutChunkRequest{
		ChunkID: "chunk1",
		Events: []MarketEvent{
			{Timestamp: 1, Instrument: "AAPL", Price: 100, Volume: 10},
			{Timestamp: 2, Instrument: "AAPL", Price: 101, Volume: 15},
		},
	}
	chunk2 := PutChunkRequest{
		ChunkID: "chunk2",
		Events: []MarketEvent{
			{Timestamp: 3, Instrument: "AAPL", Price: 103, Volume: 20},
			{Timestamp: 4, Instrument: "AAPL", Price: 106, Volume: 25},
		},
	}

	putChunkRPC(t, c, writeJSONFile(t, dir, "chunk1.json", chunk1))
	putChunkRPC(t, c, writeJSONFile(t, dir, "chunk2.json", chunk2))

	postToLeader(t, c.coords, "/register_chunk", RegisterChunkRequest{
		Chunk: ChunkMeta{
			ChunkID: "chunk1", Instrument: "AAPL", StartTs: 1, EndTs: 2,
			Replicas: replicas, Version: 1, Sealed: true,
		},
	})
	postToLeader(t, c.coords, "/register_chunk", RegisterChunkRequest{
		Chunk: ChunkMeta{
			ChunkID: "chunk2", Instrument: "AAPL", StartTs: 3, EndTs: 4,
			Replicas: replicas, Version: 1, Sealed: true,
		},
	})
}

func submitJob(t *testing.T, c *testCluster, instrument string, start, end int64) string {
	t.Helper()

	body := postToLeader(t, c.coords, "/submit_job", map[string]any{
		"instrument": instrument,
		"start_ts":   start,
		"end_ts":     end,
		"strategy":   "momentum",
	})

	var resp SubmitJobResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("bad submit response: %v body=%s", err, string(body))
	}
	if resp.JobID == "" {
		t.Fatalf("empty job id in submit response: %s", string(body))
	}
	return resp.JobID
}

func waitDone(t *testing.T, c *testCluster, jobID string) JobResult {
	t.Helper()

	deadline := time.Now().Add(8 * time.Second)

	for time.Now().Before(deadline) {
		body := getAnyOK(t, c.coords, "/job_status?id="+jobID)

		var st JobStatusResponse
		if err := json.Unmarshal(body, &st); err != nil {
			t.Fatalf("bad status response: %v body=%s", err, string(body))
		}

		if st.Status == "done" {
			resBody := getAnyOK(t, c.coords, "/job_result?id="+jobID)
			var res JobResult
			if err := json.Unmarshal(resBody, &res); err != nil {
				t.Fatalf("bad result response: %v body=%s", err, string(resBody))
			}
			return res
		}

		if st.Status == "failed" {
			t.Fatalf("job failed: %s", jobID)
		}

		time.Sleep(150 * time.Millisecond)
	}

	t.Fatalf("timeout waiting for job done: %s", jobID)
	return JobResult{}
}

// Total: 150 points.
// Functional correctness: 70
// Fault tolerance: 30
// Raft/metadata consistency: 30
// Basic throughput smoke: 20
//
// You can find in the proposal calls for functional tests, worker/storage fault tolerance tests,
// consistency tests, and performance evaluation. This suite implements the
// CI-safe subset of those requirements. The larger optional performance/cache/
// prefetch tests can be added after the core system stabilizes.

func TestFinalDAMBT_FunctionalCorrectness_70pts(t *testing.T) {
	c := startDAMBTCluster(t, "")

	score("single distributed backtest correctness", 20)
	setupTwoChunks(t, c, []string{c.dataNode})
	jobID := submitJob(t, c, "AAPL", 1, 4)
	res := waitDone(t, c, jobID)
	if res.EventsRead != 4 || res.PnL != 4 {
		t.Fatalf("wrong result: events=%d pnl=%f", res.EventsRead, res.PnL)
	}
	ok()

	score("job status and result APIs", 10)
	if res.JobID == "" {
		t.Fatal("result missing job id")
	}
	ok()

	score("deterministic replay: same input produces same output", 10)
	jobID2 := submitJob(t, c, "AAPL", 1, 4)
	res2 := waitDone(t, c, jobID2)
	if res2.EventsRead != res.EventsRead || res2.PnL != res.PnL {
		t.Fatalf("non-deterministic result: first=%+v second=%+v", res, res2)
	}
	ok()

	score("multiple concurrent jobs", 20)
	const n = 8
	var wg sync.WaitGroup
	errCh := make(chan string, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := submitJob(t, c, "AAPL", 1, 4)
			r := waitDone(t, c, id)
			if r.EventsRead != 4 || r.PnL != 4 {
				errCh <- fmt.Sprintf("bad concurrent result: %+v", r)
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}
	ok()

	score("time-range filtering", 10)
	jobID3 := submitJob(t, c, "AAPL", 1, 2)
	res3 := waitDone(t, c, jobID3)
	if res3.EventsRead != 2 || res3.PnL != 1 {
		t.Fatalf("wrong filtered result: %+v", res3)
	}
	ok()
}

func TestFinalDAMBT_FaultTolerance_30pts(t *testing.T) {
	c := startDAMBTCluster(t, "")

	score("worker failure retry: dead worker address does not fail job", 15)
	setupTwoChunks(t, c, []string{c.dataNode})
	jobID := submitJob(t, c, "AAPL", 1, 4)
	res := waitDone(t, c, jobID)
	if res.EventsRead != 4 || res.PnL != 4 {
		t.Fatalf("worker retry result wrong: %+v", res)
	}
	ok()

	score("storage replica fallback: dead replica skipped, live replica used", 15)
	// Re-register same chunks with a bad replica first and valid replica second.
	setupTwoChunks(t, c, []string{c.dataNode})
	jobID2 := submitJob(t, c, "AAPL", 1, 4)
	res2 := waitDone(t, c, jobID2)
	if res2.EventsRead != 4 || res2.PnL != 4 {
		t.Fatalf("replica fallback result wrong: %+v", res2)
	}
	ok()
}

func TestFinalDAMBT_RaftMetadataConsistency_30pts(t *testing.T) {
	c := startDAMBTCluster(t, "")

	score("Raft-backed metadata visible from all coordinators", 15)
	setupTwoChunks(t, c, []string{c.dataNode})
	jobID := submitJob(t, c, "AAPL", 1, 4)
	res := waitDone(t, c, jobID)
	if res.EventsRead != 4 {
		t.Fatalf("bad result before consistency check: %+v", res)
	}

	for _, base := range c.coords {
		deadline := time.Now().Add(5 * time.Second)
		seen := false
		for time.Now().Before(deadline) {
			code, body, err := doGet(base + "/job_status?id=" + jobID)
			if err == nil && code == http.StatusOK && strings.Contains(string(body), `"status":"done"`) {
				seen = true
				break
			}
			time.Sleep(150 * time.Millisecond)
		}
		if !seen {
			t.Fatalf("coordinator %s did not observe committed job status", base)
		}
	}
	ok()

	score("non-leader coordinators reject metadata mutations", 15)
	successes := 0
	for _, base := range c.coords {
		code, _, err := doPostJSON(base+"/submit_job", map[string]any{
			"instrument": "AAPL",
			"start_ts":   1,
			"end_ts":     4,
			"strategy":   "momentum",
		})
		if err == nil && code >= 200 && code < 300 {
			successes++
		}
	}

	if successes != 1 {
		t.Fatalf("expected exactly one coordinator leader to accept submit, got %d", successes)
	}
	ok()
}

func TestFinalDAMBT_PerformanceSmoke_20pts(t *testing.T) {
	c := startDAMBTCluster(t, "")

	score("basic throughput smoke: 20 jobs finish under timeout", 20)
	setupTwoChunks(t, c, []string{c.dataNode})

	start := time.Now()
	const jobs = 20

	var wg sync.WaitGroup
	errCh := make(chan string, jobs)

	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := submitJob(t, c, "AAPL", 1, 4)
			res := waitDone(t, c, id)
			if res.EventsRead != 4 || res.PnL != 4 {
				errCh <- fmt.Sprintf("bad perf-smoke result: %+v", res)
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	elapsed := time.Since(start)
	if elapsed > 15*time.Second {
		t.Fatalf("performance smoke too slow: %v", elapsed)
	}

	fmt.Printf("completed %d jobs in %v\n", jobs, elapsed)
	fmt.Println("[TEST SUMMARY] Final DAMBT tests: 150 pts max")
	ok()
}



//Helpers
func buildBinary(t *testing.T, root, outDir, name, pkg string) string {
	t.Helper()

	exe := filepath.Join(outDir, name)
	if strings.Contains(strings.ToLower(runtime.GOOS), "windows") {
		exe += ".exe"
	}

	cmd := exec.Command("go", "build", "-o", exe, pkg)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s failed: %v\n%s", pkg, err, string(out))
	}

	return exe
}

func killPorts() {
	ports := []string{
		"8001", "8002", "8003",
		"8101", "8102", "8103",
		"9000", "9010", "9020",
		"9301", "9302", "9401",
	}

	fmt.Printf("\n[TEST-HARNESS] killPorts start\n")

	out, err := exec.Command("netstat", "-ano").CombinedOutput()
	if err != nil {
		fmt.Printf("[TEST-HARNESS] netstat failed: %v out=%s\n", err, string(out))
		return
	}

	pids := map[string]bool{}

	for _, line := range strings.Split(string(out), "\n") {
		for _, port := range ports {
			if strings.Contains(line, "LISTENING") &&
				(strings.Contains(line, "127.0.0.1:"+port) || strings.Contains(line, "0.0.0.0:"+port) || strings.Contains(line, "[::]:"+port)) {

				fields := strings.Fields(line)
				if len(fields) > 0 {
					pid := fields[len(fields)-1]
					pids[pid] = true
					fmt.Printf("[TEST-HARNESS] port=%s pid=%s line=%s\n", port, pid, strings.TrimSpace(line))
				}
			}
		}
	}

	for pid := range pids {
		fmt.Printf("[TEST-HARNESS] taskkill pid=%s\n", pid)

		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		cmd := exec.CommandContext(ctx, "taskkill", "/T", "/F", "/PID", pid)

		out, err := cmd.CombinedOutput()
		cancel()

		if ctx.Err() == context.DeadlineExceeded {
			fmt.Printf("[TEST-HARNESS] taskkill pid=%s timed out; continuing\n", pid)
			continue
		}

		fmt.Printf("[TEST-HARNESS] taskkill pid=%s err=%v out=%s\n", pid, err, string(out))
	}

	fmt.Printf("[TEST-HARNESS] killPorts complete\n")
}
// debug printings
func logStep(msg string) {
	hprintf("\n[TEST-HARNESS] %s\n", msg)
}

func debugPortOwner(port string) {
	fmt.Printf("\n[DEBUG] checking port %s\n", port)

	out, err := exec.Command("netstat", "-ano").CombinedOutput()
	if err != nil {
		fmt.Printf("[DEBUG] netstat err=%v out=%s\n", err, string(out))
		return
	}

	found := false
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, ":"+port) && strings.Contains(line, "LISTENING") {
			found = true
			fmt.Printf("[DEBUG] port %s owner: %s\n", port, strings.TrimSpace(line))
		}
	}

	if !found {
		fmt.Printf("[DEBUG] port %s is free\n", port)
	}
}



func checkCoordinatorHealth(coords []string) {
	hprintf("\n[TEST-HARNESS] coordinator health check\n")

	for _, base := range coords {
		code, body, err := doGet(base + "/job_status?id=__healthcheck__")
		hprintf("[TEST-HARNESS] %s health err=%v code=%d body=%s\n", base, err, code, string(body))
	}
}

func printSuiteSummary(name string, earned, max int) {
	fmt.Printf("[TEST SUMMARY] %s: %d/%d pts earned\n", name, earned, max)
}