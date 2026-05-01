package dambt_test

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPerfDAMBT_ThroughputAndLatency_50pts(t *testing.T) {
	// c := startDAMBTCluster(t, "127.0.0.1:9301,127.0.0.1:9302")
	c := startDAMBTCluster(t, "")
	score("performance setup: register chunks", 0)
	// setupTwoChunks(t, c, []string{"127.0.0.1:9401"})
	setupTwoChunks(t, c, []string{c.dataNode})
	ok()

	const jobs = 50
	const eventsPerJob = 4
	totalEvents := jobs * eventsPerJob

	score("throughput: events/sec", 25)

	start := time.Now()

	var wg sync.WaitGroup
	errCh := make(chan string, jobs)
	latencies := make([]time.Duration, jobs)

	for i := 0; i < jobs; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			jobStart := time.Now()
			jobID := submitJob(t, c, "AAPL", 1, 4)
			res := waitDone(t, c, jobID)
			latencies[i] = time.Since(jobStart)

			if res.EventsRead != eventsPerJob || res.PnL != 4 {
				errCh <- fmt.Sprintf("bad result job=%s result=%+v", jobID, res)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	elapsed := time.Since(start)
	throughput := float64(totalEvents) / elapsed.Seconds()

	fmt.Printf("\n[PERF RESULT] jobs=%d events=%d elapsed=%v throughput=%.2f events/sec\n",
		jobs, totalEvents, elapsed, throughput)

	if throughput <= 0 {
		t.Fatal("throughput must be positive")
	}
	ok()

	score("job completion latency: avg/max", 25)

	var sum time.Duration
	var max time.Duration
	for _, l := range latencies {
		sum += l
		if l > max {
			max = l
		}
	}

	avg := sum / time.Duration(len(latencies))

	fmt.Printf("[PERF RESULT] avg_latency=%v max_latency=%v\n", avg, max)

	if avg > 5*time.Second {
		t.Fatalf("average latency too high: %v", avg)
	}
	ok()

	score("metadata/API overhead: status query latency", 25)

	jobID := submitJob(t, c, "AAPL", 1, 4)
	waitDone(t, c, jobID)

	const queries = 100
	metaStart := time.Now()

	for i := 0; i < queries; i++ {
		_ = getAnyOK(t, c.coords, "/job_status?id="+jobID)
	}

	metaElapsed := time.Since(metaStart)
	avgMeta := metaElapsed / queries

	fmt.Printf("[PERF RESULT] metadata_queries=%d total=%v avg=%v\n",
		queries, metaElapsed, avgMeta)

	if avgMeta > 500*time.Millisecond {
		t.Fatalf("metadata query overhead too high: avg=%v", avgMeta)
	}
	fmt.Println("[TEST SUMMARY] Performance DAMBT tests: 50 pts max")
	fmt.Println("[TEST SUMMARY] Total DAMBT autograded tests: 200 pts max")
	ok()
}