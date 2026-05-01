package dambt


// * RunStrategy - executes the requested backtesting strategy over market events.
//
// Behaviors:
// Sorts events by timestamp, applies the selected strategy logic, and returns
// a JobResult containing event count, PnL, and execution logs.
//
// Limitations / potential failure scenarios:
// 1. Mutates event order in-place by sorting the input slice.
// 2. Only implements simple momentum behavior; unknown strategies produce zero PnL.
// 3. Does not model transaction costs, positions, slippage, or risk constraints.
func RunStrategy(job JobSpec, events []MarketEvent) JobResult {
	SortEvents(events)

	pnl := 0.0
	logs := []string{}

	for i := 1; i < len(events); i++ {
		prev := events[i-1]
		curr := events[i]

		if job.Strategy == "momentum" {
			if curr.Price > prev.Price {
				pnl += curr.Price - prev.Price
			}
		} else {
			pnl += 0
		}
	}
	return JobResult{
		JobID:      job.JobID,
		EventsRead: len(events),
		PnL:        pnl,
		Logs:       logs,
	}
}


// parallel processing

// * MergeResults -- combines partial worker results into one job result.
//
// Behaviors:
// Aggregates event counts, sums partial PnL values, and concatenates logs
// from all worker-produced results.
//
// Limitations / potential failure scenarios:
// 1. Assumes partial results are independent and safe to sum.
// 2. Does not restore global event ordering across worker partitions.
func MergeResults(job JobSpec, parts []JobResult) JobResult {
	totalEvents := 0
	totalPnL := 0.0
	logs := []string{}

	for _, r := range parts {
		totalEvents += r.EventsRead
		totalPnL += r.PnL
		logs = append(logs, r.Logs...)
	}

	return JobResult{
		JobID:      job.JobID,
		EventsRead: totalEvents,
		PnL:        totalPnL,
		Logs:       logs,
	}
}