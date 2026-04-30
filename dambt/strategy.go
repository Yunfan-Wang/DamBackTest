package dambt

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