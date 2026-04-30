package dambt

import (
	"fmt"
	"sort"
	"time"
)

func NewJobID() JobID {
	return JobID(fmt.Sprintf("job-%d", time.Now().UnixNano()))
}

func SortEvents(events []MarketEvent) {
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp < events[j].Timestamp
	})
}

func ChunkOverlaps(c ChunkMeta, instrument string, startTs, endTs int64) bool {
	if c.Instrument != instrument {
		return false
	}
	return c.StartTs <= endTs && c.EndTs >= startTs
}