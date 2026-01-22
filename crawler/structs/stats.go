package structs

import "time"
import "fmt"

type Stats struct {
	PagesCrawled uint64
	PagesFailed uint64
	QueueSize uint64
	LinksFound   uint64
	ActiveWorkers uint64
}

type StatsEvent struct {
    Type string  // "crawled", "failed", "discovered"
}

func StatsTracker(eventChan <-chan StatsEvent, queueSize uint64) {
    // counters
    ticker := time.NewTicker(1 * time.Second)
	stats := Stats{QueueSize: queueSize}
    
    for {
        select {
        case event := <-eventChan:
			switch event.Type {
			case "crawled":
				stats.PagesCrawled++
				stats.QueueSize--
			case "failed":
				stats.PagesFailed++
				stats.QueueSize--
			case "discovered":
				stats.LinksFound++
				stats.QueueSize++
			}
        case <-ticker.C:
            // calculate rate (crawled in last second)
            
			fmt.Println("Stats - Crawled:", stats.PagesCrawled, "Failed:", stats.PagesFailed, "Links Found:", stats.LinksFound, "Queue Size:", stats.QueueSize)
        }
    }
}
