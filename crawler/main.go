// Example using net/http, goroutines, and channels
package main

import (
	"log"
	"net/http"
	"github.com/dtpu/searchengine/crawler/structs"
	"github.com/dtpu/searchengine/crawler/parser"
	"github.com/nats-io/nats.go/jetstream"
)

const NUM_WORKERS = 1000

func crawl(url string, q *structs.UrlQueue, statsTrackerChan chan<- structs.StatsEvent) error {
	resp, err := http.Get(url)
	if err != nil {
		statsTrackerChan <- structs.StatsEvent{Type: "failed"}
		return err
	}

	parsedHTML, err := parser.ParseHTML(resp.Body, url)
	if err != nil {
		statsTrackerChan <- structs.StatsEvent{Type: "failed"}
		return err
	}
	for _, link := range parsedHTML.Links {
		//print(link, "\n")
		err := q.Enqueue(link)
		statsTrackerChan <- structs.StatsEvent{Type: "discovered"}
		if err != nil {
			log.Println("Failed to enqueue link:", link, err)
		}
	}

	defer resp.Body.Close()
	statsTrackerChan <- structs.StatsEvent{Type: "crawled"}
	return nil
}

func main() {
	q, err := structs.InitializeQueue("nats://localhost:4222")
	if err != nil {
        panic(err)
    }
    defer q.Close()

	print("Queue size: ", q.QueueSize(), "\n")

    
    // seed initial URLs
    q.EnqueueBatch([]string{
        "https://example.com",
        "https://danielpu.dev",
    })
	
    sem := make(chan struct{}, NUM_WORKERS)
    
	statsTrackerChan := make(chan structs.StatsEvent, 1000)
	go structs.StatsTracker(statsTrackerChan, q.QueueSize())

    for {
        sem <- struct{}{} // wait for worker slot

        msg, err := q.Dequeue()
        if err != nil {
            <-sem
            log.Println("Error dequeuing:", err)
            continue
        }
		
        
        go func(msg jetstream.Msg) {
            defer func() { <-sem }()

			url := string(msg.Data())
            

			if err := crawl(url, q, statsTrackerChan); err != nil {
                msg.Nak() // requeue
            } else {
                msg.Ack() // success
            }
        }(msg)
    }

	

}
