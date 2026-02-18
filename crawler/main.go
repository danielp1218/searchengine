package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"github.com/dtpu/searchengine/crawler/parser"
	"github.com/dtpu/searchengine/crawler/structs"
	"github.com/nats-io/nats.go/jetstream"
)

const NUM_WORKERS = 1000
const DEFAULT_BUCKET_INTERVAL = time.Second

type scheduledWork struct {
	item structs.BucketedURL
	msg  jetstream.Msg
}

type scheduledWorkStore struct {
	mu   sync.Mutex
	next atomic.Uint64
	data map[string]scheduledWork
}

func newScheduledWorkStore() *scheduledWorkStore {
	return &scheduledWorkStore{data: make(map[string]scheduledWork)}
}

func (s *scheduledWorkStore) put(work scheduledWork) string {
	id := fmt.Sprintf("work-%d", s.next.Add(1))
	s.mu.Lock()
	s.data[id] = work
	s.mu.Unlock()
	return id
}

func (s *scheduledWorkStore) take(id string) (scheduledWork, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	work, ok := s.data[id]
	if ok {
		delete(s.data, id)
	}
	return work, ok
}

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

func startQueueIngestor(ctx context.Context, q *structs.UrlQueue, limiter *structs.RateLimiter, store *scheduledWorkStore) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg, err := q.Dequeue()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Println("Error dequeuing from NATS:", err)
				continue
			}

			item, err := q.DecodeMessage(msg)
			if err != nil {
				log.Println("Failed to decode queued message:", err)
				if nakErr := msg.Nak(); nakErr != nil {
					log.Println("Failed to NAK undecodable message:", nakErr)
				}
				continue
			}

			workID := store.put(scheduledWork{item: item, msg: msg})
			item.WorkID = workID

			if err := limiter.EnqueueItem(item); err != nil {
				log.Println("Failed to enqueue item into rate limiter:", err)
				if _, ok := store.take(workID); ok {
					if nakErr := msg.Nak(); nakErr != nil {
						log.Println("Failed to NAK message after limiter enqueue failure:", nakErr)
					}
				}
			}
		}
	}()
}

func startWorkers(ctx context.Context, q *structs.UrlQueue, limiter *structs.RateLimiter, store *scheduledWorkStore, statsTrackerChan chan<- structs.StatsEvent) {
	for i := 0; i < NUM_WORKERS; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				next, err := limiter.DequeueReady(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Println("Rate limiter dequeue error:", err)
					continue
				}

				work, ok := store.take(next.WorkID)
				if !ok {
					log.Println("Missing scheduled work for id:", next.WorkID)
					continue
				}

				if err := crawl(work.item.URL, q, statsTrackerChan); err != nil {
					limiter.MarkFailure(work.item.BucketKey)
					if nakErr := work.msg.Nak(); nakErr != nil {
						log.Println("Failed to NAK message:", nakErr)
					}
					continue
				}

				limiter.MarkSuccess(work.item.BucketKey)
				if ackErr := work.msg.Ack(); ackErr != nil {
					log.Println("Failed to ACK message:", ackErr)
				}
			}
		}()
	}
}

func startCrawler() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q, err := structs.InitializeQueue("nats://localhost:4222")
	if err != nil {
		panic("Failed to initialize queue:" + err.Error())
	}
	defer q.Close()

	limiter := structs.NewRateLimiterWithInterval(DEFAULT_BUCKET_INTERVAL)
	defer limiter.Close()

	workStore := newScheduledWorkStore()

	// check if there is already stuff in the queue
	if msg, err := q.Dequeue(); err == nil {
		if _, err := q.DecodeMessage(msg); err == nil {
			log.Println("Queue is not empty, skipping seeding.")
			if err := msg.Ack(); err != nil {
				log.Println("Failed to ACK message during startup check:", err)
			}
		} else {
			log.Println("Failed to decode message during startup check:", err)
			if err := msg.Nak(); err != nil {
				log.Println("Failed to NAK message during startup check:", err)
			}
		}
	}
	
	// seed initial URLs
	if err := q.EnqueueBatch([]string {
		"https://example.com",
		"https://danielpu.dev",
	}); err != nil {
		log.Println("Failed to enqueue seed URLs:", err)
	}

	statsTrackerChan := make(chan structs.StatsEvent, 1000)
	go structs.StatsTracker(statsTrackerChan)

	startQueueIngestor(ctx, q, limiter, workStore)
	startWorkers(ctx, q, limiter, workStore, statsTrackerChan)

	select {}
}

func main() {
	startCrawler()
}
