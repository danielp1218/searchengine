package structs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dtpu/searchengine/crawler/parser"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	stream_name    = "CRAWL_QUEUE"
	subject_prefix = "url."
	queue_subject  = subject_prefix + "queue"
	consumer_name  = "crawler-worker"
	iter_buffer    = 1000
)

type UrlQueue struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	stream   jetstream.Stream
	consumer jetstream.Consumer
	iter     jetstream.MessagesContext
	ctx      context.Context
	cancel   context.CancelFunc
}

func InitializeQueue(nats_url string) (*UrlQueue, error) {
	ctx, cancel := context.WithCancel(context.Background())

	nc, err := nats.Connect(
		nats_url,
		nats.Timeout(3*time.Second),
		nats.MaxReconnects(3),
		nats.ReconnectWait(1*time.Second),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()

	js, err := jetstream.New(nc)
	if err != nil {
		cancel()
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	st, err := js.Stream(initCtx, stream_name)
	if err != nil {
		st, err = js.CreateStream(initCtx, jetstream.StreamConfig{
			Name:      stream_name,
			Subjects:  []string{subject_prefix + ">"},
			Retention: jetstream.WorkQueuePolicy,
			Storage:   jetstream.FileStorage,
		})
		if err != nil {
			cancel()
			nc.Close()
			return nil, fmt.Errorf("failed to create stream: %w", err)
		}
	}

	c, err := st.Consumer(initCtx, consumer_name)
	if err != nil {
		c, err = st.CreateConsumer(initCtx, jetstream.ConsumerConfig{
			Durable:       consumer_name,
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: queue_subject,
		})
		if err != nil {
			cancel()
			nc.Close()
			return nil, fmt.Errorf("failed to create consumer: %w", err)
		}
	}

	iter, err := c.Messages(jetstream.PullMaxMessages(iter_buffer))
	if err != nil {
		cancel()
		nc.Close()
		return nil, fmt.Errorf("failed to create message iterator: %w", err)
	}

	return &UrlQueue{nc: nc, js: js, stream: st, consumer: c, iter: iter, ctx: ctx, cancel: cancel}, nil
}

func (uq *UrlQueue) Enqueue(url string) error {
	if uq.ctx.Err() != nil {
		return errors.New("queue is closed")
	}

	bucketKey, err := parser.GetBucketKey(url)
	if err != nil {
		return fmt.Errorf("failed to derive bucket key: %w", err)
	}

	payload, err := EncodeQueueMessage(QueueMessage{
		URL:       url,
		BucketKey: bucketKey,
		Attempt:   0,
	})
	if err != nil {
		return fmt.Errorf("failed to encode queue message: %w", err)
	}

	ctx, cancel := context.WithTimeout(uq.ctx, 5*time.Second)
	defer cancel()

	_, err = uq.js.Publish(ctx, queue_subject, payload)
	if err != nil {
		return fmt.Errorf("failed to enqueue URL: %w", err)
	}

	return nil
}

func (uq *UrlQueue) EnqueueBatch(urls []string) error {
	if uq.ctx.Err() != nil {
		return errors.New("queue is closed")
	}

	for _, url := range urls {
		bucketKey, err := parser.GetBucketKey(url)
		if err != nil {
			return fmt.Errorf("failed to derive bucket key from URL %s: %w", url, err)
		}

		payload, err := EncodeQueueMessage(QueueMessage{
			URL:       url,
			BucketKey: bucketKey,
			Attempt:   0,
		})
		if err != nil {
			return fmt.Errorf("failed to encode URL %s: %w", url, err)
		}

		if _, err := uq.js.PublishAsync(queue_subject, payload); err != nil {
			return fmt.Errorf("failed to enqueue URL %s: %w", url, err)
		}
	}

	select {
	case <-uq.js.PublishAsyncComplete():
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for async publish completion")
	}
}

// blocks until a message is available.
func (uq *UrlQueue) Dequeue() (jetstream.Msg, error) {
	if uq.ctx.Err() != nil {
		return nil, errors.New("queue is closed")
	}

	msg, err := uq.iter.Next()
	if err != nil {
		return nil, fmt.Errorf("failed to get next message: %w", err)
	}

	return msg, nil
}

// might change later
func (uq *UrlQueue) DecodeMessage(msg jetstream.Msg) (BucketedURL, error) {
	decoded, err := DecodeQueueMessageStrict(msg.Data())
	if err != nil {
		return BucketedURL{}, fmt.Errorf("failed to decode queue message: %w", err)
	}

    fmt.Print(decoded.URL + "\n")
	return BucketedURL{
		URL:        decoded.URL,
		BucketKey:  decoded.BucketKey,
		EnqueuedAt: decoded.EnqueuedAt,
		Attempt:    decoded.Attempt,
	}, nil
}

func (uq *UrlQueue) Empty() bool {
	return uq.QueueSize() == 0
}

func (uq *UrlQueue) QueueSize() uint64 {
	info, err := uq.consumer.Info(uq.ctx)
	if err != nil {
		panic(err)
	}

	return info.NumPending
}

func (uq *UrlQueue) Close() error {
	uq.cancel()

	if uq.iter != nil {
		uq.iter.Stop()
	}

	if uq.nc != nil {
		if err := uq.nc.Drain(); err != nil {
			uq.nc.Close()
			return fmt.Errorf("failed to drain connection: %w", err)
		}
	}

	return nil
}

func (uq *UrlQueue) IsHealthy() bool {
	return uq.nc != nil && uq.nc.IsConnected()
}
