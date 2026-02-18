package structs

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"
)

const baseDelay = time.Second

var (
	ErrQueueClosed  = errors.New("bucket queue is closed")
	ErrEmptyBucket  = errors.New("bucket key is required")
	ErrEmptyURL     = errors.New("url is required")
	ErrInvalidDelay = errors.New("bucket interval must be > 0")
)

type RateMutationFunc func(bucketKey string, currentInterval time.Duration, success bool) time.Duration

type BucketedURL struct {
	WorkID     string
	URL        string
	BucketKey  string
	EnqueuedAt time.Time
	Attempt    int
}

type bucketQueue struct {
	items []BucketedURL
	head  int
}

func (q *bucketQueue) Push(item BucketedURL) {
	q.items = append(q.items, item)
}

func (q *bucketQueue) Pop() (BucketedURL, bool) {
	if q.Len() == 0 {
		return BucketedURL{}, false
	}

	item := q.items[q.head]
	q.head++

	if q.head > 1024 && q.head*2 >= len(q.items) {
		q.items = append([]BucketedURL(nil), q.items[q.head:]...)
		q.head = 0
	}

	return item, true
}

func (q *bucketQueue) Len() int {
	return len(q.items) - q.head
}

type bucketState struct {
	queue        bucketQueue
	baseInterval time.Duration
	interval     time.Duration
	failures     int
	lastDispatch time.Time
	nextAllowed  time.Time
}

type bucketSchedule struct {
	bucketKey string
	readyAt   time.Time
	index     int
}

type bucketScheduleHeap []*bucketSchedule

func (h bucketScheduleHeap) Len() int { return len(h) }

func (h bucketScheduleHeap) Less(i, j int) bool {
	if h[i].readyAt.Equal(h[j].readyAt) {
		return h[i].bucketKey < h[j].bucketKey
	}
	return h[i].readyAt.Before(h[j].readyAt)
}

func (h bucketScheduleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *bucketScheduleHeap) Push(x any) {
	entry := x.(*bucketSchedule)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *bucketScheduleHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.index = -1
	*h = old[:n-1]
	return entry
}

type RateLimiter struct {
	mu              sync.Mutex
	buckets         map[string]*bucketState
	scheduled       map[string]*bucketSchedule
	ready           bucketScheduleHeap
	notify          chan struct{}
	defaultInterval time.Duration
	backoffConfig   BackoffConfig
	mutationFn      RateMutationFunc
	now             func() time.Time
	closed          bool
}

type BackoffConfig struct {
	Enabled        bool
	Multiplier     float64
	MaxInterval    time.Duration
	ResetOnSuccess bool
}

func defaultBackoffConfig(defaultInterval time.Duration) BackoffConfig {
	maxInterval := 30 * time.Second
	if defaultInterval > maxInterval {
		maxInterval = defaultInterval
	}

	return BackoffConfig{
		Enabled:        true,
		Multiplier:     2.0,
		MaxInterval:    maxInterval,
		ResetOnSuccess: true,
	}
}

func normalizeBackoffConfig(config BackoffConfig, defaultInterval time.Duration) BackoffConfig {
	if config.Multiplier <= 1 {
		config.Multiplier = 2.0
	}
	if config.MaxInterval <= 0 {
		config.MaxInterval = 30 * time.Second
	}
	if config.MaxInterval < defaultInterval {
		config.MaxInterval = defaultInterval
	}
	return config
}

func NewRateLimiter() *RateLimiter {
	return NewRateLimiterWithInterval(baseDelay)
}

func NewRateLimiterWithInterval(defaultInterval time.Duration) *RateLimiter {
	if defaultInterval <= 0 {
		defaultInterval = baseDelay
	}

	rl := &RateLimiter{
		buckets:         make(map[string]*bucketState),
		scheduled:       make(map[string]*bucketSchedule),
		ready:           make(bucketScheduleHeap, 0, 64),
		notify:          make(chan struct{}, 1),
		defaultInterval: defaultInterval,
		backoffConfig:   defaultBackoffConfig(defaultInterval),
		now:             time.Now,
	}

	heap.Init(&rl.ready)
	return rl
}

func (rl *RateLimiter) SetBackoffConfig(config BackoffConfig) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.backoffConfig = normalizeBackoffConfig(config, rl.defaultInterval)
}

func (rl *RateLimiter) GetBackoffConfig() BackoffConfig {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.backoffConfig
}

func (rl *RateLimiter) SetMutationFunc(fn RateMutationFunc) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.mutationFn = fn
}

func (rl *RateLimiter) Enqueue(bucketKey, url string) error {
	if bucketKey == "" {
		return ErrEmptyBucket
	}
	if url == "" {
		return ErrEmptyURL
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.closed {
		return ErrQueueClosed
	}

	bucket := rl.ensureBucketLocked(bucketKey)
	wasEmpty := bucket.queue.Len() == 0
	bucket.queue.Push(BucketedURL{
		URL:        url,
		BucketKey:  bucketKey,
		EnqueuedAt: rl.now(),
		Attempt:    0,
	})

	if wasEmpty {
		readyAt := bucket.nextAllowed
		now := rl.now()
		if readyAt.IsZero() || readyAt.Before(now) {
			readyAt = now
		}
		rl.upsertScheduleLocked(bucketKey, readyAt)
		rl.signalLocked()
	}

	return nil
}

func (rl *RateLimiter) EnqueueItem(item BucketedURL) error {
	if item.BucketKey == "" {
		return ErrEmptyBucket
	}
	if item.URL == "" {
		return ErrEmptyURL
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.closed {
		return ErrQueueClosed
	}

	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = rl.now()
	}

	bucket := rl.ensureBucketLocked(item.BucketKey)
	wasEmpty := bucket.queue.Len() == 0
	bucket.queue.Push(item)

	if wasEmpty {
		readyAt := bucket.nextAllowed
		now := rl.now()
		if readyAt.IsZero() || readyAt.Before(now) {
			readyAt = now
		}
		rl.upsertScheduleLocked(item.BucketKey, readyAt)
		rl.signalLocked()
	}

	return nil
}

// blocks until at least one bucket becomes eligible
// rate limit and returns the next URL from that bucket.
func (rl *RateLimiter) DequeueReady(ctx context.Context) (BucketedURL, error) {
	for {
		rl.mu.Lock()

		if rl.closed {
			rl.mu.Unlock()
			return BucketedURL{}, ErrQueueClosed
		}

		if len(rl.ready) == 0 {
			rl.mu.Unlock()
			if err := rl.waitForSignalOrContext(ctx, 0); err != nil {
				return BucketedURL{}, err
			}
			continue
		}

		next := rl.ready[0]
		now := rl.now()
		if next.readyAt.After(now) {
			waitFor := next.readyAt.Sub(now)
			rl.mu.Unlock()
			if err := rl.waitForSignalOrContext(ctx, waitFor); err != nil {
				return BucketedURL{}, err
			}
			continue
		}

		heap.Pop(&rl.ready)
		delete(rl.scheduled, next.bucketKey)

		bucket, ok := rl.buckets[next.bucketKey]
		if !ok || bucket.queue.Len() == 0 {
			rl.mu.Unlock()
			continue
		}

		item, ok := bucket.queue.Pop()
		if !ok {
			rl.mu.Unlock()
			continue
		}

		bucket.lastDispatch = now
		bucket.nextAllowed = now.Add(bucket.interval)

		if bucket.queue.Len() > 0 {
			rl.upsertScheduleLocked(next.bucketKey, bucket.nextAllowed)
		}

		rl.mu.Unlock()
		return item, nil
	}
}

func (rl *RateLimiter) SetBucketInterval(bucketKey string, interval time.Duration) error {
	if bucketKey == "" {
		return ErrEmptyBucket
	}
	if interval <= 0 {
		return ErrInvalidDelay
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket := rl.ensureBucketLocked(bucketKey)
	bucket.interval = interval
	bucket.baseInterval = interval
	bucket.failures = 0
	now := rl.now()
	if !bucket.lastDispatch.IsZero() {
		nextAllowed := bucket.lastDispatch.Add(interval)
		if nextAllowed.Before(now) {
			nextAllowed = now
		}
		bucket.nextAllowed = nextAllowed
	}

	if bucket.queue.Len() > 0 {
		readyAt := bucket.nextAllowed
		if readyAt.IsZero() || readyAt.Before(now) {
			readyAt = now
		}
		rl.upsertScheduleLocked(bucketKey, readyAt)
		rl.signalLocked()
	}

	return nil
}

func (rl *RateLimiter) GetBucketInterval(bucketKey string) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, ok := rl.buckets[bucketKey]
	if !ok {
		return rl.defaultInterval
	}
	return bucket.interval
}

func (rl *RateLimiter) MarkSuccess(bucketKey string) {
	rl.applyMutation(bucketKey, true)
}

func (rl *RateLimiter) MarkFailure(bucketKey string) {
	rl.applyMutation(bucketKey, false)
}

func (rl *RateLimiter) BucketSize(bucketKey string) int {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, ok := rl.buckets[bucketKey]
	if !ok {
		return 0
	}
	return bucket.queue.Len()
}

func (rl *RateLimiter) Size() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	total := 0
	for _, bucket := range rl.buckets {
		total += bucket.queue.Len()
	}

	return total
}

func (rl *RateLimiter) BucketCount() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return len(rl.buckets)
}

func (rl *RateLimiter) Close() {
	rl.mu.Lock()
	rl.closed = true
	rl.signalLocked()
	rl.mu.Unlock()
}

func (rl *RateLimiter) ensureBucketLocked(bucketKey string) *bucketState {
	bucket, ok := rl.buckets[bucketKey]
	if !ok {
		bucket = &bucketState{interval: rl.defaultInterval}
		bucket.baseInterval = rl.defaultInterval
		rl.buckets[bucketKey] = bucket
	}
	return bucket
}

func (rl *RateLimiter) upsertScheduleLocked(bucketKey string, readyAt time.Time) {
	if existing, ok := rl.scheduled[bucketKey]; ok {
		existing.readyAt = readyAt
		heap.Fix(&rl.ready, existing.index)
		return
	}

	entry := &bucketSchedule{
		bucketKey: bucketKey,
		readyAt:   readyAt,
	}
	heap.Push(&rl.ready, entry)
	rl.scheduled[bucketKey] = entry
}

func (rl *RateLimiter) signalLocked() {
	select {
	case rl.notify <- struct{}{}:
	default:
	}
}

func (rl *RateLimiter) applyMutation(bucketKey string, success bool) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if bucketKey == "" {
		return
	}

	bucket := rl.ensureBucketLocked(bucketKey)

	if rl.mutationFn != nil {
		next := rl.mutationFn(bucketKey, bucket.interval, success)
		if next <= 0 {
			return
		}
		bucket.interval = next
	} else {
		rl.applyDefaultBackoffLocked(bucket, success)
	}

	now := rl.now()
	if !bucket.lastDispatch.IsZero() {
		nextAllowed := bucket.lastDispatch.Add(bucket.interval)
		if nextAllowed.Before(now) {
			nextAllowed = now
		}
		bucket.nextAllowed = nextAllowed
	} else {
		bucket.nextAllowed = now
	}

	if bucket.queue.Len() > 0 {
		readyAt := bucket.nextAllowed
		if readyAt.IsZero() || readyAt.Before(now) {
			readyAt = now
		}
		rl.upsertScheduleLocked(bucketKey, readyAt)
		rl.signalLocked()
	}
}

func (rl *RateLimiter) applyDefaultBackoffLocked(bucket *bucketState, success bool) {
	config := rl.backoffConfig
	if !config.Enabled {
		return
	}

	if bucket.baseInterval <= 0 {
		bucket.baseInterval = rl.defaultInterval
	}
	if bucket.interval <= 0 {
		bucket.interval = bucket.baseInterval
	}

	if success {
		if config.ResetOnSuccess {
			bucket.interval = bucket.baseInterval
			bucket.failures = 0
		}
		return
	}

	bucket.failures++
	next := time.Duration(float64(bucket.interval) * config.Multiplier)
	if next < bucket.interval {
		next = bucket.interval
	}
	if next > config.MaxInterval {
		next = config.MaxInterval
	}
	bucket.interval = next
}

func (rl *RateLimiter) waitForSignalOrContext(ctx context.Context, duration time.Duration) error {
	if duration < 0 {
		duration = 0
	}

	if duration == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rl.notify:
			return nil
		}
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rl.notify:
		return nil
	case <-timer.C:
		return nil
	}
}
