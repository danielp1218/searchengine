package structs

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiterExponentialBackoffAndReset(t *testing.T) {
	rl := NewRateLimiterWithInterval(20 * time.Millisecond)
	rl.SetBackoffConfig(BackoffConfig{
		Enabled:        true,
		Multiplier:     2,
		MaxInterval:    80 * time.Millisecond,
		ResetOnSuccess: true,
	})

	if err := rl.Enqueue("example.com", "https://example.com/a"); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	if _, err := rl.DequeueReady(ctx); err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}

	rl.MarkFailure("example.com")
	if got := rl.GetBucketInterval("example.com"); got != 40*time.Millisecond {
		t.Fatalf("unexpected interval after first failure: got %v want %v", got, 40*time.Millisecond)
	}

	rl.MarkFailure("example.com")
	if got := rl.GetBucketInterval("example.com"); got != 80*time.Millisecond {
		t.Fatalf("unexpected interval after second failure: got %v want %v", got, 80*time.Millisecond)
	}

	rl.MarkFailure("example.com")
	if got := rl.GetBucketInterval("example.com"); got != 80*time.Millisecond {
		t.Fatalf("interval should cap at max: got %v want %v", got, 80*time.Millisecond)
	}

	rl.MarkSuccess("example.com")
	if got := rl.GetBucketInterval("example.com"); got != 20*time.Millisecond {
		t.Fatalf("interval should reset on success: got %v want %v", got, 20*time.Millisecond)
	}
}

func TestRateLimiterSchedulingRespectsBackoffDelay(t *testing.T) {
	rl := NewRateLimiterWithInterval(15 * time.Millisecond)
	rl.SetBackoffConfig(BackoffConfig{
		Enabled:        true,
		Multiplier:     2,
		MaxInterval:    120 * time.Millisecond,
		ResetOnSuccess: true,
	})

	if err := rl.Enqueue("example.com", "https://example.com/1"); err != nil {
		t.Fatalf("enqueue first failed: %v", err)
	}
	if err := rl.Enqueue("example.com", "https://example.com/2"); err != nil {
		t.Fatalf("enqueue second failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	first, err := rl.DequeueReady(ctx)
	if err != nil {
		t.Fatalf("first dequeue failed: %v", err)
	}
	if first.URL != "https://example.com/1" {
		t.Fatalf("unexpected first URL: got %s", first.URL)
	}

	rl.MarkFailure("example.com")

	start := time.Now()
	second, err := rl.DequeueReady(ctx)
	if err != nil {
		t.Fatalf("second dequeue failed: %v", err)
	}
	elapsed := time.Since(start)

	if second.URL != "https://example.com/2" {
		t.Fatalf("unexpected second URL: got %s", second.URL)
	}

	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected backoff scheduling delay, got %v", elapsed)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("unexpectedly long scheduling delay: %v", elapsed)
	}
}
