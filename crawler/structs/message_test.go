package structs

import "testing"

func TestMessageCompatibility(t *testing.T) {
	encoded, err := EncodeQueueMessage(QueueMessage{
		URL:       "https://example.com",
		BucketKey: "example.com",
		Attempt:   2,
	})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := DecodeQueueMessageCompatible(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.URL != "https://example.com" {
		t.Fatalf("unexpected URL: got %s", decoded.URL)
	}
	if decoded.BucketKey != "example.com" {
		t.Fatalf("unexpected bucket key: got %s", decoded.BucketKey)
	}
	if decoded.Attempt != 2 {
		t.Fatalf("unexpected attempt: got %d", decoded.Attempt)
	}
}
