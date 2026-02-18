package structs

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const QueueMessageVersion = 1

var ErrInvalidQueueMessage = errors.New("invalid queue message")

type QueueMessage struct {
	Version    int       `json:"version"`
	URL        string    `json:"url"`
	BucketKey  string    `json:"bucket_key,omitempty"`
	EnqueuedAt time.Time `json:"enqueued_at,omitempty"`
	Attempt    int       `json:"attempt,omitempty"`
}

func (m QueueMessage) Validate() error {
	if strings.TrimSpace(m.URL) == "" {
		return ErrInvalidQueueMessage
	}
	return nil
}

func EncodeQueueMessage(m QueueMessage) ([]byte, error) {
	if m.Version == 0 {
		m.Version = QueueMessageVersion
	}
	if m.EnqueuedAt.IsZero() {
		m.EnqueuedAt = time.Now().UTC()
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

// decoding but strict (wow)
func DecodeQueueMessageStrict(data []byte) (QueueMessage, error) {
	var msg QueueMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return QueueMessage{}, err
	}
	if msg.Version == 0 {
		msg.Version = QueueMessageVersion
	}
	if err := msg.Validate(); err != nil {
		return QueueMessage{}, err
	}
	return msg, nil
}

// decoding but doesnt break everything on prev formats
// TODO: remove this stuff later
func DecodeQueueMessageCompatible(data []byte) (QueueMessage, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return QueueMessage{}, ErrInvalidQueueMessage
	}

	if strings.HasPrefix(trimmed, "{") {
		msg, err := DecodeQueueMessageStrict(data)
		if err == nil {
			return msg, nil
		}
	}

	return QueueMessage{
		Version:    QueueMessageVersion,
		URL:        trimmed,
		EnqueuedAt: time.Now().UTC(),
	}, nil
}
