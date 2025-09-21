package outbox

import "time"

const (
	EventStatusNew        = 0
	EventStatusSent       = 1
	EventStatusRetry      = 2
	EventStatusError      = 3
	EventStatusProcessing = 4
)

// Event is the user-facing representation of an outbox event before it is saved.
type Event struct {
	EventID       string            `json:"event_id"`
	EventType     string            `json:"event_type"`
	AggregateType string            `json:"aggregate_type"`
	AggregateID   string            `json:"aggregate_id"`
	Topic         string            `json:"topic"`
	Payload       interface{}       `json:"payload"`
	Headers       map[string]string `json:"headers"`
}

// EventRecord is the database representation of an outbox event.
type EventRecord struct {
	ID            int64
	AggregateType string
	AggregateID   string
	EventID       string
	EventType     string
	Payload       []byte
	Headers       []byte
	Topic         string
	Status        int
	AttemptCount  int
	LastError     string
	NextAttemptAt *time.Time
}

// DeadLetterRecord is the database representation of an event that has failed processing.
type DeadLetterRecord struct {
	ID            int64
	EventID       string
	EventType     string
	AggregateType string
	AggregateID   string
	Topic         string
	Payload       []byte
	Headers       []byte
	AttemptCount  int
	LastError     string
	CreatedAt     time.Time
}
