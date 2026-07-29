package outbox

import "time"

const (
	EventRecordStatusNew        = 0
	EventRecordStatusSent       = 1
	EventRecordStatusRetry      = 2
	EventRecordStatusError      = 3
	EventRecordStatusProcessing = 4
)

// EventRecord — строка outbox_events, прочитанная для публикации.
type EventRecord struct {
	ID            int64
	AggregateType string
	AggregateID   string
	EventID       string
	EventType     string
	ContentType   string
	Payload       []byte
	Headers       []byte
	Topic         string
	AttemptCount  int
	NextAttemptAt *time.Time
}

// DeadLetterRecord — строка outbox_events, перенесённая в outbox_deadletters
// после исчерпания попыток публикации.
type DeadLetterRecord struct {
	ID            int64
	EventID       string
	EventType     string
	AggregateType string
	AggregateID   string
	Topic         string
	ContentType   string
	Payload       []byte
	Headers       []byte
	AttemptCount  int
	LastError     string
	CreatedAt     time.Time
}
