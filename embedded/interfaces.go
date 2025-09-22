package embedded

import (
	"context"
	"time"
)

const (
	EventRecordStatusNew        = 0
	EventRecordStatusSent       = 1
	EventRecordStatusRetry      = 2
	EventRecordStatusError      = 3
	EventRecordStatusProcessing = 4
)

type EventRecord struct {
	ID            int64
	AggregateType string
	AggregateID   string
	EventID       string
	EventType     string
	Payload       []byte
	Headers       []byte
	Topic         string
	AttemptCount  int
	NextAttemptAt *time.Time
}

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

type Publisher interface {
	Publish(ctx context.Context, event EventRecord) error
	Close() error
}

type EventProcessor interface {
	ProcessEvents(ctx context.Context) error
}

type DeadLetterService interface {
	MoveToDeadLetters(ctx context.Context) error
}

type StuckEventService interface {
	RecoverStuckEvents(ctx context.Context) error
}

type CleanupService interface {
	Cleanup(ctx context.Context) error
}

type MetricsCollector interface {
	IncrementCounter(name string, tags map[string]string)
	RecordDuration(name string, duration time.Duration, tags map[string]string)
	RecordGauge(name string, value float64, tags map[string]string)
}

type Worker interface {
	Start(ctx context.Context)
	Stop()
	Name() string
}
