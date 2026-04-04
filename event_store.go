package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// EventStore saves events to the outbox table using the configured Serializer.
type EventStore struct {
	serializer Serializer
}

// NewEventStore creates an EventStore with the given Serializer.
func NewEventStore(serializer Serializer) *EventStore {
	return &EventStore{serializer: serializer}
}

// Save serializes the event payload with the configured Serializer and inserts
// the event into the outbox table. exec may be a *sql.DB or *sql.Tx.
func (s *EventStore) Save(ctx context.Context, exec DBExecutor, event Event) error {
	if event.EventID == "" {
		id, _ := uuid.NewV7()
		event.EventID = id.String()
	}

	if err := validateOutboxEvent(event); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	injectTraceContext(ctx, &event)

	if err := s.insertEvent(ctx, exec, event); err != nil {
		return fmt.Errorf("failed to save outbox event: %w", convertFromDBError(err))
	}

	return nil
}

func (s *EventStore) insertEvent(ctx context.Context, exec DBExecutor, event Event) error {
	payloadBytes, err := s.serializer.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("failed to serialize payload: %w", err)
	}

	var headersJSON []byte
	if len(event.Headers) > 0 {
		headersJSON, err = json.Marshal(event.Headers)
		if err != nil {
			return fmt.Errorf("failed to marshal headers: %w", err)
		}
	}

	query := `
		INSERT INTO outbox_events
		(event_id, event_type, aggregate_type, aggregate_id, topic, content_type, payload, headers, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err = exec.ExecContext(ctx, query,
		event.EventID,
		event.EventType,
		event.AggregateType,
		event.AggregateID,
		event.Topic,
		s.serializer.ContentType(),
		payloadBytes,
		headersJSON,
		EventRecordStatusNew,
	)

	return err
}
