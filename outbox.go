package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/overtonx/outbox/v2/storage"
)

// Event represents the data for an outbox event.
type Event struct {
	EventID       string            `json:"event_id"`
	EventType     string            `json:"event_type"`
	AggregateType string            `json:"aggregate_type"`
	AggregateID   string            `json:"aggregate_id"`
	Topic         string            `json:"topic"`
	Payload       interface{}       `json:"payload"`
	Headers       map[string]string `json:"headers"`
}

// NewOutboxEvent creates and validates a new Event.
func NewOutboxEvent(eventID, eventType, aggregateType, aggregateID, topic string, payload interface{}, headers map[string]string) (Event, error) {
	event := Event{
		EventID:       eventID,
		EventType:     eventType,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Topic:         topic,
		Payload:       payload,
		Headers:       headers,
	}

	if err := validateOutboxEvent(event); err != nil {
		return Event{}, err
	}

	return event, nil
}

// SaveEvent saves an event to the database using the provided Store within a transaction.
func SaveEvent(ctx context.Context, store storage.Store, tx storage.DBTX, event Event) error {
	if err := validateOutboxEvent(event); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Inject OpenTelemetry context
	carrier := NewMessageCarrier(&event)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	var headersJSON []byte
	if len(event.Headers) > 0 {
		headersJSON, err = json.Marshal(event.Headers)
		if err != nil {
			return fmt.Errorf("failed to marshal headers: %w", err)
		}
	}

	record := &storage.EventRecord{
		EventID:       event.EventID,
		EventType:     event.EventType,
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateID,
		Topic:         event.Topic,
		Payload:       payloadJSON,
		Headers:       headersJSON,
	}

	return store.CreateEvent(ctx, tx, record)
}

func validateOutboxEvent(event Event) error {
	if event.AggregateType == "" {
		return fmt.Errorf("aggregate_type is required")
	}
	if event.AggregateID == "" {
		return fmt.Errorf("aggregate_id is required")
	}
	if event.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	// EventID is often a UUID, which can be validated here if needed
	if event.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	return nil
}
