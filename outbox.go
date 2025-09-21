package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel"
)

var (
	// ErrEventAlreadyExists is returned when trying to save an event with a duplicate event_id.
	ErrEventAlreadyExists = errors.New("event already exists")
)

// NewOutboxEvent creates a new user-facing event to be saved.
func NewOutboxEvent(eventID, eventType, aggregateType, aggregateID, topic string, payload interface{}, headers map[string]string) (Event, error) {
	if headers == nil {
		headers = make(map[string]string)
	}
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

// SaveEvent saves an outbox event to the database within the given transaction.
func SaveEvent(ctx context.Context, tx *sql.Tx, event Event) error {
	if err := validateOutboxEvent(event); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Inject OpenTelemetry trace context into the event headers.
	carrier := NewMessageCarrier(&event)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	query := `
		INSERT INTO outbox_events 
		(event_id, event_type, aggregate_type, aggregate_id, topic, payload, headers, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`

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

	_, err = tx.ExecContext(ctx, query,
		event.EventID,
		event.EventType,
		event.AggregateType,
		event.AggregateID,
		event.Topic,
		payloadJSON,
		headersJSON,
		EventStatusNew,
	)

	if err != nil {
		return fmt.Errorf("failed to save outbox event: %w", convertFromDBError(err))
	}

	return nil
}

// convertFromDBError converts specific database driver errors to application-specific errors.
func convertFromDBError(err error) error {
	var mysqlError *mysql.MySQLError
	if errors.As(err, &mysqlError) {
		if mysqlError.Number == 1062 { // Error 1062: Duplicate entry
			return ErrEventAlreadyExists
		}
	}
	return err
}

// validateOutboxEvent checks for required fields in an Event.
func validateOutboxEvent(event Event) error {
	if event.AggregateType == "" {
		return fmt.Errorf("aggregate_type is required")
	}
	if event.AggregateID == "" {
		return fmt.Errorf("aggregate_id is required")
	}
	// Topic is not strictly required here, as the publisher can have a default.
	// However, for clarity, we can enforce it if needed.
	// if event.Topic == "" {
	// 	return fmt.Errorf("topic is required")
	// }
	return nil
}
