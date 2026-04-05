package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/overtonx/outbox/v3/serializer"
	"go.opentelemetry.io/otel"
)

var (
	ErrEventAlreadyExists = errors.New("event already exists")
)

// DBExecutor defines the interface for executing database queries.
// This allows for using either a *sql.DB or *sql.Tx.

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type Event struct {
	EventID       string            `json:"event_id"`
	EventType     string            `json:"event_type"`
	AggregateType string            `json:"aggregate_type"`
	AggregateID   string            `json:"aggregate_id"`
	Topic         string            `json:"topic"`
	Payload       interface{}       `json:"payload"`
	Headers       map[string]string `json:"headers"`
}

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

// injectTraceContext injects the tracing context from the context into the event headers.
func injectTraceContext(ctx context.Context, event *Event) {
	carrier := NewMessageCarrier(event)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// SaveEvent saves an event to the outbox table using JSON serialization.
//
// Deprecated: Use EventStore.Save with an explicit Serializer instead.
// SaveEvent will be removed in a future major version.
func SaveEvent(ctx context.Context, exec DBExecutor, event Event) error {
	return NewEventStore(serializer.JSONSerializer{}).Save(ctx, exec, event)
}

func convertFromDBError(err error) error {
	var msqlError *mysql.MySQLError
	if ok := errors.As(err, &msqlError); ok {
		switch msqlError.Number {
		case 1062: // err duplicate rows
			return ErrEventAlreadyExists
		}
	}

	return err
}

func ensureOutboxTable(ctx context.Context, db *sql.DB) error {
	err := createOutboxEventsTable(ctx, db)
	if err != nil {
		return err
	}

	err = createOutboxDeadlettersTable(ctx, db)
	if err != nil {
		return err
	}

	return nil
}

func createOutboxEventsTable(ctx context.Context, db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS outbox_events (
			id              BIGINT AUTO_INCREMENT PRIMARY KEY,
			event_id        CHAR(36)     NOT NULL UNIQUE,
			event_type      VARCHAR(255) NOT NULL,
			aggregate_type  VARCHAR(255) NOT NULL,
			aggregate_id    VARCHAR(255) NOT NULL,
			status          INT          NOT NULL DEFAULT 0 COMMENT '0 - new, 1 - success, 2 - retry, 3 - error, 4 - processing',
			topic           VARCHAR(255) NOT NULL,
			content_type    VARCHAR(100) NOT NULL DEFAULT 'application/json',
			payload         LONGBLOB     NOT NULL,
			headers         JSON         NULL,
			attempt_count   INT          NOT NULL DEFAULT 0,
			next_attempt_at TIMESTAMP    NULL,
			last_error      TEXT         NULL,
			created_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			updated_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
			INDEX idx_status_next_attempt (status, next_attempt_at),
			INDEX idx_aggregate (aggregate_type, aggregate_id),
			INDEX idx_created_at (created_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	`

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create outbox_events table: %w", err)
	}

	return nil
}

func createOutboxDeadlettersTable(ctx context.Context, db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS outbox_deadletters
		(
		    id              BIGINT PRIMARY KEY,
		    event_id        CHAR(36)      NOT NULL UNIQUE,
		    event_type      VARCHAR(255)  NOT NULL,
		    aggregate_type  VARCHAR(255)  NOT NULL,
		    aggregate_id    VARCHAR(255)  NOT NULL,
		    topic           VARCHAR(255)  NOT NULL,
		    content_type    VARCHAR(100)  NOT NULL DEFAULT 'application/json',
		    payload         LONGBLOB      NOT NULL,
		    headers         JSON          NULL,
		    attempt_count   INT           NOT NULL,
		    last_error      VARCHAR(2000) NULL,
		    created_at      TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
	`

	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create outbox_deadletters table: %w", err)
	}

	return nil
}

func validateOutboxEvent(event Event) error {
	if event.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if event.AggregateType == "" {
		return fmt.Errorf("aggregate_type is required")
	}
	if event.AggregateID == "" {
		return fmt.Errorf("aggregate_id is required")
	}
	if event.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	return nil
}
