package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/otel"
)

var (
	ErrEventAlreadyExists = errors.New("event already exists")
)

// DBExecutor определяет интерфейс для выполнения запросов к базе данных.
// Позволяет использовать как *sql.DB, так и *sql.Tx.

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

// injectTraceContext внедряет трассировочный контекст из ctx в заголовки события.
func injectTraceContext(ctx context.Context, event *Event) {
	carrier := NewMessageCarrier(event)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

func convertFromDBError(err error) error {
	var msqlError *mysql.MySQLError
	if ok := errors.As(err, &msqlError); ok {
		switch msqlError.Number {
		case 1062: // дублирующаяся строка
			return ErrEventAlreadyExists
		}
	}

	return err
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
