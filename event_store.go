package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	trmsql "github.com/avito-tech/go-transaction-manager/drivers/sql/v2"
	"github.com/google/uuid"
	"github.com/overtonx/outbox/v3/serializer"
)

// EventStore saves events to the outbox table using the configured Serializer.
type EventStore struct {
	serializer serializer.Serializer
	db         *sql.DB
	getter     *trmsql.CtxGetter
}

// NewEventStore creates an EventStore with the given Serializer.
// Use Save to provide an explicit executor (db or tx).
func NewEventStore(s serializer.Serializer) *EventStore {
	return &EventStore{serializer: s}
}

// NewEventStoreWithDB creates an EventStore that can resolve the executor from
// context via go-transaction-manager, falling back to db when no transaction
// is active. getter is optional; trmsql.DefaultCtxGetter is used when omitted.
func NewEventStoreWithDB(db *sql.DB, s serializer.Serializer, getter ...*trmsql.CtxGetter) *EventStore {
	g := trmsql.DefaultCtxGetter
	if len(getter) > 0 && getter[0] != nil {
		g = getter[0]
	}
	return &EventStore{serializer: s, db: db, getter: g}
}

// Save serializes the event payload with the configured Serializer and inserts
// the event into the outbox table. exec may be a *sql.DB or *sql.Tx.
func (s *EventStore) Save(ctx context.Context, exec DBExecutor, event Event) error {
	return s.save(ctx, exec, event)
}

// SaveCtx resolves the executor from ctx using go-transaction-manager.
// When no active transaction is found in ctx it falls back to the db
// provided to NewEventStoreWithDB. Returns an error if no db was configured.
func (s *EventStore) SaveCtx(ctx context.Context, event Event) error {
	if s.getter == nil || s.db == nil {
		return fmt.Errorf("outbox: EventStore has no db configured; use NewEventStoreWithDB or call Save with an explicit executor")
	}
	exec := s.getter.DefaultTrOrDB(ctx, s.db)
	return s.save(ctx, exec, event)
}

func (s *EventStore) save(ctx context.Context, exec DBExecutor, event Event) error {
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
