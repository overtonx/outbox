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

// EventMapper преобразует Event перед сохранением в outbox.
// Вызывается после инъекции трассировочного контекста и до сериализации.
type EventMapper func(Event) Event

// outboxRow содержит поля события, готовые для передачи в SQL-INSERT:
// payload и headers уже сериализованы в []byte.
type outboxRow struct {
	eventID       string
	eventType     string
	aggregateType string
	aggregateID   string
	topic         string
	contentType   string
	payload       []byte
	headers       []byte
}

// EventStore сохраняет события в таблицу outbox с использованием настроенного Serializer.
type EventStore struct {
	serializer serializer.Serializer
	db         *sql.DB
	getter     *trmsql.CtxGetter
	mapper     EventMapper
}

// NewEventStore создаёт EventStore с указанным Serializer.
// Для передачи явного исполнителя (db или tx) используйте Save.
func NewEventStore(s serializer.Serializer) *EventStore {
	return &EventStore{serializer: s}
}

// NewEventStoreWithDB создаёт EventStore, который может получать исполнителя из
// контекста через go-transaction-manager, используя db как резервный вариант
// при отсутствии активной транзакции. getter необязателен; при его отсутствии
// используется trmsql.DefaultCtxGetter.
func NewEventStoreWithDB(db *sql.DB, s serializer.Serializer, getter ...*trmsql.CtxGetter) *EventStore {
	g := trmsql.DefaultCtxGetter
	if len(getter) > 0 && getter[0] != nil {
		g = getter[0]
	}
	return &EventStore{serializer: s, db: db, getter: g}
}

// SaveWithDB сериализует полезную нагрузку события с помощью настроенного Serializer и вставляет
// событие в таблицу outbox. exec может быть *sql.DB или *sql.Tx.
func (s *EventStore) SaveWithDB(ctx context.Context, exec DBExecutor, event Event) error {
	return s.save(ctx, exec, event)
}

// Save получает исполнителя из ctx через go-transaction-manager.
// При отсутствии активной транзакции в ctx используется db,
// переданный в NewEventStoreWithDB. Возвращает ошибку, если db не настроен.
func (s *EventStore) Save(ctx context.Context, event Event) error {
	if s.getter == nil || s.db == nil {
		return fmt.Errorf("outbox: EventStore has no db configured; use NewEventStoreWithDB or call Save with an explicit executor")
	}
	exec := s.getter.DefaultTrOrDB(ctx, s.db)
	return s.save(ctx, exec, event)
}

// WithMapper устанавливает маппер событий и возвращает EventStore для chaining.
func (s *EventStore) WithMapper(fn EventMapper) *EventStore {
	s.mapper = fn
	return s
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

	if s.mapper != nil {
		event = s.mapper(event)
	}

	row, err := s.serialize(event)
	if err != nil {
		return fmt.Errorf("failed to save outbox event: %w", err)
	}

	if err := s.insertEvent(ctx, exec, row); err != nil {
		return fmt.Errorf("failed to save outbox event: %w", convertFromDBError(err))
	}

	return nil
}

func (s *EventStore) serialize(event Event) (outboxRow, error) {
	payloadBytes, err := s.serializer.Marshal(event.Payload)
	if err != nil {
		return outboxRow{}, fmt.Errorf("failed to serialize payload: %w", err)
	}

	var headersJSON []byte
	if len(event.Headers) > 0 {
		headersJSON, err = json.Marshal(event.Headers)
		if err != nil {
			return outboxRow{}, fmt.Errorf("failed to marshal headers: %w", err)
		}
	}

	return outboxRow{
		eventID:       event.EventID,
		eventType:     event.EventType,
		aggregateType: event.AggregateType,
		aggregateID:   event.AggregateID,
		topic:         event.Topic,
		contentType:   s.serializer.ContentType(),
		payload:       payloadBytes,
		headers:       headersJSON,
	}, nil
}

func (s *EventStore) insertEvent(ctx context.Context, exec DBExecutor, row outboxRow) error {
	query := `
		INSERT INTO outbox_events
		(event_id, event_type, aggregate_type, aggregate_id, topic, content_type, payload, headers, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := exec.ExecContext(ctx, query,
		row.eventID,
		row.eventType,
		row.aggregateType,
		row.aggregateID,
		row.topic,
		row.contentType,
		row.payload,
		row.headers,
		EventRecordStatusNew,
	)

	return err
}
