package storage

import (
	"context"
	"database/sql"
	"time"
)

// DBTX - интерфейс, который может представлять как *sql.DB, так и *sql.Tx
type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

// Store определяет интерфейс для всех операций с базой данных
type Store interface {
	// CreateEvent сохраняет новое событие в рамках транзакции
	CreateEvent(ctx context.Context, tx DBTX, event *EventRecord) error
	// FetchNewEvents выбирает новые события для обработки
	FetchNewEvents(ctx context.Context, batchSize int) ([]EventRecord, error)
	// FetchStuckEvents выбирает "зависшие" события
	FetchStuckEvents(ctx context.Context, batchSize int, stuckTimeout time.Duration) ([]EventRecord, error)
	// FetchEventsToMoveToDeadLetter выбирает события, которые нужно переместить в DLQ
	FetchEventsToMoveToDeadLetter(ctx context.Context, batchSize int, maxAttempts int) ([]EventRecord, error)
	// MarkAsSent помечает событие как успешно отправленное
	MarkAsSent(ctx context.Context, eventID int64) error
	// MarkAsProcessing помечает события как находящиеся в обработке
	MarkAsProcessing(ctx context.Context, eventIDs []int64) error
	// UpdateForRetry обновляет событие для повторной попытки
	UpdateForRetry(ctx context.Context, eventID int64, nextAttemptAt time.Time, lastError string) error
	// MoveToDeadLetter перемещает событие в таблицу "мертвых писем"
	MoveToDeadLetter(ctx context.Context, record EventRecord, lastError string) error
	// ResetStuckEvents сбрасывает статус "зависших" событий
	ResetStuckEvents(ctx context.Context, eventIDs []int64, nextAttemptAt time.Time) error
	// DeleteSentEvents удаляет старые отправленные события
	DeleteSentEvents(ctx context.Context, retention time.Duration) (int64, error)
	// DeleteDeadLetterEvents удаляет старые записи из DLQ
	DeleteDeadLetterEvents(ctx context.Context, retention time.Duration) (int64, error)
	// EnsureTables создает необходимые таблицы, если они не существуют
	EnsureTables(ctx context.Context) error
}

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
	NextAttemptAt *time.Time
}
