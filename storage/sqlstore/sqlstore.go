package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/storage"
)

const (
	tableEvents      = "outbox_events"
	tableDeadletters = "outbox_deadletters"
)

// SQL queries
const (
	createQuery = `
		INSERT INTO %s (event_id, event_type, aggregate_type, aggregate_id, topic, payload, headers, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	fetchNewQuery = `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, topic, payload, headers, attempt_count
		FROM %s
		WHERE status IN (?, ?) AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
		ORDER BY id
		LIMIT ?`

	fetchStuckQuery = `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, topic, payload, headers, attempt_count
		FROM %s
		WHERE status = ? AND updated_at < ?
		ORDER BY id
		LIMIT ?`

	fetchDeadLetterQuery = `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, topic, payload, headers, attempt_count, last_error
		FROM %s
		WHERE status = ? AND attempt_count >= ?
		ORDER BY id
		LIMIT ?`

	markAsSentQuery = `UPDATE %s SET status = ? WHERE id = ?`

	markAsProcessingQuery = `UPDATE %s SET status = ?, updated_at = CURRENT_TIMESTAMP(6) WHERE id IN (%s)`

	updateForRetryQuery = `
		UPDATE %s
		SET status = ?, attempt_count = attempt_count + 1, next_attempt_at = ?, last_error = ?
		WHERE id = ?`

	moveToDeadLetterQuery = `
		INSERT INTO %s (id, event_id, event_type, aggregate_type, aggregate_id, topic, payload, headers, attempt_count, last_error, created_at)
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, topic, payload, headers, attempt_count, ?, created_at
		FROM %s
		WHERE id = ?`

	deleteFromEventsQuery = `DELETE FROM %s WHERE id = ?`

	resetStuckQuery = `UPDATE %s SET status = ?, next_attempt_at = ? WHERE id IN (%s)`

	deleteSentQuery = `DELETE FROM %s WHERE status = ? AND updated_at < ?`

	deleteDeadLetterQuery = `DELETE FROM %s WHERE created_at < ?`
)

const (
	StatusNew        = 0
	StatusSent       = 1
	StatusRetry      = 2
	StatusError      = 3
	StatusProcessing = 4
)

var (
	ErrEventAlreadyExists = errors.New("event already exists")
)

type SQLStore struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewSQLStore(db *sql.DB, logger *zap.Logger) *SQLStore {
	return &SQLStore{
		db:     db,
		logger: logger,
	}
}

func (s *SQLStore) CreateEvent(ctx context.Context, tx storage.DBTX, event *storage.EventRecord) error {
	query := fmt.Sprintf(createQuery, tableEvents)
	_, err := tx.ExecContext(ctx, query,
		event.EventID,
		event.EventType,
		event.AggregateType,
		event.AggregateID,
		event.Topic,
		event.Payload,
		event.Headers,
		StatusNew,
	)

	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return ErrEventAlreadyExists
		}
		return fmt.Errorf("failed to save outbox event: %w", err)
	}
	return nil
}

func (s *SQLStore) FetchNewEvents(ctx context.Context, batchSize int) ([]storage.EventRecord, error) {
	query := fmt.Sprintf(fetchNewQuery, tableEvents)
	rows, err := s.db.QueryContext(ctx, query, StatusNew, StatusRetry, time.Now().UTC(), batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query new events: %w", err)
	}
	defer rows.Close()

	return s.scanEvents(rows)
}

func (s *SQLStore) FetchStuckEvents(ctx context.Context, batchSize int, stuckTimeout time.Duration) ([]storage.EventRecord, error) {
	stuckTime := time.Now().UTC().Add(-stuckTimeout)
	query := fmt.Sprintf(fetchStuckQuery, tableEvents)
	rows, err := s.db.QueryContext(ctx, query, StatusProcessing, stuckTime, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query stuck events: %w", err)
	}
	defer rows.Close()

	return s.scanEvents(rows)
}

func (s *SQLStore) FetchEventsToMoveToDeadLetter(ctx context.Context, batchSize int, maxAttempts int) ([]storage.EventRecord, error) {
	query := fmt.Sprintf(fetchDeadLetterQuery, tableEvents)
	rows, err := s.db.QueryContext(ctx, query, StatusError, maxAttempts, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query events for dead-letter: %w", err)
	}
	defer rows.Close()

	return s.scanEvents(rows)
}

func (s *SQLStore) MarkAsSent(ctx context.Context, eventID int64) error {
	query := fmt.Sprintf(markAsSentQuery, tableEvents)
	_, err := s.db.ExecContext(ctx, query, StatusSent, eventID)
	return err
}

func (s *SQLStore) MarkAsProcessing(ctx context.Context, eventIDs []int64) error {
	if len(eventIDs) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(eventIDs)-1) + "?"
	query := fmt.Sprintf(markAsProcessingQuery, tableEvents, placeholders)

	args := make([]interface{}, len(eventIDs)+1)
	args[0] = StatusProcessing
	for i, id := range eventIDs {
		args[i+1] = id
	}

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLStore) UpdateForRetry(ctx context.Context, eventID int64, nextAttemptAt time.Time, lastError string) error {
	query := fmt.Sprintf(updateForRetryQuery, tableEvents)
	_, err := s.db.ExecContext(ctx, query, StatusRetry, nextAttemptAt, lastError, eventID)
	return err
}

func (s *SQLStore) MoveToDeadLetter(ctx context.Context, record storage.EventRecord, lastError string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	insertQuery := fmt.Sprintf(moveToDeadLetterQuery, tableDeadletters, tableEvents)
	_, err = tx.ExecContext(ctx, insertQuery, lastError, record.ID)
	if err != nil {
		return fmt.Errorf("failed to insert into dead-letter table: %w", err)
	}

	deleteQuery := fmt.Sprintf(deleteFromEventsQuery, tableEvents)
	_, err = tx.ExecContext(ctx, deleteQuery, record.ID)
	if err != nil {
		return fmt.Errorf("failed to delete from events table: %w", err)
	}

	return tx.Commit()
}

func (s *SQLStore) ResetStuckEvents(ctx context.Context, eventIDs []int64, nextAttemptAt time.Time) error {
	if len(eventIDs) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(eventIDs)-1) + "?"
	query := fmt.Sprintf(resetStuckQuery, tableEvents, placeholders)

	args := make([]interface{}, len(eventIDs)+2)
	args[0] = StatusRetry
	args[1] = nextAttemptAt
	for i, id := range eventIDs {
		args[i+2] = id
	}

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLStore) DeleteSentEvents(ctx context.Context, retention time.Duration) (int64, error) {
	deleteTime := time.Now().UTC().Add(-retention)
	query := fmt.Sprintf(deleteSentQuery, tableEvents)
	res, err := s.db.ExecContext(ctx, query, StatusSent, deleteTime)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLStore) DeleteDeadLetterEvents(ctx context.Context, retention time.Duration) (int64, error) {
	deleteTime := time.Now().UTC().Add(-retention)
	query := fmt.Sprintf(deleteDeadLetterQuery, tableDeadletters)
	res, err := s.db.ExecContext(ctx, query, deleteTime)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *SQLStore) scanEvents(rows *sql.Rows) ([]storage.EventRecord, error) {
	var events []storage.EventRecord
	for rows.Next() {
		var event storage.EventRecord
		if err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.EventType,
			&event.AggregateType,
			&event.AggregateID,
			&event.Topic,
			&event.Payload,
			&event.Headers,
			&event.AttemptCount,
		); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading event rows: %w", err)
	}
	return events, nil
}

// EnsureTables создает таблицы, если они не существуют
func (s *SQLStore) EnsureTables(ctx context.Context) error {
	if err := s.createOutboxEventsTable(ctx); err != nil {
		return err
	}
	if err := s.createOutboxDeadlettersTable(ctx); err != nil {
		return err
	}
	return nil
}

func (s *SQLStore) createOutboxEventsTable(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS outbox_events (
			id              BIGINT AUTO_INCREMENT PRIMARY KEY,
			event_id        CHAR(36)     NOT NULL UNIQUE,
			event_type      VARCHAR(255) NOT NULL,
			aggregate_type  VARCHAR(255) NOT NULL,
			aggregate_id    VARCHAR(255) NOT NULL,
			status          INT          NOT NULL DEFAULT 0 COMMENT '0 - new, 1 - success, 2 - retry, 3 - error, 4 - processing',
			topic           VARCHAR(255) NOT NULL,
			payload         JSON         NOT NULL,
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
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create outbox_events table: %w", err)
	}
	return nil
}

func (s *SQLStore) createOutboxDeadlettersTable(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS outbox_deadletters
		(
		    id              BIGINT PRIMARY KEY,
		    event_id        CHAR(36)      NOT NULL UNIQUE,
		    event_type      VARCHAR(255)  NOT NULL,
		    aggregate_type  VARCHAR(255)  NOT NULL,
		    aggregate_id    VARCHAR(255)  NOT NULL,
		    topic           VARCHAR(255)  NOT NULL,
		    payload         JSON          NOT NULL,
		    headers         JSON          NULL,
		    attempt_count   INT           NOT NULL,
		    last_error      VARCHAR(2000) NULL,
		    created_at      TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
	`
	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create outbox_deadletters table: %w", err)
	}
	return nil
}
