package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type DeadLetterServiceImpl struct {
	db        *sql.DB
	logger    *zap.Logger
	batchSize int
	metrics   MetricsCollector
}

func NewDeadLetterService(db *sql.DB, logger *zap.Logger, batchSize int, metrics MetricsCollector) *DeadLetterServiceImpl {
	if metrics == nil {
		metrics = NewNoOpMetricsCollector()
	}

	return &DeadLetterServiceImpl{
		db:        db,
		logger:    logger,
		batchSize: batchSize,
		metrics:   metrics,
	}
}

func (s *DeadLetterServiceImpl) MoveToDeadLetters(ctx context.Context) error {
	start := time.Now()
	defer func() {
		s.metrics.RecordDuration("deadletter.move.duration", time.Since(start), nil)
	}()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, topic, 
		       payload, headers, attempt_count, last_error, created_at
		FROM outbox_events 
		WHERE status = ? 
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query, EventRecordStatusError, s.batchSize)
	if err != nil {
		return fmt.Errorf("failed to query error events: %w", err)
	}
	defer rows.Close()

	var eventsToMove []DeadLetterRecord
	var eventIDs []int64

	for rows.Next() {
		var event DeadLetterRecord
		var lastError sql.NullString

		err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.EventType,
			&event.AggregateType,
			&event.AggregateID,
			&event.Topic,
			&event.Payload,
			&event.Headers,
			&event.AttemptCount,
			&lastError,
			&event.CreatedAt,
		)
		if err != nil {
			s.logger.Error("Failed to scan error event", zap.Error(err))
			continue
		}

		if lastError.Valid {
			event.LastError = lastError.String
		}

		eventsToMove = append(eventsToMove, event)
		eventIDs = append(eventIDs, event.ID)
	}

	if len(eventsToMove) == 0 {
		return nil
	}

	insertQuery := `
		INSERT INTO outbox_deadletters 
		(id, event_id, event_type, aggregate_type, aggregate_id, topic, payload, 
		 headers, attempt_count, last_error, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	movedCount := 0
	for _, event := range eventsToMove {
		_, err := tx.ExecContext(ctx, insertQuery,
			event.ID,
			event.EventID,
			event.EventType,
			event.AggregateType,
			event.AggregateID,
			event.Topic,
			event.Payload,
			event.Headers,
			event.AttemptCount,
			nullString(event.LastError),
			event.CreatedAt,
		)
		if err != nil {
			s.logger.Error("Failed to insert into deadletters",
				zap.Error(err),
				zap.String("event_id", event.EventID))
			continue
		}
		movedCount++
	}

	if len(eventIDs) > 0 {
		deleteQuery := fmt.Sprintf(
			"DELETE FROM outbox_events WHERE id IN (%s)",
			placeholders(len(eventIDs)),
		)

		args := make([]interface{}, len(eventIDs))
		for i, id := range eventIDs {
			args[i] = id
		}

		_, err = tx.ExecContext(ctx, deleteQuery, args...)
		if err != nil {
			return fmt.Errorf("failed to delete events from outbox_events: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit deadletter transaction: %w", err)
	}

	s.logger.Info("Moved events to deadletters",
		zap.Int("count", movedCount),
		zap.Int("total_found", len(eventsToMove)))

	s.metrics.IncrementCounter("deadletter.moved", map[string]string{"status": "success"})
	s.metrics.RecordGauge("deadletter.batch_size", float64(movedCount), nil)

	return nil
}
