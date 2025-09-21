package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// MoveToDeadLetters finds events that have failed permanently (status = EventStatusError)
// and moves them from the main outbox_events table to the outbox_deadletters table.
func (c *Carrier) MoveToDeadLetters(ctx context.Context, opts ...DeadLetterServiceOption) error {
	options := &deadLetterServiceOptions{
		batchSize: defaultBatchSize,
	}
	for _, opt := range opts {
		opt(options)
	}

	start := time.Now()
	defer func() {
		c.metrics.RecordDuration("deadletter.move.duration", time.Since(start), nil)
	}()

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
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

	rows, err := tx.QueryContext(ctx, query, EventStatusError, options.batchSize)
	if err != nil {
		return fmt.Errorf("failed to query error events: %w", err)
	}
	defer rows.Close()

	var eventsToMove []DeadLetterRecord
	var eventIDsToDelete []int64

	for rows.Next() {
		var event DeadLetterRecord
		var lastError sql.NullString
		var createdAt sql.NullTime

		if err := rows.Scan(
			&event.ID, &event.EventID, &event.EventType, &event.AggregateType, &event.AggregateID,
			&event.Topic, &event.Payload, &event.Headers, &event.AttemptCount, &lastError, &createdAt,
		); err != nil {
			c.logger.Error("Failed to scan error event", zap.Error(err))
			continue // Skip this row
		}

		event.LastError = lastError.String
		if createdAt.Valid {
			event.CreatedAt = createdAt.Time
		} else {
			event.CreatedAt = time.Now().UTC() // Fallback
		}

		eventsToMove = append(eventsToMove, event)
		eventIDsToDelete = append(eventIDsToDelete, event.ID)
	}

	if len(eventsToMove) == 0 {
		return nil
	}

	// Insert into deadletters
	insertStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO outbox_deadletters (id, event_id, event_type, aggregate_type, aggregate_id, topic,
		                                payload, headers, attempt_count, last_error, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement for deadletters: %w", err)
	}
	defer insertStmt.Close()

	for _, event := range eventsToMove {
		_, err := insertStmt.ExecContext(ctx,
			event.ID, event.EventID, event.EventType, event.AggregateType, event.AggregateID,
			event.Topic, event.Payload, event.Headers, event.AttemptCount, event.LastError, event.CreatedAt,
		)
		if err != nil {
			// Log error but continue trying to move other events
			c.logger.Error("Failed to insert event into deadletters",
				zap.String("event_id", event.EventID),
				zap.Error(err),
			)
			// Remove from the list of IDs to delete
			eventIDsToDelete = removeID(eventIDsToDelete, event.ID)
		}
	}

	// Delete from outbox_events
	if len(eventIDsToDelete) > 0 {
		deleteQuery := fmt.Sprintf(
			"DELETE FROM outbox_events WHERE id IN (%s)",
			placeholders(len(eventIDsToDelete)),
		)
		args := make([]interface{}, len(eventIDsToDelete))
		for i, id := range eventIDsToDelete {
			args[i] = id
		}

		if _, err = tx.ExecContext(ctx, deleteQuery, args...); err != nil {
			return fmt.Errorf("failed to delete moved events from outbox: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit deadletter transaction: %w", err)
	}

	movedCount := len(eventIDsToDelete)
	c.logger.Info("Moved events to deadletters", zap.Int("count", movedCount))
	c.metrics.IncrementCounter("deadletter.moved", map[string]string{"status": "success"})
	c.metrics.RecordGauge("deadletter.batch_size", float64(movedCount), nil)

	return nil
}

// removeID removes an id from a slice of int64.
func removeID(ids []int64, idToRemove int64) []int64 {
	for i, id := range ids {
		if id == idToRemove {
			return append(ids[:i], ids[i+1:]...)
		}
	}
	return ids
}
