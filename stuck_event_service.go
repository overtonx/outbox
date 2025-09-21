package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// RecoverStuckEvents finds events that have been in the "processing" state for too long
// and resets their status to "retry" or "error".
func (c *Carrier) RecoverStuckEvents(ctx context.Context, opts ...StuckEventServiceOption) error {
	options := &stuckEventServiceOptions{
		batchSize:       defaultBatchSize,
		maxAttempts:     defaultMaxAttempts,
		stuckTimeout:    defaultStuckEventTimeout,
		backoffStrategy: DefaultBackoffStrategy(),
	}
	for _, opt := range opts {
		opt(options)
	}

	start := time.Now()
	defer func() {
		c.metrics.RecordDuration("stuck_events.recovery.duration", time.Since(start), nil)
	}()

	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	timeoutThreshold := time.Now().UTC().Add(-options.stuckTimeout)

	query := `
		SELECT id, attempt_count
		FROM outbox_events 
		WHERE status = ? AND updated_at < ?
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query, EventStatusProcessing, timeoutThreshold, options.batchSize)
	if err != nil {
		return fmt.Errorf("failed to query stuck events: %w", err)
	}
	defer rows.Close()

	type stuckEvent struct {
		ID           int64
		AttemptCount int
	}
	var eventsToRecover []stuckEvent

	for rows.Next() {
		var event stuckEvent
		if err := rows.Scan(&event.ID, &event.AttemptCount); err != nil {
			c.logger.Error("Failed to scan stuck event", zap.Error(err))
			continue
		}
		eventsToRecover = append(eventsToRecover, event)
	}

	if len(eventsToRecover) == 0 {
		return nil
	}

	updateStmt, err := tx.PrepareContext(ctx, `
		UPDATE outbox_events SET status = ?, next_attempt_at = ?, last_error = ? WHERE id = ?
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare update statement for stuck events: %w", err)
	}
	defer updateStmt.Close()

	recoveredCount := 0
	for _, event := range eventsToRecover {
		var newStatus int
		var nextAttemptAt time.Time
		lastErr := "event recovered from stuck state"

		if event.AttemptCount >= options.maxAttempts {
			newStatus = EventStatusError
			c.metrics.IncrementCounter("stuck_events.marked_as_error", nil)
		} else {
			newStatus = EventStatusRetry
			nextAttemptAt = options.backoffStrategy.CalculateNextAttempt(event.AttemptCount)
			c.metrics.IncrementCounter("stuck_events.marked_as_retry", nil)
		}

		_, err := updateStmt.ExecContext(ctx, newStatus, nextAttemptAt, lastErr, event.ID)
		if err != nil {
			c.logger.Error("Failed to recover stuck event", zap.Int64("id", event.ID), zap.Error(err))
			continue
		}
		recoveredCount++
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit stuck event recovery transaction: %w", err)
	}

	c.logger.Info("Stuck event recovery completed",
		zap.Int("recovered_count", recoveredCount),
		zap.Duration("stuck_threshold", options.stuckTimeout),
	)
	c.metrics.RecordGauge("stuck_events.recovered_batch_size", float64(recoveredCount), nil)

	return nil
}
