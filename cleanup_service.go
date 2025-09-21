package outbox

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Cleanup performs maintenance tasks on the outbox tables, such as deleting old
// processed events and old dead-lettered events to prevent the tables from growing indefinitely.
func (c *Carrier) Cleanup(ctx context.Context, opts ...CleanupServiceOption) error {
	options := &cleanupServiceOptions{
		batchSize:           defaultBatchSize,
		sentRetention:       defaultSentEventsRetention,
		deadLetterRetention: defaultDeadLetterRetention,
	}
	for _, opt := range opts {
		opt(options)
	}

	start := time.Now()
	defer func() {
		c.metrics.RecordDuration("cleanup.duration", time.Since(start), nil)
	}()

	sentEventsCleaned, err := c.cleanupSentEvents(ctx, options.sentRetention, options.batchSize)
	if err != nil {
		c.logger.Error("Failed to cleanup sent events", zap.Error(err))
		// Continue to the next cleanup task even if this one fails
	}

	deadLettersCleaned, err := c.cleanupDeadLetters(ctx, options.deadLetterRetention, options.batchSize)
	if err != nil {
		c.logger.Error("Failed to cleanup dead-letter events", zap.Error(err))
	}

	c.logger.Info("Cleanup process finished",
		zap.Int64("sent_events_cleaned", sentEventsCleaned),
		zap.Int64("dead_letters_cleaned", deadLettersCleaned),
	)
	c.metrics.IncrementCounter("cleanup.executed", nil)

	return nil // Return nil as we don't want to stop the worker for cleanup errors
}

func (c *Carrier) cleanupSentEvents(ctx context.Context, retention time.Duration, batchSize int) (int64, error) {
	cutoff := time.Now().UTC().Add(-retention)
	query := `DELETE FROM outbox_events WHERE status = ? AND updated_at < ? LIMIT ?`

	result, err := c.db.ExecContext(ctx, query, EventStatusSent, cutoff, batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete for sent events: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected for sent events: %w", err)
	}

	if rowsAffected > 0 {
		c.logger.Info("Cleaned up old sent events",
			zap.Int64("count", rowsAffected),
			zap.Duration("retention", retention),
		)
		c.metrics.RecordGauge("cleanup.sent_events_cleaned", float64(rowsAffected), nil)
	}

	return rowsAffected, nil
}

func (c *Carrier) cleanupDeadLetters(ctx context.Context, retention time.Duration, batchSize int) (int64, error) {
	cutoff := time.Now().UTC().Add(-retention)
	query := `DELETE FROM outbox_deadletters WHERE created_at < ? LIMIT ?`

	result, err := c.db.ExecContext(ctx, query, cutoff, batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete for dead-letters: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected for dead-letters: %w", err)
	}

	if rowsAffected > 0 {
		c.logger.Info("Cleaned up old dead-letter events",
			zap.Int64("count", rowsAffected),
			zap.Duration("retention", retention),
		)
		c.metrics.RecordGauge("cleanup.deadletters_cleaned", float64(rowsAffected), nil)
	}

	return rowsAffected, nil
}
