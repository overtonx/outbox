package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

// ProcessEvents fetches and processes a batch of events from the outbox.
// It is the core function of the event processing worker.
func (c *Carrier) ProcessEvents(ctx context.Context, opts ...EventProcessorOption) error {
	options := &eventProcessorOptions{
		batchSize:       defaultBatchSize,
		maxAttempts:     defaultMaxAttempts,
		backoffStrategy: DefaultBackoffStrategy(),
	}
	for _, opt := range opts {
		opt(options)
	}

	start := time.Now()
	defer func() {
		c.metrics.RecordDuration("event_processor.duration", time.Since(start), nil)
	}()

	events, err := c.fetchEventBatch(ctx, options.batchSize)
	if err != nil {
		c.logger.Error("Failed to fetch event batch", zap.Error(err))
		return err
	}

	if len(events) == 0 {
		return nil
	}

	c.logger.Info("Fetched events for processing", zap.Int("count", len(events)))
	c.metrics.RecordGauge("event_processor.batch_size", float64(len(events)), nil)

	processed := 0
	failed := 0

	for _, event := range events {
		if err := c.processEvent(ctx, event, options); err != nil {
			failed++
			c.metrics.IncrementCounter("event_processor.processed", map[string]string{"status": "failed"})
		} else {
			processed++
			c.metrics.IncrementCounter("event_processor.processed", map[string]string{"status": "success"})
		}
	}

	c.logger.Info("Batch processing completed",
		zap.Int("processed", processed),
		zap.Int("failed", failed))

	return nil
}

func (c *Carrier) processEvent(ctx context.Context, event EventRecord, options *eventProcessorOptions) error {
	if err := validateEventRecord(event); err != nil {
		c.logger.Error("Invalid event record found", zap.Int64("event_id", event.ID), zap.Error(err))
		// Mark as error immediately, as it's a data integrity issue.
		return c.updateEventStatus(ctx, event.ID, EventStatusError, event.AttemptCount+1, nil, err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	default:
	}

	eventFields := []zap.Field{
		zap.Int64("event_id", event.ID),
		zap.String("event_type", event.EventType),
		zap.String("aggregate_id", event.AggregateID),
		zap.Int("attempt", event.AttemptCount+1),
	}

	c.logger.Debug("Processing event", eventFields...)

	publishErr := c.publisher.Publish(ctx, event)
	if publishErr != nil {
		c.metrics.IncrementCounter("event_processor.publish_failed", map[string]string{"event_type": event.EventType})
		c.logger.Error("Failed to publish event", append(eventFields, zap.Error(publishErr))...)
		return c.handlePublishError(ctx, event, options, publishErr)
	}

	c.metrics.IncrementCounter("event_processor.publish_success", map[string]string{"event_type": event.EventType})
	c.logger.Info("Event published successfully", eventFields...)
	return c.handlePublishSuccess(ctx, event)
}

func (c *Carrier) handlePublishError(ctx context.Context, event EventRecord, options *eventProcessorOptions, publishErr error) error {
	attempt := event.AttemptCount + 1

	var newStatus int
	var nextAttemptAt *time.Time

	if attempt >= options.maxAttempts {
		newStatus = EventStatusError
		c.logger.Error("Event exceeded max attempts, marking as error",
			zap.Int64("event_id", event.ID),
			zap.Int("max_attempts", options.maxAttempts),
			zap.Error(publishErr),
		)
		c.metrics.IncrementCounter("event_processor.max_attempts_exceeded", map[string]string{"event_type": event.EventType})
	} else {
		newStatus = EventStatusRetry
		next := options.backoffStrategy.CalculateNextAttempt(attempt)
		nextAttemptAt = &next
		c.logger.Info("Scheduling event for retry",
			zap.Int64("event_id", event.ID),
			zap.Time("next_attempt", next),
			zap.Error(publishErr),
		)
		c.metrics.IncrementCounter("event_processor.retry_scheduled", map[string]string{"event_type": event.EventType})
	}

	dbErr := c.updateEventStatus(ctx, event.ID, newStatus, attempt, nextAttemptAt, publishErr)
	if dbErr != nil {
		c.logger.Error("Failed to update event status after publish error",
			zap.Int64("event_id", event.ID),
			zap.Error(dbErr),
		)
		c.metrics.IncrementCounter("event_processor.db_update_failed", map[string]string{"operation": "error_handling"})
	}

	return fmt.Errorf("failed to publish event %d: %w", event.ID, publishErr)
}

func (c *Carrier) handlePublishSuccess(ctx context.Context, event EventRecord) error {
	err := c.updateEventStatus(ctx, event.ID, EventStatusSent, event.AttemptCount+1, nil, nil)
	if err != nil {
		c.logger.Error("Failed to update event status to sent",
			zap.Int64("event_id", event.ID),
			zap.Error(err),
		)
		c.metrics.IncrementCounter("event_processor.db_update_failed", map[string]string{"operation": "success_handling"})
		return fmt.Errorf("event %d published but failed to update status: %w", event.ID, err)
	}
	return nil
}

func (c *Carrier) fetchEventBatch(ctx context.Context, batchSize int) ([]EventRecord, error) {
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback is a no-op if Commit succeeds

	query := `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, 
		       status, topic, payload, headers, attempt_count, next_attempt_at, last_error
		FROM outbox_events 
		WHERE (status = ? OR (status = ? AND next_attempt_at <= ?))
		ORDER BY created_at ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query, EventStatusNew, EventStatusRetry, time.Now().UTC(), batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []EventRecord
	var eventIDs []int64

	for rows.Next() {
		var event EventRecord
		var nextAttemptAt sql.NullTime
		var lastError sql.NullString

		if err := rows.Scan(
			&event.ID, &event.EventID, &event.EventType, &event.AggregateType, &event.AggregateID,
			&event.Status, &event.Topic, &event.Payload, &event.Headers,
			&event.AttemptCount, &nextAttemptAt, &lastError,
		); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %w", err)
		}

		if nextAttemptAt.Valid {
			event.NextAttemptAt = &nextAttemptAt.Time
		}
		event.LastError = lastError.String

		events = append(events, event)
		eventIDs = append(eventIDs, event.ID)
	}

	if len(eventIDs) > 0 {
		updateQuery := fmt.Sprintf(
			"UPDATE outbox_events SET status = ? WHERE id IN (%s)",
			placeholders(len(eventIDs)),
		)
		args := make([]interface{}, len(eventIDs)+1)
		args[0] = EventStatusProcessing
		for i, id := range eventIDs {
			args[i+1] = id
		}

		if _, err = tx.ExecContext(ctx, updateQuery, args...); err != nil {
			return nil, fmt.Errorf("failed to mark events as processing: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return events, nil
}

func (c *Carrier) updateEventStatus(ctx context.Context, eventID int64, status int, attemptCount int, nextAttemptAt *time.Time, lastError error) error {
	var setParts []string
	var args []interface{}

	setParts = append(setParts, "status = ?")
	args = append(args, status)

	setParts = append(setParts, "attempt_count = ?")
	args = append(args, attemptCount)

	if nextAttemptAt != nil {
		setParts = append(setParts, "next_attempt_at = ?")
		args = append(args, *nextAttemptAt)
	} else {
		setParts = append(setParts, "next_attempt_at = NULL")
	}

	if lastError != nil {
		errorMsg := lastError.Error()
		if len(errorMsg) > 2000 {
			errorMsg = errorMsg[:2000]
		}
		setParts = append(setParts, "last_error = ?")
		args = append(args, errorMsg)
	} else {
		setParts = append(setParts, "last_error = NULL")
	}

	query := fmt.Sprintf("UPDATE outbox_events SET %s WHERE id = ?", strings.Join(setParts, ", "))
	args = append(args, eventID)

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute update for event %d: %w", eventID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected for event %d: %w", eventID, err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("no rows affected when updating event %d", eventID)
	}

	return nil
}

func validateEventRecord(event EventRecord) error {
	if event.ID == 0 {
		return fmt.Errorf("event ID is zero")
	}
	if event.EventType == "" {
		return fmt.Errorf("event type is empty")
	}
	if event.AggregateID == "" {
		return fmt.Errorf("aggregate ID is empty")
	}
	return nil
}
