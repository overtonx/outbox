package processor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/backoff"
	"github.com/overtonx/outbox/v2/embedded"
	"github.com/overtonx/outbox/v2/internal/metric"
	"github.com/overtonx/outbox/v2/internal/utils"
)

type EventProcessorImpl struct {
	db              *sql.DB
	logger          *zap.Logger
	backoffStrategy backoff.BackoffStrategy
	maxAttempts     int
	batchSize       int
	publisher       embedded.Publisher
	metrics         embedded.MetricsCollector
}

func NewEventProcessor(
	db *sql.DB,
	logger *zap.Logger,
	backoffStrategy backoff.BackoffStrategy,
	maxAttempts int,
	batchSize int,
	publisher embedded.Publisher,
	metrics embedded.MetricsCollector,
) *EventProcessorImpl {
	if metrics == nil {
		metrics = metric.NewNoOpMetricsCollector()
	}

	return &EventProcessorImpl{
		db:              db,
		logger:          logger,
		backoffStrategy: backoffStrategy,
		maxAttempts:     maxAttempts,
		batchSize:       batchSize,
		publisher:       publisher,
		metrics:         metrics,
	}
}

func (p *EventProcessorImpl) ProcessEvents(ctx context.Context) error {
	start := time.Now()
	defer func() {
		p.metrics.RecordDuration("event_processor.duration", time.Since(start), nil)
	}()

	events := p.fetchBatch(ctx)
	if len(events) == 0 {
		return nil
	}

	p.logger.Info("Fetched events for processing", zap.Int("count", len(events)))

	p.metrics.RecordGauge("event_processor.batch_size", float64(len(events)), nil)

	processed := 0
	failed := 0

	for _, event := range events {
		if err := p.processEvent(ctx, event); err != nil {
			failed++
			p.metrics.IncrementCounter("event_processor.processed", map[string]string{"status": "failed"})
			p.logger.Error("Failed to process event",
				zap.Int64("event_id", event.ID),
				zap.Error(err))
		} else {
			processed++
			p.metrics.IncrementCounter("event_processor.processed", map[string]string{"status": "success"})
		}
	}

	p.logger.Info("Batch processing completed",
		zap.Int("processed", processed),
		zap.Int("failed", failed))

	return nil
}

func (p *EventProcessorImpl) processEvent(ctx context.Context, event embedded.EventRecord) error {
	if err := p.validateEvent(event); err != nil {
		return fmt.Errorf("invalid event: %w", err)
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
		zap.String("aggregate_type", event.AggregateType),
		zap.String("topic", event.Topic),
		zap.Int("attempt", event.AttemptCount),
	}

	p.logger.Debug("Processing event", eventFields...)

	attempt := event.AttemptCount + 1

	publishErr := p.publisher.Publish(ctx, event)

	if publishErr != nil {
		p.metrics.IncrementCounter("event_processor.publish_failed", map[string]string{
			"event_type": event.EventType,
			"reason":     "kafka_error",
		})

		p.logger.Error("Failed to publish event to Kafka",
			append(eventFields,
				zap.Error(publishErr),
			)...,
		)

		return p.handlePublishError(ctx, event, attempt, publishErr)
	}

	p.metrics.IncrementCounter("event_processor.publish_success", map[string]string{
		"event_type": event.EventType,
	})

	p.logger.Info("Event published successfully to Kafka",
		append(eventFields)...,
	)

	return p.handlePublishSuccess(ctx, event, attempt)
}

func (p *EventProcessorImpl) validateEvent(event embedded.EventRecord) error {
	if event.ID <= 0 {
		return fmt.Errorf("invalid event ID: %d", event.ID)
	}
	if event.EventType == "" {
		return fmt.Errorf("empty event type")
	}
	if event.AggregateID == "" {
		return fmt.Errorf("empty aggregate ID")
	}
	if event.Topic == "" {
		return fmt.Errorf("empty topic")
	}
	if len(event.Payload) == 0 {
		return fmt.Errorf("empty payload")
	}
	return nil
}

func (p *EventProcessorImpl) handlePublishError(ctx context.Context, event embedded.EventRecord, attempt int, publishErr error) error {
	eventFields := []zap.Field{
		zap.Int64("event_id", event.ID),
		zap.String("event_type", event.EventType),
		zap.String("aggregate_id", event.AggregateID),
		zap.Int("attempt", attempt),
		zap.Int("max_attempts", p.maxAttempts),
	}

	var dbErr error
	var newStatus int
	var nextAttemptAt *time.Time

	if attempt >= p.maxAttempts {
		newStatus = embedded.EventRecordStatusError
		p.logger.Error("Event exceeded max attempts, marking as error",
			append(eventFields, zap.Error(publishErr))...,
		)
		p.metrics.IncrementCounter("event_processor.max_attempts_exceeded", map[string]string{
			"event_type": event.EventType,
		})
	} else {
		newStatus = embedded.EventRecordStatusRetry
		nextAttempt := p.backoffStrategy.CalculateNextAttempt(attempt)
		nextAttemptAt = &nextAttempt

		p.logger.Info("Scheduling event for retry",
			append(eventFields,
				zap.Time("next_attempt", nextAttempt),
				zap.Error(publishErr),
			)...,
		)
		p.metrics.IncrementCounter("event_processor.retry_scheduled", map[string]string{
			"event_type": event.EventType,
		})
	}

	dbErr = p.updateStatus(ctx, event.ID, newStatus, attempt, nextAttemptAt, publishErr)
	if dbErr != nil {
		p.logger.Error("Failed to update event status in database",
			append(eventFields,
				zap.Int("status", newStatus),
				zap.Error(dbErr),
			)...,
		)
		p.metrics.IncrementCounter("event_processor.db_update_failed", map[string]string{
			"event_type": event.EventType,
			"operation":  "error_handling",
		})
	}

	return fmt.Errorf("failed to publish event %d (attempt %d/%d): %w",
		event.ID, attempt, p.maxAttempts, publishErr)
}

func (p *EventProcessorImpl) handlePublishSuccess(ctx context.Context, event embedded.EventRecord, attempt int) error {
	eventFields := []zap.Field{
		zap.Int64("event_id", event.ID),
		zap.String("event_type", event.EventType),
		zap.String("aggregate_id", event.AggregateID),
		zap.Int("attempt", attempt),
	}

	if err := p.updateStatus(ctx, event.ID, embedded.EventRecordStatusSent, attempt, nil, nil); err != nil {
		p.logger.Error("Failed to update event status to sent in database",
			append(eventFields, zap.Error(err))...,
		)
		p.metrics.IncrementCounter("event_processor.db_update_failed", map[string]string{
			"event_type": event.EventType,
			"operation":  "success_handling",
		})
		return fmt.Errorf("event %d published successfully but failed to update status: %w",
			event.ID, err)
	}

	p.logger.Debug("Event status updated to sent",
		append(eventFields, zap.Int("status", embedded.EventRecordStatusSent))...,
	)

	return nil
}

func (p *EventProcessorImpl) fetchBatch(ctx context.Context) []embedded.EventRecord {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		p.logger.Error("Failed to begin transaction", zap.Error(err))
		return nil
	}
	defer tx.Rollback()

	query := `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, 
		       status, topic, payload, headers, 
		       attempt_count, next_attempt_at
		FROM outbox_events 
		WHERE (status = 0) OR (status = 2 AND next_attempt_at <= NOW())
		ORDER BY created_at ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query, p.batchSize)
	if err != nil {
		p.logger.Error("Failed to query events", zap.Error(err))
		return nil
	}
	defer rows.Close()

	var events []embedded.EventRecord
	var eventIDs []int64

	for rows.Next() {
		var event embedded.EventRecord
		var status int
		var nextAttemptAt sql.NullTime

		err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.EventType,
			&event.AggregateType,
			&event.AggregateID,
			&status,
			&event.Topic,
			&event.Payload,
			&event.Headers,
			&event.AttemptCount,
			&nextAttemptAt,
		)
		if err != nil {
			p.logger.Error("Failed to scan event record", zap.Error(err))
			continue
		}

		if nextAttemptAt.Valid {
			event.NextAttemptAt = &nextAttemptAt.Time
		}

		events = append(events, event)
		eventIDs = append(eventIDs, event.ID)
	}

	if len(eventIDs) > 0 {
		updateQuery := fmt.Sprintf(
			"UPDATE outbox_events SET status = ? WHERE id IN (%s)",
			utils.Placeholders(len(eventIDs)),
		)

		args := make([]interface{}, len(eventIDs)+1)
		args[0] = embedded.EventRecordStatusProcessing

		for i, id := range eventIDs {
			args[i+1] = id
		}

		_, err = tx.ExecContext(ctx, updateQuery, args...)
		if err != nil {
			p.logger.Error("Failed to update event status to processing", zap.Error(err))
			return nil
		}
	}

	if err = tx.Commit(); err != nil {
		p.logger.Error("Failed to commit transaction", zap.Error(err))
		return nil
	}

	p.logger.Debug("Fetched events for processing",
		zap.Int("event_count", len(events)),
	)
	return events
}

func (p *EventProcessorImpl) updateStatus(ctx context.Context, eventID int64, status int, attemptCount int, nextAttemptAt *time.Time, lastError error) error {
	if eventID <= 0 {
		return fmt.Errorf("invalid event ID: %d", eventID)
	}
	if status < 0 || status > 4 {
		return fmt.Errorf("invalid status: %d", status)
	}
	if attemptCount < 0 {
		return fmt.Errorf("invalid attempt count: %d", attemptCount)
	}

	updateFields := []zap.Field{
		zap.Int64("event_id", eventID),
		zap.Int("status", status),
		zap.Int("attempt_count", attemptCount),
	}

	query, args, err := p.buildUpdateQuery(eventID, status, attemptCount, nextAttemptAt, lastError)
	if err != nil {
		return fmt.Errorf("failed to build update query: %w", err)
	}

	result, err := p.db.ExecContext(ctx, query, args...)

	if err != nil {
		p.logger.Error("Database error: failed to update event status",
			append(updateFields,
				zap.String("query", query),
				zap.Error(err),
			)...,
		)
		p.metrics.IncrementCounter("event_processor.db_update_failed", map[string]string{
			"operation": "update_status",
			"reason":    "exec_error",
		})
		return fmt.Errorf("failed to update event %d status to %d: %w", eventID, status, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		p.logger.Error("Database error: failed to get rows affected count",
			append(updateFields,
				zap.Error(err),
			)...,
		)
		p.metrics.IncrementCounter("event_processor.db_update_failed", map[string]string{
			"operation": "update_status",
			"reason":    "rows_affected_error",
		})
		return fmt.Errorf("failed to get rows affected for event %d: %w", eventID, err)
	}

	if rowsAffected == 0 {
		p.logger.Warn("No rows were updated when updating event status",
			append(updateFields)...,
		)
		p.metrics.IncrementCounter("event_processor.db_update_failed", map[string]string{
			"operation": "update_status",
			"reason":    "no_rows_affected",
		})
		return fmt.Errorf("no rows affected when updating event %d status to %d", eventID, status)
	}

	p.logger.Debug("Event status updated successfully in database",
		append(updateFields,
			zap.Int64("rows_affected", rowsAffected),
		)...,
	)

	p.metrics.IncrementCounter("event_processor.db_update_success", map[string]string{
		"operation": "update_status",
		"status":    fmt.Sprintf("%d", status),
	})

	return nil
}

func (p *EventProcessorImpl) buildUpdateQuery(eventID int64, status int, attemptCount int, nextAttemptAt *time.Time, lastError error) (string, []interface{}, error) {
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
		if len(errorMsg) > 1000 {
			errorMsg = errorMsg[:1000] + "..."
		}
		setParts = append(setParts, "last_error = ?")
		args = append(args, errorMsg)
	} else {
		setParts = append(setParts, "last_error = NULL")
	}

	setParts = append(setParts, "updated_at = NOW()")

	query := fmt.Sprintf("UPDATE outbox_events SET %s WHERE id = ?", strings.Join(setParts, ", "))
	args = append(args, eventID)

	return query, args, nil
}
