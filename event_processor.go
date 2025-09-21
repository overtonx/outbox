package outbox

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/storage"
)

// EventProcessorImpl обрабатывает и публикует события из outbox.
type EventProcessorImpl struct {
	store           storage.Store
	publisher       Publisher
	logger          *zap.Logger
	metrics         MetricsCollector
	backoffStrategy BackoffStrategy
	maxAttempts     int
	batchSize       int
}

// NewEventProcessor создает новый экземпляр EventProcessorImpl.
func NewEventProcessor(
	store storage.Store,
	publisher Publisher,
	logger *zap.Logger,
	metrics MetricsCollector,
	backoffStrategy BackoffStrategy,
	maxAttempts int,
	batchSize int,
) *EventProcessorImpl {
	if metrics == nil {
		metrics = NewNoOpMetricsCollector()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &EventProcessorImpl{
		store:           store,
		publisher:       publisher,
		logger:          logger,
		metrics:         metrics,
		backoffStrategy: backoffStrategy,
		maxAttempts:     maxAttempts,
		batchSize:       batchSize,
	}
}

// ProcessEvents - это основная функция, которую выполняет воркер.
// Она извлекает, обрабатывает и публикует события.
func (p *EventProcessorImpl) ProcessEvents(ctx context.Context) error {
	start := time.Now()
	events, err := p.fetchAndMarkEvents(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch events: %w", err)
	}
	p.metrics.RecordDuration("event_processor.fetch_duration", time.Since(start), nil)

	if len(events) == 0 {
		return nil
	}

	p.logger.Info("Fetched events for processing", zap.Int("count", len(events)))
	p.metrics.RecordGauge("event_processor.batch_size", float64(len(events)), nil)

	processed, failed := p.processBatch(ctx, events)

	p.logger.Info("Batch processing completed",
		zap.Int("processed", processed),
		zap.Int("failed", failed))
	p.metrics.RecordDuration("event_processor.duration", time.Since(start), nil)

	return nil
}

func (p *EventProcessorImpl) fetchAndMarkEvents(ctx context.Context) ([]storage.EventRecord, error) {
	events, err := p.store.FetchNewEvents(ctx, p.batchSize)
	if err != nil || len(events) == 0 {
		return nil, err
	}

	eventIDs := make([]int64, len(events))
	for i, event := range events {
		eventIDs[i] = event.ID
	}

	if err := p.store.MarkAsProcessing(ctx, eventIDs); err != nil {
		p.logger.Error("failed to mark events as processing", zap.Error(err))
		// Продолжаем обработку, так как события уже в памяти
	}

	return events, nil
}

func (p *EventProcessorImpl) processBatch(ctx context.Context, events []storage.EventRecord) (processed, failed int) {
	for _, event := range events {
		select {
		case <-ctx.Done():
			p.logger.Warn("Context cancelled during batch processing", zap.Error(ctx.Err()))
			// Помечаем оставшиеся события для повторной попытки
			p.rescheduleEvent(context.Background(), event, ctx.Err())
			failed++
			continue
		default:
		}

		err := p.processSingleEvent(ctx, event)
		if err != nil {
			failed++
			p.logger.Error("Failed to process event",
				zap.Int64("event_id", event.ID),
				zap.Error(err))
		} else {
			processed++
		}
	}
	return
}

func (p *EventProcessorImpl) processSingleEvent(ctx context.Context, event storage.EventRecord) error {
	eventFields := []zap.Field{
		zap.Int64("event_id", event.ID),
		zap.String("event_type", event.EventType),
		zap.String("aggregate_id", event.AggregateID),
	}

	p.logger.Debug("Processing event", eventFields...)

	// Преобразуем storage.EventRecord в outbox.EventRecord для публикации
	publishableEvent := EventRecord{
		ID:            event.ID,
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateID,
		EventID:       event.EventID,
		EventType:     event.EventType,
		Payload:       event.Payload,
		Headers:       event.Headers,
		Topic:         event.Topic,
		AttemptCount:  event.AttemptCount,
		NextAttemptAt: event.NextAttemptAt,
	}

	if err := p.publisher.Publish(ctx, publishableEvent); err != nil {
		p.metrics.IncrementCounter("event_processor.publish_failed", map[string]string{"event_type": event.EventType})
		p.logger.Error("Failed to publish event", append(eventFields, zap.Error(err))...)
		return p.rescheduleEvent(ctx, event, err)
	}

	if err := p.store.MarkAsSent(ctx, event.ID); err != nil {
		p.metrics.IncrementCounter("event_processor.mark_sent_failed", map[string]string{"event_type": event.EventType})
		p.logger.Error("Failed to mark event as sent", append(eventFields, zap.Error(err))...)
		// Событие опубликовано, но статус не обновлен. StuckEventService должен это исправить.
		return err
	}

	p.metrics.IncrementCounter("event_processor.publish_success", map[string]string{"event_type": event.EventType})
	p.logger.Info("Event published successfully", eventFields...)
	return nil
}

func (p *EventProcessorImpl) rescheduleEvent(ctx context.Context, event storage.EventRecord, processingError error) error {
	attempt := event.AttemptCount + 1
	if attempt >= p.maxAttempts {
		p.logger.Error("Event exceeded max attempts, will be moved to dead letter queue later",
			zap.Int64("event_id", event.ID),
			zap.Error(processingError),
		)
		// Просто помечаем как ошибку, DeadLetterService подберет
		// В UpdateForRetry можно передавать статус
		return p.store.UpdateForRetry(ctx, event.ID, time.Now(), processingError.Error())
	}

	nextAttemptAt := p.backoffStrategy.CalculateNextAttempt(attempt)
	p.logger.Info("Scheduling event for retry",
		zap.Int64("event_id", event.ID),
		zap.Time("next_attempt_at", nextAttemptAt),
		zap.Error(processingError),
	)

	return p.store.UpdateForRetry(ctx, event.ID, nextAttemptAt, processingError.Error())
}
