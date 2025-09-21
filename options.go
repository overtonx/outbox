package outbox

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

const (
	defaultBatchSize         = 100
	defaultMaxAttempts       = 3
	defaultBaseDelay         = 1 * time.Minute
	defaultMaxDelay          = 30 * time.Minute
	defaultStuckEventTimeout = 10 * time.Minute
	defaultSentEventsRetention = 24 * time.Hour
	defaultDeadLetterRetention = 7 * 24 * time.Hour
)

//
// Carrier Options
//

type CarrierOption func(*Carrier)

func WithLogger(logger *zap.Logger) CarrierOption {
	return func(c *Carrier) {
		c.logger = logger
	}
}

func WithMetrics(metrics MetricsCollector) CarrierOption {
	return func(c *Carrier) {
		c.metrics = metrics
	}
}

func WithPublisher(publisher Publisher) CarrierOption {
	return func(c *Carrier) {
		c.publisher = publisher
	}
}

//
// KafkaPublisher Options
//

type KafkaPublisherOption func(*KafkaPublisher)

func WithKafkaProducerProps(props kafka.ConfigMap) KafkaPublisherOption {
	return func(p *KafkaPublisher) {
		for k, v := range props {
			p.producerProps[k] = v
		}
	}
}

func WithKafkaDefaultTopic(topic string) KafkaPublisherOption {
	return func(p *KafkaPublisher) {
		p.defaultTopic = topic
	}
}

func WithKafkaHeaderBuilder(builder KafkaHeaderBuilder) KafkaPublisherOption {
	return func(p *KafkaPublisher) {
		p.headerBuilder = builder
	}
}

//
// EventProcessor Options
//

type EventProcessorOption func(*eventProcessorOptions)

type eventProcessorOptions struct {
	batchSize       int
	maxAttempts     int
	backoffStrategy BackoffStrategy
}

func WithEventProcessorBatchSize(size int) EventProcessorOption {
	return func(o *eventProcessorOptions) {
		o.batchSize = size
	}
}

func WithEventProcessorMaxAttempts(attempts int) EventProcessorOption {
	return func(o *eventProcessorOptions) {
		o.maxAttempts = attempts
	}
}

func WithEventProcessorBackoffStrategy(strategy BackoffStrategy) EventProcessorOption {
	return func(o *eventProcessorOptions) {
		o.backoffStrategy = strategy
	}
}

//
// DeadLetterService Options
//

type DeadLetterServiceOption func(*deadLetterServiceOptions)

type deadLetterServiceOptions struct {
	batchSize int
}

func WithDeadLetterServiceBatchSize(size int) DeadLetterServiceOption {
	return func(o *deadLetterServiceOptions) {
		o.batchSize = size
	}
}

//
// StuckEventService Options
//

type StuckEventServiceOption func(*stuckEventServiceOptions)

type stuckEventServiceOptions struct {
	batchSize       int
	maxAttempts     int
	stuckTimeout    time.Duration
	backoffStrategy BackoffStrategy
}

func WithStuckEventServiceBatchSize(size int) StuckEventServiceOption {
	return func(o *stuckEventServiceOptions) {
		o.batchSize = size
	}
}

func WithStuckEventServiceMaxAttempts(attempts int) StuckEventServiceOption {
	return func(o *stuckEventServiceOptions) {
		o.maxAttempts = attempts
	}
}

func WithStuckEventServiceStuckTimeout(timeout time.Duration) StuckEventServiceOption {
	return func(o *stuckEventServiceOptions) {
		o.stuckTimeout = timeout
	}
}

func WithStuckEventServiceBackoffStrategy(strategy BackoffStrategy) StuckEventServiceOption {
	return func(o *stuckEventServiceOptions) {
		o.backoffStrategy = strategy
	}
}

//
// CleanupService Options
//

type CleanupServiceOption func(*cleanupServiceOptions)

type cleanupServiceOptions struct {
	batchSize           int
	sentRetention       time.Duration
	deadLetterRetention time.Duration
}

func WithCleanupServiceBatchSize(size int) CleanupServiceOption {
	return func(o *cleanupServiceOptions) {
		o.batchSize = size
	}
}

func WithCleanupServiceSentRetention(retention time.Duration) CleanupServiceOption {
	return func(o *cleanupServiceOptions) {
		o.sentRetention = retention
	}
}

func WithCleanupServiceDeadLetterRetention(retention time.Duration) CleanupServiceOption {
	return func(o *cleanupServiceOptions) {
		o.deadLetterRetention = retention
	}
}
