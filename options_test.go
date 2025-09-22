package outbox

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/backoff"
	"github.com/overtonx/outbox/v2/internal/metric"
)

func TestWithBatchSize(t *testing.T) {
	opts := &dispatcherOptions{}
	err := WithBatchSize(50)(opts)
	assert.NoError(t, err)
	assert.Equal(t, 50, opts.batchSize)
}

func TestWithPollInterval(t *testing.T) {
	opts := &dispatcherOptions{}
	interval := 5 * time.Second
	err := WithPollInterval(interval)(opts)
	assert.NoError(t, err)
	assert.Equal(t, interval, opts.pollInterval)
}

func TestWithMaxAttempts(t *testing.T) {
	opts := &dispatcherOptions{}
	err := WithMaxAttempts(5)(opts)
	assert.NoError(t, err)
	assert.Equal(t, 5, opts.maxAttempts)
}

func TestWithDeadLetterInterval(t *testing.T) {
	opts := &dispatcherOptions{}
	interval := 10 * time.Minute
	err := WithDeadLetterInterval(interval)(opts)
	assert.NoError(t, err)
	assert.Equal(t, interval, opts.deadLetterInterval)
}

func TestWithStuckEventTimeout(t *testing.T) {
	opts := &dispatcherOptions{}
	timeout := 15 * time.Minute
	err := WithStuckEventTimeout(timeout)(opts)
	assert.NoError(t, err)
	assert.Equal(t, timeout, opts.stuckEventTimeout)
}

func TestWithStuckEventCheckInterval(t *testing.T) {
	opts := &dispatcherOptions{}
	interval := 3 * time.Minute
	err := WithStuckEventCheckInterval(interval)(opts)
	assert.NoError(t, err)
	assert.Equal(t, interval, opts.stuckEventCheckInterval)
}

func TestWithDeadLetterRetention(t *testing.T) {
	opts := &dispatcherOptions{}
	retention := 14 * 24 * time.Hour
	err := WithDeadLetterRetention(retention)(opts)
	assert.NoError(t, err)
	assert.Equal(t, retention, opts.deadLetterRetention)
}

func TestWithSentEventsRetention(t *testing.T) {
	opts := &dispatcherOptions{}
	retention := 48 * time.Hour
	err := WithSentEventsRetention(retention)(opts)
	assert.NoError(t, err)
	assert.Equal(t, retention, opts.sentEventsRetention)
}

func TestWithCleanupInterval(t *testing.T) {
	opts := &dispatcherOptions{}
	interval := 2 * time.Hour
	err := WithCleanupInterval(interval)(opts)
	assert.NoError(t, err)
	assert.Equal(t, interval, opts.cleanupInterval)
}

func TestWithBackoffStrategy(t *testing.T) {
	opts := &dispatcherOptions{}
	strategy := backoff.NewFixedBackoffStrategy(1 * time.Second)
	err := WithBackoffStrategy(strategy)(opts)
	assert.NoError(t, err)
	assert.Equal(t, strategy, opts.backoffStrategy)
}

func TestWithPublisher(t *testing.T) {
	opts := &dispatcherOptions{}
	logger := zap.NewNop()
	publisher := NewDefaultPublisher(logger)
	err := WithPublisher(publisher)(opts)
	assert.NoError(t, err)
	assert.Equal(t, publisher, opts.publisher)
}

func TestWithMetrics(t *testing.T) {
	opts := &dispatcherOptions{}
	metrics := metric.NewNoOpMetricsCollector()
	err := WithMetrics(metrics)(opts)
	assert.NoError(t, err)
	assert.Equal(t, metrics, opts.metrics)
}

func TestWithLogger(t *testing.T) {
	opts := &dispatcherOptions{}
	logger := zap.NewNop()
	err := WithLogger(logger)(opts)
	assert.NoError(t, err)
	assert.Equal(t, logger, opts.logger)
}

func TestWithKafkaConfig(t *testing.T) {
	opts := &dispatcherOptions{
		logger: zap.NewNop(),
	}
	config := KafkaConfig{
		Topic: "test-topic",
		ProducerProps: kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
	}
	err := WithKafkaConfig(config)(opts)
	assert.NoError(t, err)

	assert.NotNil(t, opts.publisher)
	kafkaPublisher, ok := opts.publisher.(*KafkaPublisher)
	assert.True(t, ok)
	assert.Equal(t, config.Topic, kafkaPublisher.config.Topic)
}

func TestMultipleOptions(t *testing.T) {
	opts := &dispatcherOptions{}
	logger := zap.NewNop()
	metrics := metric.NewNoOpMetricsCollector()
	strategy := backoff.NewFixedBackoffStrategy(1 * time.Second)

	err := WithBatchSize(25)(opts)
	assert.NoError(t, err)
	err = WithPollInterval(3 * time.Second)(opts)
	assert.NoError(t, err)
	err = WithMaxAttempts(7)(opts)
	assert.NoError(t, err)
	err = WithLogger(logger)(opts)
	assert.NoError(t, err)
	err = WithMetrics(metrics)(opts)
	assert.NoError(t, err)
	err = WithBackoffStrategy(strategy)(opts)
	assert.NoError(t, err)

	assert.Equal(t, 25, opts.batchSize)
	assert.Equal(t, 3*time.Second, opts.pollInterval)
	assert.Equal(t, 7, opts.maxAttempts)
	assert.Equal(t, logger, opts.logger)
	assert.Equal(t, metrics, opts.metrics)
	assert.Equal(t, strategy, opts.backoffStrategy)
}
