package outbox

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestWithBatchSize(t *testing.T) {
	opts := &dispatcherOptions{}
	err := WithBatchSize(50)(opts)
	assert.NoError(t, err)
	assert.Equal(t, 50, opts.batchSize)
}

func TestWithBatchSize_Invalid(t *testing.T) {
	cases := []int{0, -1, -100, 10_001, 1_000_000}
	for _, size := range cases {
		opts := &dispatcherOptions{}
		err := WithBatchSize(size)(opts)
		assert.Error(t, err, "expected error for batch size %d", size)
	}
}

func TestWithPollInterval(t *testing.T) {
	opts := &dispatcherOptions{}
	interval := 5 * time.Second
	err := WithPollInterval(interval)(opts)
	assert.NoError(t, err)
	assert.Equal(t, interval, opts.pollInterval)
}

func TestWithPollInterval_Invalid(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		opts := &dispatcherOptions{}
		assert.Error(t, WithPollInterval(d)(opts), "expected error for interval %s", d)
	}
}

func TestWithMaxAttempts(t *testing.T) {
	opts := &dispatcherOptions{}
	err := WithMaxAttempts(5)(opts)
	assert.NoError(t, err)
	assert.Equal(t, 5, opts.maxAttempts)
}

func TestWithMaxAttempts_Invalid(t *testing.T) {
	for _, n := range []int{0, -1} {
		opts := &dispatcherOptions{}
		assert.Error(t, WithMaxAttempts(n)(opts), "expected error for max attempts %d", n)
	}
}

func TestWithProcessingLeaseTimeout(t *testing.T) {
	opts := &dispatcherOptions{}
	timeout := 90 * time.Second
	err := WithProcessingLeaseTimeout(timeout)(opts)
	assert.NoError(t, err)
	assert.Equal(t, timeout, opts.processingLeaseTimeout)
}

func TestWithProcessingLeaseTimeout_Invalid(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		opts := &dispatcherOptions{}
		assert.Error(t, WithProcessingLeaseTimeout(d)(opts), "expected error for timeout %s", d)
	}
}

func TestWithLockName(t *testing.T) {
	opts := &dispatcherOptions{}
	err := WithLockName("custom-lock")(opts)
	assert.NoError(t, err)
	assert.Equal(t, "custom-lock", opts.lockName)
}

func TestWithLockName_Invalid(t *testing.T) {
	opts := &dispatcherOptions{}
	assert.Error(t, WithLockName("")(opts))
}

func TestWithBackoffStrategy(t *testing.T) {
	opts := &dispatcherOptions{}
	strategy := NewFixedBackoffStrategy(1 * time.Second)
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
	metrics := NewNoOpMetricsCollector()
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
	metrics := NewNoOpMetricsCollector()
	strategy := NewFixedBackoffStrategy(1 * time.Second)

	assert.NoError(t, WithBatchSize(25)(opts))
	assert.NoError(t, WithPollInterval(3*time.Second)(opts))
	assert.NoError(t, WithMaxAttempts(7)(opts))
	assert.NoError(t, WithProcessingLeaseTimeout(45*time.Second)(opts))
	assert.NoError(t, WithLockName("my-lock")(opts))
	assert.NoError(t, WithLogger(logger)(opts))
	assert.NoError(t, WithMetrics(metrics)(opts))
	assert.NoError(t, WithBackoffStrategy(strategy)(opts))

	assert.Equal(t, 25, opts.batchSize)
	assert.Equal(t, 3*time.Second, opts.pollInterval)
	assert.Equal(t, 7, opts.maxAttempts)
	assert.Equal(t, 45*time.Second, opts.processingLeaseTimeout)
	assert.Equal(t, "my-lock", opts.lockName)
	assert.Equal(t, logger, opts.logger)
	assert.Equal(t, metrics, opts.metrics)
	assert.Equal(t, strategy, opts.backoffStrategy)
}

func TestDefaultDispatcherOptions(t *testing.T) {
	opts := defaultDispatcherOptions()

	assert.Equal(t, defaultBatchSize, opts.batchSize)
	assert.Equal(t, defaultPollInterval, opts.pollInterval)
	assert.Equal(t, defaultMaxAttempts, opts.maxAttempts)
	assert.Equal(t, defaultProcessingLeaseTimeout, opts.processingLeaseTimeout)
	assert.Empty(t, opts.lockName)
	assert.NotNil(t, opts.backoffStrategy)
	assert.NotNil(t, opts.metrics)
	assert.NotNil(t, opts.logger)
}
