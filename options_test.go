package outbox

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCarrierOptions(t *testing.T) {
	logger := zap.NewNop()
	metrics := NewNopMetricsCollector()
	publisher := NewNopPublisher()

	c := &Carrier{}

	WithLogger(logger)(c)
	assert.Equal(t, logger, c.logger)

	WithMetrics(metrics)(c)
	assert.Equal(t, metrics, c.metrics)

	WithPublisher(publisher)(c)
	assert.Equal(t, publisher, c.publisher)
}

func TestPublisherOptions(t *testing.T) {
	p := &KafkaPublisher{
		producerProps: make(kafka.ConfigMap),
	}

	WithKafkaDefaultTopic("test-topic")(p)
	assert.Equal(t, "test-topic", p.defaultTopic)

	props := kafka.ConfigMap{"bootstrap.servers": "kafka:9092"}
	WithKafkaProducerProps(props)(p)
	assert.Equal(t, "kafka:9092", p.producerProps["bootstrap.servers"])

	builder := func(record EventRecord) []kafka.Header { return nil }
	WithKafkaHeaderBuilder(builder)(p)
	assert.NotNil(t, p.headerBuilder)
}

func TestEventProcessorOptions(t *testing.T) {
	opts := &eventProcessorOptions{}

	WithEventProcessorBatchSize(50)(opts)
	assert.Equal(t, 50, opts.batchSize)

	WithEventProcessorMaxAttempts(10)(opts)
	assert.Equal(t, 10, opts.maxAttempts)

	strategy := NewFixedBackoffStrategy(5 * time.Second)
	WithEventProcessorBackoffStrategy(strategy)(opts)
	assert.Equal(t, strategy, opts.backoffStrategy)
}

func TestDeadLetterServiceOptions(t *testing.T) {
	opts := &deadLetterServiceOptions{}
	WithDeadLetterServiceBatchSize(20)(opts)
	assert.Equal(t, 20, opts.batchSize)
}

func TestStuckEventServiceOptions(t *testing.T) {
	opts := &stuckEventServiceOptions{}

	WithStuckEventServiceBatchSize(15)(opts)
	assert.Equal(t, 15, opts.batchSize)

	WithStuckEventServiceMaxAttempts(2)(opts)
	assert.Equal(t, 2, opts.maxAttempts)

	timeout := 5 * time.Minute
	WithStuckEventServiceStuckTimeout(timeout)(opts)
	assert.Equal(t, timeout, opts.stuckTimeout)

	strategy := NewFixedBackoffStrategy(1 * time.Second)
	WithStuckEventServiceBackoffStrategy(strategy)(opts)
	assert.Equal(t, strategy, opts.backoffStrategy)
}

func TestCleanupServiceOptions(t *testing.T) {
	opts := &cleanupServiceOptions{}

	WithCleanupServiceBatchSize(200)(opts)
	assert.Equal(t, 200, opts.batchSize)

	sentRetention := 48 * time.Hour
	WithCleanupServiceSentRetention(sentRetention)(opts)
	assert.Equal(t, sentRetention, opts.sentRetention)

	dlRetention := 10 * 24 * time.Hour
	WithCleanupServiceDeadLetterRetention(dlRetention)(opts)
	assert.Equal(t, dlRetention, opts.deadLetterRetention)
}
