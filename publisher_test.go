package outbox

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/embedded"
)

func TestNewDefaultPublisher(t *testing.T) {
	logger := zap.NewNop()
	publisher := NewDefaultPublisher(logger)

	assert.NotNil(t, publisher, "Expected non-nil publisher")
	assert.Equal(t, logger, publisher.logger, "Expected logger to match")
}

func TestDefaultPublisherPublish(t *testing.T) {
	publisher := NewDefaultPublisher(zap.NewNop())
	event := embedded.EventRecord{
		EventID: "test-event-id",
	}

	ctx := context.Background()
	err := publisher.Publish(ctx, event)

	assert.NoError(t, err, "Expected no error")
}

func TestDefaultKafkaConfig(t *testing.T) {
	config := DefaultKafkaConfig()

	assert.Equal(t, "outbox-events", config.Topic, "Expected topic 'outbox-events'")
	assert.Equal(t, "localhost:9092", config.ProducerProps["bootstrap.servers"], "Expected brokers 'localhost:9092'")
	assert.Equal(t, "all", config.ProducerProps["acks"], "Expected acks 'all'")
}

func TestNewKafkaPublisher(t *testing.T) {
	logger := zap.NewNop()
	publisher, err := NewKafkaPublisher(logger)
	assert.NoError(t, err)
	defer func() {
		if publisher != nil {
			publisher.Close()
		}
	}()

	assert.NotNil(t, publisher, "Expected non-nil publisher")
	assert.Equal(t, logger, publisher.logger, "Expected logger to match")
	assert.NotNil(t, publisher.producer, "Expected non-nil producer")
	assert.Equal(t, "outbox-events", publisher.config.Topic, "Expected default topic 'outbox-events'")
}

func TestNewKafkaPublisherWithConfig(t *testing.T) {
	logger := zap.NewNop()
	config := KafkaConfig{
		Topic: "custom-topic",
		ProducerProps: kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"acks":              "1",
		},
	}

	publisher, err := NewKafkaPublisherWithConfig(logger, config)
	assert.NoError(t, err)
	defer func() {
		if publisher != nil {
			publisher.Close()
		}
	}()

	assert.NotNil(t, publisher, "Expected non-nil publisher")
	assert.Equal(t, logger, publisher.logger, "Expected logger to match")
	assert.NotNil(t, publisher.producer, "Expected non-nil producer")
	assert.Equal(t, config.Topic, publisher.config.Topic, "Expected topic to match")
}

func TestKafkaPublisherClose(t *testing.T) {
	publisher, err := NewKafkaPublisher(zap.NewNop())
	assert.NoError(t, err)

	err = publisher.Close()
	assert.NoError(t, err, "Expected no error on first close")
}

func TestBuildKafkaHeaders(t *testing.T) {
	publisher, err := NewKafkaPublisher(zap.NewNop())
	assert.NoError(t, err)
	defer publisher.Close()

	event := embedded.EventRecord{
		EventID:       "test-event-id",
		EventType:     "test-event-type",
		AggregateType: "test-aggregate-type",
		AggregateID:   "test-aggregate-id",
	}

	headers := buildKafkaHeaders(event)

	expectedHeaders := map[string]string{
		"event_id":       "test-event-id",
		"event_type":     "test-event-type",
		"aggregate_type": "test-aggregate-type",
		"aggregate_id":   "test-aggregate-id",
	}

	assert.Equal(t, len(expectedHeaders), len(headers))

	for _, header := range headers {
		expectedValue, exists := expectedHeaders[header.Key]
		assert.True(t, exists, "Unexpected header key: %s", header.Key)
		assert.Equal(t, expectedValue, string(header.Value), "Header value mismatch")
	}
}

func TestBuildKafkaHeadersWithoutTraceInfo(t *testing.T) {
	publisher, err := NewKafkaPublisher(zap.NewNop())
	assert.NoError(t, err)
	defer publisher.Close()

	event := embedded.EventRecord{
		EventID:       "test-event-id",
		EventType:     "test-event-type",
		AggregateType: "test-aggregate-type",
		AggregateID:   "test-aggregate-id",
	}

	headers := buildKafkaHeaders(event)

	expectedHeaders := map[string]string{
		"event_id":       "test-event-id",
		"event_type":     "test-event-type",
		"aggregate_type": "test-aggregate-type",
		"aggregate_id":   "test-aggregate-id",
	}

	assert.Equal(t, len(expectedHeaders), len(headers))

	for _, header := range headers {
		expectedValue, exists := expectedHeaders[header.Key]
		assert.True(t, exists, "Unexpected header key: %s", header.Key)
		assert.Equal(t, expectedValue, string(header.Value), "Header value mismatch")
	}
}

func TestBuildKafkaHeadersWithCustomHeaders(t *testing.T) {
	publisher, err := NewKafkaPublisher(zap.NewNop())
	assert.NoError(t, err)
	defer publisher.Close()

	event := embedded.EventRecord{
		EventID:       "test-event-id",
		EventType:     "test-event-type",
		AggregateType: "test-aggregate-type",
		AggregateID:   "test-aggregate-id",
		Headers:       []byte(`{"custom_key":"custom_value","another":"value"}`),
	}

	headers := buildKafkaHeaders(event)

	expectedHeaders := map[string]string{
		"event_id":       "test-event-id",
		"event_type":     "test-event-type",
		"aggregate_type": "test-aggregate-type",
		"aggregate_id":   "test-aggregate-id",
		"custom_key":     "custom_value",
		"another":        "value",
	}

	assert.Equal(t, len(expectedHeaders), len(headers))

	for _, header := range headers {
		expectedValue, exists := expectedHeaders[header.Key]
		assert.True(t, exists, "Unexpected header key: %s", header.Key)
		assert.Equal(t, expectedValue, string(header.Value), "Header value mismatch")
	}
}
