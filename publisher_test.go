package outbox

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNopPublisher(t *testing.T) {
	publisher := NewNopPublisher()
	assert.NotNil(t, publisher)
	err := publisher.Publish(context.Background(), EventRecord{})
	assert.NoError(t, err)
	err = publisher.Close()
	assert.NoError(t, err)
}

func TestKafkaPublisherOptions(t *testing.T) {
	logger := zap.NewNop()
	p := &KafkaPublisher{
		logger:        logger,
		producerProps: make(kafka.ConfigMap),
	}

	// Apply options
	WithKafkaDefaultTopic("my-topic")(p)
	WithKafkaProducerProps(kafka.ConfigMap{"acks": "1"})(p)
	customBuilder := func(record EventRecord) []kafka.Header { return nil }
	WithKafkaHeaderBuilder(customBuilder)(p)

	assert.Equal(t, "my-topic", p.defaultTopic)
	assert.Equal(t, "1", p.producerProps["acks"])
	assert.NotNil(t, p.headerBuilder)
}

func TestBuildKafkaHeaders(t *testing.T) {
	event := EventRecord{
		EventID:       "test-event-id",
		EventType:     "test-event-type",
		AggregateType: "test-aggregate-type",
		AggregateID:   "test-aggregate-id",
		Headers:       json.RawMessage(`{"custom_header":"custom_value"}`),
	}

	headers := buildKafkaHeaders(event)

	expectedHeaders := map[string]string{
		"event_id":        "test-event-id",
		"event_type":      "test-event-type",
		"aggregate_type":  "test-aggregate-type",
		"aggregate_id":    "test-aggregate-id",
		"custom_header":   "custom_value",
	}

	assert.Equal(t, len(expectedHeaders), len(headers))

	for _, header := range headers {
		expectedValue, exists := expectedHeaders[header.Key]
		require.True(t, exists, "Unexpected header key: %s", header.Key)
		assert.Equal(t, expectedValue, string(header.Value), "Header value mismatch for key %s", header.Key)
	}
}
