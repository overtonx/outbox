package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/overtonx/outbox/v4/serializer"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewDefaultPublisher(t *testing.T) {
	logger := zap.NewNop()
	publisher := NewDefaultPublisher(logger)

	assert.NotNil(t, publisher, "Expected non-nil publisher")
	assert.Equal(t, logger, publisher.logger, "Expected logger to match")
}

func TestDefaultPublisherPublish(t *testing.T) {
	publisher := NewDefaultPublisher(zap.NewNop())
	event := EventRecord{
		EventID: "test-event-id",
	}

	ctx := context.Background()
	var deliveryErr error
	delivered := false
	err := publisher.Publish(ctx, event, func(err error) {
		delivered = true
		deliveryErr = err
	})

	assert.NoError(t, err, "Expected no error")
	assert.True(t, delivered, "Expected onDelivery to be called")
	assert.NoError(t, deliveryErr)
}

func TestDefaultKafkaConfig(t *testing.T) {
	config := DefaultKafkaConfig()

	assert.Equal(t, "outbox-events", config.Topic, "Expected topic 'outbox-events'")
	assert.Equal(t, "localhost:9092", config.ProducerProps["bootstrap.servers"], "Expected brokers 'localhost:9092'")
	assert.Equal(t, "all", config.ProducerProps["acks"], "Expected acks 'all'")

	// Требования пользователя: idempotence=true, max.in.flight=5 — что при
	// enable.idempotence=true является не рекомендацией, а обязательным
	// значением (librdkafka откажется создать продюсер при большем).
	assert.Equal(t, true, config.ProducerProps["enable.idempotence"])
	assert.Equal(t, 5, config.ProducerProps["max.in.flight.requests.per.connection"])

	// retries не должны быть маленьким числом при enable.idempotence=true:
	// иначе продюсер может исчерпать попытки раньше гарантированной
	// доставки. Общее время ограничивается delivery.timeout.ms.
	retries, ok := config.ProducerProps["retries"].(int)
	assert.True(t, ok)
	assert.Greater(t, retries, 1000)
	assert.NotZero(t, config.ProducerProps["delivery.timeout.ms"])
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
	assert.NotNil(t, publisher.producer.Load(), "Expected non-nil producer")
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
	assert.NotNil(t, publisher.producer.Load(), "Expected non-nil producer")
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

	event := EventRecord{
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
		"content-type":   serializer.ContentTypeJSON,
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

	event := EventRecord{
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
		"content-type":   serializer.ContentTypeJSON,
	}

	assert.Equal(t, len(expectedHeaders), len(headers))

	for _, header := range headers {
		expectedValue, exists := expectedHeaders[header.Key]
		assert.True(t, exists, "Unexpected header key: %s", header.Key)
		assert.Equal(t, expectedValue, string(header.Value), "Header value mismatch")
	}
}

func TestBuildKafkaHeaders_ReservedKeysNotOverridden(t *testing.T) {
	event := EventRecord{
		EventID:       "real-event-id",
		EventType:     "real-event-type",
		AggregateType: "real-aggregate-type",
		AggregateID:   "real-aggregate-id",
		// Attacker tries to override system headers via event payload headers
		Headers: []byte(`{"event_id":"injected","event_type":"injected","content-type":"application/malicious","legit":"value"}`),
	}

	headers := buildKafkaHeaders(event)

	headerMap := make(map[string][]string)
	for _, h := range headers {
		headerMap[h.Key] = append(headerMap[h.Key], string(h.Value))
	}

	// Reserved keys must appear exactly once with system values
	assert.Equal(t, []string{"real-event-id"}, headerMap["event_id"], "event_id must not be overridden")
	assert.Equal(t, []string{"real-event-type"}, headerMap["event_type"], "event_type must not be overridden")
	assert.Equal(t, []string{serializer.ContentTypeJSON}, headerMap["content-type"], "content-type must not be overridden")

	// Legitimate custom header must pass through
	assert.Equal(t, []string{"value"}, headerMap["legit"])
}

func TestBuildKafkaHeadersWithCustomHeaders(t *testing.T) {
	publisher, err := NewKafkaPublisher(zap.NewNop())
	assert.NoError(t, err)
	defer publisher.Close()

	event := EventRecord{
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
		"content-type":   serializer.ContentTypeJSON,
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

// TestKafkaPublisher_ProduceRetriesOnQueueFull проверяет, что ErrQueueFull
// не возвращается наружу как ошибка публикации, а обрабатывается циклом
// ожидания-и-повтора: без реального брокера искусственно маленький
// queue.buffering.max.messages=1 гарантирует, что второй Produce немедленно
// упрётся в переполненную очередь, а короткий ctx-таймаут (короче, чем
// message.timeout.ms) доказывает, что Publish реально ждал/повторял, а не
// сразу вернул ошибку.
func TestKafkaPublisher_ProduceRetriesOnQueueFull(t *testing.T) {
	config := KafkaConfig{
		Topic: "test-topic",
		ProducerProps: kafka.ConfigMap{
			"bootstrap.servers":            "localhost:1",
			"queue.buffering.max.messages": 1,
			"message.timeout.ms":           200,
		},
	}

	publisher, err := NewKafkaPublisherWithConfig(zap.NewNop(), config)
	assert.NoError(t, err)
	defer publisher.Close()

	err = publisher.Publish(context.Background(),
		EventRecord{ID: 1, Topic: "test-topic", AggregateID: "a1", Payload: []byte("x")},
		func(error) {})
	assert.NoError(t, err, "first Produce should fit into the queue")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = publisher.Publish(ctx,
		EventRecord{ID: 2, Topic: "test-topic", AggregateID: "a1", Payload: []byte("y")},
		func(error) {})
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded,
		"backpressure loop must keep retrying until ctx is done, not fail fast with ErrQueueFull")
	assert.GreaterOrEqual(t, elapsed, 25*time.Millisecond,
		"should have actually waited/retried, not returned immediately")
}

// TestKafkaPublisher_FatalProducerErrorTriggersHandler проверяет, что
// фатальная ошибка producer'а (librdkafka.TestFatalError имитирует реальный
// сценарий fenced idempotent producer) доходит до onFatal, а не просто
// логируется и игнорируется.
func TestKafkaPublisher_FatalProducerErrorTriggersHandler(t *testing.T) {
	publisher, err := NewKafkaPublisher(zap.NewNop())
	assert.NoError(t, err)
	defer publisher.Close()

	faults := make(chan error, 1)
	publisher.SetFatalHandler(func(err error) {
		select {
		case faults <- err:
		default:
		}
	})

	producer := publisher.producer.Load()
	producer.TestFatalError(kafka.ErrInvalidTimestamp, "simulated fatal error")

	select {
	case err := <-faults:
		assert.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("expected onFatal to be called after a fatal producer error")
	}
}

// TestKafkaPublisher_RecreateSwapsProducerAndPreservesWatermark проверяет,
// что Recreate заменяет внутренний producer на новый рабочий инстанс, не
// затрагивая при этом накопленный watermark (сброс watermark — отдельная,
// явная операция SeedWatermark, которую выполняет Dispatcher после
// перечитывания персистентного чекпоинта).
func TestKafkaPublisher_RecreateSwapsProducerAndPreservesWatermark(t *testing.T) {
	publisher, err := NewKafkaPublisher(zap.NewNop())
	assert.NoError(t, err)
	defer publisher.Close()

	publisher.tracker.Ack(1)
	publisher.tracker.Ack(2)
	publisher.tracker.Ack(3)
	publisher.tracker.Ack(4)
	publisher.tracker.Ack(5)
	assert.Equal(t, int64(5), publisher.Watermark())

	before := publisher.producer.Load()
	assert.NoError(t, publisher.Recreate())
	after := publisher.producer.Load()

	assert.NotSame(t, before, after, "Recreate must swap in a new producer instance")
	assert.Equal(t, int64(5), publisher.Watermark(), "Recreate itself must not reset the watermark")

	publisher.SeedWatermark(2)
	assert.Equal(t, int64(2), publisher.Watermark(), "SeedWatermark is the explicit reset used for replay")
}
