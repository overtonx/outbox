package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/overtonx/outbox/v4/serializer"
	"go.uber.org/zap"
)

// Publisher асинхронно публикует заклеймленное событие во внешнюю систему
// обмена сообщениями. Publish возвращает ошибку немедленно только при сбое
// постановки в очередь (enqueue); в остальных случаях возвращает nil сразу
// же и ровно один раз вызывает onDelivery, когда придёт отчёт о доставке
// (успех или ошибка) — как правило, из другой горутины. Это позволяет
// пайплайнить публикацию целой пачки событий, не дожидаясь подтверждения
// каждого по отдельности. Единственная реализация в проде — KafkaPublisher;
// NopPublisher существует для тестов.
type Publisher interface {
	Publish(ctx context.Context, event EventRecord, onDelivery func(error)) error
	Close() error
}

type NopPublisher struct {
	logger *zap.Logger
}

func NewDefaultPublisher(logger *zap.Logger) *NopPublisher {
	return &NopPublisher{
		logger: logger,
	}
}

func (p *NopPublisher) Publish(_ context.Context, _ EventRecord, onDelivery func(error)) error {
	onDelivery(nil)
	return nil
}

func (p *NopPublisher) Close() error {
	return nil
}

type KafkaPublisher struct {
	logger   *zap.Logger
	producer *kafka.Producer
	config   KafkaConfig
}

// KafkaHeaderBuilder определяет тип функции для построения заголовков Kafka-сообщения из EventRecord.
type KafkaHeaderBuilder func(record EventRecord) []kafka.Header

type KafkaConfig struct {
	Topic         string
	ProducerProps kafka.ConfigMap
	HeaderBuilder KafkaHeaderBuilder
}

func DefaultKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Topic: "outbox-events",
		ProducerProps: kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			// enable.idempotence требует acks=all и ограничивает
			// max.in.flight.requests.per.connection значением <= 5 —
			// librdkafka откажется создавать продюсер при большем значении.
			"enable.idempotence":                    true,
			"acks":                                  "all",
			"max.in.flight.requests.per.connection": 5,
			// При enable.idempotence=true ограничивать retries нельзя:
			// исчерпание ретраев раньше успешной доставки — фатальная
			// ошибка продюсера. Общее время ограничивается через
			// delivery.timeout.ms, а не через число попыток.
			"retries":             2147483647,
			"retry.backoff.ms":    100,
			"delivery.timeout.ms": 120000,
			"linger.ms":           10,
			"compression.type":    "snappy",
		},
		HeaderBuilder: buildKafkaHeaders,
	}
}

func NewKafkaPublisher(logger *zap.Logger) (*KafkaPublisher, error) {
	config := DefaultKafkaConfig()
	return NewKafkaPublisherWithConfig(logger, config)
}

func NewKafkaPublisherWithConfig(logger *zap.Logger, config KafkaConfig) (*KafkaPublisher, error) {
	producer, err := kafka.NewProducer(&config.ProducerProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	if config.HeaderBuilder == nil {
		config.HeaderBuilder = buildKafkaHeaders
	}

	return NewKafkaPublisherFromProducer(logger, producer, config), nil
}

func NewKafkaPublisherFromProducer(logger *zap.Logger, producer *kafka.Producer, config KafkaConfig) *KafkaPublisher {
	p := &KafkaPublisher{
		logger:   logger,
		producer: producer,
		config:   config,
	}

	// Publish передаёт Produce() nil вместо per-call deliveryChan — отчёты о
	// доставке всех сообщений идут в общий Events(), откуда одна горутина
	// разбирает их по Opaque и асинхронно вызывает соответствующий
	// onDelivery. Это позволяет пайплайнить produce всей пачки батча, не
	// дожидаясь ack каждого сообщения по отдельности: enable.idempotence
	// вместе с max.in.flight.requests.per.connection=5 гарантирует, что
	// брокер подтверждает сообщения в том же порядке, в котором они были
	// отправлены на конкретную партицию, так что порядок в пределах одного
	// aggregate_id сохраняется даже при нескольких одновременно летящих
	// запросах.
	go p.handleDeliveryReports()

	return p
}

// deliveryCallback — корреляция отчёта о доставке с исходным вызовом
// Publish через поле Opaque сообщения Kafka.
type deliveryCallback struct {
	eventID    string
	topic      string
	onDelivery func(error)
}

func (p *KafkaPublisher) Publish(_ context.Context, event EventRecord, onDelivery func(error)) error {
	topic := event.Topic
	if topic == "" {
		topic = p.config.Topic
	}

	p.logger.Debug("Publishing event to Kafka",
		zap.String("event_id", event.EventID),
		zap.String("event_type", event.EventType),
		zap.String("topic", topic),
	)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(event.AggregateID),
		Value:          event.Payload,
		Headers:        p.config.HeaderBuilder(event),
		Timestamp:      time.Now(),
		Opaque: &deliveryCallback{
			eventID:    event.EventID,
			topic:      topic,
			onDelivery: onDelivery,
		},
	}

	if err := p.producer.Produce(message, nil); err != nil {
		return fmt.Errorf("failed to enqueue message to kafka: %w", err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
	p.logger.Info("Closing kafka producer")

	if p.producer == nil {
		return nil
	}

	var flushErr error
	// Flush блокируется до тех пор, пока все сообщения не будут доставлены или не истечёт таймаут.
	// Возвращает количество сообщений, оставшихся в очереди.
	if remaining := p.producer.Flush(15 * 1000); remaining > 0 {
		flushErr = fmt.Errorf("failed to flush kafka producer: %d messages remaining", remaining)
		p.logger.Error("Failed to flush kafka producer", zap.Error(flushErr))
	} else {
		p.logger.Info("Successfully flushed kafka producer")
	}

	// Закрываем продюсер для освобождения ресурсов.
	p.producer.Close()

	return flushErr
}

func (p *KafkaPublisher) handleDeliveryReports() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			cb, ok := ev.Opaque.(*deliveryCallback)
			if !ok || cb == nil || cb.onDelivery == nil {
				continue
			}
			if ev.TopicPartition.Error != nil {
				p.logger.Debug("outbox: kafka delivery failed",
					zap.String("event_id", cb.eventID),
					zap.String("topic", cb.topic),
					zap.Error(ev.TopicPartition.Error),
				)
				cb.onDelivery(fmt.Errorf("kafka delivery failed: %w", ev.TopicPartition.Error))
				continue
			}
			p.logger.Debug("outbox: event delivered to kafka",
				zap.String("event_id", cb.eventID),
				zap.String("topic", cb.topic),
				zap.Int32("partition", ev.TopicPartition.Partition),
				zap.Any("offset", ev.TopicPartition.Offset),
			)
			cb.onDelivery(nil)
		case kafka.Error:
			p.logger.Error("outbox: kafka client error", zap.Error(ev))
		}
	}
}

// reservedKafkaHeaderKeys содержит системные ключи заголовков Kafka, устанавливаемые
// пакетом outbox. Пользовательские заголовки событий с этими ключами молча
// отбрасываются для предотвращения атак подмены заголовков, при которых специально
// сформированное событие могло бы перезаписать системные метаданные, потребляемые
// downstream-сервисами.
var reservedKafkaHeaderKeys = map[string]struct{}{
	"event_id":       {},
	"event_type":     {},
	"aggregate_type": {},
	"aggregate_id":   {},
	"content-type":   {},
}

func buildKafkaHeaders(event EventRecord) []kafka.Header {
	contentType := event.ContentType
	if contentType == "" {
		contentType = serializer.ContentTypeJSON
	}

	headers := []kafka.Header{
		{Key: "event_id", Value: []byte(event.EventID)},
		{Key: "event_type", Value: []byte(event.EventType)},
		{Key: "aggregate_type", Value: []byte(event.AggregateType)},
		{Key: "aggregate_id", Value: []byte(event.AggregateID)},
		{Key: "content-type", Value: []byte(contentType)},
	}

	if len(event.Headers) > 0 {
		var eventHeaders map[string]interface{}
		if err := json.Unmarshal(event.Headers, &eventHeaders); err == nil {
			for k, v := range eventHeaders {
				if _, reserved := reservedKafkaHeaderKeys[k]; reserved {
					continue
				}
				headers = append(headers, kafka.Header{Key: k, Value: []byte(fmt.Sprintf("%v", v))})
			}
		}
	}

	return headers
}
