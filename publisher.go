package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
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

// OrderedPublisher — опциональная возможность Publisher'а: строгие гарантии
// порядка через единый глобальный чекпоинт по непрерывному префиксу
// подтверждений вместо точечного ретрая отдельных сообщений. Dispatcher
// определяет поддержку через приведение типа: если Publisher её не
// реализует (как NopPublisher или произвольная пользовательская
// реализация), используется прежняя построчная модель status/retry/backoff.
// KafkaPublisher — единственная реализация в проде.
type OrderedPublisher interface {
	Publisher

	// Watermark возвращает последний подтверждённый непрерывный id потока
	// (ack 1,2,3,5 -> Watermark()==3).
	Watermark() int64
	// SeedWatermark инициализирует/переустанавливает watermark — вызывается
	// один раз при получении лидерства (из персистентного чекпоинта) и
	// повторно после Recreate, чтобы возобновить строго с последнего
	// персистентного значения, отбросив ненадёжное состояние до сбоя.
	SeedWatermark(id int64)
	// SetFatalHandler регистрирует обработчик, вызываемый при обнаружении
	// фатальной ошибки producer'а — под enable.idempotence она означает
	// разрыв гарантии порядка и требует остановки потока и replay.
	SetFatalHandler(handler func(error))
	// Recreate закрывает текущий producer в фоне и переключает Publish на
	// новый, созданный с той же конфигурацией. Используется перед replay.
	Recreate() error
}

var _ OrderedPublisher = (*KafkaPublisher)(nil)

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
	logger *zap.Logger
	config KafkaConfig

	producer atomic.Pointer[kafka.Producer]
	tracker  *checkpointTracker

	fatalMu sync.Mutex
	onFatal func(error)

	closing atomic.Bool
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
		logger:  logger,
		config:  config,
		tracker: newCheckpointTracker(0),
	}
	p.producer.Store(producer)

	// Publish передаёт Produce() nil вместо per-call deliveryChan — отчёты о
	// доставке всех сообщений идут в общий Events(), откуда одна горутина
	// разбирает их по Opaque. Горутина запускается до возврата конструктора,
	// то есть до первого возможного вызова Publish.
	go p.handleDeliveryReports(producer)

	return p
}

// deliveryCallback — корреляция отчёта о доставке с исходным вызовом
// Publish через поле Opaque сообщения Kafka. id — позиция события в БД,
// используется checkpointTracker для продвижения watermark по непрерывному
// префиксу.
type deliveryCallback struct {
	id         int64
	eventID    string
	topic      string
	onDelivery func(error)
}

func (p *KafkaPublisher) Publish(ctx context.Context, event EventRecord, onDelivery func(error)) error {
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
			id:         event.ID,
			eventID:    event.EventID,
			topic:      topic,
			onDelivery: onDelivery,
		},
	}

	return p.produceWithBackpressure(ctx, message)
}

// produceWithBackpressure вызывает Produce и, если внутренняя очередь
// librdkafka временно переполнена (ErrQueueFull), ждёт и повторяет попытку
// вместо того чтобы возвращать ошибку наружу как сбой публикации — очередь
// освобождается по мере ухода подтверждённых сообщений, и это временное
// состояние, а не потеря или отказ конкретного сообщения.
func (p *KafkaPublisher) produceWithBackpressure(ctx context.Context, message *kafka.Message) error {
	const queueFullRetryDelay = 10 * time.Millisecond

	for {
		producer := p.producer.Load()
		err := producer.Produce(message, nil)
		if err == nil {
			return nil
		}

		if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrQueueFull {
			select {
			case <-ctx.Done():
				return fmt.Errorf("failed to enqueue message to kafka: %w", ctx.Err())
			case <-time.After(queueFullRetryDelay):
			}
			continue
		}

		return fmt.Errorf("failed to enqueue message to kafka: %w", err)
	}
}

func (p *KafkaPublisher) Close() error {
	p.logger.Info("Closing kafka producer")

	producer := p.producer.Load()
	if producer == nil {
		return nil
	}
	// closing=true подавляет доставку в onFatal во время штатного
	// завершения: события об ошибках, приходящие из-за самого Flush/Close,
	// не должны запускать пересоздание producer'а.
	p.closing.Store(true)

	var flushErr error
	// Flush блокируется до тех пор, пока все сообщения не будут доставлены или не истечёт таймаут.
	// Горутина handleDeliveryReports продолжает читать Events() всё это время
	// и завершится сама, когда Close() ниже закроет канал.
	if remaining := producer.Flush(15 * 1000); remaining > 0 {
		flushErr = fmt.Errorf("failed to flush kafka producer: %d messages remaining", remaining)
		p.logger.Error("Failed to flush kafka producer", zap.Error(flushErr))
	} else {
		p.logger.Info("Successfully flushed kafka producer")
	}

	producer.Close()

	return flushErr
}

// Watermark возвращает последний подтверждённый непрерывный id потока.
func (p *KafkaPublisher) Watermark() int64 {
	return p.tracker.Watermark()
}

// SeedWatermark инициализирует/переустанавливает watermark — из
// персистентного чекпоинта при получении лидерства или после Recreate.
func (p *KafkaPublisher) SeedWatermark(id int64) {
	p.tracker.Reset(id)
}

// SetFatalHandler регистрирует обработчик фатальных ошибок producer'а.
func (p *KafkaPublisher) SetFatalHandler(handler func(error)) {
	p.fatalMu.Lock()
	p.onFatal = handler
	p.fatalMu.Unlock()
}

// Recreate закрывает текущий producer в фоне (Flush с коротким таймаутом —
// он уже в нерабочем состоянии, ждать долго бессмысленно) и атомарно
// переключает последующие Publish на новый producer с той же конфигурацией.
func (p *KafkaPublisher) Recreate() error {
	newProducer, err := kafka.NewProducer(&p.config.ProducerProps)
	if err != nil {
		return fmt.Errorf("outbox: failed to recreate kafka producer: %w", err)
	}

	old := p.producer.Swap(newProducer)
	go p.handleDeliveryReports(newProducer)

	go func() {
		old.Flush(2000)
		old.Close()
	}()

	return nil
}

func (p *KafkaPublisher) handleDeliveryReports(producer *kafka.Producer) {
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			cb, ok := ev.Opaque.(*deliveryCallback)
			if !ok || cb == nil {
				continue
			}

			if ev.TopicPartition.Error != nil {
				p.logger.Debug("outbox: kafka delivery failed",
					zap.String("event_id", cb.eventID),
					zap.String("topic", cb.topic),
					zap.Error(ev.TopicPartition.Error),
				)
				if cb.onDelivery != nil {
					cb.onDelivery(fmt.Errorf("kafka delivery failed: %w", ev.TopicPartition.Error))
				}
				continue
			}

			p.logger.Debug("outbox: event delivered to kafka",
				zap.String("event_id", cb.eventID),
				zap.String("topic", cb.topic),
				zap.Int32("partition", ev.TopicPartition.Partition),
				zap.Any("offset", ev.TopicPartition.Offset),
			)
			p.tracker.Ack(cb.id)
			if cb.onDelivery != nil {
				cb.onDelivery(nil)
			}
		case kafka.Error:
			if ev.IsFatal() {
				p.logger.Error("outbox: fatal kafka producer error", zap.Error(ev))
				p.reportFault(fmt.Errorf("fatal kafka producer error: %w", ev))
				continue
			}
			p.logger.Error("outbox: kafka client error", zap.Error(ev))
		}
	}
}

func (p *KafkaPublisher) reportFault(err error) {
	if p.closing.Load() {
		return
	}
	p.fatalMu.Lock()
	handler := p.onFatal
	p.fatalMu.Unlock()
	if handler != nil {
		handler(err)
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
