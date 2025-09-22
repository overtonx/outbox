package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type NopPublisher struct {
	logger *zap.Logger
}

func NewDefaultPublisher(logger *zap.Logger) *NopPublisher {
	return &NopPublisher{
		logger: logger,
	}
}

func (p *NopPublisher) Publish(_ context.Context, _ EventRecord) error {
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

// KafkaHeaderBuilder defines a function type for building Kafka message headers from an EventRecord.
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
			"bootstrap.servers":  "localhost:9092",
			"acks":               "all",
			"retries":            3,
			"linger.ms":          10,
			"enable.idempotence": true,
			"compression.type":   "snappy",
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

	go p.handleDeliveryReports()

	return p
}

func (p *KafkaPublisher) Publish(_ context.Context, event EventRecord) error {
	topic := event.Topic
	if topic == "" {
		topic = p.config.Topic
	}

	p.logger.Info("Publishing event to Kafka",
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
	}

	return p.producer.Produce(message, nil)
}

func (p *KafkaPublisher) Close() error {
	p.logger.Info("Closing kafka producer")

	if p.producer == nil {
		return nil
	}

	var flushErr error
	// Flush blocks until all messages are delivered or the timeout expires.
	// It returns the number of messages still in the queue.
	if remaining := p.producer.Flush(15 * 1000); remaining > 0 {
		flushErr = fmt.Errorf("failed to flush kafka producer: %d messages remaining", remaining)
		p.logger.Error("Failed to flush kafka producer", zap.Error(flushErr))
	} else {
		p.logger.Info("Successfully flushed kafka producer")
	}

	// Close the producer to release resources.
	p.producer.Close()

	return flushErr
}

func (p *KafkaPublisher) handleDeliveryReports() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.logger.Error("Delivery failed",
					zap.String("topic", *ev.TopicPartition.Topic),
					zap.Error(ev.TopicPartition.Error),
				)
			} else {
				p.logger.Debug("Successfully delivered message",
					zap.String("topic", *ev.TopicPartition.Topic),
					zap.Int32("partition", ev.TopicPartition.Partition),
					zap.Any("offset", ev.TopicPartition.Offset),
				)
			}
		case kafka.Error:
			p.logger.Error("Kafka error", zap.Error(ev))
		default:
			p.logger.Debug("Ignored kafka event", zap.Any("event", ev))
		}
	}
}

func buildKafkaHeaders(event EventRecord) []kafka.Header {
	headers := []kafka.Header{
		{Key: "event_id", Value: []byte(event.EventID)},
		{Key: "event_type", Value: []byte(event.EventType)},
		{Key: "aggregate_type", Value: []byte(event.AggregateType)},
		{Key: "aggregate_id", Value: []byte(event.AggregateID)},
	}

	if len(event.Headers) > 0 {
		var eventHeaders map[string]interface{}
		if err := json.Unmarshal(event.Headers, &eventHeaders); err == nil {
			for k, v := range eventHeaders {
				headers = append(headers, kafka.Header{Key: k, Value: []byte(fmt.Sprintf("%v", v))})
			}
		}
	}

	return headers
}
