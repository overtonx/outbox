package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

// KafkaHeaderBuilder defines a function type for building Kafka message headers from an EventRecord.
type KafkaHeaderBuilder func(record EventRecord) []kafka.Header

// NopPublisher is a publisher that does nothing. Useful for testing.
type NopPublisher struct{}

// NewNopPublisher creates a new NopPublisher.
func NewNopPublisher() *NopPublisher {
	return &NopPublisher{}
}

// Publish implements the Publisher interface.
func (p *NopPublisher) Publish(_ context.Context, _ EventRecord) error {
	return nil
}

// Close implements the Publisher interface.
func (p *NopPublisher) Close() error {
	return nil
}

// KafkaPublisher sends events to a Kafka topic.
type KafkaPublisher struct {
	logger        *zap.Logger
	producer      *kafka.Producer
	producerProps kafka.ConfigMap
	defaultTopic  string
	headerBuilder KafkaHeaderBuilder
}

// NewKafkaPublisher creates a new KafkaPublisher with functional options.
func NewKafkaPublisher(logger *zap.Logger, opts ...KafkaPublisherOption) (*KafkaPublisher, error) {
	p := &KafkaPublisher{
		logger: logger,
		producerProps: kafka.ConfigMap{
			// Default producer properties
			"acks":               "all",
			"retries":            3,
			"linger.ms":          10,
			"enable.idempotence": true,
			"compression.type":   "snappy",
		},
		defaultTopic:  "outbox-events",
		headerBuilder: buildKafkaHeaders,
	}

	for _, opt := range opts {
		opt(p)
	}

	producer, err := kafka.NewProducer(&p.producerProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}
	p.producer = producer

	go p.handleDeliveryReports()

	return p, nil
}

// Publish sends an event to the configured Kafka topic.
// The topic is determined by the event's Topic field, falling back to the default topic.
func (p *KafkaPublisher) Publish(_ context.Context, event EventRecord) error {
	topic := event.Topic
	if topic == "" {
		topic = p.defaultTopic
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
		Headers:        p.headerBuilder(event),
		Timestamp:      time.Now(),
	}

	return p.producer.Produce(message, nil)
}

// Close flushes the producer and closes the Kafka connection.
func (p *KafkaPublisher) Close() error {
	p.logger.Info("Closing kafka producer")
	p.producer.Flush(15 * 1000) // 15 sec
	p.producer.Close()
	return nil
}

// handleDeliveryReports consumes delivery reports from the producer's events channel.
func (p *KafkaPublisher) handleDeliveryReports() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.logger.Error("Delivery failed",
					zap.String("topic", *ev.TopicPartition.Topic),
					zap.Error(ev.TopicPartition.Error),
				)
			}
		case kafka.Error:
			p.logger.Error("Kafka error", zap.Error(ev))
		}
	}
}

// buildKafkaHeaders is the default function for creating Kafka headers from an event.
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
