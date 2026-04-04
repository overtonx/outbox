package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	"github.com/overtonx/outbox/v3"
	"github.com/overtonx/outbox/v3/serializer"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("Failed to create logger:", err)
	}
	defer logger.Sync()

	db, err := sql.Open("mysql", "root:password@tcp(localhost:3306)/outbox_db?parseTime=true")
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		logger.Fatal("Failed to ping database", zap.Error(err))
	}

	// Создаём фасад с JSON-сериализатором
	ob := outbox.New(db, serializer.JSONSerializer{})

	// Настраиваем Kafka
	kafkaConfig := outbox.KafkaConfig{
		Topic: "user-events",
		ProducerProps: kafka.ConfigMap{
			"bootstrap.servers":  "localhost:9092",
			"acks":               "all",
			"retries":            3,
			"linger.ms":          10,
			"enable.idempotence": true,
			"compression.type":   "snappy",
		},
	}

	// Создаём диспетчер
	dispatcher, err := ob.Dispatcher(
		outbox.WithLogger(logger),
		outbox.WithKafkaConfig(kafkaConfig),
		outbox.WithBatchSize(5),
		outbox.WithPollInterval(1*time.Second),
		outbox.WithMaxAttempts(3),
	)
	if err != nil {
		logger.Fatal("Failed to create dispatcher", zap.Error(err))
	}

	db.Exec("TRUNCATE outbox_events")
	db.Exec("TRUNCATE outbox_deadletters")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go dispatcher.Start(ctx)

	time.Sleep(2 * time.Second)

	logger.Info("Creating sample events...")
	createSampleEvents(ctx, db, ob, logger)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sigChan:
			logger.Info("Received shutdown signal")
			cancel()
			dispatcher.Stop()
			return
		case <-ticker.C:
			metrics := dispatcher.GetMetrics()
			logger.Info("Dispatcher metrics", zap.Any("metrics", metrics))
		}
	}
}

type userPayload struct {
	UserID    string    `json:"user_id"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

func createSampleEvents(ctx context.Context, db *sql.DB, ob *outbox.Outbox, logger *zap.Logger) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error("Failed to begin transaction", zap.Error(err))
		return
	}
	defer tx.Rollback()

	store := ob.EventStore()

	events := []outbox.Event{
		{
			EventID:       "evt-001",
			EventType:     "UserCreated",
			AggregateType: "User",
			AggregateID:   "user-123",
			Topic:         "user-events",
			Payload: userPayload{
				UserID:    "user-123",
				Email:     "john@example.com",
				Name:      "John Doe",
				CreatedAt: time.Now(),
			},
			Headers: map[string]string{"source": "example"},
		},
		{
			EventID:       "evt-002",
			EventType:     "UserUpdated",
			AggregateType: "User",
			AggregateID:   "user-123",
			Topic:         "user-events",
			Payload: userPayload{
				UserID:    "user-123",
				Email:     "john.doe@example.com",
				Name:      "John Doe",
				CreatedAt: time.Now(),
			},
		},
	}

	for _, event := range events {
		if err := store.Save(ctx, tx, event); err != nil {
			logger.Error("Failed to save outbox event",
				zap.String("event_id", event.EventID),
				zap.Error(err))
			continue
		}
		logger.Info("Event saved to outbox",
			zap.String("event_id", event.EventID),
			zap.String("event_type", event.EventType),
			zap.String("topic", event.Topic))
	}

	if err := tx.Commit(); err != nil {
		logger.Error("Failed to commit transaction", zap.Error(err))
		return
	}

	logger.Info("All events saved to outbox successfully")
}
