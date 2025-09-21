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

	"github.com/overtonx/outbox/v2"
)

func main() {
	// Создаем логгер
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("Failed to create logger:", err)
	}
	defer logger.Sync()

	// Подключаемся к базе данных
	db, err := sql.Open("mysql", "root:password@tcp(localhost:3306)/outbox_db_v2?parseTime=true")
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer db.Close()

	// Проверяем соединение
	if err := db.Ping(); err != nil {
		logger.Fatal("Failed to ping database", zap.Error(err))
	}

	ctx := context.Background()

	// Создаем конфигурацию Kafka
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

	// Создаем dispatcher с настройками
	dispatcher, _ := outbox.NewDispatcher(db,
		outbox.WithLogger(logger),
		outbox.WithKafkaConfig(kafkaConfig),
		outbox.WithBatchSize(5),
		outbox.WithPollInterval(1*time.Second),
		outbox.WithMaxAttempts(3),
	)

	db.Exec("TRUNCATE outbox_events")
	db.Exec("TRUNCATE outbox_deadletters")

	// Запускаем dispatcher в отдельной горутине
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		dispatcher.Start(ctx)
	}()

	// Ждем немного, чтобы dispatcher запустился
	time.Sleep(2 * time.Second)

	// Симулируем создание событий
	logger.Info("Starting to create sample events...")
	createSampleEvents(ctx, db, logger)

	// Ждем сигнал завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Показываем метрики каждые 5 секунд
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

func createSampleEvents(ctx context.Context, db *sql.DB, logger *zap.Logger) {
	// Начинаем транзакцию
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error("Failed to begin transaction", zap.Error(err))
		return
	}
	defer tx.Rollback()

	// Создаем несколько событий
	events := []struct {
		eventID       string
		eventType     string
		aggregateType string
		aggregateID   string
		topic         string
		payload       struct {
			UserID    string    `json:"user_id"`
			Email     string    `json:"email"`
			Name      string    `json:"name"`
			CreatedAt time.Time `json:"created_at"`
		}
	}{
		{
			eventID:       "evt-001",
			eventType:     "UserCreated",
			aggregateType: "User",
			aggregateID:   "user-123",
			topic:         "user-events",
			payload: struct {
				UserID    string    `json:"user_id"`
				Email     string    `json:"email"`
				Name      string    `json:"name"`
				CreatedAt time.Time `json:"created_at"`
			}{
				UserID:    "user-123",
				Email:     "john@example.com",
				Name:      "John Doe",
				CreatedAt: time.Now(),
			},
		},
		{
			eventID:       "evt-002",
			eventType:     "UserUpdated",
			aggregateType: "User",
			aggregateID:   "user-123",
			topic:         "user-events",
			payload: struct {
				UserID    string    `json:"user_id"`
				Email     string    `json:"email"`
				Name      string    `json:"name"`
				CreatedAt time.Time `json:"created_at"`
			}{
				UserID:    "user-123",
				Email:     "john.doe@example.com",
				Name:      "John Doe",
				CreatedAt: time.Now(),
			},
		},
	}

	// Сохраняем события в outbox
	for _, eventData := range events {
		event, err := outbox.NewOutboxEvent(
			eventData.eventID,
			eventData.eventType,
			eventData.aggregateType,
			eventData.aggregateID,
			eventData.topic,
			eventData.payload,
			map[string]string{
				"foo": "bar",
			},
		)

		if err != nil {
			logger.Error("Failed to create outbox event",
				zap.String("event_id", eventData.eventID),
				zap.Error(err))
			continue
		}

		// Сохраняем событие с автоматическим извлечением trace info
		if err := outbox.SaveEvent(ctx, tx, event); err != nil {
			logger.Error("Failed to save outbox event",
				zap.String("event_id", eventData.eventID),
				zap.Error(err))
			continue
		}

		logger.Info("Event saved to outbox",
			zap.String("event_id", eventData.eventID),
			zap.String("event_type", eventData.eventType),
			zap.String("topic", eventData.topic))
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		logger.Error("Failed to commit transaction", zap.Error(err))
		return
	}

	logger.Info("All events saved to outbox successfully")
}
