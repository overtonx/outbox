package main

import (
	"fmt"
	"time"
)

// Config содержит конфигурацию для примера outbox
type Config struct {
	Database DatabaseConfig
	Kafka    KafkaConfig
	Outbox   OutboxConfig
}

// DatabaseConfig содержит настройки базы данных
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// KafkaConfig содержит настройки Kafka
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

// OutboxConfig содержит настройки outbox dispatcher
type OutboxConfig struct {
	BatchSize               int
	PollInterval            time.Duration
	MaxAttempts             int
	DeadLetterInterval      time.Duration
	StuckEventTimeout       time.Duration
	StuckEventCheckInterval time.Duration
	DeadLetterRetention     time.Duration
	SentEventsRetention     time.Duration
	CleanupInterval         time.Duration
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() Config {
	return Config{
		Database: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "outbox_user",
			Password: "outbox_pass",
			Database: "outbox_db",
		},
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "outbox-events",
		},
		Outbox: OutboxConfig{
			BatchSize:               10,
			PollInterval:            2 * time.Second,
			MaxAttempts:             3,
			DeadLetterInterval:      5 * time.Minute,
			StuckEventTimeout:       10 * time.Minute,
			StuckEventCheckInterval: 2 * time.Minute,
			DeadLetterRetention:     7 * 24 * time.Hour,
			SentEventsRetention:     24 * time.Hour,
			CleanupInterval:         1 * time.Hour,
		},
	}
}

// GetDSN возвращает строку подключения к базе данных
func (d DatabaseConfig) GetDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		d.User, d.Password, d.Host, d.Port, d.Database)
}
