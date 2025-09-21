package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2"
)

const (
	dbDSN = "root:password@tcp(localhost:3306)/outbox_db_v2?parseTime=true"
)

// In a real application, this would be handled by a proper migration tool.
const createEventsTableQuery = `
CREATE TABLE IF NOT EXISTS outbox_events (
	id              BIGINT AUTO_INCREMENT PRIMARY KEY,
	event_id        CHAR(36)     NOT NULL UNIQUE,
	event_type      VARCHAR(255) NOT NULL,
	aggregate_type  VARCHAR(255) NOT NULL,
	aggregate_id    VARCHAR(255) NOT NULL,
	status          INT          NOT NULL DEFAULT 0 COMMENT '0 - new, 1 - success, 2 - retry, 3 - error, 4 - processing',
	topic           VARCHAR(255) NOT NULL,
	payload         JSON         NOT NULL,
	headers         JSON         NULL,
	attempt_count   INT          NOT NULL DEFAULT 0,
	next_attempt_at TIMESTAMP    NULL,
	last_error      TEXT         NULL,
	created_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	updated_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	INDEX idx_status_next_attempt (status, next_attempt_at),
	INDEX idx_created_at (created_at)
) ENGINE=InnoDB;
`

const createDeadlettersTableQuery = `
CREATE TABLE IF NOT EXISTS outbox_deadletters (
	id              BIGINT PRIMARY KEY,
	event_id        CHAR(36)      NOT NULL UNIQUE,
	event_type      VARCHAR(255)  NOT NULL,
	aggregate_type  VARCHAR(255)  NOT NULL,
	aggregate_id    VARCHAR(255)  NOT NULL,
	topic           VARCHAR(255)  NOT NULL,
	payload         JSON          NOT NULL,
	headers         JSON          NULL,
	attempt_count   INT           NOT NULL,
	last_error      VARCHAR(2000) NULL,
	created_at      TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
`

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		logger.Fatal("Failed to ping database", zap.Error(err))
	}

	// Setup: Ensure tables exist and are clean for the example.
	if err := ensureTables(context.Background(), db); err != nil {
		logger.Fatal("Failed to ensure tables", zap.Error(err))
	}
	if err := truncateTables(context.Background(), db); err != nil {
		logger.Warn("Failed to truncate tables", zap.Error(err))
	}

	// 1. Create the Publisher
	publisher, err := outbox.NewKafkaPublisher(logger) // Uses default localhost config
	if err != nil {
		logger.Fatal("Failed to create Kafka publisher", zap.Error(err))
	}
	defer publisher.Close()

	// 2. Create the Carrier with shared dependencies
	carrier, err := outbox.NewCarrier(db,
		outbox.WithLogger(logger),
		outbox.WithPublisher(publisher),
		// outbox.WithMetrics(your_metrics_collector),
	)
	if err != nil {
		logger.Fatal("Failed to create carrier", zap.Error(err))
	}

	// 3. Create the Workers, wrapping the Carrier's methods.
	// Each worker is configured with its own interval and service-specific options.
	workers := []outbox.Worker{
		outbox.NewBaseWorker("event_processor", 2*time.Second, logger, func(ctx context.Context) error {
			return carrier.ProcessEvents(ctx,
				outbox.WithEventProcessorBatchSize(10),
				outbox.WithEventProcessorMaxAttempts(5),
			)
		}),
		outbox.NewBaseWorker("stuck_event_processor", 30*time.Second, logger, func(ctx context.Context) error {
			return carrier.RecoverStuckEvents(ctx,
				outbox.WithStuckEventServiceStuckTimeout(1*time.Minute),
			)
		}),
		outbox.NewBaseWorker("deadletter_processor", 1*time.Minute, logger, func(ctx context.Context) error {
			return carrier.MoveToDeadLetters(ctx,
				outbox.WithDeadLetterServiceBatchSize(10),
			)
		}),
		outbox.NewBaseWorker("cleanup_processor", 5*time.Minute, logger, func(ctx context.Context) error {
			return carrier.Cleanup(ctx,
				outbox.WithCleanupServiceSentRetention(24*time.Hour),
				outbox.WithCleanupServiceDeadLetterRetention(7*24*time.Hour),
			)
		}),
	}

	// 4. Create the Dispatcher with the workers.
	dispatcher := outbox.NewDispatcher(logger, workers...)

	// Start the dispatcher and handle graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go dispatcher.Start(ctx)

	// Give the dispatcher a moment to start up before creating events.
	time.Sleep(1 * time.Second)
	logger.Info("Dispatcher started, creating sample events...")
	go createSampleEvents(context.Background(), db, logger)

	// Wait for the shutdown signal.
	<-ctx.Done()

	logger.Info("Shutdown signal received. Stopping dispatcher...")
	dispatcher.Stop() // This will block until all workers are stopped.
	logger.Info("Dispatcher stopped gracefully.")
}

func createSampleEvents(ctx context.Context, db *sql.DB, logger *zap.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				logger.Error("Failed to begin transaction", zap.Error(err))
				continue
			}

			event, _ := outbox.NewOutboxEvent(
				uuid.New().String(),
				"UserCreated",
				"User",
				fmt.Sprintf("user-%d", time.Now().Unix()),
				"user-events",
				map[string]interface{}{"email": "test@example.com", "ts": time.Now().UTC()},
				map[string]string{"source": "example-app"},
			)

			if err := outbox.SaveEvent(ctx, tx, event); err != nil {
				logger.Error("Failed to save outbox event", zap.Error(err))
				tx.Rollback()
				continue
			}

			if err := tx.Commit(); err != nil {
				logger.Error("Failed to commit transaction", zap.Error(err))
				continue
			}

			logger.Info("Successfully created and saved a sample event", zap.String("event_id", event.EventID))
		}
	}
}

func ensureTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, createEventsTableQuery); err != nil {
		return fmt.Errorf("failed to create outbox_events table: %w", err)
	}
	if _, err := db.ExecContext(ctx, createDeadlettersTableQuery); err != nil {
		return fmt.Errorf("failed to create outbox_deadletters table: %w", err)
	}
	log.Println("Tables 'outbox_events' and 'outbox_deadletters' are ready.")
	return nil
}

func truncateTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE outbox_events"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE outbox_deadletters"); err != nil {
		return err
	}
	log.Println("Tables truncated for a clean run.")
	return nil
}
