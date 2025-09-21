package outbox

import (
	"database/sql"

	"go.uber.org/zap"
)

// Carrier holds the shared dependencies for the outbox services.
// It acts as a dependency injection container for the various processors.
type Carrier struct {
	db        *sql.DB
	publisher Publisher
	metrics   MetricsCollector
	logger    *zap.Logger
}

// NewCarrier creates a new Carrier with the given options.
// The carrier is responsible for holding shared resources like the database connection,
// publisher, logger, and metrics collector.
func NewCarrier(db *sql.DB, opts ...CarrierOption) (*Carrier, error) {
	c := &Carrier{
		db:      db,
		logger:  zap.NewNop(),
		metrics: NewNopMetricsCollector(),
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.publisher == nil {
		c.publisher = NewNopPublisher()
	}

	return c, nil
}
