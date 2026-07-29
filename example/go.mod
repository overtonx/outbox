module outbox-example

go 1.25.0

replace github.com/overtonx/outbox/v4 => ../

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/overtonx/outbox/v4 v4.0.0
	go.uber.org/zap v1.27.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/avito-tech/go-transaction-manager/drivers/sql/v2 v2.0.2 // indirect
	github.com/avito-tech/go-transaction-manager/trm/v2 v2.0.2 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/confluentinc/confluent-kafka-go/v2 v2.13.3 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
