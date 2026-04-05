package outbox

import (
	"database/sql"

	"github.com/overtonx/outbox/v3/serializer"
)

// Outbox is the central entry point for the outbox pattern.
// It holds the shared configuration and provides factory methods
// for EventStore and Dispatcher.
type Outbox struct {
	db         *sql.DB
	serializer serializer.Serializer
}

// New creates an Outbox with the given database connection and Serializer.
// Use serializer.JSONSerializer for JSON payloads or implement the
// serializer.Serializer interface for other formats (e.g. protobuf, Avro).
func New(db *sql.DB, s serializer.Serializer) *Outbox {
	return &Outbox{
		db:         db,
		serializer: s,
	}
}

// EventStore returns an EventStore that uses the Outbox serializer.
// Use EventStore.Save inside application transactions to write events.
func (o *Outbox) EventStore() *EventStore {
	return NewEventStore(o.serializer)
}

// Dispatcher creates and returns a new Dispatcher with the given options.
// The Dispatcher reads events from the DB and publishes them to Kafka.
// It does not use the Serializer — content_type is read from each DB row.
func (o *Outbox) Dispatcher(opts ...DispatcherOption) (*Dispatcher, error) {
	return NewDispatcher(o.db, opts...)
}
