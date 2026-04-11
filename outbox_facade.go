package outbox

import (
	"database/sql"

	"github.com/overtonx/outbox/v3/serializer"
)

// Outbox — центральная точка входа для паттерна outbox.
// Хранит общую конфигурацию и предоставляет фабричные методы
// для EventStore и Dispatcher.
type Outbox struct {
	db         *sql.DB
	serializer serializer.Serializer
}

// New создаёт Outbox с указанным подключением к базе данных и Serializer.
// Используйте serializer.JSONSerializer для JSON-нагрузок или реализуйте
// интерфейс serializer.Serializer для других форматов (например, protobuf, Avro).
func New(db *sql.DB, s serializer.Serializer) *Outbox {
	return &Outbox{
		db:         db,
		serializer: s,
	}
}

// EventStore возвращает EventStore, использующий сериализатор и db из Outbox.
// SaveCtx может использоваться для получения исполнителя из контекста через go-transaction-manager.
func (o *Outbox) EventStore() *EventStore {
	return NewEventStoreWithDB(o.db, o.serializer)
}

// Dispatcher создаёт и возвращает новый Dispatcher с указанными опциями.
// Dispatcher читает события из БД и публикует их в Kafka.
// Serializer не используется — content_type читается из каждой строки БД.
func (o *Outbox) Dispatcher(opts ...DispatcherOption) (*Dispatcher, error) {
	return NewDispatcher(o.db, opts...)
}
