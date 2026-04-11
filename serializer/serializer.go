// Пакет serializer предоставляет интерфейс Serializer и встроенные реализации
// для кодирования полезных нагрузок событий outbox перед их сохранением в базе данных.
package serializer

const (
	ContentTypeJSON     = "application/json"
	ContentTypeProtobuf = "application/protobuf"
)

// Serializer сериализует полезные нагрузки событий перед их сохранением в таблицу outbox.
// Реализуйте этот интерфейс для поддержки пользовательских форматов сериализации (Avro, MessagePack и др.).
type Serializer interface {
	Marshal(v interface{}) ([]byte, error)
	ContentType() string
}
