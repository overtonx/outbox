// Package serializer provides the Serializer interface and built-in implementations
// for encoding outbox event payloads before storing them in the database.
package serializer

const (
	ContentTypeJSON     = "application/json"
	ContentTypeProtobuf = "application/protobuf"
)

// Serializer marshals event payloads before storing them in the outbox table.
// Implement this interface to support custom serialization formats (Avro, MessagePack, etc.).
type Serializer interface {
	Marshal(v interface{}) ([]byte, error)
	ContentType() string
}
