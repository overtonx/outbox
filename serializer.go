package outbox

import (
	"encoding/json"
	"fmt"
)

const (
	ContentTypeJSON     = "application/json"
	ContentTypeProtobuf = "application/protobuf"
)

// Serializer marshals event payloads before storing them in the outbox table.
// Implement this interface to support custom serialization formats.
type Serializer interface {
	Marshal(v interface{}) ([]byte, error)
	ContentType() string
}

// JSONSerializer serializes payloads as JSON.
type JSONSerializer struct{}

func (JSONSerializer) Marshal(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json serializer: %w", err)
	}
	return b, nil
}

func (JSONSerializer) ContentType() string { return ContentTypeJSON }
