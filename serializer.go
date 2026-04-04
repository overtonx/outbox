package outbox

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
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

// ProtoSerializer serializes payloads using protobuf binary encoding.
// The payload passed to EventStore.Save must implement proto.Message.
type ProtoSerializer struct{}

func (ProtoSerializer) Marshal(v interface{}) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("protoserializer: expected proto.Message, got %T", v)
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("protoserializer: %w", err)
	}
	return b, nil
}

func (ProtoSerializer) ContentType() string { return ContentTypeProtobuf }
