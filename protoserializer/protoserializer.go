// Package protoserializer provides a protobuf implementation of the outbox.Serializer interface.
// Import this package to send protobuf-encoded payloads through the outbox.
//
//	ob := outbox.New(db, protoserializer.ProtoSerializer{})
package protoserializer

import (
	"fmt"

	outbox "github.com/overtonx/outbox/v3"
	"google.golang.org/protobuf/proto"
)

// ProtoSerializer implements outbox.Serializer using protobuf binary encoding.
// The payload passed to EventStore.Save must implement proto.Message.
type ProtoSerializer struct{}

// Marshal encodes v as a protobuf binary message.
// Returns an error if v does not implement proto.Message.
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

// ContentType returns the MIME type for protobuf payloads.
func (ProtoSerializer) ContentType() string { return outbox.ContentTypeProtobuf }
