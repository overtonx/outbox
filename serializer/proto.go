package serializer

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ProtoSerializer сериализует полезные нагрузки в бинарный формат protobuf.
// Полезная нагрузка, передаваемая в EventStore.Save, должна реализовывать proto.Message.
type ProtoSerializer struct{}

func (ProtoSerializer) Marshal(v interface{}) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("proto serializer: expected proto.Message, got %T", v)
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("proto serializer: %w", err)
	}
	return b, nil
}

func (ProtoSerializer) ContentType() string { return ContentTypeProtobuf }
