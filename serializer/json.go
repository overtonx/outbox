package serializer

import (
	"encoding/json"
	"fmt"
)

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
