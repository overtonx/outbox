package outbox

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOutboxEvent(t *testing.T) {
	payload := struct {
		Name string `json:"name"`
	}{
		Name: "test name",
	}

	event, err := NewOutboxEvent("1", "test", "event", "2", "topic", payload, map[string]string{})
	require.NoError(t, err)

	jsonRaw, err := json.Marshal(event.Payload)
	require.NoError(t, err)
	require.Equal(t, string(jsonRaw), `{"name":"test name"}`)

}
