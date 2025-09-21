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

func TestValidateOutboxEvent(t *testing.T) {
	tests := []struct {
		name    string
		event   Event
		wantErr bool
	}{
		{
			name: "valid event",
			event: Event{
				AggregateType: "test",
				AggregateID:   "1",
				Topic:         "topic",
			},
			wantErr: false,
		},
		{
			name: "missing aggregate_type",
			event: Event{
				AggregateID: "1",
				Topic:       "topic",
			},
			wantErr: true,
		},
		{
			name: "missing aggregate_id",
			event: Event{
				AggregateType: "test",
				Topic:         "topic",
			},
			wantErr: true,
		},
		{
			name: "missing topic",
			event: Event{
				AggregateType: "test",
				AggregateID:   "1",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOutboxEvent(tt.event)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
