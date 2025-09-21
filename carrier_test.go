package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageCarrier_Get(t *testing.T) {
	event := &Event{
		Headers: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}
	carrier := NewMessageCarrier(event)

	assert.Equal(t, "value1", carrier.Get("key1"))
	assert.Equal(t, "value2", carrier.Get("key2"))
	assert.Equal(t, "", carrier.Get("nonexistent"))
}

func TestMessageCarrier_Set(t *testing.T) {
	event := &Event{
		Headers: map[string]string{},
	}
	carrier := NewMessageCarrier(event)

	carrier.Set("key1", "value1")
	carrier.Set("key2", "value2")

	assert.Equal(t, "value1", event.Headers["key1"])
	assert.Equal(t, "value2", event.Headers["key2"])
}

func TestMessageCarrier_Keys(t *testing.T) {
	event := &Event{
		Headers: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}
	carrier := NewMessageCarrier(event)

	keys := carrier.Keys()
	assert.Len(t, keys, 2)
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
}
