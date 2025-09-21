package outbox

import "go.opentelemetry.io/otel/propagation"

var _ propagation.TextMapCarrier = &MessageCarrier{}

// MessageCarrier implements propagation.TextMapCarrier.
// It is used to extract trace context from a message's headers.
type MessageCarrier struct {
	event *Event
}

// NewMessageCarrier creates a new MessageCarrier.
func NewMessageCarrier(event *Event) *MessageCarrier {
	return &MessageCarrier{event: event}
}

// Get returns the value associated with the given key.
func (mc *MessageCarrier) Get(key string) string {
	for k, v := range mc.event.Headers {
		if k == key {
			return v
		}
	}
	return ""
}

// Set sets the value associated with the given key.
func (mc *MessageCarrier) Set(key string, value string) {
	mc.event.Headers[key] = value
}

// Keys returns a slice of all keys in the carrier.
func (mc *MessageCarrier) Keys() []string {
	keys := make([]string, 0, len(mc.event.Headers))
	for k, _ := range mc.event.Headers {
		keys = append(keys, k)
	}
	return keys
}
