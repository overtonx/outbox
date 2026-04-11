package outbox

import "go.opentelemetry.io/otel/propagation"

var _ propagation.TextMapCarrier = &MessageCarrier{}

// MessageCarrier реализует propagation.TextMapCarrier.
// Используется для извлечения трассировочного контекста из заголовков сообщения.
type MessageCarrier struct {
	event *Event
}

// NewMessageCarrier создаёт новый MessageCarrier.
func NewMessageCarrier(event *Event) *MessageCarrier {
	return &MessageCarrier{event: event}
}

// Get возвращает значение, связанное с указанным ключом.
func (mc *MessageCarrier) Get(key string) string {
	for k, v := range mc.event.Headers {
		if k == key {
			return v
		}
	}
	return ""
}

// Set устанавливает значение для указанного ключа.
func (mc *MessageCarrier) Set(key string, value string) {
	if mc.event.Headers == nil {
		mc.event.Headers = make(map[string]string)
	}

	mc.event.Headers[key] = value
}

// Keys возвращает срез всех ключей носителя.
func (mc *MessageCarrier) Keys() []string {
	keys := make([]string, 0, len(mc.event.Headers))
	for k, _ := range mc.event.Headers {
		keys = append(keys, k)
	}
	return keys
}
