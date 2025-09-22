package processor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/backoff"
	"github.com/overtonx/outbox/v2/embedded"
)

// MockPublisher is a mock implementation of Publisher interface
type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Publish(ctx context.Context, event embedded.EventRecord) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *MockPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockMetricsCollector is a mock implementation of MetricsCollector interface
type MockMetricsCollector struct {
	mock.Mock
}

func (m *MockMetricsCollector) IncrementCounter(name string, tags map[string]string) {
	m.Called(name, tags)
}

func (m *MockMetricsCollector) RecordDuration(name string, duration time.Duration, tags map[string]string) {
	m.Called(name, duration, tags)
}

func (m *MockMetricsCollector) RecordGauge(name string, value float64, tags map[string]string) {
	m.Called(name, value, tags)
}

func TestEventProcessorImpl_validateEvent(t *testing.T) {
	logger := zap.NewNop()
	backoff := backoff.DefaultBackoffStrategy()
	maxAttempts := 3
	batchSize := 10
	mockPublisher := &MockPublisher{}
	mockMetrics := &MockMetricsCollector{}

	processor := NewEventProcessor(nil, logger, backoff, maxAttempts, batchSize, mockPublisher, mockMetrics)

	tests := []struct {
		name    string
		event   embedded.EventRecord
		wantErr bool
	}{
		{
			name: "valid event",
			event: embedded.EventRecord{
				ID:          1,
				EventType:   "test",
				AggregateID: "1",
				Topic:       "topic",
				Payload:     []byte("payload"),
			},
			wantErr: false,
		},
		{
			name: "invalid ID",
			event: embedded.EventRecord{
				ID:          0,
				EventType:   "test",
				AggregateID: "1",
				Topic:       "topic",
				Payload:     []byte("payload"),
			},
			wantErr: true,
		},
		{
			name: "empty event type",
			event: embedded.EventRecord{
				ID:          1,
				AggregateID: "1",
				Topic:       "topic",
				Payload:     []byte("payload"),
			},
			wantErr: true,
		},
		{
			name: "empty aggregate ID",
			event: embedded.EventRecord{
				ID:        1,
				EventType: "test",
				Topic:     "topic",
				Payload:   []byte("payload"),
			},
			wantErr: true,
		},
		{
			name: "empty topic",
			event: embedded.EventRecord{
				ID:          1,
				EventType:   "test",
				AggregateID: "1",
				Payload:     []byte("payload"),
			},
			wantErr: true,
		},
		{
			name: "empty payload",
			event: embedded.EventRecord{
				ID:          1,
				EventType:   "test",
				AggregateID: "1",
				Topic:       "topic",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := processor.validateEvent(tt.event)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEventProcessorImpl_ProcessEvents_NoEvents(t *testing.T) {
	// This test would require mocking the DB, which is complex
	// For now, skip as it requires integration setup
	t.Skip("Requires DB mocking")
}
