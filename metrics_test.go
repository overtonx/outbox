package outbox

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestOpenTelemetryMetricsCollector_ConcurrentAccess verifies that concurrent
// calls to IncrementCounter, RecordDuration, and RecordGauge do not cause
// data races on the internal maps. Run with: go test -race ./...
func TestOpenTelemetryMetricsCollector_ConcurrentAccess(t *testing.T) {
	collector := NewOpenTelemetryMetricsCollector()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			collector.IncrementCounter("concurrent.counter", map[string]string{"worker": "a"})
		}()
		go func() {
			defer wg.Done()
			collector.RecordDuration("concurrent.duration", time.Millisecond, nil)
		}()
		go func() {
			defer wg.Done()
			collector.RecordGauge("concurrent.gauge", 1.0, nil)
		}()
	}

	wg.Wait()
}

// TestOpenTelemetryMetricsCollector_SameMetricCreatedOnce verifies that
// getOrCreate* with double-checked locking returns the same instrument
// when called concurrently with the same name.
func TestOpenTelemetryMetricsCollector_SameMetricCreatedOnce(t *testing.T) {
	collector := NewOpenTelemetryMetricsCollector()

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			collector.IncrementCounter("shared.counter", nil)
		}()
	}
	wg.Wait()

	collector.mu.RLock()
	defer collector.mu.RUnlock()
	assert.Len(t, collector.counters, 1, "only one counter instrument should be created")
}

func TestNoOpMetricsCollector_DoesNotPanic(t *testing.T) {
	m := NewNoOpMetricsCollector()
	assert.NotPanics(t, func() {
		m.IncrementCounter("x", map[string]string{"k": "v"})
		m.RecordDuration("x", time.Second, nil)
		m.RecordGauge("x", 42.0, nil)
	})
}
