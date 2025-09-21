package outbox

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// NopMetricsCollector is a metrics collector that does nothing.
// It is used as a default when no other collector is provided.
type NopMetricsCollector struct{}

// NewNopMetricsCollector creates a new NopMetricsCollector.
func NewNopMetricsCollector() *NopMetricsCollector {
	return &NopMetricsCollector{}
}

// IncrementCounter implements the MetricsCollector interface.
func (m *NopMetricsCollector) IncrementCounter(name string, tags map[string]string) {}

// RecordDuration implements the MetricsCollector interface.
func (m *NopMetricsCollector) RecordDuration(name string, duration time.Duration, tags map[string]string) {
}

// RecordGauge implements the MetricsCollector interface.
func (m *NopMetricsCollector) RecordGauge(name string, value float64, tags map[string]string) {}

// OpenTelemetryMetricsCollector is a metrics collector that uses the OpenTelemetry SDK.
type OpenTelemetryMetricsCollector struct {
	meter      metric.Meter
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Float64UpDownCounter
}

// NewOpenTelemetryMetricsCollector creates a new OpenTelemetryMetricsCollector with the default meter.
func NewOpenTelemetryMetricsCollector() *OpenTelemetryMetricsCollector {
	return NewOpenTelemetryMetricsCollectorWithMeter(otel.Meter("outbox"))
}

// NewOpenTelemetryMetricsCollectorWithMeter creates a new OpenTelemetryMetricsCollector with a specific meter.
func NewOpenTelemetryMetricsCollectorWithMeter(meter metric.Meter) *OpenTelemetryMetricsCollector {
	return &OpenTelemetryMetricsCollector{
		meter:      meter,
		counters:   make(map[string]metric.Int64Counter),
		histograms: make(map[string]metric.Float64Histogram),
		gauges:     make(map[string]metric.Float64UpDownCounter),
	}
}

// IncrementCounter implements the MetricsCollector interface using OpenTelemetry.
func (m *OpenTelemetryMetricsCollector) IncrementCounter(name string, tags map[string]string) {
	counter, err := m.getOrCreateCounter(name)
	if err != nil {
		return // Ignore errors for simplicity
	}
	attrs := m.convertTagsToAttributes(tags)
	counter.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

// RecordDuration implements the MetricsCollector interface using OpenTelemetry.
func (m *OpenTelemetryMetricsCollector) RecordDuration(name string, duration time.Duration, tags map[string]string) {
	histogram, err := m.getOrCreateHistogram(name)
	if err != nil {
		return // Ignore errors for simplicity
	}
	attrs := m.convertTagsToAttributes(tags)
	histogram.Record(context.Background(), duration.Seconds(), metric.WithAttributes(attrs...))
}

// RecordGauge implements the MetricsCollector interface using OpenTelemetry.
func (m *OpenTelemetryMetricsCollector) RecordGauge(name string, value float64, tags map[string]string) {
	// Note: OpenTelemetry gauges are tricky. An UpDownCounter is more like a delta.
	// For a true gauge, you would use an async gauge, which is more complex.
	// This implementation is a simplification.
	gauge, err := m.getOrCreateGauge(name)
	if err != nil {
		return // Ignore errors for simplicity
	}
	attrs := m.convertTagsToAttributes(tags)
	gauge.Add(context.Background(), value, metric.WithAttributes(attrs...))
}

func (m *OpenTelemetryMetricsCollector) getOrCreateCounter(name string) (metric.Int64Counter, error) {
	if counter, exists := m.counters[name]; exists {
		return counter, nil
	}
	counter, err := m.meter.Int64Counter(name)
	if err != nil {
		return nil, err
	}
	m.counters[name] = counter
	return counter, nil
}

func (m *OpenTelemetryMetricsCollector) getOrCreateHistogram(name string) (metric.Float64Histogram, error) {
	if histogram, exists := m.histograms[name]; exists {
		return histogram, nil
	}
	histogram, err := m.meter.Float64Histogram(name, metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	m.histograms[name] = histogram
	return histogram, nil
}

func (m *OpenTelemetryMetricsCollector) getOrCreateGauge(name string) (metric.Float64UpDownCounter, error) {
	if gauge, exists := m.gauges[name]; exists {
		return gauge, nil
	}
	gauge, err := m.meter.Float64UpDownCounter(name)
	if err != nil {
		return nil, err
	}
	m.gauges[name] = gauge
	return gauge, nil
}

func (m *OpenTelemetryMetricsCollector) convertTagsToAttributes(tags map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for key, value := range tags {
		attrs = append(attrs, attribute.String(key, value))
	}
	return attrs
}
