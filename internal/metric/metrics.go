package metric

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type NoOpMetricsCollector struct{}

func NewNoOpMetricsCollector() *NoOpMetricsCollector {
	return &NoOpMetricsCollector{}
}

func (m *NoOpMetricsCollector) IncrementCounter(name string, tags map[string]string) {}

func (m *NoOpMetricsCollector) RecordDuration(name string, duration time.Duration, tags map[string]string) {
}

func (m *NoOpMetricsCollector) RecordGauge(name string, value float64, tags map[string]string) {}

type PrometheusMetricsCollector struct {
}

func NewPrometheusMetricsCollector() *PrometheusMetricsCollector {
	return &PrometheusMetricsCollector{}
}

func (m *PrometheusMetricsCollector) IncrementCounter(name string, tags map[string]string) {
}

func (m *PrometheusMetricsCollector) RecordDuration(name string, duration time.Duration, tags map[string]string) {
}

func (m *PrometheusMetricsCollector) RecordGauge(name string, value float64, tags map[string]string) {
}

type OpenTelemetryMetricsCollector struct {
	meter metric.Meter

	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Float64UpDownCounter
}

func NewOpenTelemetryMetricsCollector() *OpenTelemetryMetricsCollector {
	meter := otel.Meter("outbox")

	return &OpenTelemetryMetricsCollector{
		meter:      meter,
		counters:   make(map[string]metric.Int64Counter),
		histograms: make(map[string]metric.Float64Histogram),
		gauges:     make(map[string]metric.Float64UpDownCounter),
	}
}

func NewOpenTelemetryMetricsCollectorWithMeter(meter metric.Meter) *OpenTelemetryMetricsCollector {
	return &OpenTelemetryMetricsCollector{
		meter:      meter,
		counters:   make(map[string]metric.Int64Counter),
		histograms: make(map[string]metric.Float64Histogram),
		gauges:     make(map[string]metric.Float64UpDownCounter),
	}
}

func (m *OpenTelemetryMetricsCollector) IncrementCounter(name string, tags map[string]string) {
	counter, err := m.getOrCreateCounter(name)
	if err != nil {
		return // Игнорируем ошибки для простоты
	}

	attrs := m.convertTagsToAttributes(tags)
	counter.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

func (m *OpenTelemetryMetricsCollector) RecordDuration(name string, duration time.Duration, tags map[string]string) {
	histogram, err := m.getOrCreateHistogram(name)
	if err != nil {
		return // Игнорируем ошибки для простоты
	}

	attrs := m.convertTagsToAttributes(tags)
	histogram.Record(context.Background(), float64(duration.Nanoseconds())/1e9, metric.WithAttributes(attrs...))
}

func (m *OpenTelemetryMetricsCollector) RecordGauge(name string, value float64, tags map[string]string) {
	gauge, err := m.getOrCreateGauge(name)
	if err != nil {
		return // Игнорируем ошибки для простоты
	}

	attrs := m.convertTagsToAttributes(tags)
	gauge.Add(context.Background(), value, metric.WithAttributes(attrs...))
}

func (m *OpenTelemetryMetricsCollector) getOrCreateCounter(name string) (metric.Int64Counter, error) {
	if counter, exists := m.counters[name]; exists {
		return counter, nil
	}

	counter, err := m.meter.Int64Counter(
		name,
		metric.WithDescription("Counter for "+name),
		metric.WithUnit("1"),
	)
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

	histogram, err := m.meter.Float64Histogram(
		name,
		metric.WithDescription("Histogram for "+name),
		metric.WithUnit("s"),
	)
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

	gauge, err := m.meter.Float64UpDownCounter(
		name,
		metric.WithDescription("Gauge for "+name),
		metric.WithUnit("1"),
	)
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
