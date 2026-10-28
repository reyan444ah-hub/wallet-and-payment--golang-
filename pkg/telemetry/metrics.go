package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	HttpRequestsTotal    *prometheus.CounterVec
	HttpRequestDuration  *prometheus.HistogramVec
	TransfersTotal       *prometheus.CounterVec
	SagaDurationSeconds  *prometheus.HistogramVec
	OutboxLagGauge       prometheus.Gauge
	OutboxRetriesTotal   prometheus.Counter
	DLQMessagesTotal     prometheus.Counter
	RateLimitRejections  *prometheus.CounterVec
	ReconciliationErrors prometheus.Counter
}

func InitMetrics(namespace string) *Metrics {
	m := &Metrics{
		HttpRequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "http_requests_total",
				Help:      "Total number of HTTP requests processed",
			},
			[]string{"method", "path", "status"},
		),
		HttpRequestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "http_request_duration_seconds",
				Help:      "HTTP request latency distribution",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"method", "path"},
		),
		TransfersTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "transfers_total",
				Help:      "Total number of wallet transfers by status",
			},
			[]string{"currency", "status"},
		),
		SagaDurationSeconds: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "saga_duration_seconds",
				Help:      "Latency of saga execution from reservation to completion",
				Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			[]string{"status"},
		),
		OutboxLagGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "outbox_lag_count",
				Help:      "Current count of pending events in transactional outbox",
			},
		),
		OutboxRetriesTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "outbox_retries_total",
				Help:      "Total number of outbox publish retries",
			},
		),
		DLQMessagesTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dlq_messages_total",
				Help:      "Total number of messages moved to dead letter queue",
			},
		),
		RateLimitRejections: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rate_limit_rejections_total",
				Help:      "Total rate limit rejections",
			},
			[]string{"identifier_type"},
		),
		ReconciliationErrors: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "reconciliation_discrepancies_total",
				Help:      "Total ledger or balance discrepancies identified by reconciliation job",
			},
		),
	}

	return m
}
