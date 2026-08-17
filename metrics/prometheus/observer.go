// Package prometheus exposes HTTP governance client metrics to Prometheus.
package prometheus

import (
	"context"
	"errors"
	"fmt"

	httpclient "github.com/duanyubin/http-governance-client"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

const namespace = "http_governance_client"

var metricLabels = []string{"caller", "downstream", "operation", "method", "class"}

// Observer records HTTP governance request results as Prometheus metrics.
type Observer struct {
	requests               *stdprometheus.CounterVec
	retriedRequests        *stdprometheus.CounterVec
	retries                *stdprometheus.CounterVec
	retrySucceededRequests *stdprometheus.CounterVec
	requestFailures        *stdprometheus.CounterVec
	requestTimeouts        *stdprometheus.CounterVec
	requestDuration        *stdprometheus.HistogramVec
}

var _ stdprometheus.Collector = (*Observer)(nil)
var _ httpclient.RetryResultObserver = (*Observer)(nil)

// New creates and registers an Observer with registerer.
func New(registerer stdprometheus.Registerer) (*Observer, error) {
	if registerer == nil {
		return nil, errors.New("Prometheus registerer is nil")
	}

	observer := &Observer{
		requests: stdprometheus.NewCounterVec(stdprometheus.CounterOpts{
			Namespace: namespace,
			Name:      "requests_total",
			Help:      "Total number of completed HTTP governance client requests.",
		}, metricLabels),
		retriedRequests: stdprometheus.NewCounterVec(stdprometheus.CounterOpts{
			Namespace: namespace,
			Name:      "retried_requests_total",
			Help:      "Total number of completed HTTP governance client requests that were retried.",
		}, metricLabels),
		retries: stdprometheus.NewCounterVec(stdprometheus.CounterOpts{
			Namespace: namespace,
			Name:      "retries_total",
			Help:      "Total number of HTTP governance client retry attempts.",
		}, metricLabels),
		retrySucceededRequests: stdprometheus.NewCounterVec(stdprometheus.CounterOpts{
			Namespace: namespace,
			Name:      "retry_succeeded_requests_total",
			Help:      "Total number of HTTP governance client requests that succeeded after retrying.",
		}, metricLabels),
		requestFailures: stdprometheus.NewCounterVec(stdprometheus.CounterOpts{
			Namespace: namespace,
			Name:      "request_failures_total",
			Help:      "Total number of HTTP governance client requests that ended in failure.",
		}, metricLabels),
		requestTimeouts: stdprometheus.NewCounterVec(stdprometheus.CounterOpts{
			Namespace: namespace,
			Name:      "request_timeouts_total",
			Help:      "Total number of HTTP governance client requests that ended because of a timeout.",
		}, metricLabels),
		requestDuration: stdprometheus.NewHistogramVec(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "request_duration_seconds",
			Help:      "Duration of completed HTTP governance client requests in seconds.",
			Buckets:   stdprometheus.DefBuckets,
		}, metricLabels),
	}

	if err := registerer.Register(observer); err != nil {
		return nil, fmt.Errorf("register HTTP governance Prometheus metrics: %w", err)
	}
	return observer, nil
}

// Describe implements prometheus.Collector.
func (o *Observer) Describe(ch chan<- *stdprometheus.Desc) {
	o.requests.Describe(ch)
	o.retriedRequests.Describe(ch)
	o.retries.Describe(ch)
	o.retrySucceededRequests.Describe(ch)
	o.requestFailures.Describe(ch)
	o.requestTimeouts.Describe(ch)
	o.requestDuration.Describe(ch)
}

// Collect implements prometheus.Collector.
func (o *Observer) Collect(ch chan<- stdprometheus.Metric) {
	o.requests.Collect(ch)
	o.retriedRequests.Collect(ch)
	o.retries.Collect(ch)
	o.retrySucceededRequests.Collect(ch)
	o.requestFailures.Collect(ch)
	o.requestTimeouts.Collect(ch)
	o.requestDuration.Collect(ch)
}

// ObserveRequestResult records the final result of one logical request.
func (o *Observer) ObserveRequestResult(_ context.Context, event httpclient.RequestResultEvent) {
	labels := []string{event.Caller, event.Downstream, event.Operation, event.Method, string(event.Class)}
	requests := o.requests.WithLabelValues(labels...)
	retriedRequests := o.retriedRequests.WithLabelValues(labels...)
	retries := o.retries.WithLabelValues(labels...)
	retrySucceededRequests := o.retrySucceededRequests.WithLabelValues(labels...)
	requestFailures := o.requestFailures.WithLabelValues(labels...)
	requestTimeouts := o.requestTimeouts.WithLabelValues(labels...)

	requests.Inc()
	if event.Retried {
		retriedRequests.Inc()
	}
	if event.RetryCount > 0 {
		retries.Add(float64(event.RetryCount))
	}
	if event.RetrySucceeded {
		retrySucceededRequests.Inc()
	}
	if event.FinalFailed {
		requestFailures.Inc()
	}
	if event.TimedOut {
		requestTimeouts.Inc()
	}
	o.requestDuration.WithLabelValues(labels...).Observe(event.Duration.Seconds())
}
