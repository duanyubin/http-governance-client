package prometheus

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	httpclient "github.com/duanyubin/http-governance-client"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

func TestNewRejectsNilRegisterer(t *testing.T) {
	observer, err := New(nil)
	if err == nil {
		t.Fatal("New(nil) returned no error")
	}
	if observer != nil {
		t.Fatalf("New(nil) observer = %#v, want nil", observer)
	}
}

func TestNewReturnsDuplicateRegistrationError(t *testing.T) {
	registry := stdprometheus.NewRegistry()
	if _, err := New(registry); err != nil {
		t.Fatalf("first New() error = %v", err)
	}

	observer, err := New(registry)
	if err == nil {
		t.Fatal("second New() returned no error")
	}
	if observer != nil {
		t.Fatalf("second New() observer = %#v, want nil", observer)
	}

	var duplicate stdprometheus.AlreadyRegisteredError
	if !errors.As(err, &duplicate) {
		t.Fatalf("second New() error = %T %v, want AlreadyRegisteredError", err, err)
	}
}

func TestObserveRequestResultRecordsSuccessfulRetry(t *testing.T) {
	registry := stdprometheus.NewRegistry()
	observer, err := New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	event := httpclient.RequestResultEvent{
		Caller:         "shop",
		Downstream:     "billing",
		Operation:      "queryBalance",
		Method:         "POST",
		Class:          httpclient.RequestClassInternalRead,
		URL:            "http://billing/private?token=secret",
		FinalError:     "must not become a label",
		RetryCount:     2,
		Retried:        true,
		RetrySucceeded: true,
		Duration:       1500 * time.Millisecond,
	}
	observer.ObserveRequestResult(context.Background(), event)

	assertCounterValue(t, registry, namespace+"_requests_total", 1)
	assertCounterValue(t, registry, namespace+"_retried_requests_total", 1)
	assertCounterValue(t, registry, namespace+"_retries_total", 2)
	assertCounterValue(t, registry, namespace+"_retry_succeeded_requests_total", 1)
	assertCounterValue(t, registry, namespace+"_request_failures_total", 0)
	assertCounterValue(t, registry, namespace+"_request_timeouts_total", 0)

	count, sum := histogramValue(t, registry, namespace+"_request_duration_seconds")
	if count != 1 || sum != 1.5 {
		t.Fatalf("request duration histogram = (count %d, sum %v), want (1, 1.5)", count, sum)
	}

	wantLabels := map[string]string{
		"caller":     "shop",
		"class":      "internal_read",
		"downstream": "billing",
		"method":     "POST",
		"operation":  "queryBalance",
	}
	if got := metricLabelsFor(t, registry, namespace+"_requests_total"); !reflect.DeepEqual(got, wantLabels) {
		t.Fatalf("request labels = %v, want %v", got, wantLabels)
	}
}

func TestObserveRequestResultRecordsFailureAndTimeout(t *testing.T) {
	registry := stdprometheus.NewRegistry()
	observer, err := New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	event := httpclient.RequestResultEvent{
		Caller:      "shop",
		Downstream:  "billing",
		Operation:   "queryBalance",
		Method:      "POST",
		Class:       httpclient.RequestClassInternalRead,
		FinalFailed: true,
		TimedOut:    true,
		Duration:    2 * time.Second,
	}
	observer.ObserveRequestResult(context.Background(), event)

	assertCounterValue(t, registry, namespace+"_requests_total", 1)
	assertCounterValue(t, registry, namespace+"_request_failures_total", 1)
	assertCounterValue(t, registry, namespace+"_request_timeouts_total", 1)
	assertCounterValue(t, registry, namespace+"_retried_requests_total", 0)
	assertCounterValue(t, registry, namespace+"_retries_total", 0)
	assertCounterValue(t, registry, namespace+"_retry_succeeded_requests_total", 0)
}

func TestObserveRequestResultIsConcurrentSafe(t *testing.T) {
	registry := stdprometheus.NewRegistry()
	observer, err := New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	event := httpclient.RequestResultEvent{
		Caller:     "shop",
		Downstream: "billing",
		Operation:  "queryBalance",
		Method:     "GET",
		Class:      httpclient.RequestClassInternalRead,
		RetryCount: 1,
		Retried:    true,
		Duration:   time.Second,
	}
	const count = 100
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			observer.ObserveRequestResult(context.Background(), event)
		}()
	}
	wg.Wait()

	assertCounterValue(t, registry, namespace+"_requests_total", count)
	assertCounterValue(t, registry, namespace+"_retries_total", count)
}

func assertCounterValue(t *testing.T, registry *stdprometheus.Registry, name string, want float64) {
	t.Helper()
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range metricFamilies {
		if family.GetName() == name {
			got := family.GetMetric()[0].GetCounter().GetValue()
			if got != want {
				t.Fatalf("counter value = %v, want %v", got, want)
			}
			return
		}
	}
	t.Fatalf("metric %q not found", name)
}

func histogramValue(t *testing.T, registry *stdprometheus.Registry, name string) (uint64, float64) {
	t.Helper()
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range metricFamilies {
		if family.GetName() == name {
			histogram := family.GetMetric()[0].GetHistogram()
			return histogram.GetSampleCount(), histogram.GetSampleSum()
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0, 0
}

func metricLabelsFor(t *testing.T, registry *stdprometheus.Registry, name string) map[string]string {
	t.Helper()
	metricFamilies, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range metricFamilies {
		if family.GetName() != name {
			continue
		}
		labels := family.GetMetric()[0].GetLabel()
		values := make(map[string]string, len(labels))
		for _, label := range labels {
			values[label.GetName()] = label.GetValue()
		}
		return values
	}
	t.Fatalf("metric %q not found", name)
	return nil
}
