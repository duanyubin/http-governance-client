package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type testRetryObserver struct {
	retryEvents  []RetryEvent
	resultEvents []RequestResultEvent
}

type testResultObserver struct {
	resultEvents []RequestResultEvent
}

func (o *testResultObserver) ObserveRequestResult(_ context.Context, event RequestResultEvent) {
	o.resultEvents = append(o.resultEvents, event)
}

func TestRetryDelayNeverExceedsMaxBackoff(t *testing.T) {
	policy := RetryPolicy{
		InitialBackoff: 75 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}
	for i := 0; i < 100; i++ {
		if delay := retryDelay(policy, 0, nil); delay > policy.MaxBackoff {
			t.Fatalf("retryDelay() = %v, want <= %v", delay, policy.MaxBackoff)
		}
	}
}

func TestRetryDelayUsesExponentialBackoffRange(t *testing.T) {
	policy := RetryPolicy{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
	}
	tests := []struct {
		attempt int
		min     time.Duration
		max     time.Duration
	}{
		{attempt: 0, min: 100 * time.Millisecond, max: 200 * time.Millisecond},
		{attempt: 1, min: 200 * time.Millisecond, max: 400 * time.Millisecond},
		{attempt: 2, min: 400 * time.Millisecond, max: 800 * time.Millisecond},
	}
	for _, tt := range tests {
		delay := retryDelay(policy, tt.attempt, nil)
		if delay < tt.min || delay >= tt.max {
			t.Errorf("retryDelay(attempt %d) = %v, want [%v, %v)", tt.attempt, delay, tt.min, tt.max)
		}
	}
}

func TestRetryDelayDoesNotOverflow(t *testing.T) {
	maxBackoff := time.Duration(math.MaxInt64)
	policy := RetryPolicy{
		InitialBackoff: maxBackoff/2 + 1,
		MaxBackoff:     maxBackoff,
	}
	if delay := retryDelay(policy, 1, nil); delay != maxBackoff {
		t.Fatalf("retryDelay() = %v, want %v", delay, maxBackoff)
	}
}

func TestParseRetryAfterRejectsDurationOverflow(t *testing.T) {
	value := strconv.FormatInt(math.MaxInt64, 10)
	if delay, ok := parseRetryAfter(value); ok {
		t.Fatalf("parseRetryAfter(%q) = (%v, true), want invalid", value, delay)
	}
}

func (o *testRetryObserver) ObserveRetry(_ context.Context, event RetryEvent) {
	o.retryEvents = append(o.retryEvents, event)
}

func (o *testRetryObserver) ObserveRequestResult(_ context.Context, event RequestResultEvent) {
	o.resultEvents = append(o.resultEvents, event)
}

type sleepingRetryObserver struct {
	delay time.Duration
}

func (o sleepingRetryObserver) ObserveRetry(_ context.Context, _ RetryEvent) {
	time.Sleep(o.delay)
}

type timeoutNetworkError struct{}

func (timeoutNetworkError) Error() string   { return "network timeout" }
func (timeoutNetworkError) Timeout() bool   { return true }
func (timeoutNetworkError) Temporary() bool { return true }

func TestOverallDeadlineBoundsRetryWaitAfterObserver(t *testing.T) {
	var attempts atomic.Int32
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts.Add(1)
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusServiceUnavailable,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
		ResolvePolicy: func(_ *stdhttp.Request) RetryPolicy {
			return RetryPolicy{
				MaxRetries:        1,
				PerAttemptTimeout: time.Second,
				MaxElapsedTime:    80 * time.Millisecond,
				InitialBackoff:    40 * time.Millisecond,
				MaxBackoff:        40 * time.Millisecond,
				RetryOnStatuses:   map[int]struct{}{stdhttp.StatusServiceUnavailable: {}},
			}
		},
		Observer: sleepingRetryObserver{delay: 60 * time.Millisecond},
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	start := time.Now()
	resp, err := transport.RoundTrip(req)
	elapsed := time.Since(start)
	if resp != nil {
		_ = resp.Body.Close()
		t.Fatalf("RoundTrip() response = %#v, want nil", resp)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip() error = %v, want context deadline exceeded", err)
	}
	if elapsed >= 95*time.Millisecond {
		t.Fatalf("RoundTrip() elapsed = %v, want overall deadline to bound retry wait", elapsed)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestDedicatedResultObserverDoesNotRequireRetryObserver(t *testing.T) {
	observer := &testResultObserver{}
	transport := NewTransportWithOptions(ClientOptions{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
		ResultObserver: observer,
	})
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	_ = resp.Body.Close()
	if got := len(observer.resultEvents); got != 1 {
		t.Fatalf("result events = %d, want 1", got)
	}
}

func TestGetRetriesReadRequest(t *testing.T) {
	var attempts atomic.Int32
	var retryAttempts []string
	var noMoreRetry []string

	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		current := attempts.Add(1)
		retryAttempts = append(retryAttempts, r.Header.Get(HeaderRetryAttempt))
		noMoreRetry = append(noMoreRetry, r.Header.Get(HeaderNoMoreRetry))
		w.Header().Set("Content-Type", "application/json")
		if current == 1 {
			w.WriteHeader(stdhttp.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"message":"retry"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "ok"})
	}))
	defer server.Close()

	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        time.Millisecond,
		RetryOnStatuses: map[int]struct{}{
			stdhttp.StatusServiceUnavailable: {},
		},
	})

	var resp struct {
		Message string `json:"message"`
	}
	if err := Get(ctx, server.URL, &resp); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if resp.Message != "ok" {
		t.Fatalf("resp.Message = %q, want ok", resp.Message)
	}
	if len(retryAttempts) != 2 || retryAttempts[0] != "0" || retryAttempts[1] != "1" {
		t.Fatalf("retryAttempts = %v, want [0 1]", retryAttempts)
	}
	if len(noMoreRetry) != 2 || noMoreRetry[0] != "false" || noMoreRetry[1] != "true" {
		t.Fatalf("noMoreRetry = %v, want [false true]", noMoreRetry)
	}
}

func TestPostDoesNotRetryWriteRequestByDefault(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(stdhttp.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message":"retry"}`))
	}))
	defer server.Close()

	ctx := context.Background()
	var resp struct {
		Message string `json:"message"`
	}
	if err := Post(ctx, server.URL, "application/json", map[string]string{"name": "test"}, &resp); err != nil {
		t.Fatalf("Post() error = %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestDynamicPolicyCanEnableIdempotentWriteRetry(t *testing.T) {
	var attempts atomic.Int32
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			status := stdhttp.StatusOK
			if attempts.Add(1) == 1 {
				status = stdhttp.StatusServiceUnavailable
			}
			return &stdhttp.Response{
				StatusCode: status,
				Header:     make(stdhttp.Header),
				Body:       io.NopCloser(strings.NewReader("response")),
				Request:    req,
			}, nil
		}),
		ConfigProvider: RetryConfigProviderFunc(func(_ context.Context, _ RetryConfigScope, base RetryPolicy) RetryPolicy {
			maxRetries := 1
			backoff := time.Millisecond
			return RetryPolicyPatch{
				MaxRetries:      &maxRetries,
				InitialBackoff:  &backoff,
				MaxBackoff:      &backoff,
				RetryOnStatuses: []int{stdhttp.StatusServiceUnavailable},
			}.Apply(base)
		}),
	}

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/test", strings.NewReader(`{"id":"request-id"}`))
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestNegativeProgrammaticMaxRetriesStillPerformsInitialAttempt(t *testing.T) {
	var attempts atomic.Int32
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts.Add(1)
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusServiceUnavailable,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	}
	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        -1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        time.Millisecond,
		RetryOnStatuses: map[int]struct{}{
			stdhttp.StatusServiceUnavailable: {},
		},
	})
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if resp == nil {
		t.Fatal("RoundTrip() response = nil, want initial attempt response")
	}
	defer resp.Body.Close()
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestIndependentClientUsesItsManagedProviderForRequestClassification(t *testing.T) {
	ClearRequestClassResolver()
	t.Cleanup(ClearRequestClassResolver)

	provider, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
internal_hosts: ["*.internal.example.com"]
policies:
  internal_read:
    max_retries: 0
  external_read:
    max_retries: 1
    initial_backoff: 1ms
    max_backoff: 1ms
    retry_on_statuses: [503]
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}

	var attempts atomic.Int32
	client := NewClientWithOptions(ClientOptions{
		ConfigProvider: provider,
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			status := stdhttp.StatusOK
			if attempts.Add(1) == 1 {
				status = stdhttp.StatusServiceUnavailable
			}
			return &stdhttp.Response{
				StatusCode: status,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	})

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://api.example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do() error = %v", err)
	}
	defer resp.Body.Close()

	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestIndependentClientUsesCustomProviderClassForBaselinePolicy(t *testing.T) {
	ClearRequestClassResolver()
	t.Cleanup(ClearRequestClassResolver)

	var attempts atomic.Int32
	provider := &fixedRequestClassProvider{class: RequestClassExternalWrite}
	client := NewClientWithOptions(ClientOptions{
		ConfigProvider: provider,
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts.Add(1)
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusServiceUnavailable,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	})

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://api.example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do() error = %v", err)
	}
	defer resp.Body.Close()

	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 for write-class baseline", got)
	}
}

func TestCustomProviderReceivesDefaultScopeBeforePolicyResolution(t *testing.T) {
	SetCallerServiceName("orders-api")
	t.Cleanup(clearCallerServiceName)

	var captured RetryConfigScope
	provider := RetryConfigProviderFunc(func(_ context.Context, scope RetryConfigScope, base RetryPolicy) RetryPolicy {
		captured = scope
		return base
	})
	client := NewClientWithOptions(ClientOptions{
		ConfigProvider: provider,
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	})

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://payments.example.com/v1/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do() error = %v", err)
	}
	defer resp.Body.Close()

	if captured.Caller != "orders-api" {
		t.Fatalf("scope.Caller = %q, want orders-api", captured.Caller)
	}
	if captured.Downstream != "payments" {
		t.Fatalf("scope.Downstream = %q, want payments", captured.Downstream)
	}
	if captured.Operation != "GET /v1/orders" {
		t.Fatalf("scope.Operation = %q, want GET /v1/orders", captured.Operation)
	}
}

func TestDynamicRetryConfigProviderOverridesDefaultPolicy(t *testing.T) {
	oldTransport := cli.Transport
	cli.Transport = NewTransport()
	t.Cleanup(func() {
		cli.Transport = oldTransport
	})

	SetRetryConfigProvider(RetryConfigProviderFunc(func(ctx context.Context, scope RetryConfigScope, base RetryPolicy) RetryPolicy {
		if scope.Caller == "shop" && scope.Downstream == "billing" && scope.Operation == "queryOrder" {
			disable := true
			return (RetryPolicyPatch{
				DisableRetry: &disable,
			}).Apply(base)
		}
		return base
	}))

	var attempts atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(stdhttp.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message":"retry"}`))
	}))
	defer server.Close()

	ctx := WithRetryConfigScope(context.Background(), RetryConfigScope{
		Caller:     "shop",
		Downstream: "billing",
		Operation:  "queryOrder",
	})
	var resp struct {
		Message string `json:"message"`
	}
	if err := Get(ctx, server.URL, &resp); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestRetryPolicyPatchOverridesStatusSet(t *testing.T) {
	oldTransport := cli.Transport
	cli.Transport = NewTransport()
	t.Cleanup(func() {
		cli.Transport = oldTransport
	})

	SetRetryConfigProvider(RetryConfigProviderFunc(func(ctx context.Context, scope RetryConfigScope, base RetryPolicy) RetryPolicy {
		if scope.Operation == "readProfile" {
			maxRetries := 1
			return (RetryPolicyPatch{
				MaxRetries:      &maxRetries,
				RetryOnStatuses: []int{stdhttp.StatusBadRequest},
			}).Apply(base)
		}
		return base
	}))

	var attempts atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		current := attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if current == 1 {
			w.WriteHeader(stdhttp.StatusBadRequest)
			_, _ = w.Write([]byte(`{"message":"retry"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "ok"})
	}))
	defer server.Close()

	ctx := WithRetryConfigScope(context.Background(), RetryConfigScope{
		Operation: "readProfile",
	})
	var resp struct {
		Message string `json:"message"`
	}
	if err := Get(ctx, server.URL, &resp); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if resp.Message != "ok" {
		t.Fatalf("resp.Message = %q, want ok", resp.Message)
	}
}

func TestNoMoreRetryHeaderDisablesRetry(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(stdhttp.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message":"retry"}`))
	}))
	defer server.Close()

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set(HeaderNoMoreRetry, "true")
	req = req.WithContext(WithRetryPolicy(req.Context(), RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        time.Millisecond,
		RetryOnStatuses: map[int]struct{}{
			stdhttp.StatusServiceUnavailable: {},
		},
	}))

	var resp struct {
		Message string `json:"message"`
	}
	if err := Do(req, &resp); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestNoMoreRetryContextOverridesResolvedPolicy(t *testing.T) {
	var attempts atomic.Int32
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts.Add(1)
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusServiceUnavailable,
				Header:     make(stdhttp.Header),
				Body:       io.NopCloser(strings.NewReader("retry")),
				Request:    req,
			}, nil
		}),
		ConfigProvider: RetryConfigProviderFunc(func(_ context.Context, _ RetryConfigScope, base RetryPolicy) RetryPolicy {
			base.MaxRetries = 2
			base.InitialBackoff = time.Millisecond
			base.MaxBackoff = time.Millisecond
			base.RetryOnStatuses = map[int]struct{}{stdhttp.StatusServiceUnavailable: {}}
			return base
		}),
	}

	ctx := WithNoMoreRetry(context.Background())
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://example.com/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestResolveOverallDeadlineAppliesMaxElapsedTimeAsHardUpperBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://example.com/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	start := time.Now()
	deadline, ok := resolveOverallDeadline(req, RetryPolicy{MaxElapsedTime: 50 * time.Millisecond})
	if !ok {
		t.Fatal("resolveOverallDeadline() has no deadline")
	}
	if latest := start.Add(100 * time.Millisecond); deadline.After(latest) {
		t.Fatalf("deadline = %v, want no later than %v", deadline, latest)
	}
}

func TestRoundTripReturnsDeadlineExceededWhenHeaderDeadlineAlreadyExpired(t *testing.T) {
	var attempts atomic.Int32
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts.Add(1)
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       io.NopCloser(strings.NewReader("ok")),
				Request:    req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set(HeaderRequestDeadline, strconv.FormatInt(time.Now().Add(-time.Second).UnixMilli(), 10))

	resp, err := transport.RoundTrip(req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip() error = %v, want context deadline exceeded", err)
	}
	if resp != nil {
		t.Fatalf("RoundTrip() response = %v, want nil", resp)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("attempts = %d, want 0", got)
	}
}

func TestRoundTripCancelsInFlightAttemptAtHeaderDeadline(t *testing.T) {
	var attempts atomic.Int32
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts.Add(1)
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set(HeaderRequestDeadline, strconv.FormatInt(time.Now().Add(50*time.Millisecond).UnixMilli(), 10))

	start := time.Now()
	resp, err := transport.RoundTrip(req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip() error = %v, want context deadline exceeded", err)
	}
	if resp != nil {
		t.Fatalf("RoundTrip() response = %v, want nil", resp)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("RoundTrip() elapsed = %v, want header deadline to stop request promptly", elapsed)
	}
}

func TestRoundTripSkipsRetryWhenBackoffWouldExceedHeaderDeadline(t *testing.T) {
	var attempts atomic.Int32
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts.Add(1)
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusServiceUnavailable,
				Header:     make(stdhttp.Header),
				Body:       io.NopCloser(strings.NewReader("unavailable")),
				Request:    req,
			}, nil
		}),
	}
	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        100 * time.Millisecond,
		RetryOnStatuses:   map[int]struct{}{stdhttp.StatusServiceUnavailable: {}},
	})
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set(HeaderRequestDeadline, strconv.FormatInt(time.Now().Add(30*time.Millisecond).UnixMilli(), 10))

	start := time.Now()
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v, want final HTTP response", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusServiceUnavailable {
		t.Fatalf("response status = %d, want 503", resp.StatusCode)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("RoundTrip() elapsed = %v, want no retry backoff wait", elapsed)
	}
}

func TestRoundTripSkipsRetryWhenRetryAfterWouldExceedHeaderDeadline(t *testing.T) {
	var attempts atomic.Int32
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts.Add(1)
			header := make(stdhttp.Header)
			header.Set("Retry-After", "1")
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusServiceUnavailable,
				Header:     header,
				Body:       io.NopCloser(strings.NewReader("unavailable")),
				Request:    req,
			}, nil
		}),
	}
	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        time.Millisecond,
		RetryOnStatuses:   map[int]struct{}{stdhttp.StatusServiceUnavailable: {}},
	})
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set(HeaderRequestDeadline, strconv.FormatInt(time.Now().Add(30*time.Millisecond).UnixMilli(), 10))

	start := time.Now()
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v, want final HTTP response", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != stdhttp.StatusServiceUnavailable {
		t.Fatalf("response status = %d, want 503", resp.StatusCode)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("RoundTrip() elapsed = %v, want no Retry-After wait", elapsed)
	}
}

func TestRetryAttemptAndReasonStartFreshForEachDownstreamCall(t *testing.T) {
	var attempts []string
	var reasons []string
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			attempts = append(attempts, req.Header.Get(HeaderRetryAttempt))
			reasons = append(reasons, req.Header.Get(HeaderRetryReason))
			status := stdhttp.StatusOK
			if len(attempts) == 1 {
				status = stdhttp.StatusServiceUnavailable
			}
			return &stdhttp.Response{
				StatusCode: status,
				Header:     make(stdhttp.Header),
				Body:       io.NopCloser(strings.NewReader("response")),
				Request:    req,
			}, nil
		}),
	}

	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        time.Millisecond,
		RetryOnStatuses:   map[int]struct{}{stdhttp.StatusServiceUnavailable: {}},
	})
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://example.com/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set(HeaderRetryAttempt, "7")
	req.Header.Set(HeaderRetryReason, "inherited")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if got, want := strings.Join(attempts, ","), "0,1"; got != want {
		t.Fatalf("retry attempts = %q, want %q", got, want)
	}
	if got, want := strings.Join(reasons, ","), ",5xx"; got != want {
		t.Fatalf("retry reasons = %q, want %q", got, want)
	}
}

func TestRetryOn429CanBeDisabled(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(stdhttp.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"message":"limited"}`))
	}))
	defer server.Close()

	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        time.Millisecond,
		RetryOnStatuses: map[int]struct{}{
			stdhttp.StatusTooManyRequests: {},
		},
		RetryOn429: false,
	})

	var resp struct {
		Message string `json:"message"`
	}
	if err := Get(ctx, server.URL, &resp); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"deadline", context.DeadlineExceeded, true},
		{"canceled", context.Canceled, false},
		{"connection refused", &net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: syscall.ECONNREFUSED,
		}, true},
		{"connection reset", &net.OpError{
			Op:  "read",
			Net: "tcp",
			Err: syscall.ECONNRESET,
		}, true},
		{"broken pipe", &net.OpError{
			Op:  "write",
			Net: "tcp",
			Err: syscall.EPIPE,
		}, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"temporary DNS", &net.DNSError{
			Err:         "temporary failure",
			Name:        "member.internal",
			IsTemporary: true,
		}, true},
		{"permanent DNS", &net.DNSError{
			Err:  "no such host",
			Name: "member.invalid",
		}, false},
		{"certificate", &tls.CertificateVerificationError{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableError(tt.err); got != tt.want {
				t.Fatalf("isRetryableError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test429RequiresStatusAndFlag(t *testing.T) {
	policy := RetryPolicy{
		MaxRetries:      1,
		RetryOnStatuses: map[int]struct{}{},
		RetryOn429:      true,
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://api/query", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	resp := &stdhttp.Response{StatusCode: stdhttp.StatusTooManyRequests}

	if shouldRetry(req, policy, 0, 1, resp, nil, time.Time{}, false) {
		t.Fatal("429 retried without membership in RetryOnStatuses")
	}
}

func TestNormalizeRetryPolicyPreservesExplicitEmptyStatusSet(t *testing.T) {
	policy := normalizeRetryPolicy(RetryPolicy{
		RetryOnStatuses: map[int]struct{}{},
	})
	if len(policy.RetryOnStatuses) != 0 {
		t.Fatalf("RetryOnStatuses = %v, want empty", policy.RetryOnStatuses)
	}
}

func TestMaxElapsedTimeStopsRetryBeforeSecondAttempt(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(stdhttp.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message":"retry"}`))
	}))
	defer server.Close()

	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        2,
		PerAttemptTimeout: time.Second,
		MaxElapsedTime:    20 * time.Millisecond,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        50 * time.Millisecond,
		RetryOnStatuses: map[int]struct{}{
			stdhttp.StatusServiceUnavailable: {},
		},
		RetryOn429: true,
	})

	var resp struct {
		Message string `json:"message"`
	}
	if err := Get(ctx, server.URL, &resp); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestObserverEmitsRetrySuccessResult(t *testing.T) {
	oldTransport := cli.Transport
	cli.Transport = NewTransport()
	t.Cleanup(func() {
		cli.Transport = oldTransport
	})

	observer := &testRetryObserver{}
	SetRetryObserver(observer)

	var attempts atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		current := attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if current == 1 {
			w.WriteHeader(stdhttp.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"message":"retry"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "ok"})
	}))
	defer server.Close()

	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        time.Millisecond,
		RetryOnStatuses: map[int]struct{}{
			stdhttp.StatusServiceUnavailable: {},
		},
	})
	ctx = WithRetryConfigScope(ctx, RetryConfigScope{
		Caller:     "shop",
		Downstream: "billing",
		Operation:  "queryOrder",
	})

	var resp struct {
		Message string `json:"message"`
	}
	if err := Get(ctx, server.URL, &resp); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if len(observer.retryEvents) != 1 {
		t.Fatalf("retry events = %d, want 1", len(observer.retryEvents))
	}
	if len(observer.resultEvents) != 1 {
		t.Fatalf("result events = %d, want 1", len(observer.resultEvents))
	}
	event := observer.resultEvents[0]
	if !event.Retried {
		t.Fatalf("Retried = false, want true")
	}
	if !event.RetrySucceeded {
		t.Fatalf("RetrySucceeded = false, want true")
	}
	if event.FinalFailed {
		t.Fatalf("FinalFailed = true, want false")
	}
	if event.AttemptCount != 2 {
		t.Fatalf("AttemptCount = %d, want 2", event.AttemptCount)
	}
	if event.RetryCount != 1 {
		t.Fatalf("RetryCount = %d, want 1", event.RetryCount)
	}
	if event.StatusCode != stdhttp.StatusOK {
		t.Fatalf("StatusCode = %d, want 200", event.StatusCode)
	}
	if event.FinalReason != "success" {
		t.Fatalf("FinalReason = %q, want success", event.FinalReason)
	}
	if event.Caller != "shop" || event.Downstream != "billing" || event.Operation != "queryOrder" {
		t.Fatalf("scope = %+v, want shop/billing/queryOrder", event)
	}
}

func TestObserverEmitsFinalFailureOnTimeout(t *testing.T) {
	oldTransport := cli.Transport
	cli.Transport = NewTransport()
	t.Cleanup(func() {
		cli.Transport = oldTransport
	})

	observer := &testRetryObserver{}
	SetRetryObserver(observer)

	timeoutErr := errors.New("timeout")
	timeoutTransport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return nil, context.DeadlineExceeded
		}),
		ResolvePolicy: DefaultRetryPolicyResolver,
		Observer:      observer,
	}
	cli.Transport = timeoutTransport
	_ = timeoutErr

	ctx := WithRetryPolicy(context.Background(), RetryPolicy{
		MaxRetries:        0,
		PerAttemptTimeout: 5 * time.Millisecond,
	})

	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://member.staging.svc.example.com/member/profile", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	if err := Do(req, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() error = %v, want context deadline exceeded", err)
	}
	if len(observer.resultEvents) != 1 {
		t.Fatalf("result events = %d, want 1", len(observer.resultEvents))
	}
	event := observer.resultEvents[0]
	if event.FinalReason != "timeout" {
		t.Fatalf("FinalReason = %q, want timeout", event.FinalReason)
	}
	if !event.FinalFailed {
		t.Fatalf("FinalFailed = false, want true")
	}
	if !event.TimedOut {
		t.Fatalf("TimedOut = false, want true")
	}
	if event.RetrySucceeded {
		t.Fatalf("RetrySucceeded = true, want false")
	}
	if event.StatusCode != 0 {
		t.Fatalf("StatusCode = %d, want 0", event.StatusCode)
	}
}

func TestObserverClassifiesNetworkTimeout(t *testing.T) {
	observer := &testRetryObserver{}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(_ *stdhttp.Request) (*stdhttp.Response, error) {
			return nil, timeoutNetworkError{}
		}),
		ResolvePolicy: func(_ *stdhttp.Request) RetryPolicy {
			return RetryPolicy{MaxRetries: 0}
		},
		Observer: observer,
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://payments.example.com/v1/orders?trace_id=123", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	if _, err := transport.RoundTrip(req); err == nil {
		t.Fatal("RoundTrip() error = nil, want timeout error")
	}
	if got := len(observer.resultEvents); got != 1 {
		t.Fatalf("result events = %d, want 1", got)
	}
	event := observer.resultEvents[0]
	if event.FinalReason != "timeout" {
		t.Fatalf("FinalReason = %q, want timeout", event.FinalReason)
	}
	if !event.TimedOut {
		t.Fatal("TimedOut = false, want true")
	}
}

func TestObserverMarksGatewayTimeoutResponseAsTimedOut(t *testing.T) {
	observer := &testRetryObserver{}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusGatewayTimeout,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
		ResolvePolicy: func(_ *stdhttp.Request) RetryPolicy {
			return RetryPolicy{MaxRetries: 0}
		},
		Observer: observer,
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://payments.example.com/v1/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	_ = resp.Body.Close()
	if got := len(observer.resultEvents); got != 1 {
		t.Fatalf("result events = %d, want 1", got)
	}
	if !observer.resultEvents[0].TimedOut {
		t.Fatal("TimedOut = false, want true for HTTP 504")
	}
}

func TestObserverFillsMetricDimensionsWithoutManagedProvider(t *testing.T) {
	oldCaller := CallerServiceName()
	SetCallerServiceName("orders-api")
	t.Cleanup(func() {
		SetCallerServiceName(oldCaller)
	})

	observer := &testRetryObserver{}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
		ResolvePolicy: DefaultRetryPolicyResolver,
		Observer:      observer,
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://payments.example.com/v1/orders?trace_id=123", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	_ = resp.Body.Close()
	if got := len(observer.resultEvents); got != 1 {
		t.Fatalf("result events = %d, want 1", got)
	}
	event := observer.resultEvents[0]
	if event.Caller != "orders-api" || event.Downstream != "payments" || event.Operation != "GET /v1/orders" {
		t.Fatalf("scope = %q/%q/%q, want orders-api/payments/GET /v1/orders", event.Caller, event.Downstream, event.Operation)
	}
}

func TestExtractBoundarySupportsQuotedValue(t *testing.T) {
	transport := &Transport{}
	got := transport.extractBoundary(`multipart/form-data; boundary="----abc123"; charset=utf-8`)
	if got != "----abc123" {
		t.Fatalf("extractBoundary() = %q, want %q", got, "----abc123")
	}
}

func TestRoundTripLeavesBodyUnreadWhenDebugDisabled(t *testing.T) {
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("ReadAll(req.Body) error = %v", err)
			}
			if string(body) != `{"name":"test"}` {
				t.Fatalf("request body = %q, want %q", string(body), `{"name":"test"}`)
			}
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     stdhttp.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
				Request:    req,
			}, nil
		}),
		ResolvePolicy: DefaultRetryPolicyResolver,
	}

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/api/test", strings.NewReader(`{"name":"test"}`))
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll(resp.Body) error = %v", err)
	}
	if string(body) != `{"ok":true}` {
		t.Fatalf("response body = %q, want %q", string(body), `{"ok":true}`)
	}
}

func TestSkippedRequestStillPropagatesChainConstraints(t *testing.T) {
	deadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ctx = WithNoMoreRetry(ctx)

	var gotHeader stdhttp.Header
	transport := &Transport{
		Skipper: func(*stdhttp.Request) bool { return true },
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			gotHeader = req.Header.Clone()
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       io.NopCloser(strings.NewReader("ok")),
				Request:    req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://example.com/healthcheck", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set(HeaderRetryAttempt, "7")
	req.Header.Set(HeaderRetryReason, "inherited")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if got := gotHeader.Get(HeaderNoMoreRetry); got != "true" {
		t.Fatalf("%s = %q, want true", HeaderNoMoreRetry, got)
	}
	if got := gotHeader.Get(HeaderRequestDeadline); got != formatRequestDeadline(deadline) {
		t.Fatalf("%s = %q, want %q", HeaderRequestDeadline, got, formatRequestDeadline(deadline))
	}
	if got := gotHeader.Get(HeaderRetryAttempt); got != "0" {
		t.Fatalf("%s = %q, want 0", HeaderRetryAttempt, got)
	}
	if got := gotHeader.Get(HeaderRetryReason); got != "" {
		t.Fatalf("%s = %q, want empty", HeaderRetryReason, got)
	}
}

func TestSkippedRequestEnforcesHeaderDeadlineInContext(t *testing.T) {
	headerDeadline := time.Now().Add(50 * time.Millisecond).Truncate(time.Millisecond)
	var gotDeadline time.Time
	transport := &Transport{
		Skipper: func(*stdhttp.Request) bool { return true },
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			var ok bool
			gotDeadline, ok = req.Context().Deadline()
			if !ok {
				return nil, errors.New("request context has no deadline")
			}
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}
	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodGet, "http://example.com/healthcheck", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Header.Set(HeaderRequestDeadline, formatRequestDeadline(headerDeadline))

	start := time.Now()
	_, err = transport.RoundTrip(req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip() error = %v, want context deadline exceeded", err)
	}
	if !gotDeadline.Equal(headerDeadline) {
		t.Fatalf("request context deadline = %v, want %v", gotDeadline, headerDeadline)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("RoundTrip() elapsed = %v, want header deadline to stop request promptly", elapsed)
	}
}

func TestRoundTripUsesAndClosesOriginalRequestBodyOnFirstAttempt(t *testing.T) {
	originalBody := &trackingReadCloser{reader: strings.NewReader("payload")}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			_, _ = io.ReadAll(req.Body)
			_ = req.Body.Close()
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/orders", originalBody)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("payload")), nil
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()
	if closed := originalBody.closes.Load(); closed != 1 {
		t.Fatalf("original request body closes = %d, want 1", closed)
	}
}

func TestRoundTripClosesRetryResponseWhenBackoffIsCanceled(t *testing.T) {
	retryBody := &trackingReadCloser{reader: strings.NewReader("retry")}
	responseReturned := make(chan struct{})
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			close(responseReturned)
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusServiceUnavailable,
				Header:     make(stdhttp.Header),
				Body:       retryBody,
				Request:    req,
			}, nil
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = WithRetryPolicy(ctx, RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    time.Second,
		MaxBackoff:        time.Second,
		RetryOnStatuses: map[int]struct{}{
			stdhttp.StatusServiceUnavailable: {},
		},
	})
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	result := make(chan struct {
		resp *stdhttp.Response
		err  error
	}, 1)
	go func() {
		resp, err := transport.RoundTrip(req)
		result <- struct {
			resp *stdhttp.Response
			err  error
		}{resp: resp, err: err}
	}()
	<-responseReturned
	cancel()
	got := <-result

	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("RoundTrip() error = %v, want context canceled", got.err)
	}
	if got.resp != nil {
		t.Fatalf("RoundTrip() response = %v, want nil after canceled retry backoff", got.resp)
	}
	if closed := retryBody.closes.Load(); closed != 1 {
		t.Fatalf("retry response body closes = %d, want 1", closed)
	}
	if !retryBody.sawEOF.Load() {
		t.Fatal("retry response body was not drained to EOF before close")
	}
}

func TestRoundTripLeavesResponseBodyUnreadWhenDebugDisabled(t *testing.T) {
	trackedBody := &trackingReadCloser{reader: strings.NewReader(`{"ok":true}`)}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode:    stdhttp.StatusOK,
				Header:        stdhttp.Header{"Content-Type": []string{"application/json"}},
				Body:          trackedBody,
				ContentLength: int64(len(`{"ok":true}`)),
				Request:       req,
			}, nil
		}),
		ResolvePolicy: DefaultRetryPolicyResolver,
	}

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodGet, "http://example.com/api/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if got := trackedBody.reads.Load(); got != 0 {
		t.Fatalf("response body reads after RoundTrip = %d, want 0", got)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll(resp.Body) error = %v", err)
	}
	if string(body) != `{"ok":true}` {
		t.Fatalf("response body = %q, want %q", string(body), `{"ok":true}`)
	}
	if got := trackedBody.reads.Load(); got == 0 {
		t.Fatal("response body should be read by caller, got 0 reads")
	}
}

func TestRoundTripSkipsLargeRequestBodyCaptureWhenDebugEnabled(t *testing.T) {
	oldLogger := slog.Default()
	oldBodyLogLevel := bodyLogLevelValue()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		SetBodyLogLevel(oldBodyLogLevel)
	})

	payload := strings.Repeat("a", int(defaultBodyLogLimit)+16)
	trackedBody := &trackingReadCloser{reader: strings.NewReader(payload)}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			if got := trackedBody.reads.Load(); got != 0 {
				t.Fatalf("request body reads before RoundTripper = %d, want 0", got)
			}
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("ReadAll(req.Body) error = %v", err)
			}
			if string(body) != payload {
				t.Fatalf("request body length = %d, want %d", len(body), len(payload))
			}
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     stdhttp.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
				Request:    req,
			}, nil
		}),
		ResolvePolicy: DefaultRetryPolicyResolver,
	}

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/api/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Body = trackedBody
	req.ContentLength = int64(len(payload))
	req.Header.Set("Content-Type", "application/json")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()
}

func TestRoundTripDoesNotPreReadUnknownLengthRequestBody(t *testing.T) {
	setupBodyLogTest(t)
	payload := `{"request":true}`
	trackedBody := &trackingReadCloser{reader: strings.NewReader(payload)}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			if got := trackedBody.reads.Load(); got != 0 {
				t.Fatalf("request body reads before RoundTripper = %d, want 0", got)
			}
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("ReadAll(req.Body) error = %v", err)
			}
			if string(body) != payload {
				t.Fatalf("request body = %q, want %q", body, payload)
			}
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/api/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Body = trackedBody
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()
}

func TestRoundTripLogsRequestBodyWithoutContentType(t *testing.T) {
	handler := setupBodyLogTest(t)
	payload := `{"request":true}`
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("ReadAll(req.Body) error = %v", err)
			}
			if string(body) != payload {
				t.Fatalf("request body = %q, want %q", body, payload)
			}
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/api/test", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	bodyLog, ok := handler.attrValue("HTTPClient Request", "Body")
	if !ok {
		t.Fatal("request body log not found")
	}
	if bodyLog != payload {
		t.Fatalf("request body log = %#v, want %q", bodyLog, payload)
	}
}

func TestRoundTripDoesNotPreReadEventStreamResponse(t *testing.T) {
	setupBodyLogTest(t)
	payload := "data: ready\n\n"
	trackedBody := &trackingReadCloser{reader: strings.NewReader(payload)}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode:    stdhttp.StatusOK,
				Header:        stdhttp.Header{"Content-Type": []string{"text/event-stream"}},
				Body:          trackedBody,
				ContentLength: -1,
				Request:       req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodGet, "http://example.com/events", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if got := trackedBody.reads.Load(); got != 0 {
		t.Fatalf("event stream reads before caller = %d, want 0", got)
	}
}

func TestRoundTripBoundsRequestBodyCaptureWhenContentLengthIsIncorrect(t *testing.T) {
	oldLogger := slog.Default()
	oldBodyLogLevel := bodyLogLevelValue()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		SetBodyLogLevel(oldBodyLogLevel)
	})

	payload := strings.Repeat("a", int(defaultBodyLogLimit)+128)
	trackedBody := &trackingReadCloser{reader: strings.NewReader(payload)}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			if got := trackedBody.bytesRead.Load(); got > defaultBodyLogLimit+1 {
				t.Fatalf("request body reads before RoundTripper = %d, want at most %d", got, defaultBodyLogLimit+1)
			}
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("ReadAll(req.Body) error = %v", err)
			}
			if string(body) != payload {
				t.Fatalf("request body length = %d, want %d", len(body), len(payload))
			}
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	}

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/api/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Body = trackedBody
	req.ContentLength = 1
	req.Header.Set("Content-Type", "application/json")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()
}

func TestRoundTripPreservesResponseBodyReadErrorWhenDebugEnabled(t *testing.T) {
	oldLogger := slog.Default()
	oldBodyLogLevel := bodyLogLevelValue()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		SetBodyLogLevel(oldBodyLogLevel)
	})

	bodyErr := context.DeadlineExceeded
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode:    stdhttp.StatusOK,
				Header:        stdhttp.Header{"Content-Type": []string{"application/json"}},
				Body:          &dataThenErrorReadCloser{data: []byte(`{"ok":`), err: bodyErr},
				ContentLength: int64(len(`{"ok":true}`)),
				Request:       req,
			}, nil
		}),
	}

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodGet, "http://example.com/api/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if !errors.Is(err, bodyErr) {
		t.Fatalf("ReadAll(resp.Body) error = %v, want %v", err, bodyErr)
	}
	if string(body) != `{"ok":` {
		t.Fatalf("response body = %q, want partial body", string(body))
	}
}

func TestRoundTripPreservesMultipartRequestBodyReadErrorWhenDebugEnabled(t *testing.T) {
	oldLogger := slog.Default()
	oldBodyLogLevel := bodyLogLevelValue()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		SetBodyLogLevel(oldBodyLogLevel)
	})

	bodyErr := io.ErrUnexpectedEOF
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			_, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	}

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/api/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Body = &dataThenErrorReadCloser{data: []byte("--test-boundary\r\n"), err: bodyErr}
	req.ContentLength = int64(len("--test-boundary\r\n"))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=test-boundary")

	resp, err := transport.RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if !errors.Is(err, bodyErr) {
		t.Fatalf("RoundTrip() error = %v, want %v", err, bodyErr)
	}
}

func TestRoundTripSkipsPartialMultipartBodyLog(t *testing.T) {
	handler := setupBodyLogTest(t)
	oldBodyLogLimit := bodyLogLimitValue()
	t.Cleanup(func() {
		if err := SetBodyLogLimit(oldBodyLogLimit); err != nil {
			t.Fatalf("restore body log limit: %v", err)
		}
	})
	if err := SetBodyLogLimit(16); err != nil {
		t.Fatalf("SetBodyLogLimit() error = %v", err)
	}

	payload := strings.Repeat("a", 17)
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("ReadAll(req.Body) error = %v", err)
			}
			if string(body) != payload {
				t.Fatalf("request body = %q, want %q", body, payload)
			}
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/upload", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.Body = io.NopCloser(strings.NewReader(payload))
	req.ContentLength = int64(len(payload))
	req.Header.Set("Content-Type", "multipart/form-data; boundary=test")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if _, ok := handler.attrValue("HTTPClient Request", "multipart/form-data"); ok {
		t.Fatal("partial multipart body must not be logged as complete form data")
	}
	if _, ok := handler.attrValue("HTTPClient Request", "Body"); !ok {
		t.Fatal("multipart body skip log not found")
	}
}

func TestRoundTripCanLogBodyAtInfoLevel(t *testing.T) {
	handler := &captureLogHandler{level: slog.LevelInfo}
	oldLogger := slog.Default()
	oldBodyLogLevel := bodyLogLevelValue()
	slog.SetDefault(slog.New(handler))
	SetBodyLogLevel(slog.LevelInfo)
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		SetBodyLogLevel(oldBodyLogLevel)
	})

	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode:    stdhttp.StatusOK,
				Header:        stdhttp.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(strings.NewReader(`{"ok":true}`)),
				ContentLength: int64(len(`{"ok":true}`)),
				Request:       req,
			}, nil
		}),
		ResolvePolicy: DefaultRetryPolicyResolver,
	}

	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/api/test", strings.NewReader(`{"name":"test"}`))
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.ContentLength = int64(len(`{"name":"test"}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if !handler.hasRecord("HTTPClient Request", "Body") {
		t.Fatal("request body log not found at info level")
	}
	if !handler.hasRecord("HTTPClient Response", "Body") {
		t.Fatal("response body log not found at info level")
	}
}

func TestRoundTripLogsResponseBodyWithUnknownOrZeroContentLength(t *testing.T) {
	for _, contentLength := range []int64{-1, 0} {
		t.Run(strconv.FormatInt(contentLength, 10), func(t *testing.T) {
			handler := setupBodyLogTest(t)
			bodyLog := roundTripResponseBodyLog(t, handler, `{"response":true}`, contentLength)
			logged, ok := bodyLog.(map[string]any)
			if !ok || logged["response"] != true {
				t.Fatalf("response body log = %#v, want decoded response body", bodyLog)
			}
		})
	}
}

func TestShouldCaptureBodyAllowsProtobuf(t *testing.T) {
	if !shouldCaptureBody("Application/X-Protobuf", 1) {
		t.Fatal("protobuf body should be captured")
	}
}

func TestRoundTripLogsNonObjectJSONBody(t *testing.T) {
	handler := setupBodyLogTest(t)
	payload := `[{"id":1}]`
	bodyLog := roundTripResponseBodyLog(t, handler, payload, int64(len(payload)))
	if logged, ok := bodyLog.([]any); !ok || len(logged) != 1 {
		t.Fatalf("response body log = %#v, want decoded JSON array", bodyLog)
	}
}

func TestRoundTripLogsInvalidJSONBodyAsText(t *testing.T) {
	handler := setupBodyLogTest(t)
	payload := `{"incomplete":`
	bodyLog := roundTripResponseBodyLog(t, handler, payload, int64(len(payload)))
	if bodyLog != payload {
		t.Fatalf("response body log = %#v, want %q", bodyLog, payload)
	}
}

func TestSetBodyLogLimitAllowsLargerBody(t *testing.T) {
	handler := setupBodyLogTest(t)
	oldBodyLogLimit := bodyLogLimitValue()
	t.Cleanup(func() {
		if err := SetBodyLogLimit(oldBodyLogLimit); err != nil {
			t.Fatalf("restore body log limit: %v", err)
		}
	})

	payload := `{"value":"` + strings.Repeat("a", int(defaultBodyLogLimit)) + `"}`
	if err := SetBodyLogLimit(int64(len(payload))); err != nil {
		t.Fatalf("SetBodyLogLimit() error = %v", err)
	}
	bodyLog := roundTripResponseBodyLog(t, handler, payload, int64(len(payload)))
	logged, ok := bodyLog.(map[string]any)
	if !ok || logged["value"] != strings.Repeat("a", int(defaultBodyLogLimit)) {
		t.Fatal("response body log does not contain the complete response")
	}
}

func TestSetBodyLogLimitRejectsNonPositiveValues(t *testing.T) {
	oldBodyLogLimit := bodyLogLimitValue()
	t.Cleanup(func() {
		if err := SetBodyLogLimit(oldBodyLogLimit); err != nil {
			t.Fatalf("restore body log limit: %v", err)
		}
	})

	for _, limit := range []int64{0, -1} {
		if err := SetBodyLogLimit(limit); err == nil {
			t.Fatalf("SetBodyLogLimit(%d) error = nil, want error", limit)
		}
		if got := bodyLogLimitValue(); got != oldBodyLogLimit {
			t.Fatalf("body log limit = %d after invalid update, want %d", got, oldBodyLogLimit)
		}
	}
}

func TestRoundTripLogsCompleteURLAndHeaders(t *testing.T) {
	handler := &captureLogHandler{level: slog.LevelDebug}
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
	})

	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header: stdhttp.Header{
					"Set-Cookie": []string{"session=response-secret"},
				},
				Body:    stdhttp.NoBody,
				Request: req,
			}, nil
		}),
		ResolvePolicy: DefaultRetryPolicyResolver,
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://user:password@example.com/orders?token=query-secret", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Authorization", "Bearer authorization-secret")
	req.Header.Set("Cookie", "session=request-secret")
	req.Header.Set("X-Api-Key", "api-key-secret")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	_ = resp.Body.Close()

	logged := handler.String()
	for _, value := range []string{
		"password",
		"query-secret",
		"authorization-secret",
		"request-secret",
		"api-key-secret",
		"response-secret",
	} {
		if !strings.Contains(logged, value) {
			t.Errorf("logs do not contain %q: %s", value, logged)
		}
	}
}

func TestRoundTripBodyLoggingPreservesOriginalClosers(t *testing.T) {
	oldLogger := slog.Default()
	oldBodyLogLevel := bodyLogLevelValue()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	SetBodyLogLevel(slog.LevelDebug)
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		SetBodyLogLevel(oldBodyLogLevel)
	})

	requestBody := &trackingReadCloser{reader: strings.NewReader(`{"name":"test"}`)}
	responseBody := &trackingReadCloser{reader: strings.NewReader(`{"ok":true}`)}
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			_, _ = io.ReadAll(req.Body)
			_ = req.Body.Close()
			return &stdhttp.Response{
				StatusCode:    stdhttp.StatusOK,
				Header:        stdhttp.Header{"Content-Type": []string{"application/json"}},
				Body:          responseBody,
				ContentLength: int64(len(`{"ok":true}`)),
				Request:       req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodPost, "http://example.com/orders", requestBody)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	req.ContentLength = int64(len(`{"name":"test"}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("resp.Body.Close() error = %v", err)
	}
	if closed := requestBody.closes.Load(); closed != 1 {
		t.Fatalf("original request body closes = %d, want 1", closed)
	}
	if closed := responseBody.closes.Load(); closed != 1 {
		t.Fatalf("original response body closes = %d, want 1", closed)
	}
}

func TestSharedTransportSettersAreSafeDuringRequests(t *testing.T) {
	oldTransport := cli.Transport
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode: stdhttp.StatusOK,
				Header:     make(stdhttp.Header),
				Body:       stdhttp.NoBody,
				Request:    req,
			}, nil
		}),
		ResolvePolicy: DefaultRetryPolicyResolver,
	}
	cli.Transport = transport
	t.Cleanup(func() {
		cli.Transport = oldTransport
		slog.SetDefault(oldLogger)
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://example.com/health", nil)
			if err != nil {
				t.Errorf("NewRequest() error = %v", err)
				return
			}
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Errorf("RoundTrip() error = %v", err)
				return
			}
			_ = resp.Body.Close()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			SetSkipper(nil)
			SetRetryPolicyResolver(DefaultRetryPolicyResolver)
			SetRetryConfigProvider(nil)
			SetRetryObserver(nil)
			SetRetryResultObserver(nil)
		}
	}()
	close(start)
	wg.Wait()
}

func TestInternalHelpersAllowNilAuthorizationSetter(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer server.Close()

	assertNoPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s panicked: %v", name, r)
			}
		}()
		fn()
	}

	assertNoPanic("InternalGet", func() {
		var resp map[string]bool
		if err := InternalGet(context.Background(), server.URL, &resp, nil); err != nil {
			t.Fatalf("InternalGet() error = %v", err)
		}
	})

	assertNoPanic("InternalPost", func() {
		var resp map[string]bool
		if err := InternalPost(context.Background(), server.URL, "application/json", map[string]string{"name": "test"}, &resp, nil); err != nil {
			t.Fatalf("InternalPost() error = %v", err)
		}
	})

	assertNoPanic("InternalPostMultipartForm", func() {
		var resp map[string]bool
		err := InternalPostMultipartForm(context.Background(), server.URL, MultipartFormData{
			Form: struct {
				Name string `form:"name"`
			}{Name: "tester"},
		}, &resp, nil)
		if err != nil {
			t.Fatalf("InternalPostMultipartForm() error = %v", err)
		}
	})
}

type trackingReadCloser struct {
	reader    *strings.Reader
	reads     atomic.Int32
	bytesRead atomic.Int64
	closes    atomic.Int32
	sawEOF    atomic.Bool
}

type dataThenErrorReadCloser struct {
	data []byte
	err  error
	done bool
}

type fixedRequestClassProvider struct {
	class RequestClass
}

func (p *fixedRequestClassProvider) ResolveRequestClass(*stdhttp.Request) (RequestClass, bool) {
	return p.class, true
}

func (*fixedRequestClassProvider) ResolveRetryPolicy(_ context.Context, _ RetryConfigScope, base RetryPolicy) RetryPolicy {
	return base
}

func (r *dataThenErrorReadCloser) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if len(r.data) == 0 {
		r.done = true
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		r.done = true
		return n, r.err
	}
	return n, nil
}

func (*dataThenErrorReadCloser) Close() error {
	return nil
}

func (t *trackingReadCloser) Read(p []byte) (int, error) {
	t.reads.Add(1)
	n, err := t.reader.Read(p)
	t.bytesRead.Add(int64(n))
	if errors.Is(err, io.EOF) {
		t.sawEOF.Store(true)
	}
	return n, err
}

func (t *trackingReadCloser) Close() error {
	t.closes.Add(1)
	return nil
}

type roundTripperFunc func(req *stdhttp.Request) (*stdhttp.Response, error)

func (fn roundTripperFunc) RoundTrip(req *stdhttp.Request) (*stdhttp.Response, error) {
	return fn(req)
}

type captureLogHandler struct {
	level   slog.Level
	mu      sync.Mutex
	records []capturedLogRecord
}

type capturedLogRecord struct {
	message string
	attrs   map[string]any
}

func setupBodyLogTest(t *testing.T) *captureLogHandler {
	t.Helper()
	handler := &captureLogHandler{level: slog.LevelDebug}
	oldLogger := slog.Default()
	oldBodyLogLevel := bodyLogLevelValue()
	slog.SetDefault(slog.New(handler))
	SetBodyLogLevel(slog.LevelDebug)
	t.Cleanup(func() {
		slog.SetDefault(oldLogger)
		SetBodyLogLevel(oldBodyLogLevel)
	})
	return handler
}

func roundTripResponseBodyLog(t *testing.T, handler *captureLogHandler, payload string, contentLength int64) any {
	t.Helper()
	transport := &Transport{
		RoundTripper: roundTripperFunc(func(req *stdhttp.Request) (*stdhttp.Response, error) {
			return &stdhttp.Response{
				StatusCode:    stdhttp.StatusOK,
				Header:        stdhttp.Header{"Content-Type": []string{"application/json"}},
				Body:          io.NopCloser(strings.NewReader(payload)),
				ContentLength: contentLength,
				Request:       req,
			}, nil
		}),
	}
	req, err := stdhttp.NewRequestWithContext(context.Background(), stdhttp.MethodGet, "http://example.com/api/test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		t.Fatalf("ReadAll(resp.Body) error = %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("resp.Body.Close() error = %v", closeErr)
	}
	if string(body) != payload {
		t.Fatalf("response body = %q, want %q", body, payload)
	}
	bodyLog, ok := handler.attrValue("HTTPClient Response", "Body")
	if !ok {
		t.Fatal("response body log not found")
	}
	return bodyLog
}

func (h *captureLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *captureLogHandler) Handle(_ context.Context, record slog.Record) error {
	entry := capturedLogRecord{
		message: record.Message,
		attrs:   make(map[string]any),
	}
	record.Attrs(func(attr slog.Attr) bool {
		entry.attrs[attr.Key] = attr.Value.Any()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, entry)
	h.mu.Unlock()
	return nil
}

func (h *captureLogHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *captureLogHandler) WithGroup(_ string) slog.Handler {
	return h
}

func (h *captureLogHandler) hasRecord(message, key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, record := range h.records {
		if record.message != message {
			continue
		}
		if _, ok := record.attrs[key]; ok {
			return true
		}
	}
	return false
}

func (h *captureLogHandler) attrValue(message, key string) (any, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, record := range h.records {
		if record.message != message {
			continue
		}
		value, ok := record.attrs[key]
		if ok {
			return value, true
		}
	}
	return nil, false
}

func (h *captureLogHandler) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return fmt.Sprint(h.records)
}
