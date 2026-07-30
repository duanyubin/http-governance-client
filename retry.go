package http

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPerAttemptTimeout = 2 * time.Second
	defaultInitialBackoff    = 100 * time.Millisecond
	defaultMaxBackoff        = 2 * time.Second
)

const (
	// HeaderRetryAttempt carries the zero-based attempt number for the current request.
	HeaderRetryAttempt = "X-Retry-Attempt"
	// HeaderNoMoreRetry tells downstream callers not to relax the current retry budget.
	HeaderNoMoreRetry = "X-No-More-Retry"
	// HeaderRequestDeadline carries the overall request deadline as a Unix millisecond timestamp.
	HeaderRequestDeadline = "X-Request-Deadline"
	// HeaderRetryReason records the reason for the latest retry decision.
	HeaderRetryReason = "X-Retry-Reason"
)

// RequestClass describes whether a request is internal/external and read/write.
type RequestClass string

const (
	RequestClassInternalRead  RequestClass = "internal_read"
	RequestClassInternalWrite RequestClass = "internal_write"
	RequestClassExternalRead  RequestClass = "external_read"
	RequestClassExternalWrite RequestClass = "external_write"
)

// RetryPolicy describes retry, timeout and backoff controls for a request.
type RetryPolicy struct {
	MaxRetries        int
	PerAttemptTimeout time.Duration
	MaxElapsedTime    time.Duration
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	RetryOnStatuses   map[int]struct{}
	RetryOn429        bool
}

// RetryEvent records a scheduled retry attempt.
type RetryEvent struct {
	Method     string
	URL        string
	Class      RequestClass
	Attempt    int
	MaxRetries int
	Delay      time.Duration
	StatusCode int
	Reason     string
	WillRetry  bool
}

// RequestResultEvent records the transport-level outcome after all retries.
// It does not include response-body decoding performed by helper functions.
type RequestResultEvent struct {
	Method         string
	URL            string
	Caller         string
	Downstream     string
	Operation      string
	Class          RequestClass
	AttemptCount   int
	RetryCount     int
	MaxRetries     int
	StatusCode     int
	FinalReason    string
	FinalError     string
	Retried        bool
	RetrySucceeded bool
	FinalFailed    bool
	TimedOut       bool
	Duration       time.Duration
}

// RetryObserver receives retry scheduling events.
type RetryObserver interface {
	ObserveRetry(ctx context.Context, event RetryEvent)
}

// RetryResultObserver receives the final transport-level request outcome event.
type RetryResultObserver interface {
	ObserveRequestResult(ctx context.Context, event RequestResultEvent)
}

// RetryPolicyResolver resolves the baseline retry policy before dynamic
// configuration is applied.
type RetryPolicyResolver func(req *http.Request) RetryPolicy

type requestClassContextKey struct{}
type retryPolicyContextKey struct{}

// WithRequestClass stores an explicit request class in context.
func WithRequestClass(ctx context.Context, class RequestClass) context.Context {
	return context.WithValue(ctx, requestClassContextKey{}, class)
}

// WithRetryPolicy stores an explicit baseline retry policy in context.
func WithRetryPolicy(ctx context.Context, policy RetryPolicy) context.Context {
	return context.WithValue(ctx, retryPolicyContextKey{}, policy)
}

// DefaultRetryPolicyResolver returns the built-in baseline policy: read
// requests retry once by default while write requests do not retry
// automatically.
func DefaultRetryPolicyResolver(req *http.Request) RetryPolicy {
	if policy, ok := req.Context().Value(retryPolicyContextKey{}).(RetryPolicy); ok {
		return normalizeRetryPolicy(policy)
	}
	policy := RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: defaultPerAttemptTimeout,
		InitialBackoff:    defaultInitialBackoff,
		MaxBackoff:        defaultMaxBackoff,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}
	switch requestClassFromContextOrMethod(req) {
	case RequestClassInternalWrite, RequestClassExternalWrite:
		policy.MaxRetries = 0
	}
	return normalizeRetryPolicy(policy)
}

func requestClassFromContextOrMethod(req *http.Request) RequestClass {
	return requestClassFromContextOrResolver(req, nil)
}

func requestClassFromContextOrResolver(req *http.Request, resolver RequestClassResolver) RequestClass {
	if class, ok := explicitRequestClassFromContext(req); ok {
		return class
	}
	if resolver != nil {
		if class, ok := resolver.ResolveRequestClass(req); ok {
			return class
		}
	}
	if class, ok := resolveRequestClassByExtension(req); ok {
		return class
	}
	if isReadMethod(req.Method) {
		return RequestClassInternalRead
	}
	return RequestClassInternalWrite
}

func normalizeRetryPolicy(policy RetryPolicy) RetryPolicy {
	if policy.MaxRetries < 0 {
		policy.MaxRetries = 0
	}
	if policy.PerAttemptTimeout <= 0 {
		policy.PerAttemptTimeout = defaultPerAttemptTimeout
	}
	if policy.InitialBackoff <= 0 {
		policy.InitialBackoff = defaultInitialBackoff
	}
	if policy.MaxBackoff <= 0 {
		policy.MaxBackoff = defaultMaxBackoff
	}
	if policy.RetryOnStatuses == nil {
		policy.RetryOnStatuses = defaultRetryStatusSet()
	}
	return policy
}

func defaultRetryStatusSet() map[int]struct{} {
	return map[int]struct{}{
		http.StatusRequestTimeout:      {},
		http.StatusTooManyRequests:     {},
		http.StatusInternalServerError: {},
		http.StatusBadGateway:          {},
		http.StatusServiceUnavailable:  {},
		http.StatusGatewayTimeout:      {},
	}
}

func isReadMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}

func parseBoolHeader(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "true")
}

func parseRequestDeadline(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ts), true
}

func formatRequestDeadline(deadline time.Time) string {
	return strconv.FormatInt(deadline.UnixMilli(), 10)
}

func retryDelay(policy RetryPolicy, attempt int, resp *http.Response) time.Duration {
	if resp != nil {
		if delay, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			return delay
		}
	}
	base := policy.InitialBackoff
	for i := 0; i < attempt; i++ {
		if base >= policy.MaxBackoff || base > policy.MaxBackoff-base {
			base = policy.MaxBackoff
			break
		}
		base *= 2
	}
	if base <= 0 {
		return 0
	}
	if base >= policy.MaxBackoff {
		return policy.MaxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(base)))
	if jitter >= policy.MaxBackoff-base {
		return policy.MaxBackoff
	}
	return base + jitter
}

func parseRetryAfter(value string) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > math.MaxInt64/int64(time.Second) {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	if when, err := http.ParseTime(value); err == nil {
		delay := time.Until(when)
		if delay < 0 {
			return 0, true
		}
		return delay, true
	}
	return 0, false
}

func resolveOverallDeadline(req *http.Request, policy RetryPolicy) (time.Time, bool) {
	var deadline time.Time
	var ok bool
	if headerDeadline, headerOK := parseRequestDeadline(req.Header.Get(HeaderRequestDeadline)); headerOK {
		deadline = headerDeadline
		ok = true
	}
	if ctxDeadline, ctxOK := req.Context().Deadline(); ctxOK {
		if !ok || ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
			ok = true
		}
	}
	if policy.MaxElapsedTime > 0 {
		maxElapsedDeadline := time.Now().Add(policy.MaxElapsedTime)
		if !ok || maxElapsedDeadline.Before(deadline) {
			deadline = maxElapsedDeadline
			ok = true
		}
	}
	return deadline, ok
}

func retryReason(resp *http.Response, err error) string {
	switch {
	case err != nil:
		if isTimeoutError(err) {
			return "timeout"
		}
		return "transport_error"
	case resp == nil:
		return "unknown"
	case resp.StatusCode == http.StatusTooManyRequests:
		return "429"
	case resp.StatusCode >= 500:
		return "5xx"
	case resp.StatusCode == http.StatusRequestTimeout:
		return "timeout"
	default:
		return fmt.Sprintf("status_%d", resp.StatusCode)
	}
}

func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func requestResultReason(resp *http.Response, err error) string {
	switch {
	case err != nil:
		return retryReason(resp, err)
	case resp == nil:
		return "unknown"
	case resp.StatusCode < http.StatusBadRequest:
		return "success"
	default:
		return retryReason(resp, nil)
	}
}

func isFinalFailure(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return false
	}
	return resp.StatusCode >= http.StatusBadRequest
}
