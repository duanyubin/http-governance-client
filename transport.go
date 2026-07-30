package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrUrlNotFound reports an outgoing request without a URL.
	ErrUrlNotFound = errors.New("request URL not found")
	bodyLogLevel   atomic.Int64
)

const (
	debugBodyReadLimit      = int64(64 << 10)
	retryResponseDrainLimit = int64(64 << 10)
)

func init() {
	bodyLogLevel.Store(int64(slog.LevelDebug))
}

// SkipperFunc reports whether retry governance should be bypassed for a request.
type SkipperFunc func(req *http.Request) bool

// Transport is the retry-aware RoundTripper used by the shared HTTP client.
type Transport struct {
	mu sync.RWMutex

	http.RoundTripper
	Skipper        SkipperFunc
	ResolvePolicy  RetryPolicyResolver
	ConfigProvider RetryConfigProvider
	Observer       RetryObserver
	ResultObserver RetryResultObserver
}

// RoundTrip applies request classification, policy resolution, retry budgeting
// and retry headers before delegating the request to the wrapped RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.RLock()
	roundTripper := t.RoundTripper
	skipper := t.Skipper
	policyResolver := t.ResolvePolicy
	configProvider := t.ConfigProvider
	observer := t.Observer
	resultObserver := t.ResultObserver
	t.mu.RUnlock()

	if skipper != nil && skipper(req) {
		skippedReq, cancel := prepareSkippedRequest(req)
		resp, err := roundTripper.RoundTrip(skippedReq)
		if err == nil && resp != nil {
			resp.Body = wrapBodyWithCancel(resp.Body, cancel)
		} else {
			cancel()
		}
		return resp, err
	}

	start := time.Now()
	if policyResolver == nil {
		policyResolver = DefaultRetryPolicyResolver
	}
	var policy RetryPolicy
	var class RequestClass
	var scope RetryConfigScope
	resolveDynamicPolicy := false
	if provider, ok := configProvider.(*ManagedRetryConfigProvider); ok {
		policy = policyResolver(req)
		scope, policy = provider.resolveRequestConfig(req, policy)
		class = scope.Class
		policy = normalizeRetryPolicy(policy)
	} else {
		var classResolver RequestClassResolver
		if resolver, ok := configProvider.(RequestClassResolver); ok {
			classResolver = resolver
		}
		class = requestClassFromContextOrResolver(req, classResolver)
		policyReq := req.Clone(WithRequestClass(req.Context(), class))
		policy = policyResolver(policyReq)
		scope = retryConfigScopeFromRequest(req, class)
		if configProvider != nil {
			if scopeResolver, ok := configProvider.(RetryConfigScopeDefaultsResolver); ok {
				scope = scopeResolver.ResolveRetryConfigScope(req, scope)
			}
			resolveDynamicPolicy = true
		}
	}
	if scope.Caller == "" {
		scope.Caller = CallerServiceName()
	}
	if scope.Downstream == "" {
		scope.Downstream = inferDownstream(req, nil)
	}
	if scope.Operation == "" {
		scope.Operation = inferOperation(req, scope, nil)
	}
	if resolveDynamicPolicy {
		policy = normalizeRetryPolicy(configProvider.ResolveRetryPolicy(req.Context(), scope, policy))
	}
	overallDeadline, hasOverallDeadline := resolveOverallDeadline(req, policy)
	loopReq := req
	overallCancel := context.CancelFunc(func() {})
	if hasOverallDeadline {
		var overallCtx context.Context
		overallCtx, overallCancel = context.WithDeadline(req.Context(), overallDeadline)
		loopReq = req.Clone(overallCtx)
	}
	originalNoMoreRetry := NoMoreRetryFromContext(req.Context()) || parseBoolHeader(req.Header.Get(HeaderNoMoreRetry))
	maxRetries := policy.MaxRetries
	if originalNoMoreRetry || !isRetryableRequest(req) {
		maxRetries = 0
	}
	if req.Body != nil && req.GetBody == nil && maxRetries > 0 {
		maxRetries = 0
		slog.WarnContext(req.Context(), "HTTPClient retry disabled because request body cannot be replayed", "method", req.Method, "url", req.URL.String())
	}
	totalAttempts := 0

	var lastResp *http.Response
	var lastErr error
	var finalResp *http.Response
	var finalErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		attemptReq, cancel, err := prepareAttemptRequest(loopReq, policy, attempt, attempt == maxRetries || originalNoMoreRetry, overallDeadline, hasOverallDeadline, lastResp, lastErr)
		if err != nil {
			if attempt == 0 && req.Body != nil {
				_ = req.Body.Close()
			}
			finalResp = nil
			finalErr = err
			break
		}
		totalAttempts = attempt + 1
		resp, err := t.roundTripOnce(attemptReq, roundTripper)
		if err == nil && resp != nil {
			resp.Body = wrapBodyWithCancel(resp.Body, cancel)
		} else {
			cancel()
		}
		finalResp = resp
		finalErr = err

		shouldRetry := shouldRetry(attemptReq, policy, attempt, maxRetries, resp, err, overallDeadline, hasOverallDeadline)
		if !shouldRetry {
			break
		}

		delay := retryDelay(policy, attempt, resp)
		if hasOverallDeadline && delay > 0 && time.Now().Add(delay).After(overallDeadline) {
			finalResp = resp
			finalErr = err
			break
		}
		if observer != nil {
			observer.ObserveRetry(attemptReq.Context(), RetryEvent{
				Method:     attemptReq.Method,
				URL:        attemptReq.URL.String(),
				Class:      class,
				Attempt:    attempt + 1,
				MaxRetries: maxRetries,
				Delay:      delay,
				StatusCode: statusCode(resp),
				Reason:     retryReason(resp, err),
				WillRetry:  true,
			})
		}
		slog.WarnContext(attemptReq.Context(), "HTTPClient retry scheduled",
			"method", attemptReq.Method,
			"url", attemptReq.URL.String(),
			"class", class,
			"attempt", attempt+1,
			"maxRetries", maxRetries,
			"delay", delay.String(),
			"reason", retryReason(resp, err),
			"status", statusCode(resp),
		)
		discardRetryResponse(resp)
		finalResp = nil
		lastResp = resp
		lastErr = err
		if err := waitRetryDelay(loopReq.Context(), delay); err != nil {
			finalErr = err
			break
		}
	}
	observeRequestResult(observer, resultObserver, loopReq.Context(), req, scope, class, maxRetries, totalAttempts, start, finalResp, finalErr)
	if finalResp != nil || finalErr != nil {
		if finalResp != nil {
			finalResp.Body = wrapBodyWithCancel(finalResp.Body, overallCancel)
		} else {
			overallCancel()
		}
		return finalResp, finalErr
	}
	overallCancel()
	return lastResp, lastErr
}

func prepareSkippedRequest(req *http.Request) (*http.Request, context.CancelFunc) {
	ctx := req.Context()
	var cancel context.CancelFunc
	deadline, hasDeadline := resolveOverallDeadline(req, RetryPolicy{})
	if hasDeadline {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	skippedReq := req.Clone(ctx)
	if skippedReq.Header == nil {
		skippedReq.Header = make(http.Header)
	}
	noMoreRetry := NoMoreRetryFromContext(req.Context()) || parseBoolHeader(req.Header.Get(HeaderNoMoreRetry))
	skippedReq.Header.Set(HeaderNoMoreRetry, strconv.FormatBool(noMoreRetry))
	if hasDeadline {
		skippedReq.Header.Set(HeaderRequestDeadline, formatRequestDeadline(deadline))
	}
	skippedReq.Header.Set(HeaderRetryAttempt, "0")
	skippedReq.Header.Del(HeaderRetryReason)
	return skippedReq, cancel
}

func shouldRetry(req *http.Request, policy RetryPolicy, attempt, maxRetries int, resp *http.Response, err error, overallDeadline time.Time, hasOverallDeadline bool) bool {
	if attempt >= maxRetries {
		return false
	}
	if hasOverallDeadline && time.Now().After(overallDeadline) {
		return false
	}
	if !isRetryableRequest(req) {
		return false
	}
	if err != nil {
		return isRetryableError(err)
	}
	if resp == nil {
		return false
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		_, configured := policy.RetryOnStatuses[resp.StatusCode]
		return configured && policy.RetryOn429
	}
	_, ok := policy.RetryOnStatuses[resp.StatusCode]
	return ok
}

func (t *Transport) roundTripOnce(req *http.Request, roundTripper http.RoundTripper) (*http.Response, error) {
	ctx := req.Context()
	start := time.Now()
	header := req.Header
	u := req.URL
	if u == nil {
		return nil, ErrUrlNotFound
	}
	path := u.Path
	raw := u.RawQuery
	if len(raw) > 0 {
		path = path + "?" + raw
	}
	slog.DebugContext(ctx, "HTTPClient Request", "URL", u.String())
	slog.DebugContext(ctx, "HTTPClient Request", "Header", header)

	bodyLogEnabled := bodyLogsEnabled(ctx)
	requestContentType := header.Get("Content-Type")

	if bodyLogEnabled && req.Body != nil && strings.Contains(requestContentType, "multipart/form-data") {
		readTime := time.Now()
		formData := make(map[string]interface{})
		bodyBytes, replacement, _ := captureDebugBody(req.Body)
		req.Body = replacement
		if boundary := t.extractBoundary(req.Header.Get("Content-Type")); boundary != "" {
			reader := multipart.NewReader(bytes.NewReader(bodyBytes), boundary)
			for {
				part, err := reader.NextPart()
				if err != nil {
					break
				}
				if part.FileName() == "" {
					value, _ := io.ReadAll(io.LimitReader(part, 1024))
					formData[part.FormName()] = string(value)
				} else {
					formData[part.FormName()] = part.FileName()
				}
				part.Close()
			}
		}
		readLatency := time.Since(readTime)
		logBodyContext(ctx, "HTTPClient Request", "multipart/form-data", formData, "read-Latency", readLatency.String())
	} else if bodyLogEnabled && req.Body != nil {
		if !shouldCaptureDebugResponseBody(requestContentType, req.ContentLength) {
			logBodyContext(ctx, "HTTPClient Request", "Body", "请求体过大或类型未知，跳过打印")
		} else {
			var captured bool
			var reqBody []byte
			reqBody, req.Body, captured = captureDebugBody(req.Body)
			if !captured {
				logBodyContext(ctx, "HTTPClient Request", "Body", "请求体过大或读取失败，跳过打印")
			} else if strings.Contains(requestContentType, "application/json") {
				var rst map[string]any
				if err := json.Unmarshal(reqBody, &rst); err == nil {
					logBodyContext(ctx, "HTTPClient Request", "Body", rst)
				}
			} else {
				logBodyContext(ctx, "HTTPClient Request", "Body", string(reqBody))
			}
		}
	}
	resp, err := roundTripper.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	slog.DebugContext(ctx, "HTTPClient Response", "Header", resp.Header)
	if bodyLogEnabled {
		responseContentType := resp.Header.Get("Content-Type")
		contentDisposition := resp.Header.Get("Content-Disposition")
		if strings.Contains(contentDisposition, "attachment") || strings.Contains(responseContentType, "application/octet-stream") {
			logBodyContext(ctx, "HTTPClient Response", "Body", "文件不打印")
		} else if shouldCaptureDebugResponseBody(responseContentType, resp.ContentLength) {
			var captured bool
			var respBody []byte
			respBody, resp.Body, captured = captureDebugBody(resp.Body)
			if !captured {
				logBodyContext(ctx, "HTTPClient Response", "Body", "响应体过大或读取失败，跳过打印")
			} else if strings.Contains(responseContentType, "application/json") {
				var rst map[string]any
				if err := json.Unmarshal(respBody, &rst); err == nil {
					logBodyContext(ctx, "HTTPClient Response", "Body", rst)
				}
			} else {
				logBodyContext(ctx, "HTTPClient Response", "Body", string(respBody))
			}
		} else {
			logBodyContext(ctx, "HTTPClient Response", "Body", "响应体过大或类型未知，跳过打印")
		}
	}

	// Stop timer
	latency := time.Since(start)
	method := req.Method
	statusCode := resp.StatusCode
	slog.DebugContext(ctx, "HTTPClient", "method", method, "uri", path, "status", statusCode, "latency", latency.String())

	return resp, nil
}

func shouldCaptureDebugResponseBody(contentType string, contentLength int64) bool {
	contentType = strings.ToLower(contentType)
	if contentLength < 0 || contentLength > debugBodyReadLimit {
		return false
	}
	return strings.HasPrefix(contentType, "text/") ||
		strings.Contains(contentType, "json") ||
		strings.Contains(contentType, "xml") ||
		strings.Contains(contentType, "javascript") ||
		strings.Contains(contentType, "x-www-form-urlencoded")
}

func captureDebugBody(body io.ReadCloser) ([]byte, io.ReadCloser, bool) {
	captured, err := io.ReadAll(io.LimitReader(body, debugBodyReadLimit+1))
	var replay io.Reader
	switch {
	case err != nil:
		replay = io.MultiReader(bytes.NewReader(captured), errorReader{err: err})
	case int64(len(captured)) > debugBodyReadLimit:
		replay = io.MultiReader(bytes.NewReader(captured), body)
	default:
		replay = bytes.NewReader(captured)
	}
	replacement := &readerWithCloser{Reader: replay, Closer: body}
	return captured, replacement, err == nil && int64(len(captured)) <= debugBodyReadLimit
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type readerWithCloser struct {
	io.Reader
	io.Closer
}

func discardRetryResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, retryResponseDrainLimit+1))
	_ = resp.Body.Close()
}

func prepareAttemptRequest(req *http.Request, policy RetryPolicy, attempt int, noMoreRetry bool, overallDeadline time.Time, hasOverallDeadline bool, lastResp *http.Response, lastErr error) (*http.Request, context.CancelFunc, error) {
	attemptReq := req.Clone(req.Context())
	if attempt > 0 && req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return nil, nil, err
		}
		attemptReq.Body = body
		attemptReq.GetBody = req.GetBody
		attemptReq.ContentLength = req.ContentLength
	}

	attemptTimeout := policy.PerAttemptTimeout
	if hasOverallDeadline {
		remaining := time.Until(overallDeadline)
		if remaining <= 0 {
			return nil, nil, context.DeadlineExceeded
		}
		if attemptTimeout <= 0 || remaining < attemptTimeout {
			attemptTimeout = remaining
		}
	}
	// The per-attempt timeout can only shrink when an upstream deadline already
	// exists; downstream hops must not widen the remaining budget.
	if attemptTimeout <= 0 {
		attemptTimeout = defaultPerAttemptTimeout
	}
	attemptCtx, cancel := context.WithTimeout(req.Context(), attemptTimeout)
	attemptReq = attemptReq.Clone(attemptCtx)
	attemptReq.GetBody = req.GetBody
	attemptReq.ContentLength = req.ContentLength
	if attemptReq.Header == nil {
		attemptReq.Header = make(http.Header)
	}
	attemptReq.Header.Set(HeaderRetryAttempt, strconv.Itoa(attempt))
	attemptReq.Header.Set(HeaderNoMoreRetry, strconv.FormatBool(noMoreRetry))
	if hasOverallDeadline {
		attemptReq.Header.Set(HeaderRequestDeadline, formatRequestDeadline(overallDeadline))
	}
	if attempt > 0 {
		attemptReq.Header.Set(HeaderRetryReason, retryReason(lastResp, lastErr))
	} else {
		attemptReq.Header.Del(HeaderRetryReason)
	}
	return attemptReq, cancel, nil
}

func isRetryableRequest(req *http.Request) bool {
	if NoMoreRetryFromContext(req.Context()) || parseBoolHeader(req.Header.Get(HeaderNoMoreRetry)) {
		return false
	}
	return true
}

func isRetryableError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var certificateErr *tls.CertificateVerificationError
	if errors.As(err, &certificateErr) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTimeout || dnsErr.IsTemporary
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

func waitRetryDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func statusCode(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func observeRequestResult(observer RetryObserver, resultObserver RetryResultObserver, ctx context.Context, req *http.Request, scope RetryConfigScope, class RequestClass, maxRetries, totalAttempts int, start time.Time, resp *http.Response, err error) {
	retryCount := totalAttempts - 1
	if retryCount < 0 {
		retryCount = 0
	}
	finalReason := requestResultReason(resp, err)
	finalFailed := isFinalFailure(resp, err)
	timedOut := isTimeoutError(err) ||
		resp != nil && (resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusGatewayTimeout)
	event := RequestResultEvent{
		Method:         scope.Method,
		URL:            scope.Path,
		Caller:         scope.Caller,
		Downstream:     scope.Downstream,
		Operation:      scope.Operation,
		Class:          class,
		AttemptCount:   totalAttempts,
		RetryCount:     retryCount,
		MaxRetries:     maxRetries,
		StatusCode:     statusCode(resp),
		FinalReason:    finalReason,
		FinalError:     errorString(err),
		Retried:        retryCount > 0,
		RetrySucceeded: retryCount > 0 && !finalFailed,
		FinalFailed:    finalFailed,
		TimedOut:       timedOut,
		Duration:       time.Since(start),
	}
	if req != nil && req.URL != nil {
		event.URL = req.URL.String()
	}
	if resultObserver != nil {
		resultObserver.ObserveRequestResult(ctx, event)
	} else if legacyResultObserver, ok := observer.(RetryResultObserver); ok {
		legacyResultObserver.ObserveRequestResult(ctx, event)
	}
	slog.InfoContext(ctx, "HTTPClient request finished",
		"method", event.Method,
		"url", event.URL,
		"caller", event.Caller,
		"downstream", event.Downstream,
		"operation", event.Operation,
		"class", event.Class,
		"attemptCount", event.AttemptCount,
		"retryCount", event.RetryCount,
		"maxRetries", event.MaxRetries,
		"status", event.StatusCode,
		"finalReason", event.FinalReason,
		"finalError", event.FinalError,
		"retried", event.Retried,
		"retrySucceeded", event.RetrySucceeded,
		"finalFailed", event.FinalFailed,
		"timedOut", event.TimedOut,
		"duration", event.Duration.String(),
	)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func wrapBodyWithCancel(body io.ReadCloser, cancel context.CancelFunc) io.ReadCloser {
	if body == nil {
		cancel()
		return nil
	}
	return &cancelOnCloseReadCloser{ReadCloser: body, cancel: cancel}
}

func (c *cancelOnCloseReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) {
		c.cancel()
	}
	return n, err
}

func (c *cancelOnCloseReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

func (t *Transport) extractBoundary(contentType string) string {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(params["boundary"])
}

func bodyLogLevelValue() slog.Level {
	return slog.Level(bodyLogLevel.Load())
}

func bodyLogsEnabled(ctx context.Context) bool {
	return slog.Default().Enabled(ctx, bodyLogLevelValue())
}

func logBodyContext(ctx context.Context, msg string, args ...any) {
	slog.Log(ctx, bodyLogLevelValue(), msg, args...)
}
