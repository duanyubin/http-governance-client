package http

import (
	"context"
	"log/slog"
	stdhttp "net/http"
)

type noMoreRetryContextKey struct{}

// WithNoMoreRetry marks all downstream calls derived from ctx as non-retryable.
func WithNoMoreRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, noMoreRetryContextKey{}, true)
}

// NoMoreRetryFromContext reports whether an upstream caller disabled retries.
func NoMoreRetryFromContext(ctx context.Context) bool {
	noMoreRetry, _ := ctx.Value(noMoreRetryContextKey{}).(bool)
	return noMoreRetry
}

// ContextWithRetryConstraints imports chain-wide retry constraints from headers.
func ContextWithRetryConstraints(ctx context.Context, headers stdhttp.Header) (context.Context, context.CancelFunc) {
	if parseBoolHeader(headers.Get(HeaderNoMoreRetry)) {
		ctx = WithNoMoreRetry(ctx)
	}

	headerValue := headers.Get(HeaderRequestDeadline)
	headerDeadline, ok := parseRequestDeadline(headerValue)
	if !ok {
		if headerValue != "" {
			slog.Warn("ignoring invalid request deadline", "header", HeaderRequestDeadline, "value", headerValue)
		}
		return ctx, func() {}
	}

	if contextDeadline, hasDeadline := ctx.Deadline(); hasDeadline && !headerDeadline.Before(contextDeadline) {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, headerDeadline)
}
