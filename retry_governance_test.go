package http

import (
	"context"
	stdhttp "net/http"
	"testing"
	"time"
)

func TestContextWithRetryConstraintsStoresHeaders(t *testing.T) {
	deadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	headers := make(stdhttp.Header)
	headers.Set(HeaderNoMoreRetry, "true")
	headers.Set(HeaderRequestDeadline, formatRequestDeadline(deadline))

	ctx, cancel := ContextWithRetryConstraints(context.Background(), headers)
	defer cancel()

	if !NoMoreRetryFromContext(ctx) {
		t.Fatal("NoMoreRetryFromContext() = false, want true")
	}
	gotDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("Context deadline is missing")
	}
	if !gotDeadline.Equal(deadline) {
		t.Fatalf("Context deadline = %v, want %v", gotDeadline, deadline)
	}
}

func TestContextWithRetryConstraintsKeepsEarlierContextDeadline(t *testing.T) {
	contextDeadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	parent, parentCancel := context.WithDeadline(context.Background(), contextDeadline)
	defer parentCancel()

	headers := make(stdhttp.Header)
	headers.Set(HeaderRequestDeadline, formatRequestDeadline(contextDeadline.Add(time.Minute)))

	ctx, cancel := ContextWithRetryConstraints(parent, headers)
	defer cancel()

	gotDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("Context deadline is missing")
	}
	if !gotDeadline.Equal(contextDeadline) {
		t.Fatalf("Context deadline = %v, want %v", gotDeadline, contextDeadline)
	}
}

func TestContextWithRetryConstraintsIgnoresInvalidDeadline(t *testing.T) {
	headers := make(stdhttp.Header)
	headers.Set(HeaderRequestDeadline, "not-a-deadline")

	ctx, cancel := ContextWithRetryConstraints(context.Background(), headers)
	defer cancel()

	if _, ok := ctx.Deadline(); ok {
		t.Fatal("Context deadline is present, want no deadline")
	}
}
