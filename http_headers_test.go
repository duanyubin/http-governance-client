package http

import (
	"context"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetWithHeadersAppliesSetter(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer server.Close()

	var response map[string]bool
	err := GetWithHeaders(context.Background(), server.URL, &response, func(req *stdhttp.Request) error {
		req.Header.Set("Authorization", "Bearer test-token")
		return nil
	})
	if err != nil {
		t.Fatalf("GetWithHeaders() error = %v", err)
	}
	if !response["ok"] {
		t.Fatalf("response = %#v", response)
	}
}

func TestGetWithHeadersReturnsSetterErrorBeforeSending(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	wantErr := errors.New("token unavailable")
	err := GetWithHeaders(context.Background(), server.URL, nil, func(*stdhttp.Request) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("GetWithHeaders() error = %v, want %v", err, wantErr)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("request count = %d, want 0", got)
	}
}

func TestBodyMethodsWithHeadersApplySetter(t *testing.T) {
	tests := []struct {
		name   string
		method string
		call   func(context.Context, string, RequestHeaderSetter) error
	}{
		{
			name:   "post",
			method: stdhttp.MethodPost,
			call: func(ctx context.Context, url string, setter RequestHeaderSetter) error {
				return PostWithHeaders(ctx, url, "application/json", map[string]bool{"ok": true}, nil, setter)
			},
		},
		{
			name:   "put",
			method: stdhttp.MethodPut,
			call: func(ctx context.Context, url string, setter RequestHeaderSetter) error {
				return PutWithHeaders(ctx, url, "application/json", map[string]bool{"ok": true}, nil, setter)
			},
		},
		{
			name:   "delete",
			method: stdhttp.MethodDelete,
			call: func(ctx context.Context, url string, setter RequestHeaderSetter) error {
				return DeleteWithHeaders(ctx, url, "application/json", map[string]bool{"ok": true}, nil, setter)
			},
		},
		{
			name:   "custom method",
			method: stdhttp.MethodPatch,
			call: func(ctx context.Context, url string, setter RequestHeaderSetter) error {
				return SendWithHeaders(ctx, stdhttp.MethodPatch, url, "application/json", map[string]bool{"ok": true}, nil, setter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				if r.Method != tt.method {
					t.Errorf("method = %q, want %q", r.Method, tt.method)
				}
				if got := r.Header.Get("X-Test"); got != "set" {
					t.Errorf("X-Test = %q", got)
				}
				if got := r.Header.Get("Content-Type"); got != "application/custom" {
					t.Errorf("Content-Type = %q", got)
				}
				w.WriteHeader(stdhttp.StatusNoContent)
			}))
			defer server.Close()

			err := tt.call(context.Background(), server.URL, func(req *stdhttp.Request) error {
				if got := req.Header.Get("Content-Type"); got != "application/json" {
					t.Errorf("setter observed Content-Type = %q", got)
				}
				req.Header.Set("Content-Type", "application/custom")
				req.Header.Set("X-Test", "set")
				return nil
			})
			if err != nil {
				t.Fatalf("request error = %v", err)
			}
		})
	}
}

func TestGetWithHeadersCallsSetterOnceWhenRetried(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(stdhttp.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(stdhttp.StatusNoContent)
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
	var setterCalls atomic.Int32
	err := GetWithHeaders(ctx, server.URL, nil, func(req *stdhttp.Request) error {
		setterCalls.Add(1)
		req.Header.Set("X-Test", "set")
		return nil
	})
	if err != nil {
		t.Fatalf("GetWithHeaders() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
	if got := setterCalls.Load(); got != 1 {
		t.Fatalf("setter calls = %d, want 1", got)
	}
}

func TestLegacyInternalHelpersApplyHeaderSetters(t *testing.T) {
	tests := []struct {
		name string
		want string
		call func(string) error
	}{
		{
			name: "get function setter",
			want: "legacy-get",
			call: func(url string) error {
				return InternalGet(context.Background(), url, nil, func(req *stdhttp.Request) error {
					req.Header.Set("X-Test", "legacy-get")
					return nil
				})
			},
		},
		{
			name: "post interface setter",
			want: "legacy-post",
			call: func(url string) error {
				return InternalPost(
					context.Background(),
					url,
					"application/json",
					map[string]bool{"ok": true},
					nil,
					AuthorizationInHeaderSetterFunc(func(req *stdhttp.Request) error {
						req.Header.Set("X-Test", "legacy-post")
						return nil
					}),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				if got := r.Header.Get("X-Test"); got != tt.want {
					t.Errorf("X-Test = %q, want %q", got, tt.want)
				}
				w.WriteHeader(stdhttp.StatusNoContent)
			}))
			defer server.Close()

			if err := tt.call(server.URL); err != nil {
				t.Fatalf("request error = %v", err)
			}
		})
	}
}
