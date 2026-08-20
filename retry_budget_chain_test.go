package http

import (
	"context"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestRetryBudgetBoundsMultiHopAmplification(t *testing.T) {
	tests := []struct {
		name       string
		capacity   int
		retryCost  int
		wantDCalls int
		wantNoMore bool
	}{
		{name: "one retry token per downstream", capacity: 1, retryCost: 1, wantDCalls: 4, wantNoMore: true},
		{name: "budget blocks every retry", capacity: 1, retryCost: 2, wantDCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var dNoMoreHeaders []string
			d := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				mu.Lock()
				dNoMoreHeaders = append(dNoMoreHeaders, r.Header.Get(HeaderNoMoreRetry))
				mu.Unlock()
				w.WriteHeader(stdhttp.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "unavailable")
			}))
			defer d.Close()

			cClient := newRetryBudgetChainClient(t, tt.capacity, tt.retryCost)
			c := newRetryBudgetChainServer(t, cClient, d.URL)
			defer c.Close()
			bClient := newRetryBudgetChainClient(t, tt.capacity, tt.retryCost)
			b := newRetryBudgetChainServer(t, bClient, c.URL)
			defer b.Close()
			aClient := newRetryBudgetChainClient(t, tt.capacity, tt.retryCost)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, b.URL, nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext() error = %v", err)
			}
			resp, err := aClient.Do(req)
			if err != nil {
				t.Fatalf("A->B request error = %v", err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			mu.Lock()
			gotHeaders := append([]string(nil), dNoMoreHeaders...)
			mu.Unlock()
			if len(gotHeaders) != tt.wantDCalls {
				t.Fatalf("D calls = %d, want %d; %s headers = %v", len(gotHeaders), tt.wantDCalls, HeaderNoMoreRetry, gotHeaders)
			}
			if tt.wantNoMore {
				found := false
				for _, value := range gotHeaders {
					if parseBoolHeader(value) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("D never received %s=true; headers = %v", HeaderNoMoreRetry, gotHeaders)
				}
			}
		})
	}
}

func newRetryBudgetChainClient(t *testing.T, capacity, retryCost int) *stdhttp.Client {
	t.Helper()
	provider := newRetryBudgetTransportProvider(t, capacity, retryCost, 1)
	return NewClientWithOptions(ClientOptions{ConfigProvider: provider})
}

func newRetryBudgetChainServer(t *testing.T, client *stdhttp.Client, nextURL string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		ctx, cancel := ContextWithRetryConstraints(r.Context(), r.Header)
		defer cancel()
		outbound, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, nextURL, nil)
		if err != nil {
			t.Errorf("NewRequestWithContext() error = %v", err)
			w.WriteHeader(stdhttp.StatusInternalServerError)
			return
		}
		resp, err := client.Do(outbound)
		if err != nil {
			t.Errorf("client.Do() error = %v", err)
			w.WriteHeader(stdhttp.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
}
