package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeRetryConfigStore struct {
	mu          sync.Mutex
	data        map[string][]byte
	errs        map[string]error
	getFailures map[string]int
	calls       []string
}

type fakeBlockingResponse struct {
	data  []byte
	index uint64
	err   error
}

type fakeBlockingRetryConfigStore struct {
	fakeRetryConfigStore
	mu        sync.Mutex
	responses map[string][]fakeBlockingResponse
	calls     map[string]int
}

func (s *fakeRetryConfigStore) Get(path string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, path)
	if s.getFailures[path] > 0 {
		s.getFailures[path]--
		return nil, errors.New("temporary load failure")
	}
	if err, ok := s.errs[path]; ok {
		return nil, err
	}
	if value, ok := s.data[path]; ok {
		return value, nil
	}
	return nil, errors.New("not found")
}

type alwaysMissingBlockingStore struct {
	calls atomic.Int32
}

func (s *alwaysMissingBlockingStore) Get(string) ([]byte, error) {
	return nil, errors.New("not found")
}

func (s *alwaysMissingBlockingStore) GetWithIndex(context.Context, string, uint64, time.Duration) ([]byte, uint64, error) {
	s.calls.Add(1)
	return nil, 0, errors.New("not found")
}

func (s *alwaysMissingBlockingStore) NotFound(string, error) bool {
	return true
}

type contextBlockingRetryConfigStore struct {
	entered    chan struct{}
	exited     chan struct{}
	releaseGet chan struct{}
	enterOnce  sync.Once
	exitOnce   sync.Once
}

func (s *contextBlockingRetryConfigStore) Get(string) ([]byte, error) {
	s.enterOnce.Do(func() { close(s.entered) })
	<-s.releaseGet
	return nil, context.Canceled
}

func (s *contextBlockingRetryConfigStore) GetContext(ctx context.Context, _ string) ([]byte, error) {
	s.enterOnce.Do(func() { close(s.entered) })
	<-ctx.Done()
	s.exitOnce.Do(func() { close(s.exited) })
	return nil, ctx.Err()
}

func (s *fakeRetryConfigStore) Set(path string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string][]byte)
	}
	s.data[path] = value
}

func (s *fakeRetryConfigStore) Delete(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, path)
}

func (s *fakeBlockingRetryConfigStore) GetWithIndex(ctx context.Context, path string, index uint64, wait time.Duration) ([]byte, uint64, error) {
	s.mu.Lock()
	if s.calls == nil {
		s.calls = make(map[string]int)
	}
	callIndex := s.calls[path]
	s.calls[path] = callIndex + 1
	sequence := s.responses[path]
	if callIndex >= len(sequence) {
		s.mu.Unlock()
		<-ctx.Done()
		return nil, 0, ctx.Err()
	}
	result := sequence[callIndex]
	s.mu.Unlock()
	return result.data, result.index, result.err
}

func TestResolveConsulEndpoint(t *testing.T) {
	got := resolveConsulEndpoint("staging", "consul-${profile}.example.com:8500")
	want := "http://consul-staging.example.com:8500"
	if got != want {
		t.Fatalf("resolveConsulEndpoint() = %q, want %q", got, want)
	}

	got = resolveConsulEndpoint("staging", "http://consul-staging.example.com:8500/")
	want = "http://consul-staging.example.com:8500"
	if got != want {
		t.Fatalf("resolveConsulEndpoint() = %q, want %q", got, want)
	}
}

func TestSanitizeConsulEndpointForLogRemovesCredentialsAndQuery(t *testing.T) {
	got := sanitizeConsulEndpointForLog("http://user:password@consul.example.com:8500/base?token=secret")
	if got != "http://consul.example.com:8500/base" {
		t.Fatalf("sanitizeConsulEndpointForLog() = %q", got)
	}
}

func TestLoadRetryConfigFileFromStoreRejectsDuplicatesBeforeMerge(t *testing.T) {
	store := &fakeRetryConfigStore{
		data: map[string][]byte{
			"config/go/application/retry": []byte(`
version: v1
downstreams:
  - name: orders
    hosts: ["orders-a.example.com"]
  - name: orders
    hosts: ["orders-b.example.com"]
`),
		},
	}
	if _, err := loadRetryConfigFileFromStore(store, "orders-api"); err == nil {
		t.Fatal("loadRetryConfigFileFromStore() error = nil, want duplicate-name error")
	}
}

func TestLoadRetryConfigFileFromStorePreservesEmptyRetryStatusesOverride(t *testing.T) {
	store := &fakeRetryConfigStore{data: map[string][]byte{
		"config/go/application/retry": []byte(`
version: v1
policies:
  internal_read:
    retry_on_statuses: [503]
`),
		"config/go/shop/retry": []byte(`
version: v1
policies:
  internal_read:
    retry_on_statuses: []
`),
	}}

	file, err := loadRetryConfigFileFromStore(store, "shop")
	if err != nil {
		t.Fatalf("loadRetryConfigFileFromStore() error = %v", err)
	}
	statuses := file.Policies.InternalRead.RetryOnStatuses
	if statuses == nil {
		t.Fatal("RetryOnStatuses = nil, want explicit empty slice")
	}
	if len(statuses) != 0 {
		t.Fatalf("RetryOnStatuses = %v, want empty", statuses)
	}
}

func TestLoadRetryConfigFileFromStoreMergesRetryBudgetFields(t *testing.T) {
	store := &fakeRetryConfigStore{data: map[string][]byte{
		"config/go/application/retry": []byte(`
version: v1
retry_budget:
  enabled: true
  capacity: 20
  retry_cost: 10
  success_increment: 1
`),
		"config/go/shop/retry": []byte(`
version: v1
retry_budget:
  capacity: 8
`),
	}}

	file, err := loadRetryConfigFileFromStore(store, "shop")
	if err != nil {
		t.Fatalf("loadRetryConfigFileFromStore() error = %v", err)
	}
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(file); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://billing.example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	_, _, budget := provider.resolveRequestConfig(req, RetryPolicy{})
	want := retryBudgetConfig{Enabled: true, Capacity: 8, RetryCost: 10, SuccessIncrement: 1}
	if budget != want {
		t.Fatalf("retry budget = %+v, want %+v", budget, want)
	}
}

func TestLoadRetryConfigFileFromStoreRejectsInvalidRetryBudgetBeforeMerge(t *testing.T) {
	store := &fakeRetryConfigStore{data: map[string][]byte{
		"config/go/application/retry": []byte("version: v1\nretry_budget:\n  capacity: 0\n"),
		"config/go/shop/retry":        []byte("version: v1\nretry_budget:\n  capacity: 8\n"),
	}}
	if _, err := loadRetryConfigFileFromStore(store, "shop"); err == nil {
		t.Fatal("loadRetryConfigFileFromStore() error = nil, want invalid application layer error")
	}
}

func TestRetryBudgetStateSurvivesManagedConfigReload(t *testing.T) {
	config := func(enabled bool, capacity int) []byte {
		return []byte(fmt.Sprintf(`
version: v1
retry_budget:
  enabled: %t
  capacity: %d
  retry_cost: 1
  success_increment: 1
policies:
  external_read:
    max_retries: 1
    initial_backoff: 1ms
    max_backoff: 1ms
    retry_on_statuses: [503]
`, enabled, capacity))
	}
	store := &fakeRetryConfigStore{}
	store.Set("config/go/application/retry", config(true, 1))
	provider := NewManagedRetryConfigProvider()
	if err := reloadRetryConfigFromStore(context.Background(), provider, store, "shop"); err != nil {
		t.Fatalf("initial reloadRetryConfigFromStore() error = %v", err)
	}

	var attempts atomic.Int32
	var responseStatus atomic.Int32
	responseStatus.Store(http.StatusServiceUnavailable)
	transport := &Transport{
		ConfigProvider: provider,
		RoundTripper: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			attempts.Add(1)
			return testHTTPResponse(req, int(responseStatus.Load())), nil
		}),
	}
	doRequest := func() int32 {
		t.Helper()
		before := attempts.Load()
		req, err := http.NewRequest(http.MethodGet, "http://billing.example.com/orders", nil)
		if err != nil {
			t.Fatalf("NewRequest() error = %v", err)
		}
		resp, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip() error = %v", err)
		}
		_ = resp.Body.Close()
		return attempts.Load() - before
	}
	reload := func(enabled bool, capacity int) {
		t.Helper()
		store.Set("config/go/application/retry", config(enabled, capacity))
		if err := reloadRetryConfigFromStore(context.Background(), provider, store, "shop"); err != nil {
			t.Fatalf("reloadRetryConfigFromStore() error = %v", err)
		}
	}

	if got := doRequest(); got != 2 {
		t.Fatalf("initial request attempts = %d, want 2", got)
	}
	reload(true, 1)
	if got := doRequest(); got != 1 {
		t.Fatalf("attempts after identical reload = %d, want 1", got)
	}
	reload(false, 1)
	if got := doRequest(); got != 2 {
		t.Fatalf("attempts while budget disabled = %d, want 2", got)
	}
	reload(true, 1)
	if got := doRequest(); got != 1 {
		t.Fatalf("attempts after re-enable = %d, want retained exhausted state", got)
	}
	reload(true, 2)
	if got := doRequest(); got != 1 {
		t.Fatalf("attempts after capacity growth = %d, want no refill", got)
	}

	responseStatus.Store(http.StatusOK)
	if got := doRequest(); got != 1 {
		t.Fatalf("successful request attempts = %d, want 1", got)
	}
	responseStatus.Store(http.StatusServiceUnavailable)
	if got := doRequest(); got != 2 {
		t.Fatalf("attempts after success increment = %d, want 2", got)
	}

	reload(true, 5)
	responseStatus.Store(http.StatusOK)
	for i := 0; i < 4; i++ {
		doRequest()
	}
	reload(true, 1)
	responseStatus.Store(http.StatusServiceUnavailable)
	if got := doRequest(); got != 2 {
		t.Fatalf("attempts after capacity shrink = %d, want 2", got)
	}
	if got := doRequest(); got != 1 {
		t.Fatalf("attempts after consuming clamped balance = %d, want 1", got)
	}
}

func TestLoadRetryConfigFileFromStoreRejectsUnknownYAMLFields(t *testing.T) {
	store := &fakeRetryConfigStore{data: map[string][]byte{
		"config/go/application/retry": []byte(`
version: v1
policies:
  internal_read:
    max_retry: 1
`),
	}}

	_, err := loadRetryConfigFileFromStore(store, "shop")
	if err == nil {
		t.Fatal("loadRetryConfigFileFromStore() error = nil, want unknown field error")
	}
}

func TestWithConsulBlockingQuery(t *testing.T) {
	got, err := withConsulBlockingQuery("http://consul-staging.example.com:8500/v1/kv/config/go/shop/retry?raw=", 12, 10*time.Second)
	if err != nil {
		t.Fatalf("withConsulBlockingQuery() error = %v", err)
	}
	want := "http://consul-staging.example.com:8500/v1/kv/config/go/shop/retry?index=12&raw=&wait=10s"
	if got != want {
		t.Fatalf("withConsulBlockingQuery() = %q, want %q", got, want)
	}
}

func TestParseConsulIndex(t *testing.T) {
	if got := parseConsulIndex("12345"); got != 12345 {
		t.Fatalf("parseConsulIndex() = %d, want 12345", got)
	}
	if got := parseConsulIndex("invalid"); got != 0 {
		t.Fatalf("parseConsulIndex() = %d, want 0 for invalid value", got)
	}
}

func TestWaitForRetryConfigChangeDetectsFirstCreatedServiceKey(t *testing.T) {
	store := &fakeBlockingRetryConfigStore{
		fakeRetryConfigStore: fakeRetryConfigStore{
			data: map[string][]byte{
				"config/go/application/retry": []byte("version: v1\n"),
			},
		},
		responses: map[string][]fakeBlockingResponse{
			"config/go/application/retry": {
				{data: []byte("version: v1\n"), index: 1},
				{data: []byte("version: v1\n"), index: 1},
			},
			"config/go/shop/retry": {
				{err: errors.New("not found")},
				{data: []byte("version: v1\npolicies:\n  internal_read:\n    max_retries: 3\n"), index: 2},
			},
		},
	}

	states := make(map[string]retryConfigWatchState)
	changed, err := waitForRetryConfigChange(context.Background(), store, "shop", states, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("waitForRetryConfigChange() first call error = %v", err)
	}
	if changed {
		t.Fatal("waitForRetryConfigChange() first call = true, want false")
	}

	changed, err = waitForRetryConfigChange(context.Background(), store, "shop", states, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("waitForRetryConfigChange() second call error = %v", err)
	}
	if !changed {
		t.Fatal("waitForRetryConfigChange() second call = false, want true")
	}
}

func TestLoadRetryConfigFileFromStoreRespectsPriority(t *testing.T) {
	store := &fakeRetryConfigStore{
		data: map[string][]byte{
			"config/go/application/retry": []byte(`
version: v1
internal_hosts:
  - "billing.*.svc.example.com"
downstreams:
  - name: "genericqq"
    hosts: ["*.qq.com"]
  - name: "billing"
    hosts: ["billing.*.svc.example.com"]
policies:
  internal_read:
    max_retries: 1
    per_attempt_timeout: 2s
rules:
  - name: "common-internal-read"
    priority: 10
    match:
      classes: ["internal_read"]
    policy:
      initial_backoff: 100ms
`),
			"config/go/shop/retry": []byte(`
version: v1
internal_hosts:
  - "shop.*.svc.example.com"
downstreams:
  - name: "midas"
    hosts: ["sandbox.api.unipay.qq.com"]
  - name: "genericqq"
    hosts: ["sandbox.api.unipay.qq.com"]
operations:
  - name: "queryOrder"
    priority: 100
    methods: ["GET"]
    downstreams: ["billing"]
    paths: ["/api/order/query"]
policies:
  internal_read:
    max_retries: 2
rules:
  - name: "shop-query-order"
    priority: 100
    match:
      callers: ["shop"]
      downstreams: ["billing"]
      classes: ["internal_read"]
    policy:
      max_retries: 3
`),
		},
	}

	file, err := loadRetryConfigFileFromStore(store, "shop")
	if err != nil {
		t.Fatalf("loadRetryConfigFileFromStore() error = %v", err)
	}
	if len(store.calls) != 2 {
		t.Fatalf("store calls = %d, want 2", len(store.calls))
	}
	if store.calls[0] != "config/go/application/retry" || store.calls[1] != "config/go/shop/retry" {
		t.Fatalf("store calls = %v, want application then shop", store.calls)
	}
	if file.Policies.InternalRead == nil || file.Policies.InternalRead.MaxRetries == nil || *file.Policies.InternalRead.MaxRetries != 2 {
		t.Fatalf("internal_read.max_retries not overridden by shop config: %+v", file.Policies.InternalRead)
	}
	if len(file.InternalHosts) != 2 {
		t.Fatalf("internal_hosts len = %d, want 2", len(file.InternalHosts))
	}
	if len(file.Downstreams) < 2 || file.Downstreams[0].Name != "midas" {
		t.Fatalf("downstreams[0] = %+v, want service-specific midas first", file.Downstreams)
	}
	if len(file.Rules) == 0 || file.Rules[0].Name != "shop-query-order" {
		t.Fatalf("rules[0] = %+v, want service-specific rule first", file.Rules)
	}

	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(file); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	base := RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        200 * time.Millisecond,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}
	policy := provider.ResolveRetryPolicy(context.Background(), RetryConfigScope{
		Caller:     "shop",
		Downstream: "billing",
		Class:      RequestClassInternalRead,
		Method:     "GET",
	}, base)
	if policy.MaxRetries != 3 {
		t.Fatalf("policy.MaxRetries = %d, want 3", policy.MaxRetries)
	}
}

func TestLoadRetryConfigFileFromStoreAllowsServiceToDisableApplicationRetries(t *testing.T) {
	store := &fakeRetryConfigStore{data: map[string][]byte{
		"config/go/application/retry": []byte(`
version: v1
policies:
  internal_read:
    max_retries: 2
`),
		"config/go/shop/retry": []byte(`
version: v1
policies:
  internal_read:
    disable_retry: true
`),
	}}

	file, err := loadRetryConfigFileFromStore(store, "shop")
	if err != nil {
		t.Fatalf("loadRetryConfigFileFromStore() error = %v", err)
	}
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(file); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	policy := provider.ResolveRetryPolicy(context.Background(), RetryConfigScope{
		Class:  RequestClassInternalRead,
		Method: "GET",
	}, RetryPolicy{MaxRetries: 1})
	if policy.MaxRetries != 0 {
		t.Fatalf("policy.MaxRetries = %d, want 0", policy.MaxRetries)
	}
}

func TestLoadRetryConfigFileFromStoreAllowsServiceToEnableApplicationDisabledRetries(t *testing.T) {
	store := &fakeRetryConfigStore{data: map[string][]byte{
		"config/go/application/retry": []byte(`
version: v1
policies:
  internal_read:
    disable_retry: true
`),
		"config/go/shop/retry": []byte(`
version: v1
policies:
  internal_read:
    max_retries: 2
`),
	}}

	file, err := loadRetryConfigFileFromStore(store, "shop")
	if err != nil {
		t.Fatalf("loadRetryConfigFileFromStore() error = %v", err)
	}
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(file); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	policy := provider.ResolveRetryPolicy(context.Background(), RetryConfigScope{
		Class:  RequestClassInternalRead,
		Method: "GET",
	}, RetryPolicy{MaxRetries: 1})
	if policy.MaxRetries != 2 {
		t.Fatalf("policy.MaxRetries = %d, want 2", policy.MaxRetries)
	}
}

func TestLoadRetryConfigFileFromStoreIgnoresMissingOptionalKey(t *testing.T) {
	store := &fakeRetryConfigStore{
		data: map[string][]byte{
			"config/go/application/retry": []byte("version: v1\n"),
		},
	}

	if _, err := loadRetryConfigFileFromStore(store, "shop"); err != nil {
		t.Fatalf("loadRetryConfigFileFromStore() error = %v, want nil", err)
	}
}

func TestLoadRetryConfigFileFromStoreFailsOnStoreError(t *testing.T) {
	store := &fakeRetryConfigStore{
		data: map[string][]byte{
			"config/go/application/retry": []byte("version: v1\n"),
		},
		errs: map[string]error{
			"config/go/shop/retry": errors.New("connection refused"),
		},
	}

	_, err := loadRetryConfigFileFromStore(store, "shop")
	if err == nil {
		t.Fatal("loadRetryConfigFileFromStore() error = nil, want non-nil")
	}
	if err.Error() != `load retry config from "config/go/shop/retry": connection refused` {
		t.Fatalf("unexpected error = %v", err)
	}
}

func TestStartRetryConfigAutoReloadFromConsulRefreshesProvider(t *testing.T) {
	oldStoreFactory := newRetryConfigStore
	oldInterval := RetryConfigAutoReloadInterval()
	t.Cleanup(func() {
		StopRetryConfigAutoReload()
		newRetryConfigStore = oldStoreFactory
		SetRetryConfigAutoReloadInterval(oldInterval)
	})

	store := &fakeRetryConfigStore{
		data: map[string][]byte{
			"config/go/application/retry": []byte(`
version: v1
policies:
  internal_read:
    max_retries: 1
`),
			"config/go/shop/retry": []byte("version: v1\n"),
		},
	}
	newRetryConfigStore = func(env, endpoint string) (retryConfigStore, error) {
		return store, nil
	}
	SetRetryConfigAutoReloadInterval(10 * time.Millisecond)

	provider := NewManagedRetryConfigProvider()
	if err := provider.UpdateFromConsul("staging", "shop", "consul-staging.example.com:8500"); err != nil {
		t.Fatalf("provider.UpdateFromConsul() error = %v", err)
	}

	base := RetryPolicy{
		MaxRetries:        0,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        200 * time.Millisecond,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}
	scope := RetryConfigScope{
		Caller:     "shop",
		Downstream: "billing",
		Class:      RequestClassInternalRead,
		Method:     "GET",
	}
	if policy := provider.ResolveRetryPolicy(context.Background(), scope, base); policy.MaxRetries != 1 {
		t.Fatalf("policy.MaxRetries = %d, want 1", policy.MaxRetries)
	}

	if err := StartRetryConfigAutoReloadFromConsul(provider, "staging", "shop", "consul-staging.example.com:8500"); err != nil {
		t.Fatalf("StartRetryConfigAutoReloadFromConsul() error = %v", err)
	}

	store.Set("config/go/shop/retry", []byte(`
version: v1
policies:
  internal_read:
    max_retries: 3
`))

	waitForCondition(t, 500*time.Millisecond, func() bool {
		return provider.ResolveRetryPolicy(context.Background(), scope, base).MaxRetries == 3
	})
}

func TestBlockingReloadRetriesConsumedChangeAfterTransientLoadFailure(t *testing.T) {
	store := &fakeBlockingRetryConfigStore{
		fakeRetryConfigStore: fakeRetryConfigStore{
			data: map[string][]byte{
				"config/go/application/retry": []byte("version: v1\n"),
				"config/go/shop/retry": []byte(`
version: v1
policies:
  internal_read:
    max_retries: 3
`),
			},
			getFailures: map[string]int{
				"config/go/application/retry": 1,
			},
		},
		responses: map[string][]fakeBlockingResponse{
			"config/go/application/retry": {
				{data: []byte("version: v1\n"), index: 1},
				{data: []byte("version: v1\n"), index: 1},
			},
			"config/go/shop/retry": {
				{data: []byte("version: v1\n"), index: 1},
				{data: []byte("version: v1\n"), index: 2},
			},
		},
	}
	provider := NewManagedRetryConfigProvider()
	if err := provider.UpdateFromYAML([]byte(`
version: v1
policies:
  internal_read:
    max_retries: 1
`)); err != nil {
		t.Fatalf("provider.UpdateFromYAML() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runRetryConfigBlockingReloadLoop(ctx, provider, store, "shop", 5*time.Millisecond)

	waitForCondition(t, 500*time.Millisecond, func() bool {
		policy := provider.ResolveRetryPolicy(context.Background(), RetryConfigScope{
			Class:  RequestClassInternalRead,
			Method: "GET",
		}, RetryPolicy{})
		return policy.MaxRetries == 3
	})
}

func TestBlockingReloadBacksOffWhenAllKeysAreMissing(t *testing.T) {
	store := &alwaysMissingBlockingStore{}
	ctx, cancel := context.WithCancel(context.Background())
	go runRetryConfigBlockingReloadLoop(ctx, NewManagedRetryConfigProvider(), store, "shop", 20*time.Millisecond)

	time.Sleep(65 * time.Millisecond)
	cancel()
	if calls := store.calls.Load(); calls > 8 {
		t.Fatalf("GetWithIndex() calls = %d, want at most 8 while keys remain missing", calls)
	}
}

func TestStopRetryConfigAutoReloadCancelsAndJoinsPollingLoad(t *testing.T) {
	oldInterval := RetryConfigAutoReloadInterval()
	store := &contextBlockingRetryConfigStore{
		entered:    make(chan struct{}),
		exited:     make(chan struct{}),
		releaseGet: make(chan struct{}),
	}
	defer close(store.releaseGet)
	t.Cleanup(func() {
		StopRetryConfigAutoReload()
		SetRetryConfigAutoReloadInterval(oldInterval)
	})
	SetRetryConfigAutoReloadInterval(time.Millisecond)
	if err := startRetryConfigAutoReloadFromStore(NewManagedRetryConfigProvider(), store, "", "shop", "test"); err != nil {
		t.Fatalf("startRetryConfigAutoReloadFromStore() error = %v", err)
	}
	select {
	case <-store.entered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("polling load did not start")
	}

	stopped := make(chan struct{})
	go func() {
		StopRetryConfigAutoReload()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StopRetryConfigAutoReload() did not return")
	}
	select {
	case <-store.exited:
	default:
		t.Fatal("StopRetryConfigAutoReload() returned before the polling load exited")
	}
}

func TestStartRetryConfigAutoReloadFromConsulFallsBackAfterServiceConfigDeletion(t *testing.T) {
	oldStoreFactory := newRetryConfigStore
	oldInterval := RetryConfigAutoReloadInterval()
	t.Cleanup(func() {
		StopRetryConfigAutoReload()
		newRetryConfigStore = oldStoreFactory
		SetRetryConfigAutoReloadInterval(oldInterval)
	})

	store := &fakeRetryConfigStore{
		data: map[string][]byte{
			"config/go/application/retry": []byte(`
version: v1
policies:
  internal_read:
    max_retries: 1
`),
			"config/go/shop/retry": []byte(`
version: v1
policies:
  internal_read:
    max_retries: 3
`),
		},
	}
	newRetryConfigStore = func(env, endpoint string) (retryConfigStore, error) {
		return store, nil
	}
	SetRetryConfigAutoReloadInterval(10 * time.Millisecond)

	provider := NewManagedRetryConfigProvider()
	if err := provider.UpdateFromConsul("staging", "shop", "consul-staging.example.com:8500"); err != nil {
		t.Fatalf("provider.UpdateFromConsul() error = %v", err)
	}

	base := RetryPolicy{
		MaxRetries:        0,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        200 * time.Millisecond,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}
	scope := RetryConfigScope{
		Caller:     "shop",
		Downstream: "billing",
		Class:      RequestClassInternalRead,
		Method:     "GET",
	}
	if policy := provider.ResolveRetryPolicy(context.Background(), scope, base); policy.MaxRetries != 3 {
		t.Fatalf("policy.MaxRetries = %d, want 3", policy.MaxRetries)
	}

	if err := StartRetryConfigAutoReloadFromConsul(provider, "staging", "shop", "consul-staging.example.com:8500"); err != nil {
		t.Fatalf("StartRetryConfigAutoReloadFromConsul() error = %v", err)
	}

	store.Delete("config/go/shop/retry")

	waitForCondition(t, 500*time.Millisecond, func() bool {
		return provider.ResolveRetryPolicy(context.Background(), scope, base).MaxRetries == 1
	})
}

func TestStartRetryConfigAutoReloadFromStoreDoesNotOverrideCallerServiceName(t *testing.T) {
	oldInterval := RetryConfigAutoReloadInterval()
	SetCallerServiceName("existing")
	t.Cleanup(func() {
		StopRetryConfigAutoReload()
		SetRetryConfigAutoReloadInterval(oldInterval)
		clearCallerServiceName()
	})

	SetRetryConfigAutoReloadInterval(0)

	provider := NewManagedRetryConfigProvider()
	store := &fakeRetryConfigStore{}
	if err := startRetryConfigAutoReloadFromStore(provider, store, "staging", "shop", "consul-staging.example.com:8500"); err != nil {
		t.Fatalf("startRetryConfigAutoReloadFromStore() error = %v", err)
	}
	if got := CallerServiceName(); got != "existing" {
		t.Fatalf("CallerServiceName() = %q, want existing", got)
	}
}

func TestStopRetryConfigAutoReloadStopsRefresh(t *testing.T) {
	oldStoreFactory := newRetryConfigStore
	oldInterval := RetryConfigAutoReloadInterval()
	t.Cleanup(func() {
		StopRetryConfigAutoReload()
		newRetryConfigStore = oldStoreFactory
		SetRetryConfigAutoReloadInterval(oldInterval)
	})

	store := &fakeRetryConfigStore{
		data: map[string][]byte{
			"config/go/application/retry": []byte(`
version: v1
policies:
  internal_read:
    max_retries: 1
`),
			"config/go/shop/retry": []byte("version: v1\n"),
		},
	}
	newRetryConfigStore = func(env, endpoint string) (retryConfigStore, error) {
		return store, nil
	}
	SetRetryConfigAutoReloadInterval(10 * time.Millisecond)

	provider := NewManagedRetryConfigProvider()
	if err := provider.UpdateFromConsul("staging", "shop", "consul-staging.example.com:8500"); err != nil {
		t.Fatalf("provider.UpdateFromConsul() error = %v", err)
	}
	if err := StartRetryConfigAutoReloadFromConsul(provider, "staging", "shop", "consul-staging.example.com:8500"); err != nil {
		t.Fatalf("StartRetryConfigAutoReloadFromConsul() error = %v", err)
	}
	StopRetryConfigAutoReload()

	store.Set("config/go/shop/retry", []byte(`
version: v1
policies:
  internal_read:
    max_retries: 4
`))

	base := RetryPolicy{
		MaxRetries:        0,
		PerAttemptTimeout: time.Second,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        200 * time.Millisecond,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}
	scope := RetryConfigScope{Class: RequestClassInternalRead, Method: "GET"}
	time.Sleep(50 * time.Millisecond)
	if policy := provider.ResolveRetryPolicy(context.Background(), scope, base); policy.MaxRetries != 1 {
		t.Fatalf("policy.MaxRetries = %d, want 1 after StopRetryConfigAutoReload", policy.MaxRetries)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
