package http

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseSetupArgsSupportsSeparateFlags(t *testing.T) {
	options := parseSetupArgs([]string{
		"--env", "staging",
		"--name", "shop",
		"--consul", "consul-${profile}.example.com:8500",
		"--port", "7309",
	})
	if options.Env != "staging" {
		t.Fatalf("Env = %q, want staging", options.Env)
	}
	if options.Name != "shop" {
		t.Fatalf("Name = %q, want shop", options.Name)
	}
	if options.Consul != "consul-${profile}.example.com:8500" {
		t.Fatalf("Consul = %q, want consul-${profile}.example.com:8500", options.Consul)
	}
}

func TestParseSetupArgsSupportsEqualFlags(t *testing.T) {
	options := parseSetupArgs([]string{
		"--env=staging",
		"--name=shop",
		"--consul=http://consul-staging.example.com:8500/",
	})
	if options.Env != "staging" {
		t.Fatalf("Env = %q, want staging", options.Env)
	}
	if options.Name != "shop" {
		t.Fatalf("Name = %q, want shop", options.Name)
	}
	if options.Consul != "http://consul-staging.example.com:8500/" {
		t.Fatalf("Consul = %q, want full endpoint", options.Consul)
	}
}

func TestSetupLoadsRetryConfigFromArgs(t *testing.T) {
	oldArgs := os.Args
	oldSetup := setupRetryConfigFromConsul
	t.Cleanup(func() {
		os.Args = oldArgs
		setupRetryConfigFromConsul = oldSetup
		clearCallerServiceName()
	})

	os.Args = []string{
		"shop",
		"--env", "staging",
		"--name", "shop",
		"--consul", "consul-${profile}.example.com:8500",
		"--port", "7309",
	}

	called := false
	setupRetryConfigFromConsul = func(env, appName, endpoint string) (*ManagedRetryConfigProvider, error) {
		called = true
		if env != "staging" {
			t.Fatalf("env = %q, want staging", env)
		}
		if appName != "shop" {
			t.Fatalf("appName = %q, want shop", appName)
		}
		if endpoint != "consul-${profile}.example.com:8500" {
			t.Fatalf("endpoint = %q, want consul-${profile}.example.com:8500", endpoint)
		}
		return NewManagedRetryConfigProvider(), nil
	}

	if err := Setup(); err != nil {
		t.Fatalf("Setup() error = %v", err)
	}
	if !called {
		t.Fatalf("setupRetryConfigFromConsul not called")
	}
	if CallerServiceName() != "shop" {
		t.Fatalf("CallerServiceName() = %q, want shop", CallerServiceName())
	}
}

func TestSetupWithOptionsLoadsRetryConfigWithoutArgs(t *testing.T) {
	oldSetup := setupRetryConfigFromConsul
	t.Cleanup(func() {
		setupRetryConfigFromConsul = oldSetup
		clearCallerServiceName()
	})

	called := false
	setupRetryConfigFromConsul = func(env, appName, endpoint string) (*ManagedRetryConfigProvider, error) {
		called = true
		if env != "staging" {
			t.Fatalf("env = %q, want staging", env)
		}
		if appName != "shop" {
			t.Fatalf("appName = %q, want shop", appName)
		}
		if endpoint != "consul-staging.example.com:8500" {
			t.Fatalf("endpoint = %q, want consul-staging.example.com:8500", endpoint)
		}
		return NewManagedRetryConfigProvider(), nil
	}

	err := SetupWithOptions(SetupOptions{
		Env:    "staging",
		Name:   "shop",
		Consul: "consul-staging.example.com:8500",
	})
	if err != nil {
		t.Fatalf("SetupWithOptions() error = %v", err)
	}
	if !called {
		t.Fatal("setupRetryConfigFromConsul not called")
	}
	if got := CallerServiceName(); got != "shop" {
		t.Fatalf("CallerServiceName() = %q, want shop", got)
	}
}

func TestSetupWithOptionsWithoutConsulOnlySetsCaller(t *testing.T) {
	oldSetup := setupRetryConfigFromConsul
	t.Cleanup(func() {
		setupRetryConfigFromConsul = oldSetup
		clearCallerServiceName()
	})

	called := false
	setupRetryConfigFromConsul = func(env, appName, endpoint string) (*ManagedRetryConfigProvider, error) {
		called = true
		return NewManagedRetryConfigProvider(), nil
	}

	err := SetupWithOptions(SetupOptions{Name: "shop"})
	if err != nil {
		t.Fatalf("SetupWithOptions() error = %v", err)
	}
	if called {
		t.Fatal("setupRetryConfigFromConsul called unexpectedly")
	}
	if got := CallerServiceName(); got != "shop" {
		t.Fatalf("CallerServiceName() = %q, want shop", got)
	}
}

func TestNewClientWithOptionsDoesNotMutateSharedClient(t *testing.T) {
	sharedBefore := Instance()
	customProvider := NewManagedRetryConfigProvider()
	customObserver := &testRetryObserver{}
	called := false
	customRoundTripper := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
			Request:    req,
		}, nil
	})

	client := NewClientWithOptions(ClientOptions{
		RoundTripper:   customRoundTripper,
		ConfigProvider: customProvider,
		Observer:       customObserver,
		RequestTimeout: 3 * time.Second,
	})

	if client == sharedBefore {
		t.Fatal("NewClientWithOptions() returned shared client")
	}
	transport, ok := client.Transport.(*Transport)
	if !ok {
		t.Fatalf("client.Transport type = %T, want *Transport", client.Transport)
	}
	if transport.ConfigProvider != customProvider {
		t.Fatal("transport.ConfigProvider not applied")
	}
	if transport.Observer != customObserver {
		t.Fatal("transport.Observer not applied")
	}
	if client.Timeout != 3*time.Second {
		t.Fatalf("client.Timeout = %s, want 3s", client.Timeout)
	}
	req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if err != nil {
		t.Fatalf("http.NewRequest() error = %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do() error = %v", err)
	}
	resp.Body.Close()
	if !called {
		t.Fatal("custom round tripper not used")
	}
	if Instance() != sharedBefore {
		t.Fatal("shared client mutated unexpectedly")
	}
}

func TestRespHandlerSupportsExpectedPtr(t *testing.T) {
	var body struct {
		Message string `json:"message"`
	}
	result := ResponsePtr{ExpectedPtr: &body}
	resp := &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"message":"invalid"}`)),
	}

	if err := RespHandler(resp, &result); err != nil {
		t.Fatalf("RespHandler() error = %v", err)
	}
	if result.StatusCode != http.StatusBadRequest {
		t.Fatalf("result.StatusCode = %d, want %d", result.StatusCode, http.StatusBadRequest)
	}
	if body.Message != "invalid" {
		t.Fatalf("body.Message = %q, want invalid", body.Message)
	}
}

func TestRespHandlerDrainsSmallBodyWhenNoTargetIsProvided(t *testing.T) {
	body := &trackingReadCloser{reader: strings.NewReader(`{"ignored":true}`)}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       body,
	}

	if err := RespHandler(resp, nil); err != nil {
		t.Fatalf("RespHandler() error = %v", err)
	}
	if !body.sawEOF.Load() {
		t.Fatal("RespHandler() did not drain the response body to EOF")
	}
	if got := body.closes.Load(); got != 1 {
		t.Fatalf("body closes = %d, want 1", got)
	}
}

func TestRespHandlerSupportsStatusOnlyResponsePtr(t *testing.T) {
	result := ResponsePtr{}
	resp := &http.Response{
		StatusCode: http.StatusAccepted,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"ignored":true}`)),
	}

	if err := RespHandler(resp, &result); err != nil {
		t.Fatalf("RespHandler() error = %v", err)
	}
	if result.StatusCode != http.StatusAccepted {
		t.Fatalf("StatusCode = %d, want %d", result.StatusCode, http.StatusAccepted)
	}
}

func TestNewTransportUsesDefaultPolicyResolver(t *testing.T) {
	transport := NewTransport()
	if transport == nil {
		t.Fatal("NewTransport() = nil")
	}
	if transport.ResolvePolicy == nil {
		t.Fatal("transport.ResolvePolicy = nil")
	}
	if transport.RoundTripper == nil {
		t.Fatal("transport.RoundTripper = nil")
	}
}

func TestSetRetryConfigProviderClearsPreviousRequestClassResolver(t *testing.T) {
	oldTransport := cli.Transport
	t.Cleanup(func() {
		cli.Transport = oldTransport
		ClearRequestClassResolver()
	})

	managed, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
internal_hosts: ["internal.example.com"]
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}
	SetRetryConfigProvider(managed)
	SetRetryConfigProvider(RetryConfigProviderFunc(func(_ context.Context, _ RetryConfigScope, base RetryPolicy) RetryPolicy {
		return base
	}))

	req, err := http.NewRequest(http.MethodGet, "http://public.example.com/orders", nil)
	if err != nil {
		t.Fatalf("http.NewRequest() error = %v", err)
	}
	if got := requestClassFromContextOrMethod(req); got != RequestClassInternalRead {
		t.Fatalf("request class = %q, want %q after replacing provider", got, RequestClassInternalRead)
	}
}
