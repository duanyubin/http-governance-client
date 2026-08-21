package http

import (
	"context"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	liveConsulEndpointEnv = "HTTP_GOVERNANCE_LIVE_CONSUL_ENDPOINT"
	liveConsulServiceEnv  = "HTTP_GOVERNANCE_LIVE_CONSUL_SERVICE"
	liveConsulProfileEnv  = "HTTP_GOVERNANCE_LIVE_CONSUL_PROFILE"
)

func TestLiveConsulLoadRetryConfig(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv(liveConsulEndpointEnv))
	if endpoint == "" {
		t.Skipf("set %s to run the live Consul test", liveConsulEndpointEnv)
	}
	service := strings.TrimSpace(os.Getenv(liveConsulServiceEnv))
	if service == "" {
		service = "orders-api"
	}

	provider, err := NewManagedRetryConfigProviderFromConsul(
		strings.TrimSpace(os.Getenv(liveConsulProfileEnv)),
		service,
		endpoint,
	)
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromConsul() error = %v", err)
	}
	if provider == nil {
		t.Fatal("provider = nil")
	}
}

func TestPublishedRetryConfigExamplesLoad(t *testing.T) {
	files := []string{
		filepath.Join("examples", "retry.minimal.yaml"),
		filepath.Join("examples", "consul", "application.retry.yaml"),
		filepath.Join("examples", "consul", "service.retry.yaml"),
	}
	for _, filename := range files {
		data, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("ReadFile(%q) error = %v", filename, err)
		}
		if _, err := NewManagedRetryConfigProviderFromYAML(data); err != nil {
			t.Fatalf("NewManagedRetryConfigProviderFromYAML(%q) error = %v", filename, err)
		}
	}
}

func TestMergedPublishedRetryConfigExample(t *testing.T) {
	application := readRetryConfigExample(t, filepath.Join("examples", "consul", "application.retry.yaml"))
	service := readRetryConfigExample(t, filepath.Join("examples", "consul", "service.retry.yaml"))

	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(mergeRetryConfigFiles(application, service)); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	SetCallerServiceName("orders-api")
	t.Cleanup(clearCallerServiceName)

	req, err := stdhttp.NewRequestWithContext(
		context.Background(),
		stdhttp.MethodPost,
		"http://payments.svc.cluster.local/v1/payments",
		strings.NewReader(`{"idempotency_key":"payment-1001"}`),
	)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	scope, policy, _ := provider.resolveRequestConfig(req, DefaultRetryPolicyResolver(req))

	if scope.Class != RequestClassInternalWrite {
		t.Fatalf("scope.Class = %q, want %q", scope.Class, RequestClassInternalWrite)
	}
	if scope.Caller != "orders-api" || scope.Downstream != "payments" || scope.Operation != "createPayment" {
		t.Fatalf("scope = %+v, want orders-api/payments/createPayment", scope)
	}
	if policy.MaxRetries != 1 {
		t.Fatalf("policy.MaxRetries = %d, want 1", policy.MaxRetries)
	}
}

func readRetryConfigExample(t *testing.T, filename string) RetryConfigFile {
	t.Helper()
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", filename, err)
	}
	var file RetryConfigFile
	if err := decodeRetryConfigYAML(data, &file); err != nil {
		t.Fatalf("decodeRetryConfigYAML(%q) error = %v", filename, err)
	}
	return file
}
