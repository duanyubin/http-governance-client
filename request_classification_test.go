package http

import (
	"context"
	stdhttp "net/http"
	"testing"
)

func TestManagedRetryConfigProviderResolvesInternalHost(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		InternalHosts: []string{"*.svc.cluster.local", "billing", "billing:8080"},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	SetRequestClassResolver(provider)
	t.Cleanup(func() {
		ClearRequestClassResolver()
	})

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://billing:8080/api/order/query", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	class := requestClassFromContextOrMethod(req)
	if class != RequestClassInternalRead {
		t.Fatalf("class = %s, want %s", class, RequestClassInternalRead)
	}
}

func TestManagedRetryConfigProviderFallsBackToExternal(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		InternalHosts: []string{"*.svc.cluster.local", "billing"},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	SetRequestClassResolver(provider)
	t.Cleanup(func() {
		ClearRequestClassResolver()
	})

	req, err := stdhttp.NewRequest(stdhttp.MethodPost, "https://ace.qq.com/api/query", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	class := requestClassFromContextOrMethod(req)
	if class != RequestClassExternalWrite {
		t.Fatalf("class = %s, want %s", class, RequestClassExternalWrite)
	}
}

func TestExplicitRequestClassOverridesConfigClassification(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		InternalHosts: []string{"*.svc.cluster.local", "billing"},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	SetRequestClassResolver(provider)
	t.Cleanup(func() {
		ClearRequestClassResolver()
	})

	ctx := WithExternalRead(context.Background())
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://billing/api/order/query", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	class := requestClassFromContextOrMethod(req)
	if class != RequestClassExternalRead {
		t.Fatalf("class = %s, want %s", class, RequestClassExternalRead)
	}
}

func TestRetryConfigScopeClassOverridesConfigClassification(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		InternalHosts: []string{"*.svc.cluster.local", "billing"},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}
	SetRequestClassResolver(provider)
	t.Cleanup(func() {
		ClearRequestClassResolver()
	})

	ctx := WithRetryConfigScope(context.Background(), RetryConfigScope{
		Class: RequestClassExternalRead,
	})
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://billing/api/order/query", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}

	class := requestClassFromContextOrMethod(req)
	if class != RequestClassExternalRead {
		t.Fatalf("class = %s, want %s", class, RequestClassExternalRead)
	}
}
