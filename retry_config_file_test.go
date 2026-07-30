package http

import (
	"context"
	stdhttp "net/http"
	"testing"
	"time"
)

func TestManagedRetryConfigProviderFromYAML(t *testing.T) {
	data := []byte(`
version: v1
policies:
  external_read:
    max_retries: 1
    per_attempt_timeout: 7s
rules:
  - name: disable-query-order
    priority: 100
    match:
      caller: ["shop"]
      downstream: ["billing"]
      operation: ["queryOrder"]
      class: ["internal_read"]
    policy:
      disable_retry: true
`)

	provider, err := NewManagedRetryConfigProviderFromYAML(data)
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}

	base := RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: 2 * time.Second,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        400 * time.Millisecond,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}
	policy := provider.ResolveRetryPolicy(context.Background(), RetryConfigScope{
		Caller:     "shop",
		Downstream: "billing",
		Operation:  "queryOrder",
		Class:      RequestClassInternalRead,
		Method:     "GET",
	}, base)

	if policy.MaxRetries != 0 {
		t.Fatalf("policy.MaxRetries = %d, want 0", policy.MaxRetries)
	}

	externalPolicy := provider.ResolveRetryPolicy(context.Background(), RetryConfigScope{
		Class:  RequestClassExternalRead,
		Method: "GET",
	}, base)
	if externalPolicy.PerAttemptTimeout != 7*time.Second {
		t.Fatalf("externalPolicy.PerAttemptTimeout = %v, want 7s", externalPolicy.PerAttemptTimeout)
	}
}

func TestManagedRetryConfigProviderRejectsUnsupportedVersion(t *testing.T) {
	data := []byte(`
version: v2
`)

	_, err := NewManagedRetryConfigProviderFromYAML(data)
	if err == nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = nil, want unsupported version error")
	}
}

func TestManagedRetryConfigProviderRejectsUnknownYAMLFields(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{
			name: "root",
			data: "version: v1\nunknown: true\n",
		},
		{
			name: "nested policy",
			data: "version: v1\npolicies:\n  internal_read:\n    max_retry: 1\n",
		},
		{
			name: "misspelled match",
			data: "version: v1\nrules:\n  - macth:\n      method: [GET]\n    policy:\n      max_retries: 1\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewManagedRetryConfigProviderFromYAML([]byte(tt.data))
			if err == nil {
				t.Fatal("NewManagedRetryConfigProviderFromYAML() error = nil, want unknown field error")
			}
		})
	}
}

func TestManagedRetryConfigProviderRejectsInvalidGlobPatterns(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{
			name: "internal host",
			data: "version: v1\ninternal_hosts: ['[']\n",
		},
		{
			name: "downstream host",
			data: "version: v1\ndownstreams:\n  - name: payments\n    hosts: ['[']\n",
		},
		{
			name: "operation path",
			data: "version: v1\noperations:\n  - name: queryPayment\n    paths: ['[']\n",
		},
		{
			name: "rule path",
			data: "version: v1\nrules:\n  - name: queryPayment\n    match:\n      path: ['[']\n    policy:\n      max_retries: 1\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewManagedRetryConfigProviderFromYAML([]byte(tt.data)); err == nil {
				t.Fatal("NewManagedRetryConfigProviderFromYAML() error = nil, want invalid glob error")
			}
		})
	}
}

func TestManagedRetryConfigProviderTreatsNonHostPathValuesAsExact(t *testing.T) {
	tests := []struct {
		name  string
		match string
		scope RetryConfigScope
	}{
		{
			name:  "caller",
			match: "caller: ['orders-*']",
			scope: RetryConfigScope{Caller: "orders-api"},
		},
		{
			name:  "downstream",
			match: "downstream: ['pay*']",
			scope: RetryConfigScope{Downstream: "payments"},
		},
		{
			name:  "operation",
			match: "operation: ['query*']",
			scope: RetryConfigScope{Operation: "queryPayment"},
		},
		{
			name:  "method",
			match: "method: ['G*']",
			scope: RetryConfigScope{Method: stdhttp.MethodGet},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte("version: v1\nrules:\n  - name: exact-only\n    match:\n      " + tt.match + "\n    policy:\n      max_retries: 3\n")
			provider, err := NewManagedRetryConfigProviderFromYAML(data)
			if err != nil {
				t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
			}
			base := RetryPolicy{MaxRetries: 1}
			if got := provider.ResolveRetryPolicy(context.Background(), tt.scope, base).MaxRetries; got != 1 {
				t.Fatalf("MaxRetries = %d, want 1 when %s glob-like value is not an exact match", got, tt.name)
			}
		})
	}
}

func TestManagedRetryConfigProviderAllowsGlobSyntaxCharactersInExactValues(t *testing.T) {
	provider, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
rules:
  - name: exact-caller
    match:
      caller: ["["]
    policy:
      max_retries: 3
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}
	base := RetryPolicy{MaxRetries: 1}
	scope := RetryConfigScope{Caller: "["}
	if got := provider.ResolveRetryPolicy(context.Background(), scope, base).MaxRetries; got != 3 {
		t.Fatalf("MaxRetries = %d, want 3 for exact caller value", got)
	}
}

func TestManagedRetryConfigProviderTreatsOperationMethodAndDownstreamAsExact(t *testing.T) {
	tests := []struct {
		name      string
		condition string
	}{
		{
			name:      "method",
			condition: "methods: ['G*']",
		},
		{
			name:      "downstream",
			condition: "downstreams: ['pay*']",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte("version: v1\noperations:\n  - name: wildcard-operation\n    " + tt.condition + "\n")
			provider, err := NewManagedRetryConfigProviderFromYAML(data)
			if err != nil {
				t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
			}
			req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://payments.example.com/v1/payments", nil)
			if err != nil {
				t.Fatalf("NewRequest() error = %v", err)
			}
			scope := provider.ResolveRetryConfigScope(req, RetryConfigScope{Downstream: "payments"})
			if scope.Operation != "GET /v1/payments" {
				t.Fatalf("scope.Operation = %q, want exact-match fallback", scope.Operation)
			}
		})
	}
}

func TestManagedRetryConfigProviderRejectsActiveRuleWithoutMatch(t *testing.T) {
	_, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
rules:
  - name: unsafe-global-write-retry
    match:
      method: [" "]
    policy:
      max_retries: 1
`))
	if err == nil {
		t.Fatal("NewManagedRetryConfigProviderFromYAML() error = nil, want empty match error")
	}
}

func TestManagedRetryConfigProviderRejectsActiveRuleWithoutPolicy(t *testing.T) {
	_, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
rules:
  - name: no-op-rule
    match:
      method: ["GET"]
`))
	if err == nil {
		t.Fatal("NewManagedRetryConfigProviderFromYAML() error = nil, want empty policy error")
	}
}

func TestManagedRetryConfigProviderRejectsDownstreamWithoutNameOrHosts(t *testing.T) {
	tests := []string{
		"version: v1\ndownstreams:\n  - hosts: [\"billing\"]\n",
		"version: v1\ndownstreams:\n  - name: billing\n",
		"version: v1\ndownstreams:\n  - name: billing\n    hosts: [\" \"]\n",
	}
	for _, data := range tests {
		if _, err := NewManagedRetryConfigProviderFromYAML([]byte(data)); err == nil {
			t.Fatalf("NewManagedRetryConfigProviderFromYAML(%q) error = nil, want invalid downstream error", data)
		}
	}
}

func TestManagedRetryConfigProviderRejectsOperationWithoutName(t *testing.T) {
	_, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
operations:
  - methods: ["GET"]
    paths: ["/orders/*"]
`))
	if err == nil {
		t.Fatal("NewManagedRetryConfigProviderFromYAML() error = nil, want missing operation name error")
	}
}

func TestManagedRetryConfigProviderAllowsDisabledEmptyRule(t *testing.T) {
	_, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
rules:
  - name: retained-for-later
    disabled: true
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v, want nil", err)
	}
}

func TestRetryPolicyPatchFileRejectsNegativeMaxRetries(t *testing.T) {
	_, err := (RetryPolicyPatchFile{MaxRetries: intPtr(-1)}).toPatch()
	if err == nil {
		t.Fatal("toPatch() error = nil, want negative max_retries error")
	}
}

func TestRetryPolicyPatchFileRejectsInvalidHTTPStatuses(t *testing.T) {
	for _, status := range []int{99, 600, 4001} {
		_, err := (RetryPolicyPatchFile{RetryOnStatuses: []int{status}}).toPatch()
		if err == nil {
			t.Errorf("toPatch() status %d error = nil, want range error", status)
		}
	}
}

func TestManagedRetryConfigProviderRejectsMultipleYAMLDocuments(t *testing.T) {
	_, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
---
version: v1
`))
	if err == nil {
		t.Fatal("NewManagedRetryConfigProviderFromYAML() error = nil, want multiple-document error")
	}
}

func TestManagedRetryConfigProviderRejectsDuplicateNames(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{
			name: "downstreams",
			data: `
version: v1
downstreams:
  - name: orders
    hosts: ["orders-a.example.com"]
  - name: orders
    hosts: ["orders-b.example.com"]
`,
		},
		{
			name: "operations",
			data: `
version: v1
operations:
  - name: get-order
    methods: ["GET"]
  - name: get-order
    methods: ["POST"]
`,
		},
		{
			name: "rules",
			data: `
version: v1
rules:
  - name: order-rule
    match:
      method: ["GET"]
    policy:
      max_retries: 1
  - name: order-rule
    match:
      method: ["POST"]
    policy:
      max_retries: 1
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewManagedRetryConfigProviderFromYAML([]byte(tt.data)); err == nil {
				t.Fatal("NewManagedRetryConfigProviderFromYAML() error = nil, want duplicate-name error")
			}
		})
	}
}

func TestDownstreamInferencePrefersExactMatchedPattern(t *testing.T) {
	provider, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
downstreams:
  - name: broad
    hosts: ["*", "*.example.com"]
  - name: exact
    hosts: ["api.example.com"]
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://api.example.com/orders/123", nil)
	if err != nil {
		t.Fatalf("http.NewRequest() error = %v", err)
	}
	scope := provider.ResolveRetryConfigScope(req, RetryConfigScope{})
	if scope.Downstream != "exact" {
		t.Fatalf("Downstream = %q, want exact", scope.Downstream)
	}
}

func TestRetryPolicyPatchFileRejectsDisableRetryWithMaxRetries(t *testing.T) {
	_, err := (RetryPolicyPatchFile{
		DisableRetry: boolPtr(true),
		MaxRetries:   intPtr(1),
	}).toPatch()
	if err == nil {
		t.Fatal("toPatch() error = nil, want conflicting retry controls error")
	}
}

func TestRetryPolicyPatchFileRejectsNonPositiveDurations(t *testing.T) {
	tests := []struct {
		name  string
		patch RetryPolicyPatchFile
	}{
		{name: "per attempt timeout", patch: RetryPolicyPatchFile{PerAttemptTimeout: "0s"}},
		{name: "max elapsed time", patch: RetryPolicyPatchFile{MaxElapsedTime: "-1s"}},
		{name: "initial backoff", patch: RetryPolicyPatchFile{InitialBackoff: "0s"}},
		{name: "max backoff", patch: RetryPolicyPatchFile{MaxBackoff: "-1ms"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.patch.toPatch(); err == nil {
				t.Fatal("toPatch() error = nil, want non-positive duration error")
			}
		})
	}
}

func TestManagedRetryConfigProviderPreservesEmptyRetryStatuses(t *testing.T) {
	provider, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
policies:
  internal_read:
    retry_on_statuses: []
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}

	policy := provider.ResolveRetryPolicy(context.Background(), RetryConfigScope{
		Class:  RequestClassInternalRead,
		Method: stdhttp.MethodGet,
	}, RetryPolicy{RetryOnStatuses: defaultRetryStatusSet()})
	if policy.RetryOnStatuses == nil {
		t.Fatal("RetryOnStatuses = nil, want explicit empty set")
	}
	if len(policy.RetryOnStatuses) != 0 {
		t.Fatalf("RetryOnStatuses = %v, want empty", policy.RetryOnStatuses)
	}
}

func TestManagedRetryConfigProviderTreatsUnmatchedHostAsExternalWithoutInternalHosts(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://api.example.com/orders", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	class, ok := provider.ResolveRequestClass(req)
	if !ok {
		t.Fatal("ResolveRequestClass() ok = false, want true")
	}
	if class != RequestClassExternalRead {
		t.Fatalf("ResolveRequestClass() = %q, want %q", class, RequestClassExternalRead)
	}
}

func TestManagedRetryConfigProviderUpdateHotReload(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	base := RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: 2 * time.Second,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        400 * time.Millisecond,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}

	first := RetryConfigFile{
		Rules: []RetryConfigRuleFile{
			{
				Name:     "disable-profile",
				Priority: 100,
				Match: RetryConfigMatchFile{
					Operation: []string{"readProfile"},
					Class:     []string{"internal_read"},
				},
				Policy: RetryPolicyPatchFile{
					DisableRetry: boolPtr(true),
				},
			},
		},
	}
	if err := provider.Update(first); err != nil {
		t.Fatalf("provider.Update(first) error = %v", err)
	}

	scope := RetryConfigScope{Operation: "readProfile", Class: RequestClassInternalRead, Method: "GET"}
	policy := provider.ResolveRetryPolicy(context.Background(), scope, base)
	if policy.MaxRetries != 0 {
		t.Fatalf("policy.MaxRetries = %d, want 0", policy.MaxRetries)
	}

	second := RetryConfigFile{
		Rules: []RetryConfigRuleFile{
			{
				Name:     "enable-profile-retry",
				Priority: 100,
				Match: RetryConfigMatchFile{
					Operation: []string{"readProfile"},
					Class:     []string{"internal_read"},
				},
				Policy: RetryPolicyPatchFile{
					MaxRetries:      intPtr(2),
					InitialBackoff:  "200ms",
					RetryOnStatuses: []int{503},
				},
			},
		},
	}
	if err := provider.Update(second); err != nil {
		t.Fatalf("provider.Update(second) error = %v", err)
	}

	policy = provider.ResolveRetryPolicy(context.Background(), scope, base)
	if policy.MaxRetries != 2 {
		t.Fatalf("policy.MaxRetries = %d, want 2", policy.MaxRetries)
	}
	if policy.InitialBackoff != 200*time.Millisecond {
		t.Fatalf("policy.InitialBackoff = %v, want 200ms", policy.InitialBackoff)
	}
	if _, ok := policy.RetryOnStatuses[503]; !ok {
		t.Fatalf("policy.RetryOnStatuses does not contain 503")
	}
}

func TestManagedRetryConfigProviderRulePriorityDisabledAndPathMatch(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		Rules: []RetryConfigRuleFile{
			{
				Name:     "disabled-top-priority",
				Priority: 200,
				Disabled: true,
				Match: RetryConfigMatchFile{
					Method: []string{"GET"},
					Host:   []string{"api.example.com"},
					Path:   []string{"/v1/orders/*"},
					Class:  []string{"external_read"},
				},
				Policy: RetryPolicyPatchFile{
					MaxRetries: intPtr(9),
				},
			},
			{
				Name:     "lower-priority",
				Priority: 80,
				Match: RetryConfigMatchFile{
					Method: []string{"GET"},
					Host:   []string{"api.example.com"},
					Path:   []string{"/v1/orders/*"},
					Class:  []string{"external_read"},
				},
				Policy: RetryPolicyPatchFile{
					MaxRetries: intPtr(1),
				},
			},
			{
				Name:     "higher-priority",
				Priority: 100,
				Match: RetryConfigMatchFile{
					Method: []string{"GET"},
					Host:   []string{"api.example.com"},
					Path:   []string{"/v1/orders/*"},
					Class:  []string{"external_read"},
				},
				Policy: RetryPolicyPatchFile{
					MaxRetries: intPtr(2),
				},
			},
		},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}

	base := RetryPolicy{
		MaxRetries:        0,
		PerAttemptTimeout: 2 * time.Second,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        400 * time.Millisecond,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}
	policy := provider.ResolveRetryPolicy(context.Background(), RetryConfigScope{
		Method: "GET",
		Host:   "api.example.com",
		Path:   "/v1/orders/123",
		Class:  RequestClassExternalRead,
	}, base)
	if policy.MaxRetries != 2 {
		t.Fatalf("policy.MaxRetries = %d, want 2", policy.MaxRetries)
	}
}

func TestManagedRetryConfigProviderMatchesURLPathCaseInsensitively(t *testing.T) {
	provider, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
rules:
  - name: exact-path
    match:
      path: ["/v1/orders"]
    policy:
      max_retries: 3
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}

	base := RetryPolicy{MaxRetries: 1}
	scope := RetryConfigScope{
		Class:  RequestClassInternalRead,
		Method: stdhttp.MethodGet,
		Path:   "/V1/orders",
	}
	if got := provider.ResolveRetryPolicy(context.Background(), scope, base).MaxRetries; got != 3 {
		t.Fatalf("MaxRetries = %d, want 3 for differently cased path", got)
	}
}

func TestManagedRetryConfigProviderMatchesHostGlobCaseInsensitively(t *testing.T) {
	provider, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
internal_hosts: ["*.SVC.CLUSTER.LOCAL"]
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://payments.svc.cluster.local/v1/payments", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	if class, ok := provider.ResolveRequestClass(req); !ok || class != RequestClassInternalRead {
		t.Fatalf("ResolveRequestClass() = %q, %t, want %q, true", class, ok, RequestClassInternalRead)
	}
}

func TestManagedRetryConfigProviderInfersDownstreamFromHostGlobCaseInsensitively(t *testing.T) {
	provider, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
downstreams:
  - name: payments
    hosts: ["*.EXAMPLE.COM"]
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://api.example.com/v1/payments", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	if scope := provider.ResolveRetryConfigScope(req, RetryConfigScope{}); scope.Downstream != "payments" {
		t.Fatalf("Downstream = %q, want payments", scope.Downstream)
	}
}

func TestManagedRetryConfigProviderMatchesDownstreamPathCaseInsensitively(t *testing.T) {
	provider, err := NewManagedRetryConfigProviderFromYAML([]byte(`
version: v1
downstreams:
  - name: payments
    hosts: ["api.example.com/Payments"]
`))
	if err != nil {
		t.Fatalf("NewManagedRetryConfigProviderFromYAML() error = %v", err)
	}

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://api.example.com/payments/123", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	if scope := provider.ResolveRetryConfigScope(req, RetryConfigScope{}); scope.Downstream != "payments" {
		t.Fatalf("Downstream = %q, want payments for differently cased path", scope.Downstream)
	}
}

func TestManagedRetryConfigProviderOperationRulePriorityAndHostMatch(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		Operations: []RetryOperationRuleFile{
			{
				Name:     "genericQuery",
				Priority: 80,
				Methods:  []string{"GET"},
				Hosts:    []string{"*.qq.com"},
				Paths:    []string{"/api/query"},
			},
			{
				Name:        "queryACE",
				Priority:    100,
				Methods:     []string{"GET"},
				Hosts:       []string{"ace.qq.com"},
				Downstreams: []string{"ace"},
				Paths:       []string{"/api/query"},
			},
		},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://ace.qq.com/api/query", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	scope := provider.ResolveRetryConfigScope(req, RetryConfigScope{
		Method:     "GET",
		Host:       req.URL.Host,
		Path:       req.URL.Path,
		Downstream: "ace",
		Class:      RequestClassExternalRead,
	})
	if scope.Operation != "queryACE" {
		t.Fatalf("scope.Operation = %q, want queryACE", scope.Operation)
	}
}

func TestRetryPolicyPatchFileParsesAllFields(t *testing.T) {
	patch, err := (RetryPolicyPatchFile{
		DisableRetry:      boolPtr(false),
		MaxRetries:        intPtr(3),
		PerAttemptTimeout: "5s",
		MaxElapsedTime:    "7s",
		InitialBackoff:    "150ms",
		MaxBackoff:        "900ms",
		RetryOnStatuses:   []int{408, 502},
		RetryOn429:        boolPtr(false),
	}).toPatch()
	if err != nil {
		t.Fatalf("toPatch() error = %v", err)
	}

	base := RetryPolicy{
		MaxRetries:        1,
		PerAttemptTimeout: 2 * time.Second,
		MaxElapsedTime:    3 * time.Second,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        400 * time.Millisecond,
		RetryOnStatuses:   defaultRetryStatusSet(),
		RetryOn429:        true,
	}
	policy := patch.Apply(base)
	if policy.MaxRetries != 3 {
		t.Fatalf("policy.MaxRetries = %d, want 3", policy.MaxRetries)
	}
	if policy.PerAttemptTimeout != 5*time.Second {
		t.Fatalf("policy.PerAttemptTimeout = %v, want 5s", policy.PerAttemptTimeout)
	}
	if policy.MaxElapsedTime != 7*time.Second {
		t.Fatalf("policy.MaxElapsedTime = %v, want 7s", policy.MaxElapsedTime)
	}
	if policy.InitialBackoff != 150*time.Millisecond {
		t.Fatalf("policy.InitialBackoff = %v, want 150ms", policy.InitialBackoff)
	}
	if policy.MaxBackoff != 900*time.Millisecond {
		t.Fatalf("policy.MaxBackoff = %v, want 900ms", policy.MaxBackoff)
	}
	if policy.RetryOn429 {
		t.Fatalf("policy.RetryOn429 = true, want false")
	}
	if _, ok := policy.RetryOnStatuses[408]; !ok {
		t.Fatalf("policy.RetryOnStatuses missing 408")
	}
	if _, ok := policy.RetryOnStatuses[502]; !ok {
		t.Fatalf("policy.RetryOnStatuses missing 502")
	}
}

func TestManagedRetryConfigProviderResolvesScopeDefaults(t *testing.T) {
	SetCallerServiceName("shop")
	t.Cleanup(clearCallerServiceName)
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		InternalHosts: []string{"billing", "billing:8080"},
		Downstreams: []RetryDownstreamFile{
			{Name: "billing", Hosts: []string{"billing", "billing:8080"}},
			{Name: "ace", Hosts: []string{"ace.qq.com", "*.qq.com"}},
		},
		Operations: []RetryOperationRuleFile{
			{
				Name:        "queryOrder",
				Priority:    100,
				Methods:     []string{"GET"},
				Paths:       []string{"/api/order/query"},
				Downstreams: []string{"billing"},
			},
		},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://billing:8080/api/order/query", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	scope := provider.ResolveRetryConfigScope(req, RetryConfigScope{
		Method: "GET",
		Class:  RequestClassInternalRead,
		Host:   req.URL.Host,
		Path:   req.URL.Path,
	})
	if scope.Caller != "shop" {
		t.Fatalf("scope.Caller = %q, want shop", scope.Caller)
	}
	if scope.Downstream != "billing" {
		t.Fatalf("scope.Downstream = %q, want billing", scope.Downstream)
	}
	if scope.Operation != "queryOrder" {
		t.Fatalf("scope.Operation = %q, want queryOrder", scope.Operation)
	}
}

func TestManagedRetryConfigProviderOperationFallsBackToMethodAndPath(t *testing.T) {
	SetCallerServiceName("check")
	t.Cleanup(clearCallerServiceName)
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		Downstreams: []RetryDownstreamFile{
			{Name: "ace", Hosts: []string{"ace.qq.com"}},
		},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://ace.qq.com/api/query", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	scope := provider.ResolveRetryConfigScope(req, RetryConfigScope{
		Method: "GET",
		Class:  RequestClassExternalRead,
		Host:   req.URL.Host,
		Path:   req.URL.Path,
	})
	if scope.Caller != "check" {
		t.Fatalf("scope.Caller = %q, want check", scope.Caller)
	}
	if scope.Downstream != "ace" {
		t.Fatalf("scope.Downstream = %q, want ace", scope.Downstream)
	}
	if scope.Operation != "GET /api/query" {
		t.Fatalf("scope.Operation = %q, want GET /api/query", scope.Operation)
	}
}

func TestManagedRetryConfigProviderResolvesDownstreamFromHostAndPathPattern(t *testing.T) {
	SetCallerServiceName("shop")
	t.Cleanup(clearCallerServiceName)
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		Downstreams: []RetryDownstreamFile{
			{Name: "member", Hosts: []string{"api-*.example.com/member"}},
		},
		Operations: []RetryOperationRuleFile{
			{
				Name:        "queryMemberCharacter",
				Priority:    100,
				Methods:     []string{"GET"},
				Downstreams: []string{"member"},
				Paths:       []string{"/member/v3.2/games/*/characters/member/*"},
			},
		},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://api-staging.example.com/member/v3.2/games/1001/characters/member/2002", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	scope := provider.ResolveRetryConfigScope(req, RetryConfigScope{
		Method: "GET",
		Class:  RequestClassInternalRead,
		Host:   req.URL.Host,
		Path:   req.URL.Path,
	})
	if scope.Caller != "shop" {
		t.Fatalf("scope.Caller = %q, want shop", scope.Caller)
	}
	if scope.Downstream != "member" {
		t.Fatalf("scope.Downstream = %q, want member", scope.Downstream)
	}
	if scope.Operation != "queryMemberCharacter" {
		t.Fatalf("scope.Operation = %q, want queryMemberCharacter", scope.Operation)
	}
}

func TestManagedRetryConfigProviderResolvesRequestFromOneConfigGeneration(t *testing.T) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.Update(RetryConfigFile{
		InternalHosts: []string{"billing"},
		Downstreams: []RetryDownstreamFile{
			{Name: "billing", Hosts: []string{"billing"}},
		},
		Operations: []RetryOperationRuleFile{
			{
				Name:        "queryOrder",
				Methods:     []string{"GET"},
				Paths:       []string{"/api/order/query"},
				Downstreams: []string{"billing"},
			},
		},
		Policies: RetryClassPolicyFile{
			InternalRead: &RetryPolicyPatchFile{MaxRetries: intPtr(2)},
		},
	}); err != nil {
		t.Fatalf("provider.Update() error = %v", err)
	}

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://billing/api/order/query", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	scope, policy := provider.resolveRequestConfig(req, RetryPolicy{})

	if scope.Class != RequestClassInternalRead {
		t.Fatalf("scope.Class = %q, want %q", scope.Class, RequestClassInternalRead)
	}
	if scope.Downstream != "billing" {
		t.Fatalf("scope.Downstream = %q, want billing", scope.Downstream)
	}
	if scope.Operation != "queryOrder" {
		t.Fatalf("scope.Operation = %q, want queryOrder", scope.Operation)
	}
	if policy.MaxRetries != 2 {
		t.Fatalf("policy.MaxRetries = %d, want 2", policy.MaxRetries)
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(v int) *int {
	return &v
}
