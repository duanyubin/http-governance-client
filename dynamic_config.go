package http

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// RetryConfigScope describes the dimensions used to resolve retry policies for
// an outgoing request.
type RetryConfigScope struct {
	Caller     string
	Downstream string
	Operation  string
	Method     string
	Host       string
	Path       string
	Class      RequestClass
}

// RetryPolicyPatch describes the fields that should override a base retry
// policy. Nil values keep the base policy unchanged.
type RetryPolicyPatch struct {
	DisableRetry      *bool
	MaxRetries        *int
	PerAttemptTimeout *time.Duration
	MaxElapsedTime    *time.Duration
	InitialBackoff    *time.Duration
	MaxBackoff        *time.Duration
	RetryOnStatuses   []int
	RetryOn429        *bool
}

// RetryConfigProvider resolves the effective retry policy for the current
// request scope after combining defaults and dynamic configuration.
type RetryConfigProvider interface {
	ResolveRetryPolicy(ctx context.Context, scope RetryConfigScope, base RetryPolicy) RetryPolicy
}

// RetryConfigScopeDefaultsResolver fills missing scope fields such as caller,
// downstream and operation before policy matching runs.
type RetryConfigScopeDefaultsResolver interface {
	ResolveRetryConfigScope(req *http.Request, scope RetryConfigScope) RetryConfigScope
}

// RetryConfigProviderFunc adapts a function to RetryConfigProvider.
type RetryConfigProviderFunc func(ctx context.Context, scope RetryConfigScope, base RetryPolicy) RetryPolicy

// ResolveRetryPolicy calls fn.
func (fn RetryConfigProviderFunc) ResolveRetryPolicy(ctx context.Context, scope RetryConfigScope, base RetryPolicy) RetryPolicy {
	return fn(ctx, scope, base)
}

type retryConfigScopeContextKey struct{}

// WithRetryConfigScope stores explicit scope values in the request context.
// Callers can use it as an escape hatch when automatic scope inference is not
// precise enough for a particular request.
func WithRetryConfigScope(ctx context.Context, scope RetryConfigScope) context.Context {
	return context.WithValue(ctx, retryConfigScopeContextKey{}, scope)
}

// Apply merges the patch into the provided base policy and returns the
// normalized result.
func (p RetryPolicyPatch) Apply(base RetryPolicy) RetryPolicy {
	policy := cloneRetryPolicy(base)
	if p.DisableRetry != nil && *p.DisableRetry {
		policy.MaxRetries = 0
	}
	if p.MaxRetries != nil {
		policy.MaxRetries = *p.MaxRetries
	}
	if p.PerAttemptTimeout != nil {
		policy.PerAttemptTimeout = *p.PerAttemptTimeout
	}
	if p.MaxElapsedTime != nil {
		policy.MaxElapsedTime = *p.MaxElapsedTime
	}
	if p.InitialBackoff != nil {
		policy.InitialBackoff = *p.InitialBackoff
	}
	if p.MaxBackoff != nil {
		policy.MaxBackoff = *p.MaxBackoff
	}
	if p.RetryOnStatuses != nil {
		policy.RetryOnStatuses = make(map[int]struct{}, len(p.RetryOnStatuses))
		for _, status := range p.RetryOnStatuses {
			policy.RetryOnStatuses[status] = struct{}{}
		}
	}
	if p.RetryOn429 != nil {
		policy.RetryOn429 = *p.RetryOn429
	}
	return normalizeRetryPolicy(policy)
}

func retryConfigScopeFromRequest(req *http.Request, class RequestClass) RetryConfigScope {
	scope := RetryConfigScope{
		Method: strings.ToUpper(req.Method),
		Class:  class,
	}
	if req.URL != nil {
		scope.Host = req.URL.Host
		scope.Path = req.URL.Path
	}
	if value, ok := req.Context().Value(retryConfigScopeContextKey{}).(RetryConfigScope); ok {
		if value.Caller != "" {
			scope.Caller = value.Caller
		}
		if value.Downstream != "" {
			scope.Downstream = value.Downstream
		}
		if value.Operation != "" {
			scope.Operation = value.Operation
		}
		if value.Method != "" {
			scope.Method = strings.ToUpper(value.Method)
		}
		if value.Host != "" {
			scope.Host = value.Host
		}
		if value.Path != "" {
			scope.Path = value.Path
		}
		if value.Class != "" {
			scope.Class = value.Class
		}
	}
	return scope
}

func cloneRetryPolicy(policy RetryPolicy) RetryPolicy {
	cloned := policy
	if policy.RetryOnStatuses != nil {
		cloned.RetryOnStatuses = make(map[int]struct{}, len(policy.RetryOnStatuses))
		for status := range policy.RetryOnStatuses {
			cloned.RetryOnStatuses[status] = struct{}{}
		}
	}
	return cloned
}
