package http

import (
	"context"
	"net/http"
	"sync"
)

// RequestClassResolver classifies outgoing requests for policy selection.
type RequestClassResolver interface {
	ResolveRequestClass(req *http.Request) (RequestClass, bool)
}

var (
	requestClassResolverMu sync.RWMutex
	requestClassResolver   RequestClassResolver
)

// SetRequestClassResolver registers a resolver that can classify requests as
// internal/external read/write before method-based defaults are applied.
func SetRequestClassResolver(resolver RequestClassResolver) {
	requestClassResolverMu.Lock()
	defer requestClassResolverMu.Unlock()
	requestClassResolver = resolver
}

// ClearRequestClassResolver removes the currently registered request class resolver.
func ClearRequestClassResolver() {
	SetRequestClassResolver(nil)
}

func resolveRequestClassByExtension(req *http.Request) (RequestClass, bool) {
	requestClassResolverMu.RLock()
	resolver := requestClassResolver
	requestClassResolverMu.RUnlock()
	if resolver == nil {
		return "", false
	}
	return resolver.ResolveRequestClass(req)
}

func explicitRequestClassFromContext(req *http.Request) (RequestClass, bool) {
	if req == nil {
		return "", false
	}
	if class, ok := req.Context().Value(requestClassContextKey{}).(RequestClass); ok && class != "" {
		return class, true
	}
	if scope, ok := req.Context().Value(retryConfigScopeContextKey{}).(RetryConfigScope); ok && scope.Class != "" {
		return scope.Class, true
	}
	return "", false
}

// WithInternalRead forces the current request context to use the internal read
// classification.
func WithInternalRead(ctx context.Context) context.Context {
	return WithRequestClass(ctx, RequestClassInternalRead)
}

// WithInternalWrite forces the current request context to use the internal write
// classification.
func WithInternalWrite(ctx context.Context) context.Context {
	return WithRequestClass(ctx, RequestClassInternalWrite)
}

// WithExternalRead forces the current request context to use the external read
// classification.
func WithExternalRead(ctx context.Context) context.Context {
	return WithRequestClass(ctx, RequestClassExternalRead)
}

// WithExternalWrite forces the current request context to use the external
// write classification.
func WithExternalWrite(ctx context.Context) context.Context {
	return WithRequestClass(ctx, RequestClassExternalWrite)
}
