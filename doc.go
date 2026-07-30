// Package http provides an HTTP client governance component built on top of
// Go net/http.
//
// It focuses on timeout control, bounded retries, request classification,
// Consul-based dynamic configuration, and request-level observability.
//
// The package is designed to preserve existing business calling patterns as
// much as possible while adding governance capabilities through the transport
// layer.
package http
