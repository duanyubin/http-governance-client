//go:build !windows

package http

func isRetryablePlatformError(error) bool {
	return false
}
