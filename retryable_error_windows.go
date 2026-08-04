//go:build windows

package http

import (
	"errors"

	"golang.org/x/sys/windows"
)

func isRetryablePlatformError(err error) bool {
	return errors.Is(err, windows.WSAECONNREFUSED)
}
