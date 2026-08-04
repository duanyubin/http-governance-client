//go:build windows

package http

import (
	"net"
	"net/url"
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func TestIsRetryableErrorRecognizesWindowsConnectionRefused(t *testing.T) {
	err := &url.Error{
		Op:  "Post",
		URL: "http://127.0.0.1:7308/billing",
		Err: &net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: &os.SyscallError{
				Syscall: "connectex",
				Err:     windows.WSAECONNREFUSED,
			},
		},
	}

	if !isRetryableError(err) {
		t.Fatal("isRetryableError() = false, want true for Windows connection refusal")
	}
}
