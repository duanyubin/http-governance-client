package http

import (
	"strings"
	"sync"
)

var (
	callerServiceNameMu sync.RWMutex
	callerServiceName   string
)

// SetCallerServiceName sets the process-level caller service name used to fill
// RetryConfigScope.Caller when business code does not provide it explicitly.
//
// In normal startup flow this value is injected by Setup() from the `--name`
// or `-n` command-line flag.
func SetCallerServiceName(name string) {
	callerServiceNameMu.Lock()
	defer callerServiceNameMu.Unlock()
	callerServiceName = strings.TrimSpace(name)
}

// CallerServiceName returns the process-level caller service name that is used
// as the default caller identity for outgoing requests.
func CallerServiceName() string {
	callerServiceNameMu.RLock()
	defer callerServiceNameMu.RUnlock()
	return callerServiceName
}

func clearCallerServiceName() {
	SetCallerServiceName("")
}
