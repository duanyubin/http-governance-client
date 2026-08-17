package prometheus

import (
	"errors"
	"testing"

	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

func TestNewRejectsNilRegisterer(t *testing.T) {
	observer, err := New(nil)
	if err == nil {
		t.Fatal("New(nil) returned no error")
	}
	if observer != nil {
		t.Fatalf("New(nil) observer = %#v, want nil", observer)
	}
}

func TestNewReturnsDuplicateRegistrationError(t *testing.T) {
	registry := stdprometheus.NewRegistry()
	if _, err := New(registry); err != nil {
		t.Fatalf("first New() error = %v", err)
	}

	observer, err := New(registry)
	if err == nil {
		t.Fatal("second New() returned no error")
	}
	if observer != nil {
		t.Fatalf("second New() observer = %#v, want nil", observer)
	}

	var duplicate stdprometheus.AlreadyRegisteredError
	if !errors.As(err, &duplicate) {
		t.Fatalf("second New() error = %T %v, want AlreadyRegisteredError", err, err)
	}
}
