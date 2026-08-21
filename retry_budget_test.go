package http

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRetryBudgetReservationLifecycle(t *testing.T) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: 20, RetryCost: 10, SuccessIncrement: 1}
	var budget retryBudget

	for i := 0; i < 2; i++ {
		reservation, ok := budget.reserve("billing", cfg)
		if !ok {
			t.Fatalf("reservation %d denied", i+1)
		}
		reservation.commit()
	}
	if _, ok := budget.reserve("billing", cfg); ok {
		t.Fatal("third reservation succeeded, want exhausted budget")
	}
	for i := 0; i < 9; i++ {
		budget.recordSuccess("billing", cfg)
	}
	if _, ok := budget.reserve("billing", cfg); ok {
		t.Fatal("reservation succeeded after 9 credits, want insufficient balance")
	}
	budget.recordSuccess("billing", cfg)
	if _, ok := budget.reserve("billing", cfg); !ok {
		t.Fatal("reservation denied after 10 credits")
	}
}

func TestRetryBudgetIsolatesDownstreams(t *testing.T) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: 10, RetryCost: 10, SuccessIncrement: 1}
	var budget retryBudget

	if _, ok := budget.reserve("billing", cfg); !ok {
		t.Fatal("billing reservation denied")
	}
	if _, ok := budget.reserve("billing", cfg); ok {
		t.Fatal("second billing reservation succeeded")
	}
	if _, ok := budget.reserve("member", cfg); !ok {
		t.Fatal("member reservation was affected by billing")
	}
}

func TestRetryBudgetRefundRestoresReservation(t *testing.T) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: 10, RetryCost: 10, SuccessIncrement: 1}
	var budget retryBudget

	reservation, ok := budget.reserve("billing", cfg)
	if !ok {
		t.Fatal("initial reservation denied")
	}
	reservation.refund()
	reservation.refund()
	if _, ok := budget.reserve("billing", cfg); !ok {
		t.Fatal("reservation denied after refund")
	}
}

func TestRetryBudgetCapacityChangesDoNotRefill(t *testing.T) {
	var budget retryBudget
	original := retryBudgetConfig{Enabled: true, Capacity: 20, RetryCost: 10, SuccessIncrement: 1}
	if _, ok := budget.reserve("billing", original); !ok {
		t.Fatal("initial reservation denied")
	}

	shrunk := retryBudgetConfig{Enabled: true, Capacity: 8, RetryCost: 8, SuccessIncrement: 1}
	if _, ok := budget.reserve("billing", shrunk); !ok {
		t.Fatal("reservation denied after balance was clamped to smaller capacity")
	}

	grown := retryBudgetConfig{Enabled: true, Capacity: 20, RetryCost: 10, SuccessIncrement: 1}
	if _, ok := budget.reserve("billing", grown); ok {
		t.Fatal("capacity growth refilled exhausted budget")
	}
}

func TestRetryBudgetSuccessStopsAtCapacity(t *testing.T) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: 10, RetryCost: 10, SuccessIncrement: 1}
	var budget retryBudget
	budget.recordSuccess("billing", cfg)
	if _, ok := budget.reserve("billing", cfg); !ok {
		t.Fatal("initial capacity was not available")
	}
	for i := 0; i < 20; i++ {
		budget.recordSuccess("billing", cfg)
	}
	if _, ok := budget.reserve("billing", cfg); !ok {
		t.Fatal("reservation denied after successful recovery")
	}
	if _, ok := budget.reserve("billing", cfg); ok {
		t.Fatal("success recovery exceeded capacity")
	}
}

func TestRetryBudgetSuccessDoesNotCreateUnusedEntry(t *testing.T) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: 10, RetryCost: 10, SuccessIncrement: 1}
	var budget retryBudget
	budget.recordSuccess("billing", cfg)
	entries := 0
	budget.entries.Range(func(_, _ any) bool {
		entries++
		return true
	})
	if entries != 0 {
		t.Fatalf("entries = %d, want 0 for an already-full unused budget", entries)
	}
}

func TestRetryBudgetConcurrentReservationsDoNotOverdraw(t *testing.T) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: 20, RetryCost: 1, SuccessIncrement: 1}
	var budget retryBudget
	var successes atomic.Int32
	var workers sync.WaitGroup
	workers.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer workers.Done()
			if _, ok := budget.reserve("billing", cfg); ok {
				successes.Add(1)
			}
		}()
	}
	workers.Wait()
	if got := successes.Load(); got != 20 {
		t.Fatalf("successful reservations = %d, want 20", got)
	}
}

func BenchmarkRetryBudgetDisabled(b *testing.B) {
	var budget retryBudget
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		budget.reserve("billing", retryBudgetConfig{})
	}
}

func BenchmarkRetryBudgetSuccessAtCapacity(b *testing.B) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: 20, RetryCost: 10, SuccessIncrement: 1}
	var budget retryBudget
	reservation, _ := budget.reserve("billing", cfg)
	reservation.refund()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		budget.recordSuccess("billing", cfg)
	}
}

func BenchmarkRetryBudgetRecordSuccessParallel(b *testing.B) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: 20, RetryCost: 10, SuccessIncrement: 1}
	var budget retryBudget
	budget.recordSuccess("billing", cfg)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			budget.recordSuccess("billing", cfg)
		}
	})
}

func BenchmarkRetryBudgetReserveParallelSameDownstream(b *testing.B) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: int64(b.N) + 1, RetryCost: 1, SuccessIncrement: 1}
	var budget retryBudget
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			budget.reserve("billing", cfg)
		}
	})
}

func BenchmarkRetryBudgetReserveParallelDifferentDownstreams(b *testing.B) {
	cfg := retryBudgetConfig{Enabled: true, Capacity: int64(b.N) + 1, RetryCost: 1, SuccessIncrement: 1}
	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprintf("downstream-%d", i)
	}
	var budget retryBudget
	var index atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[index.Add(1)%uint64(len(keys))]
			budget.reserve(key, cfg)
		}
	})
}
