package http

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// RetryBudgetFile configures the local retry budget shared by one Transport.
type RetryBudgetFile struct {
	Enabled          *bool `yaml:"enabled" json:"enabled"`
	Capacity         *int  `yaml:"capacity" json:"capacity"`
	RetryCost        *int  `yaml:"retry_cost" json:"retry_cost"`
	SuccessIncrement *int  `yaml:"success_increment" json:"success_increment"`
}

type retryBudgetConfig struct {
	Enabled          bool
	Capacity         int64
	RetryCost        int64
	SuccessIncrement int64
}

type retryBudget struct {
	entries sync.Map
}

type retryBudgetEntry struct {
	balance atomic.Int64
}

type retryBudgetReservation struct {
	budget   *retryBudget
	key      string
	cost     int64
	capacity int64
	done     sync.Once
}

func (f *RetryBudgetFile) toConfig() (retryBudgetConfig, error) {
	if f == nil || f.Enabled == nil || !*f.Enabled {
		return retryBudgetConfig{}, nil
	}
	if f.Capacity == nil || *f.Capacity <= 0 {
		return retryBudgetConfig{}, fmt.Errorf("retry_budget.capacity must be positive when enabled")
	}
	if f.RetryCost == nil || *f.RetryCost <= 0 {
		return retryBudgetConfig{}, fmt.Errorf("retry_budget.retry_cost must be positive when enabled")
	}
	if f.SuccessIncrement == nil || *f.SuccessIncrement <= 0 {
		return retryBudgetConfig{}, fmt.Errorf("retry_budget.success_increment must be positive when enabled")
	}
	return retryBudgetConfig{
		Enabled:          true,
		Capacity:         int64(*f.Capacity),
		RetryCost:        int64(*f.RetryCost),
		SuccessIncrement: int64(*f.SuccessIncrement),
	}, nil
}

func validateRetryBudgetFileLayer(f *RetryBudgetFile) error {
	if f == nil {
		return nil
	}
	if f.Capacity != nil && *f.Capacity <= 0 {
		return fmt.Errorf("retry_budget.capacity must be positive")
	}
	if f.RetryCost != nil && *f.RetryCost <= 0 {
		return fmt.Errorf("retry_budget.retry_cost must be positive")
	}
	if f.SuccessIncrement != nil && *f.SuccessIncrement <= 0 {
		return fmt.Errorf("retry_budget.success_increment must be positive")
	}
	return nil
}

func (b *retryBudget) reserve(key string, cfg retryBudgetConfig) (*retryBudgetReservation, bool) {
	if !cfg.Enabled {
		return nil, true
	}
	key = normalizeRetryBudgetKey(key)
	entry := b.entry(key, cfg.Capacity)
	for {
		balance := entry.balance.Load()
		if balance > cfg.Capacity {
			if !entry.balance.CompareAndSwap(balance, cfg.Capacity) {
				continue
			}
			balance = cfg.Capacity
		}
		if balance < cfg.RetryCost {
			return nil, false
		}
		if entry.balance.CompareAndSwap(balance, balance-cfg.RetryCost) {
			return &retryBudgetReservation{
				budget:   b,
				key:      key,
				cost:     cfg.RetryCost,
				capacity: cfg.Capacity,
			}, true
		}
	}
}

func (r *retryBudgetReservation) commit() {
	if r != nil {
		r.done.Do(func() {})
	}
}

func (r *retryBudgetReservation) refund() {
	if r != nil {
		r.done.Do(func() {
			r.budget.add(r.key, r.cost, r.capacity)
		})
	}
}

func (b *retryBudget) recordSuccess(key string, cfg retryBudgetConfig) {
	if !cfg.Enabled {
		return
	}
	key = normalizeRetryBudgetKey(key)
	existing, ok := b.entries.Load(key)
	if !ok {
		return
	}
	addRetryBudgetBalance(existing.(*retryBudgetEntry), cfg.SuccessIncrement, cfg.Capacity)
}

func (b *retryBudget) add(key string, amount, capacity int64) {
	addRetryBudgetBalance(b.entry(key, capacity), amount, capacity)
}

func addRetryBudgetBalance(entry *retryBudgetEntry, amount, capacity int64) {
	for {
		balance := entry.balance.Load()
		if balance >= capacity {
			if balance == capacity || entry.balance.CompareAndSwap(balance, capacity) {
				return
			}
			continue
		}
		next := capacity
		if amount < capacity-balance {
			next = balance + amount
		}
		if entry.balance.CompareAndSwap(balance, next) {
			return
		}
	}
}

func (b *retryBudget) entry(key string, capacity int64) *retryBudgetEntry {
	if existing, ok := b.entries.Load(key); ok {
		return existing.(*retryBudgetEntry)
	}
	candidate := &retryBudgetEntry{}
	candidate.balance.Store(capacity)
	actual, _ := b.entries.LoadOrStore(key, candidate)
	return actual.(*retryBudgetEntry)
}

func normalizeRetryBudgetKey(key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return "unclassified"
	}
	return key
}
