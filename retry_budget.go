package http

import "fmt"

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
