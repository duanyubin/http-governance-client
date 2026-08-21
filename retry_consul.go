package http

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	stdhttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultRetryConfigAutoReloadInterval = 30 * time.Second

var newRetryConfigStore = newConsulRetryConfigStore

var (
	retryConfigAutoReloadMu       sync.Mutex
	retryConfigAutoReloadCancel   context.CancelFunc
	retryConfigAutoReloadDone     <-chan struct{}
	retryConfigAutoReloadInterval = defaultRetryConfigAutoReloadInterval
)

type retryConfigStore interface {
	Get(path string) ([]byte, error)
}

type retryConfigBlockingStore interface {
	retryConfigStore
	GetWithIndex(ctx context.Context, path string, index uint64, wait time.Duration) ([]byte, uint64, error)
}

type retryConfigContextStore interface {
	GetContext(ctx context.Context, path string) ([]byte, error)
}

type retryConfigWatchState struct {
	seen   bool
	exists bool
	index  uint64
}

type retryConfigStoreNotFound interface {
	NotFound(path string, err error) bool
}

type consulRetryConfigStore struct {
	baseURL string
	client  *stdhttp.Client
}

func (s consulRetryConfigStore) Get(path string) ([]byte, error) {
	body, _, err := s.getWithContext(context.Background(), path, 0, 0)
	return body, err
}

func (s consulRetryConfigStore) GetContext(ctx context.Context, path string) ([]byte, error) {
	body, _, err := s.getWithContext(ctx, path, 0, 0)
	return body, err
}

func (s consulRetryConfigStore) GetWithIndex(ctx context.Context, path string, index uint64, wait time.Duration) ([]byte, uint64, error) {
	return s.getWithContext(ctx, path, index, wait)
}

func (s consulRetryConfigStore) getWithContext(ctx context.Context, path string, index uint64, wait time.Duration) ([]byte, uint64, error) {
	requestURL, err := joinConsulKVURL(s.baseURL, path)
	if err != nil {
		return nil, 0, err
	}
	if index > 0 || wait > 0 {
		requestURL, err = withConsulBlockingQuery(requestURL, index, wait)
		if err != nil {
			return nil, 0, err
		}
	}
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, requestURL, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode == stdhttp.StatusNotFound {
		discardRetryResponse(resp)
		return nil, 0, fmt.Errorf("consul key %q not found", path)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		discardRetryResponse(resp)
		return nil, 0, fmt.Errorf("consul key %q returned status %d", path, resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	return body, parseConsulIndex(resp.Header.Get("X-Consul-Index")), nil
}

func (s consulRetryConfigStore) NotFound(path string, err error) bool {
	return err != nil && strings.Contains(err.Error(), fmt.Sprintf("consul key %q not found", path))
}

// SetRetryConfigProviderFromConsul loads retry configuration from Consul,
// installs the provider into the shared HTTP client and starts background
// reload for subsequent configuration changes.
func SetRetryConfigProviderFromConsul(env, appName, endpoint string) (*ManagedRetryConfigProvider, error) {
	store, err := newRetryConfigStore(env, endpoint)
	if err != nil {
		return nil, err
	}
	provider, err := newManagedRetryConfigProviderFromStore(store, appName)
	if err != nil {
		return nil, err
	}
	SetRetryConfigProvider(provider)
	if err := startRetryConfigAutoReloadFromStore(provider, store, env, appName, endpoint); err != nil {
		return nil, err
	}
	return provider, nil
}

// NewManagedRetryConfigProviderFromConsul creates a managed provider from
// Consul without installing it into the shared HTTP client.
func NewManagedRetryConfigProviderFromConsul(env, appName, endpoint string) (*ManagedRetryConfigProvider, error) {
	store, err := newRetryConfigStore(env, endpoint)
	if err != nil {
		return nil, err
	}
	return newManagedRetryConfigProviderFromStore(store, appName)
}

// UpdateFromConsul reloads the provider from Consul immediately.
func (p *ManagedRetryConfigProvider) UpdateFromConsul(env, appName, endpoint string) error {
	store, err := newRetryConfigStore(env, endpoint)
	if err != nil {
		return err
	}
	return p.updateFromStore(store, appName)
}

func (p *ManagedRetryConfigProvider) updateFromStore(store retryConfigStore, appName string) error {
	file, err := loadRetryConfigFileFromStore(store, appName)
	if err != nil {
		return err
	}
	return p.Update(file)
}

func newManagedRetryConfigProviderFromStore(store retryConfigStore, appName string) (*ManagedRetryConfigProvider, error) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.updateFromStore(store, appName); err != nil {
		return nil, err
	}
	return provider, nil
}

// SetRetryConfigAutoReloadInterval sets the wait interval used by retry config
// auto reload. For blocking-query mode, it is the maximum wait time for each
// Consul watch request. For fallback/polling mode, it is the reload interval.
//
// Call this before Setup() or before StartRetryConfigAutoReloadFromConsul() if
// you want the current process to use the new value immediately.
//
// If interval <= 0, auto reload will be disabled for newly started watchers.
func SetRetryConfigAutoReloadInterval(interval time.Duration) {
	retryConfigAutoReloadMu.Lock()
	defer retryConfigAutoReloadMu.Unlock()
	retryConfigAutoReloadInterval = interval
}

// RetryConfigAutoReloadInterval returns the current retry config auto reload interval.
func RetryConfigAutoReloadInterval() time.Duration {
	retryConfigAutoReloadMu.Lock()
	defer retryConfigAutoReloadMu.Unlock()
	return retryConfigAutoReloadInterval
}

// StartRetryConfigAutoReloadFromConsul starts a background task that watches
// Consul and refreshes the provider when retry configuration changes.
func StartRetryConfigAutoReloadFromConsul(provider *ManagedRetryConfigProvider, env, appName, endpoint string) error {
	if provider == nil {
		return fmt.Errorf("retry config provider nil")
	}
	store, err := newRetryConfigStore(env, endpoint)
	if err != nil {
		return err
	}
	return startRetryConfigAutoReloadFromStore(provider, store, env, appName, endpoint)
}

func startRetryConfigAutoReloadFromStore(provider *ManagedRetryConfigProvider, store retryConfigStore, env, appName, endpoint string) error {
	if provider == nil {
		return fmt.Errorf("retry config provider nil")
	}
	if store == nil {
		return fmt.Errorf("retry config store nil")
	}

	retryConfigAutoReloadMu.Lock()
	defer retryConfigAutoReloadMu.Unlock()

	stopRetryConfigAutoReloadLocked()
	if retryConfigAutoReloadInterval <= 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	retryConfigAutoReloadCancel = cancel
	retryConfigAutoReloadDone = done
	interval := retryConfigAutoReloadInterval
	go func() {
		defer close(done)
		runRetryConfigAutoReloadLoop(ctx, provider, store, env, appName, endpoint, interval)
	}()
	return nil
}

// StopRetryConfigAutoReload stops the background retry-config reload task if it
// is currently running.
func StopRetryConfigAutoReload() {
	retryConfigAutoReloadMu.Lock()
	defer retryConfigAutoReloadMu.Unlock()
	stopRetryConfigAutoReloadLocked()
}

func stopRetryConfigAutoReloadLocked() {
	if retryConfigAutoReloadCancel == nil {
		return
	}
	retryConfigAutoReloadCancel()
	if retryConfigAutoReloadDone != nil {
		<-retryConfigAutoReloadDone
	}
	retryConfigAutoReloadCancel = nil
	retryConfigAutoReloadDone = nil
}

func runRetryConfigAutoReloadLoop(ctx context.Context, provider *ManagedRetryConfigProvider, store retryConfigStore, env, appName, endpoint string, interval time.Duration) {
	logEndpoint := sanitizeConsulEndpointForLog(endpoint)
	if blockingStore, ok := store.(retryConfigBlockingStore); ok {
		runRetryConfigBlockingReloadLoop(ctx, provider, blockingStore, appName, interval)
		return
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := reloadRetryConfigFromStore(ctx, provider, store, appName); err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Warn("HTTPClient retry config hot reload failed", "env", env, "appName", appName, "endpoint", logEndpoint, "error", err)
			} else {
				slog.Info("HTTPClient retry config reloaded", "mode", "polling", "env", env, "appName", appName, "endpoint", logEndpoint)
			}
			timer.Reset(interval)
		}
	}
}

func sanitizeConsulEndpointForLog(endpoint string) string {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return ""
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String()
}

func runRetryConfigBlockingReloadLoop(ctx context.Context, provider *ManagedRetryConfigProvider, store retryConfigBlockingStore, appName string, interval time.Duration) {
	states := make(map[string]retryConfigWatchState, len(retryConfigLookupPaths(appName)))
	pendingReload := false
	for {
		if pendingReload {
			if err := reloadRetryConfigFromStore(ctx, provider, store, appName); err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Warn("HTTPClient retry config hot reload failed", "appName", appName, "error", err)
				if !sleepWithContext(ctx, interval) {
					return
				}
				continue
			}
			pendingReload = false
			slog.Info("HTTPClient retry config reloaded", "mode", "blocking", "appName", appName)
			continue
		}

		changed, err := waitForRetryConfigChange(ctx, store, appName, states, interval)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("HTTPClient retry config blocking watch failed, fallback to reload", "appName", appName, "error", err)
			pendingReload = true
			continue
		}
		if !changed {
			if allRetryConfigWatchPathsMissing(appName, states) && !sleepWithContext(ctx, interval) {
				return
			}
			continue
		}
		pendingReload = true
	}
}

func allRetryConfigWatchPathsMissing(appName string, states map[string]retryConfigWatchState) bool {
	for _, path := range retryConfigLookupPaths(appName) {
		state := states[path]
		if !state.seen || state.exists {
			return false
		}
	}
	return true
}

func waitForRetryConfigChange(ctx context.Context, store retryConfigBlockingStore, appName string, states map[string]retryConfigWatchState, wait time.Duration) (bool, error) {
	paths := retryConfigLookupPaths(appName)
	for _, path := range paths {
		state := states[path]
		_, nextIndex, err := store.GetWithIndex(ctx, path, state.index, wait)
		if err != nil {
			if isRetryConfigPathNotFound(store, path, err) {
				// Treat key deletion as a change after the initial baseline has been observed
				// so the provider can fall back to the remaining configuration layers.
				nextState := retryConfigWatchState{
					seen:   true,
					exists: false,
					index:  0,
				}
				states[path] = nextState
				if state.seen && state.exists {
					return true, nil
				}
				continue
			}
			return false, err
		}

		nextState := retryConfigWatchState{
			seen:   true,
			exists: true,
			index:  nextIndex,
		}
		states[path] = nextState

		if !state.seen {
			continue
		}
		// A previously missing key becoming available is a real configuration change
		// and must trigger a reload.
		if !state.exists {
			return true, nil
		}
		if nextIndex != 0 && nextIndex != state.index {
			return true, nil
		}
	}
	return false, nil
}

func reloadRetryConfigFromStore(ctx context.Context, provider *ManagedRetryConfigProvider, store retryConfigStore, appName string) error {
	file, err := loadRetryConfigFileFromStoreContext(ctx, store, appName)
	if err != nil {
		return err
	}
	return provider.Update(file)
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func newConsulRetryConfigStore(env, endpoint string) (retryConfigStore, error) {
	resolved := resolveConsulEndpoint(env, endpoint)
	if resolved == "" {
		return nil, fmt.Errorf("consul endpoint empty")
	}
	return consulRetryConfigStore{
		baseURL: resolved,
		client: &stdhttp.Client{
			Timeout: consulStoreHTTPTimeout(),
		},
	}, nil
}

func consulStoreHTTPTimeout() time.Duration {
	wait := RetryConfigAutoReloadInterval()
	if wait <= 0 {
		wait = defaultRetryConfigAutoReloadInterval
	}
	return wait + 5*time.Second
}

func resolveConsulEndpoint(env, endpoint string) string {
	replacer := strings.NewReplacer("${profile}", strings.TrimSpace(env))
	resolved := strings.TrimSpace(replacer.Replace(strings.TrimSpace(endpoint)))
	resolved = strings.TrimRight(resolved, "/")
	if resolved == "" {
		return ""
	}
	if strings.Contains(resolved, "://") {
		return resolved
	}
	return "http://" + resolved
}

func joinConsulKVURL(baseURL, key string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	segments := strings.Split(strings.Trim(key, "/"), "/")
	escaped := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		escaped = append(escaped, url.PathEscape(segment))
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/v1/kv/" + strings.Join(escaped, "/")
	query := parsed.Query()
	query.Set("raw", "")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func withConsulBlockingQuery(requestURL string, index uint64, wait time.Duration) (string, error) {
	parsed, err := url.Parse(requestURL)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	if index > 0 {
		query.Set("index", fmt.Sprintf("%d", index))
	}
	if wait > 0 {
		query.Set("wait", formatConsulWaitTime(wait))
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func formatConsulWaitTime(wait time.Duration) string {
	if wait <= 0 {
		return ""
	}
	seconds := int(wait / time.Second)
	if seconds <= 0 {
		seconds = 1
	}
	return fmt.Sprintf("%ds", seconds)
}

func parseConsulIndex(value string) uint64 {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func loadRetryConfigFileFromStore(store retryConfigStore, appName string) (RetryConfigFile, error) {
	return loadRetryConfigFileFromStoreContext(context.Background(), store, appName)
}

func loadRetryConfigFileFromStoreContext(ctx context.Context, store retryConfigStore, appName string) (RetryConfigFile, error) {
	merged := RetryConfigFile{}
	for _, path := range retryConfigLookupPaths(appName) {
		var data []byte
		var err error
		if contextStore, ok := store.(retryConfigContextStore); ok {
			data, err = contextStore.GetContext(ctx, path)
		} else {
			data, err = store.Get(path)
		}
		if err != nil {
			if isRetryConfigPathNotFound(store, path, err) {
				continue
			}
			return RetryConfigFile{}, fmt.Errorf("load retry config from %q: %w", path, err)
		}
		if len(data) == 0 {
			continue
		}
		var file RetryConfigFile
		if err := decodeRetryConfigYAML(data, &file); err != nil {
			return RetryConfigFile{}, fmt.Errorf("unmarshal retry config from %q: %w", path, err)
		}
		if err := validateRetryBudgetFileLayer(file.RetryBudget); err != nil {
			return RetryConfigFile{}, fmt.Errorf("validate retry config from %q: %w", path, err)
		}
		if _, _, _, _, _, err := compileRetryConfigFile(file); err != nil {
			return RetryConfigFile{}, fmt.Errorf("validate retry config from %q: %w", path, err)
		}
		merged = mergeRetryConfigFiles(merged, file)
	}
	if _, err := merged.RetryBudget.toConfig(); err != nil {
		return RetryConfigFile{}, fmt.Errorf("validate merged retry config: %w", err)
	}
	return merged, nil
}

func isRetryConfigPathNotFound(store retryConfigStore, path string, err error) bool {
	if err == nil {
		return false
	}
	if checker, ok := store.(retryConfigStoreNotFound); ok {
		return checker.NotFound(path, err)
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

func retryConfigLookupPaths(appName string) []string {
	paths := []string{"config/go/application/retry"}
	if trimmed := strings.TrimSpace(appName); trimmed != "" {
		paths = append(paths, fmt.Sprintf("config/go/%s/retry", trimmed))
	}
	return paths
}

func mergeRetryConfigFiles(base, override RetryConfigFile) RetryConfigFile {
	return RetryConfigFile{
		Version:       firstNonBlank(override.Version, base.Version),
		RetryBudget:   mergeRetryBudgetFiles(base.RetryBudget, override.RetryBudget),
		InternalHosts: mergeStringLists(base.InternalHosts, override.InternalHosts),
		Downstreams: mergeNamedItems(
			base.Downstreams,
			override.Downstreams,
			func(item RetryDownstreamFile) string { return item.Name },
			cloneRetryDownstreamFile,
		),
		Operations: mergeNamedItems(
			base.Operations,
			override.Operations,
			func(item RetryOperationRuleFile) string { return item.Name },
			cloneRetryOperationRuleFile,
		),
		Policies: mergeRetryClassPolicyFiles(base.Policies, override.Policies),
		Rules: mergeNamedItems(
			base.Rules,
			override.Rules,
			func(item RetryConfigRuleFile) string { return item.Name },
			cloneRetryConfigRuleFile,
		),
	}
}

func mergeRetryBudgetFiles(base, override *RetryBudgetFile) *RetryBudgetFile {
	if base == nil && override == nil {
		return nil
	}
	merged := RetryBudgetFile{}
	if base != nil {
		merged = *base
	}
	if override == nil {
		return &merged
	}
	if override.Enabled != nil {
		merged.Enabled = override.Enabled
	}
	if override.Capacity != nil {
		merged.Capacity = override.Capacity
	}
	if override.RetryCost != nil {
		merged.RetryCost = override.RetryCost
	}
	if override.SuccessIncrement != nil {
		merged.SuccessIncrement = override.SuccessIncrement
	}
	return &merged
}

func mergeRetryClassPolicyFiles(base, override RetryClassPolicyFile) RetryClassPolicyFile {
	return RetryClassPolicyFile{
		InternalRead:  mergeRetryPolicyPatchFiles(base.InternalRead, override.InternalRead),
		InternalWrite: mergeRetryPolicyPatchFiles(base.InternalWrite, override.InternalWrite),
		ExternalRead:  mergeRetryPolicyPatchFiles(base.ExternalRead, override.ExternalRead),
		ExternalWrite: mergeRetryPolicyPatchFiles(base.ExternalWrite, override.ExternalWrite),
	}
}

func mergeRetryPolicyPatchFiles(base, override *RetryPolicyPatchFile) *RetryPolicyPatchFile {
	switch {
	case base == nil && override == nil:
		return nil
	case base == nil:
		return cloneRetryPolicyPatchFilePtr(override)
	case override == nil:
		return cloneRetryPolicyPatchFilePtr(base)
	}
	merged := *base
	if override.DisableRetry != nil {
		merged.DisableRetry = override.DisableRetry
		if *override.DisableRetry && override.MaxRetries == nil {
			merged.MaxRetries = nil
		}
	}
	if override.MaxRetries != nil {
		merged.MaxRetries = override.MaxRetries
		if override.DisableRetry == nil {
			merged.DisableRetry = nil
		}
	}
	if override.PerAttemptTimeout != "" {
		merged.PerAttemptTimeout = override.PerAttemptTimeout
	}
	if override.MaxElapsedTime != "" {
		merged.MaxElapsedTime = override.MaxElapsedTime
	}
	if override.InitialBackoff != "" {
		merged.InitialBackoff = override.InitialBackoff
	}
	if override.MaxBackoff != "" {
		merged.MaxBackoff = override.MaxBackoff
	}
	if override.RetryOnStatuses != nil {
		merged.RetryOnStatuses = cloneIntSlice(override.RetryOnStatuses)
	}
	if override.RetryOn429 != nil {
		merged.RetryOn429 = override.RetryOn429
	}
	return &merged
}

func mergeNamedItems[T any](base, override []T, keyFn func(T) string, cloneFn func(T) T) []T {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	result := make([]T, 0, len(base)+len(override))
	seen := make(map[string]struct{}, len(base)+len(override))
	appendItems := func(items []T) {
		for _, item := range items {
			key := strings.ToLower(strings.TrimSpace(keyFn(item)))
			if key != "" {
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
			}
			result = append(result, cloneFn(item))
		}
	}
	appendItems(override)
	appendItems(base)
	return result
}

func cloneRetryDownstreamFile(item RetryDownstreamFile) RetryDownstreamFile {
	return RetryDownstreamFile{
		Name:  item.Name,
		Hosts: append([]string(nil), item.Hosts...),
	}
}

func cloneRetryOperationRuleFile(item RetryOperationRuleFile) RetryOperationRuleFile {
	return RetryOperationRuleFile{
		Name:        item.Name,
		Priority:    item.Priority,
		Methods:     append([]string(nil), item.Methods...),
		Hosts:       append([]string(nil), item.Hosts...),
		Paths:       append([]string(nil), item.Paths...),
		Downstreams: append([]string(nil), item.Downstreams...),
	}
}

func cloneRetryPolicyPatchFilePtr(item *RetryPolicyPatchFile) *RetryPolicyPatchFile {
	if item == nil {
		return nil
	}
	cloned := *item
	cloned.RetryOnStatuses = cloneIntSlice(item.RetryOnStatuses)
	return &cloned
}

func cloneRetryConfigRuleFile(item RetryConfigRuleFile) RetryConfigRuleFile {
	cloned := item
	cloned.Match = RetryConfigMatchFile{
		Callers:     append([]string(nil), item.Match.Callers...),
		Downstreams: append([]string(nil), item.Match.Downstreams...),
		Operations:  append([]string(nil), item.Match.Operations...),
		Methods:     append([]string(nil), item.Match.Methods...),
		Hosts:       append([]string(nil), item.Match.Hosts...),
		Paths:       append([]string(nil), item.Match.Paths...),
		Classes:     append([]string(nil), item.Match.Classes...),
	}
	cloned.Policy = RetryPolicyPatchFile{
		DisableRetry:      item.Policy.DisableRetry,
		MaxRetries:        item.Policy.MaxRetries,
		PerAttemptTimeout: item.Policy.PerAttemptTimeout,
		MaxElapsedTime:    item.Policy.MaxElapsedTime,
		InitialBackoff:    item.Policy.InitialBackoff,
		MaxBackoff:        item.Policy.MaxBackoff,
		RetryOnStatuses:   cloneIntSlice(item.Policy.RetryOnStatuses),
		RetryOn429:        item.Policy.RetryOn429,
	}
	return cloned
}

func mergeStringLists(base, override []string) []string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	result := make([]string, 0, len(base)+len(override))
	seen := make(map[string]struct{}, len(base)+len(override))
	appendItems := func(items []string) {
		for _, item := range items {
			trimmed := strings.TrimSpace(item)
			if trimmed == "" {
				continue
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			result = append(result, trimmed)
		}
	}
	appendItems(base)
	appendItems(override)
	return result
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
