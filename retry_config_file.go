package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// RetryConfigFile is the versioned YAML/JSON model for retry governance.
type RetryConfigFile struct {
	Version       string                   `yaml:"version" json:"version"`
	RetryBudget   *RetryBudgetFile         `yaml:"retry_budget" json:"retry_budget"`
	InternalHosts []string                 `yaml:"internal_hosts" json:"internal_hosts"`
	Downstreams   []RetryDownstreamFile    `yaml:"downstreams" json:"downstreams"`
	Operations    []RetryOperationRuleFile `yaml:"operations" json:"operations"`
	Policies      RetryClassPolicyFile     `yaml:"policies" json:"policies"`
	Rules         []RetryConfigRuleFile    `yaml:"rules" json:"rules"`
}

// RetryDownstreamFile maps one logical downstream name to host patterns.
type RetryDownstreamFile struct {
	Name  string   `yaml:"name" json:"name"`
	Hosts []string `yaml:"hosts" json:"hosts"`
}

// RetryOperationRuleFile maps request attributes to a logical operation name.
type RetryOperationRuleFile struct {
	Name        string   `yaml:"name" json:"name"`
	Priority    int      `yaml:"priority" json:"priority"`
	Methods     []string `yaml:"methods" json:"methods"`
	Hosts       []string `yaml:"hosts" json:"hosts"`
	Paths       []string `yaml:"paths" json:"paths"`
	Downstreams []string `yaml:"downstreams" json:"downstreams"`
}

// RetryClassPolicyFile defines policy patches for the four request classes.
type RetryClassPolicyFile struct {
	InternalRead  *RetryPolicyPatchFile `yaml:"internal_read" json:"internal_read"`
	InternalWrite *RetryPolicyPatchFile `yaml:"internal_write" json:"internal_write"`
	ExternalRead  *RetryPolicyPatchFile `yaml:"external_read" json:"external_read"`
	ExternalWrite *RetryPolicyPatchFile `yaml:"external_write" json:"external_write"`
}

// RetryConfigRuleFile defines a named conditional policy override.
type RetryConfigRuleFile struct {
	Name     string               `yaml:"name" json:"name"`
	Priority int                  `yaml:"priority" json:"priority"`
	Disabled bool                 `yaml:"disabled" json:"disabled"`
	Match    RetryConfigMatchFile `yaml:"match" json:"match"`
	Policy   RetryPolicyPatchFile `yaml:"policy" json:"policy"`
}

// RetryConfigMatchFile contains the match dimensions for a configuration rule.
type RetryConfigMatchFile struct {
	Callers     []string `yaml:"callers" json:"callers"`
	Downstreams []string `yaml:"downstreams" json:"downstreams"`
	Operations  []string `yaml:"operations" json:"operations"`
	Methods     []string `yaml:"methods" json:"methods"`
	Hosts       []string `yaml:"hosts" json:"hosts"`
	Paths       []string `yaml:"paths" json:"paths"`
	Classes     []string `yaml:"classes" json:"classes"`
}

// RetryPolicyPatchFile is the serialized form of a partial retry policy.
type RetryPolicyPatchFile struct {
	DisableRetry      *bool  `yaml:"disable_retry" json:"disable_retry"`
	MaxRetries        *int   `yaml:"max_retries" json:"max_retries"`
	PerAttemptTimeout string `yaml:"per_attempt_timeout" json:"per_attempt_timeout"`
	MaxElapsedTime    string `yaml:"max_elapsed_time" json:"max_elapsed_time"`
	InitialBackoff    string `yaml:"initial_backoff" json:"initial_backoff"`
	MaxBackoff        string `yaml:"max_backoff" json:"max_backoff"`
	RetryOnStatuses   []int  `yaml:"retry_on_statuses" json:"retry_on_statuses"`
	RetryOn429        *bool  `yaml:"retry_on_429" json:"retry_on_429"`
}

// ManagedRetryConfigProvider atomically serves validated retry configuration.
type ManagedRetryConfigProvider struct {
	mu            sync.RWMutex
	classPatches  map[RequestClass]RetryPolicyPatch
	rules         []compiledRetryConfigRule
	internalHosts []string
	downstreams   []compiledDownstream
	operations    []compiledOperationRule
	retryBudget   retryBudgetConfig
}

type compiledRetryConfigRule struct {
	Name        string
	Priority    int
	Match       compiledRetryConfigMatch
	Patch       RetryPolicyPatch
	Specificity int
}

type compiledRetryConfigMatch struct {
	Callers     []string
	Downstreams []string
	Operations  []string
	Methods     []string
	Hosts       []string
	Paths       []string
	Classes     []RequestClass
}

type compiledDownstream struct {
	Name  string
	Hosts []string
}

type compiledOperationRule struct {
	Name        string
	Priority    int
	Methods     []string
	Hosts       []string
	Paths       []string
	Downstreams []string
	Specificity int
}

// NewManagedRetryConfigProvider returns an empty managed provider.
func NewManagedRetryConfigProvider() *ManagedRetryConfigProvider {
	return &ManagedRetryConfigProvider{
		classPatches: make(map[RequestClass]RetryPolicyPatch),
	}
}

// NewManagedRetryConfigProviderFromYAML validates YAML and creates a provider.
func NewManagedRetryConfigProviderFromYAML(data []byte) (*ManagedRetryConfigProvider, error) {
	provider := NewManagedRetryConfigProvider()
	if err := provider.UpdateFromYAML(data); err != nil {
		return nil, err
	}
	return provider, nil
}

// ResolveRetryPolicy applies the first matching rule to the class-adjusted policy.
func (p *ManagedRetryConfigProvider) ResolveRetryPolicy(ctx context.Context, scope RetryConfigScope, base RetryPolicy) RetryPolicy {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.resolveRetryPolicy(scope, base)
}

func (p *ManagedRetryConfigProvider) resolveRetryPolicy(scope RetryConfigScope, base RetryPolicy) RetryPolicy {
	policy := cloneRetryPolicy(base)
	if patch, ok := p.classPatches[scope.Class]; ok {
		policy = patch.Apply(policy)
	}
	for _, rule := range p.rules {
		if rule.Match.match(scope) {
			policy = rule.Patch.Apply(policy)
			break
		}
	}
	return policy
}

// ResolveRequestClass classifies a request using the configured internal hosts.
func (p *ManagedRetryConfigProvider) ResolveRequestClass(req *http.Request) (RequestClass, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.resolveRequestClass(req)
}

func (p *ManagedRetryConfigProvider) resolveRequestClass(req *http.Request) (RequestClass, bool) {
	if req == nil || req.URL == nil {
		return "", false
	}
	host := req.URL.Host
	hostname := req.URL.Hostname()
	if len(p.internalHosts) > 0 && (matchHostPatterns(p.internalHosts, hostname) || matchHostPatterns(p.internalHosts, host)) {
		if isReadMethod(req.Method) {
			return RequestClassInternalRead, true
		}
		return RequestClassInternalWrite, true
	}
	if isReadMethod(req.Method) {
		return RequestClassExternalRead, true
	}
	return RequestClassExternalWrite, true
}

// ResolveRetryConfigScope fills missing caller, downstream and operation values.
func (p *ManagedRetryConfigProvider) ResolveRetryConfigScope(req *http.Request, scope RetryConfigScope) RetryConfigScope {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.resolveRetryConfigScope(req, scope)
}

func (p *ManagedRetryConfigProvider) resolveRetryConfigScope(req *http.Request, scope RetryConfigScope) RetryConfigScope {
	if req == nil {
		return scope
	}
	if scope.Caller == "" {
		scope.Caller = CallerServiceName()
	}
	if scope.Downstream == "" {
		scope.Downstream = inferDownstream(req, p.downstreams)
	}
	if scope.Operation == "" {
		scope.Operation = inferOperation(req, scope, p.operations)
	}
	return scope
}

func (p *ManagedRetryConfigProvider) resolveRequestConfig(req *http.Request, base RetryPolicy) (RetryConfigScope, RetryPolicy, retryBudgetConfig) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	class, ok := explicitRequestClassFromContext(req)
	if !ok {
		class, ok = p.resolveRequestClass(req)
	}
	if !ok {
		if isReadMethod(req.Method) {
			class = RequestClassInternalRead
		} else {
			class = RequestClassInternalWrite
		}
	}
	scope := p.resolveRetryConfigScope(req, retryConfigScopeFromRequest(req, class))
	return scope, p.resolveRetryPolicy(scope, base), p.retryBudget
}

// UpdateFromYAML validates and atomically replaces the current configuration.
func (p *ManagedRetryConfigProvider) UpdateFromYAML(data []byte) error {
	var file RetryConfigFile
	if err := decodeRetryConfigYAML(data, &file); err != nil {
		return err
	}
	return p.Update(file)
}

func decodeRetryConfigYAML(data []byte, file *RetryConfigFile) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(file); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("configuration must contain exactly one YAML document")
		}
		return fmt.Errorf("invalid trailing YAML document: %w", err)
	}
	return nil
}

// Update validates and atomically replaces the current configuration.
func (p *ManagedRetryConfigProvider) Update(file RetryConfigFile) error {
	if err := validateRetryBudgetFileLayer(file.RetryBudget); err != nil {
		return err
	}
	retryBudget, err := file.RetryBudget.toConfig()
	if err != nil {
		return err
	}
	classPatches, rules, internalHosts, downstreams, operations, err := compileRetryConfigFile(file)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.classPatches = classPatches
	p.rules = rules
	p.internalHosts = internalHosts
	p.downstreams = downstreams
	p.operations = operations
	p.retryBudget = retryBudget
	return nil
}

func compileRetryConfigFile(file RetryConfigFile) (map[RequestClass]RetryPolicyPatch, []compiledRetryConfigRule, []string, []compiledDownstream, []compiledOperationRule, error) {
	if err := validateConfigVersion(file.Version); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	classPatches := make(map[RequestClass]RetryPolicyPatch)
	internalHosts := normalizePatterns(file.InternalHosts)
	if err := validateGlobPatterns("internal_hosts", internalHosts); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	downstreams, err := compileDownstreams(file.Downstreams)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	operations, err := compileOperationRules(file.Operations)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	addClassPatch := func(class RequestClass, patchFile *RetryPolicyPatchFile) error {
		if patchFile == nil {
			return nil
		}
		patch, err := patchFile.toPatch()
		if err != nil {
			return fmt.Errorf("class %s policy invalid: %w", class, err)
		}
		classPatches[class] = patch
		return nil
	}
	if err := addClassPatch(RequestClassInternalRead, file.Policies.InternalRead); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if err := addClassPatch(RequestClassInternalWrite, file.Policies.InternalWrite); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if err := addClassPatch(RequestClassExternalRead, file.Policies.ExternalRead); err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if err := addClassPatch(RequestClassExternalWrite, file.Policies.ExternalWrite); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	rules := make([]compiledRetryConfigRule, 0, len(file.Rules))
	ruleNames := make(map[string]struct{}, len(file.Rules))
	for idx, rule := range file.Rules {
		if name := strings.TrimSpace(rule.Name); name != "" {
			key := strings.ToLower(name)
			if _, exists := ruleNames[key]; exists {
				return nil, nil, nil, nil, nil, fmt.Errorf("rule name %q is duplicated", name)
			}
			ruleNames[key] = struct{}{}
		}
		if rule.Disabled {
			continue
		}
		if rule.Policy.empty() {
			return nil, nil, nil, nil, nil, fmt.Errorf("rule %q invalid: policy must contain at least one field", rule.Name)
		}
		patch, err := rule.Policy.toPatch()
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("rule %q invalid: %w", rule.Name, err)
		}
		compiled, err := compileRetryConfigMatch(rule.Match)
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("rule %q invalid: %w", rule.Name, err)
		}
		specificity := compiled.specificity()
		if specificity == 0 {
			return nil, nil, nil, nil, nil, fmt.Errorf("rule %q invalid: match must contain at least one value", rule.Name)
		}
		rules = append(rules, compiledRetryConfigRule{
			Name:        defaultRuleName(rule.Name, idx),
			Priority:    rule.Priority,
			Match:       compiled,
			Patch:       patch,
			Specificity: specificity,
		})
	}

	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority > rules[j].Priority
		}
		return rules[i].Specificity > rules[j].Specificity
	})
	return classPatches, rules, internalHosts, downstreams, operations, nil
}

func validateConfigVersion(version string) error {
	version = strings.TrimSpace(version)
	if version == "" || version == "v1" {
		return nil
	}
	return fmt.Errorf("unsupported config version %q", version)
}

func defaultRuleName(name string, index int) string {
	if name != "" {
		return name
	}
	return fmt.Sprintf("rule-%d", index)
}

func (f RetryPolicyPatchFile) empty() bool {
	return f.DisableRetry == nil &&
		f.MaxRetries == nil &&
		f.PerAttemptTimeout == "" &&
		f.MaxElapsedTime == "" &&
		f.InitialBackoff == "" &&
		f.MaxBackoff == "" &&
		f.RetryOnStatuses == nil &&
		f.RetryOn429 == nil
}

func (f RetryPolicyPatchFile) toPatch() (RetryPolicyPatch, error) {
	var patch RetryPolicyPatch
	if f.MaxRetries != nil && *f.MaxRetries < 0 {
		return RetryPolicyPatch{}, fmt.Errorf("max_retries must not be negative")
	}
	if f.DisableRetry != nil && *f.DisableRetry && f.MaxRetries != nil {
		return RetryPolicyPatch{}, fmt.Errorf("disable_retry and max_retries cannot be set together")
	}
	patch.DisableRetry = f.DisableRetry
	patch.MaxRetries = f.MaxRetries
	patch.RetryOn429 = f.RetryOn429
	if f.RetryOnStatuses != nil {
		patch.RetryOnStatuses = make([]int, 0, len(f.RetryOnStatuses))
		seen := make(map[int]struct{}, len(f.RetryOnStatuses))
		for _, status := range f.RetryOnStatuses {
			if status < 100 || status > 599 {
				return RetryPolicyPatch{}, fmt.Errorf("retry_on_statuses value %d must be between 100 and 599", status)
			}
			if _, exists := seen[status]; exists {
				continue
			}
			seen[status] = struct{}{}
			patch.RetryOnStatuses = append(patch.RetryOnStatuses, status)
		}
	}

	var err error
	if patch.PerAttemptTimeout, err = parsePositiveRetryDuration("per_attempt_timeout", f.PerAttemptTimeout); err != nil {
		return RetryPolicyPatch{}, err
	}
	if patch.MaxElapsedTime, err = parsePositiveRetryDuration("max_elapsed_time", f.MaxElapsedTime); err != nil {
		return RetryPolicyPatch{}, err
	}
	if patch.InitialBackoff, err = parsePositiveRetryDuration("initial_backoff", f.InitialBackoff); err != nil {
		return RetryPolicyPatch{}, err
	}
	if patch.MaxBackoff, err = parsePositiveRetryDuration("max_backoff", f.MaxBackoff); err != nil {
		return RetryPolicyPatch{}, err
	}
	return patch, nil
}

func parsePositiveRetryDuration(name, value string) (*time.Duration, error) {
	if value == "" {
		return nil, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", name, err)
	}
	if duration <= 0 {
		return nil, fmt.Errorf("%s must be greater than zero", name)
	}
	return &duration, nil
}

func cloneIntSlice(values []int) []int {
	if values == nil {
		return nil
	}
	cloned := make([]int, len(values))
	copy(cloned, values)
	return cloned
}

func compileRetryConfigMatch(match RetryConfigMatchFile) (compiledRetryConfigMatch, error) {
	var compiled compiledRetryConfigMatch
	compiled.Callers = normalizePatterns(match.Callers)
	compiled.Downstreams = normalizePatterns(match.Downstreams)
	compiled.Operations = normalizePatterns(match.Operations)
	compiled.Methods = normalizePatterns(match.Methods)
	compiled.Hosts = normalizePatterns(match.Hosts)
	compiled.Paths = normalizePatterns(match.Paths)
	if err := validateGlobPatterns("hosts", compiled.Hosts); err != nil {
		return compiledRetryConfigMatch{}, err
	}
	if err := validateGlobPatterns("paths", compiled.Paths); err != nil {
		return compiledRetryConfigMatch{}, err
	}
	if len(match.Classes) > 0 {
		compiled.Classes = make([]RequestClass, 0, len(match.Classes))
		for _, item := range match.Classes {
			class := RequestClass(strings.TrimSpace(item))
			switch class {
			case RequestClassInternalRead, RequestClassInternalWrite, RequestClassExternalRead, RequestClassExternalWrite:
				compiled.Classes = append(compiled.Classes, class)
			default:
				return compiledRetryConfigMatch{}, fmt.Errorf("unsupported class %q", item)
			}
		}
	}
	return compiled, nil
}

func normalizePatterns(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, item := range values {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}

func validateGlobPatterns(field string, patterns []string) error {
	for _, patternValue := range patterns {
		if _, err := path.Match(patternValue, ""); err != nil {
			return fmt.Errorf("%s pattern %q invalid: %w", field, patternValue, err)
		}
	}
	return nil
}

func (m compiledRetryConfigMatch) specificity() int {
	score := 0
	if len(m.Callers) > 0 {
		score++
	}
	if len(m.Downstreams) > 0 {
		score++
	}
	if len(m.Operations) > 0 {
		score++
	}
	if len(m.Methods) > 0 {
		score++
	}
	if len(m.Hosts) > 0 {
		score++
	}
	if len(m.Paths) > 0 {
		score++
	}
	if len(m.Classes) > 0 {
		score++
	}
	return score
}

func (m compiledRetryConfigMatch) match(scope RetryConfigScope) bool {
	if !matchExactValues(m.Callers, scope.Caller) {
		return false
	}
	if !matchExactValues(m.Downstreams, scope.Downstream) {
		return false
	}
	if !matchExactValues(m.Operations, scope.Operation) {
		return false
	}
	if !matchExactValues(m.Methods, strings.ToUpper(scope.Method)) {
		return false
	}
	if !matchHostPatterns(m.Hosts, scope.Host) {
		return false
	}
	if !matchPathPatterns(m.Paths, scope.Path) {
		return false
	}
	if len(m.Classes) > 0 {
		matched := false
		for _, class := range m.Classes {
			if class == scope.Class {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func matchExactValues(values []string, value string) bool {
	if len(values) == 0 {
		return true
	}
	for _, expected := range values {
		if strings.EqualFold(expected, value) {
			return true
		}
	}
	return false
}

func matchHostPatterns(patterns []string, value string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, patternValue := range patterns {
		if patternValue == "*" {
			return true
		}
		ok, err := path.Match(strings.ToLower(patternValue), strings.ToLower(value))
		if err == nil && ok {
			return true
		}
	}
	return false
}

func matchPathPatterns(patterns []string, value string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, patternValue := range patterns {
		ok, _ := path.Match(strings.ToLower(patternValue), strings.ToLower(value))
		if ok {
			return true
		}
	}
	return false
}

func compileDownstreams(items []RetryDownstreamFile) ([]compiledDownstream, error) {
	if len(items) == 0 {
		return nil, nil
	}
	result := make([]compiledDownstream, 0, len(items))
	names := make(map[string]struct{}, len(items))
	for index, item := range items {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			return nil, fmt.Errorf("downstream %d invalid: name must not be empty", index)
		}
		key := strings.ToLower(name)
		if _, exists := names[key]; exists {
			return nil, fmt.Errorf("downstream name %q is duplicated", name)
		}
		names[key] = struct{}{}
		compiled := compiledDownstream{
			Name:  name,
			Hosts: normalizePatterns(item.Hosts),
		}
		if len(compiled.Hosts) == 0 {
			return nil, fmt.Errorf("downstream %q invalid: hosts must contain at least one value", name)
		}
		if err := validateGlobPatterns("hosts", compiled.Hosts); err != nil {
			return nil, fmt.Errorf("downstream %q invalid: %w", name, err)
		}
		result = append(result, compiled)
	}
	return result, nil
}

func compileOperationRules(items []RetryOperationRuleFile) ([]compiledOperationRule, error) {
	if len(items) == 0 {
		return nil, nil
	}
	result := make([]compiledOperationRule, 0, len(items))
	names := make(map[string]struct{}, len(items))
	for index, item := range items {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			return nil, fmt.Errorf("operation %d invalid: name must not be empty", index)
		}
		key := strings.ToLower(name)
		if _, exists := names[key]; exists {
			return nil, fmt.Errorf("operation name %q is duplicated", name)
		}
		names[key] = struct{}{}
		methods := normalizePatterns(item.Methods)
		for i := range methods {
			methods[i] = strings.ToUpper(methods[i])
		}
		rule := compiledOperationRule{
			Name:        name,
			Priority:    item.Priority,
			Methods:     methods,
			Hosts:       normalizePatterns(item.Hosts),
			Paths:       normalizePatterns(item.Paths),
			Downstreams: normalizePatterns(item.Downstreams),
		}
		if err := validateGlobPatterns("hosts", rule.Hosts); err != nil {
			return nil, fmt.Errorf("operation %q invalid: %w", name, err)
		}
		if err := validateGlobPatterns("paths", rule.Paths); err != nil {
			return nil, fmt.Errorf("operation %q invalid: %w", name, err)
		}
		rule.Specificity = operationRuleSpecificity(rule)
		result = append(result, rule)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Priority != result[j].Priority {
			return result[i].Priority > result[j].Priority
		}
		return result[i].Specificity > result[j].Specificity
	})
	return result, nil
}

func operationRuleSpecificity(rule compiledOperationRule) int {
	score := 0
	if len(rule.Methods) > 0 {
		score++
	}
	if len(rule.Hosts) > 0 {
		score++
	}
	if len(rule.Paths) > 0 {
		score++
	}
	if len(rule.Downstreams) > 0 {
		score++
	}
	return score
}

func inferDownstream(req *http.Request, mappings []compiledDownstream) string {
	if req == nil || req.URL == nil {
		return ""
	}
	host := req.URL.Host
	hostname := req.URL.Hostname()
	hostWithFirstPathSegment := joinHostAndFirstPathSegment(host, req.URL.Path)
	hostnameWithFirstPathSegment := joinHostAndFirstPathSegment(hostname, req.URL.Path)
	bestName := ""
	bestScore := 0
	for _, item := range mappings {
		for _, patternValue := range item.Hosts {
			for _, candidate := range []string{hostname, host, hostnameWithFirstPathSegment, hostWithFirstPathSegment} {
				if score := downstreamPatternMatchSpecificity(patternValue, candidate); score > bestScore {
					bestName = item.Name
					bestScore = score
				}
			}
		}
	}
	if bestName != "" {
		return bestName
	}
	if hostname == "" {
		return ""
	}
	parts := strings.Split(hostname, ".")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return hostname
}

func downstreamPatternMatchSpecificity(patternValue, candidate string) int {
	patternValue = strings.TrimSpace(patternValue)
	if patternValue == "" || candidate == "" {
		return 0
	}
	normalizedPattern := strings.ToLower(patternValue)
	normalizedCandidate := strings.ToLower(candidate)
	exact := normalizedPattern == normalizedCandidate
	matched, err := path.Match(normalizedPattern, normalizedCandidate)
	if !exact && (err != nil || !matched) {
		return 0
	}
	score := 100
	if exact {
		score = 1000
	}
	score += len(patternValue) - strings.Count(patternValue, "*") - strings.Count(patternValue, "?")
	if strings.Contains(patternValue, "/") {
		score += 200
	}
	return score
}

func joinHostAndFirstPathSegment(host, pathValue string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return firstPathSegment(pathValue)
	}
	segment := firstPathSegment(pathValue)
	if segment == "" {
		return host
	}
	return host + "/" + segment
}

func firstPathSegment(pathValue string) string {
	pathValue = strings.TrimSpace(pathValue)
	pathValue = strings.Trim(pathValue, "/")
	if pathValue == "" {
		return ""
	}
	segments := strings.Split(pathValue, "/")
	return strings.TrimSpace(segments[0])
}

func inferOperation(req *http.Request, scope RetryConfigScope, rules []compiledOperationRule) string {
	if req == nil || req.URL == nil {
		return ""
	}
	host := req.URL.Host
	hostname := req.URL.Hostname()
	method := strings.ToUpper(req.Method)
	pathValue := req.URL.Path
	for _, rule := range rules {
		if !matchExactValues(rule.Methods, method) {
			continue
		}
		if !matchHostPatterns(rule.Hosts, hostname) && !matchHostPatterns(rule.Hosts, host) {
			continue
		}
		if !matchPathPatterns(rule.Paths, pathValue) {
			continue
		}
		if !matchExactValues(rule.Downstreams, scope.Downstream) {
			continue
		}
		return rule.Name
	}
	return defaultOperationName(method, pathValue)
}

func defaultOperationName(method, pathValue string) string {
	method = strings.ToUpper(strings.TrimSpace(method))
	pathValue = strings.TrimSpace(pathValue)
	if pathValue == "" {
		pathValue = "/"
	}
	return strings.TrimSpace(method + " " + pathValue)
}
