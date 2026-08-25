package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/go-playground/form/v4"

	"google.golang.org/protobuf/proto"
)

const protobufContentType = "application/x-protobuf"

var cli = NewClient()
var setupRetryConfigFromConsul = SetRetryConfigProviderFromConsul

// ResponsePtr captures both a decoded response body and its HTTP status code.
type ResponsePtr struct {
	// ExpectedPtr receives the decoded response body.
	ExpectedPtr any
	// Deprecated: use ExpectedPtr. ExceptPtr is retained for compatibility.
	ExceptPtr any
	// StatusCode receives the HTTP response status code.
	StatusCode int
}

func (p *ResponsePtr) target() any {
	if p.ExpectedPtr != nil {
		return p.ExpectedPtr
	}
	return p.ExceptPtr
}

// MultipartFormData describes a multipart form with an optional file.
type MultipartFormData struct {
	FileName string
	File     *multipart.FileHeader
	Form     interface{}
}

// SetupOptions contains the explicit initialization parameters used by
// SetupWithOptions.
type SetupOptions struct {
	Env    string
	Name   string
	Consul string
}

// ClientOptions configures independently instantiated clients and transports.
type ClientOptions struct {
	RoundTripper   http.RoundTripper
	Skipper        SkipperFunc
	ResolvePolicy  RetryPolicyResolver
	ConfigProvider RetryConfigProvider
	Observer       RetryObserver
	ResultObserver RetryResultObserver
	RequestTimeout time.Duration
	CheckRedirect  func(req *http.Request, via []*http.Request) error
	Jar            http.CookieJar
}

// HealthcheckSkipper bypasses retry governance for health-check paths.
var HealthcheckSkipper = SkipperFunc(func(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	return strings.Contains(req.URL.Path, "/healthcheck")
})

// GetInstance returns the shared HTTP client instance.
// Deprecated: use Instance instead.
func GetInstance() *http.Client {
	return cli
}

// Instance returns the shared HTTP client used by the helper functions in this
// package.
func Instance() *http.Client {
	return cli
}

// NewTransport returns a retry-aware transport with the package defaults.
func NewTransport() *Transport {
	return newTransportWithOptions(ClientOptions{})
}

// NewTransportWithOptions returns an independently configured retry-aware
// transport without mutating the package-level shared client.
func NewTransportWithOptions(options ClientOptions) *Transport {
	return newTransportWithOptions(options)
}

// NewClient returns an independently instantiated HTTP client with the package
// defaults.
func NewClient() *http.Client {
	return newClientWithOptions(ClientOptions{})
}

// NewClientWithOptions returns an independently configured HTTP client without
// mutating the package-level shared client.
func NewClientWithOptions(options ClientOptions) *http.Client {
	return newClientWithOptions(options)
}

// SetSkipper configures a request skipper on the shared transport.
func SetSkipper(skipper SkipperFunc) {
	mutateClientTransport(func(transport *Transport) {
		transport.Skipper = skipper
	})
}

// SetHealthcheckSkipper skips requests whose URL path contains `/healthcheck`.
func SetHealthcheckSkipper() {
	SetSkipper(HealthcheckSkipper)
}

// SetRetryPolicyResolver configures the baseline retry policy resolver for the
// shared transport.
func SetRetryPolicyResolver(resolver RetryPolicyResolver) {
	mutateClientTransport(func(transport *Transport) {
		transport.ResolvePolicy = resolver
	})
}

// SetRetryObserver registers a retry observer on the shared transport.
func SetRetryObserver(observer RetryObserver) {
	mutateClientTransport(func(transport *Transport) {
		transport.Observer = observer
	})
}

// SetRetryResultObserver registers a transport-result observer on the shared
// transport.
func SetRetryResultObserver(observer RetryResultObserver) {
	mutateClientTransport(func(transport *Transport) {
		transport.ResultObserver = observer
	})
}

// SetBodyLogLevel configures the log level used for request/response body
// logging. The default level is slog.LevelDebug.
func SetBodyLogLevel(level slog.Level) {
	bodyLogLevel.Store(int64(level))
}

// SetBodyLogLimit configures, in bytes, the maximum request or response body
// size that can be captured for logging. The default is 64 KiB.
func SetBodyLogLimit(limit int64) error {
	if limit <= 0 {
		return fmt.Errorf("body log limit must be greater than zero")
	}
	bodyLogLimit.Store(limit)
	return nil
}

// SetRetryConfigProvider installs a dynamic retry-config provider on the shared
// transport.
func SetRetryConfigProvider(provider RetryConfigProvider) {
	mutateClientTransport(func(transport *Transport) {
		transport.ConfigProvider = provider
	})
	resolver, _ := provider.(RequestClassResolver)
	SetRequestClassResolver(resolver)
}

// SetRetryConfigProviderFromYAML loads retry configuration from YAML and
// installs the resulting provider into the shared transport.
func SetRetryConfigProviderFromYAML(data []byte) (*ManagedRetryConfigProvider, error) {
	provider, err := NewManagedRetryConfigProviderFromYAML(data)
	if err != nil {
		return nil, err
	}
	SetRetryConfigProvider(provider)
	return provider, nil
}

func mutateClientTransport(apply func(*Transport)) {
	transport, exists := cli.Transport.(*Transport)
	if !exists {
		transport = NewTransport()
	}
	transport.mu.Lock()
	apply(transport)
	transport.mu.Unlock()
	if !exists {
		cli.Transport = transport
	}
}

// Setup initializes caller identity and retry configuration from the current
// process arguments. It recognizes `--env`/`-e`, `--name`/`-n` and
// `--consul`/`-c`.
func Setup() error {
	return SetupWithOptions(parseSetupArgs(os.Args[1:]))
}

// SetupWithOptions initializes caller identity and retry configuration from the
// provided options instead of reading os.Args.
func SetupWithOptions(options SetupOptions) error {
	if options.Name != "" {
		SetCallerServiceName(options.Name)
	}
	if options.Consul == "" {
		return nil
	}
	_, err := setupRetryConfigFromConsul(options.Env, options.Name, options.Consul)
	if err != nil {
		return err
	}
	return nil
}

func parseSetupArgs(args []string) SetupOptions {
	var options SetupOptions
	for i := 0; i < len(args); i++ {
		key, value, consumed := parseSetupArg(args, i)
		if consumed {
			i++
		}
		switch key {
		case "env", "e":
			options.Env = value
		case "name", "n":
			options.Name = value
		case "consul", "c":
			options.Consul = value
		}
	}
	return options
}

func parseSetupArg(args []string, index int) (key, value string, consumedNext bool) {
	if index < 0 || index >= len(args) {
		return "", "", false
	}
	current := strings.TrimSpace(args[index])
	var trimmed string
	if strings.HasPrefix(current, "--") {
		trimmed = strings.TrimPrefix(current, "--")
	} else if strings.HasPrefix(current, "-") {
		trimmed = strings.TrimPrefix(current, "-")
	} else {
		return "", "", false
	}
	if trimmed == "" {
		return "", "", false
	}
	if before, after, ok := strings.Cut(trimmed, "="); ok {
		return strings.TrimSpace(before), strings.TrimSpace(after), false
	}
	if index+1 >= len(args) {
		return strings.TrimSpace(trimmed), "", false
	}
	next := strings.TrimSpace(args[index+1])
	if strings.HasPrefix(next, "-") {
		return strings.TrimSpace(trimmed), "", false
	}
	return strings.TrimSpace(trimmed), next, true
}

// RequestHeaderSetter adds or overrides headers before a request is sent.
type RequestHeaderSetter func(request *http.Request) error

func applyRequestHeaderSetter(req *http.Request, setHeaders RequestHeaderSetter) error {
	if setHeaders == nil {
		return nil
	}
	return setHeaders(req)
}

// AuthorizationInHeaderSetter adds authorization data to an outgoing request.
type AuthorizationInHeaderSetter interface {
	SetAuthorizationInHeader(request *http.Request) error
}

// AuthorizationInHeaderSetterFunc adapts a function to AuthorizationInHeaderSetter.
type AuthorizationInHeaderSetterFunc func(request *http.Request) error

// SetAuthorizationInHeader calls fn when it is non-nil.
func (fn AuthorizationInHeaderSetterFunc) SetAuthorizationInHeader(request *http.Request) error {
	if fn == nil {
		return nil
	}
	return fn(request)
}

func legacyRequestHeaderSetter(setter AuthorizationInHeaderSetter) RequestHeaderSetter {
	if isNilAuthorizationSetter(setter) {
		return nil
	}
	return setter.SetAuthorizationInHeader
}

func isNilAuthorizationSetter(setter AuthorizationInHeaderSetter) bool {
	if setter == nil {
		return true
	}
	value := reflect.ValueOf(setter)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// MultipartFormWithHeaders sends a multipart request after applying setHeaders.
func MultipartFormWithHeaders(ctx context.Context, method, url string, mf MultipartFormData, expectedPtr any, setHeaders RequestHeaderSetter) error {
	body, boundary, err := createMultipart(ctx, mf)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", boundary)
	if err := applyRequestHeaderSetter(req, setHeaders); err != nil {
		return err
	}

	return Do(req, expectedPtr)
}

// PostMultipartFormWithHeaders sends a multipart POST request after applying setHeaders.
func PostMultipartFormWithHeaders(ctx context.Context, url string, mf MultipartFormData, expectedPtr any, setHeaders RequestHeaderSetter) error {
	return MultipartFormWithHeaders(ctx, http.MethodPost, url, mf, expectedPtr, setHeaders)
}

// PutMultipartFormWithHeaders sends a multipart PUT request after applying setHeaders.
func PutMultipartFormWithHeaders(ctx context.Context, url string, mf MultipartFormData, expectedPtr any, setHeaders RequestHeaderSetter) error {
	return MultipartFormWithHeaders(ctx, http.MethodPut, url, mf, expectedPtr, setHeaders)
}

// InternalPostMultipartForm sends a multipart POST request with an optional
// authorization-header setter through the shared client.
func InternalPostMultipartForm(ctx context.Context, url string, mf MultipartFormData, expectedPtr any, authorizationInHeaderSetter AuthorizationInHeaderSetter) error {
	return InternalMultipartForm(ctx, http.MethodPost, url, mf, expectedPtr, authorizationInHeaderSetter)
}

// InternalPutMultipartForm sends a multipart PUT request with an optional
// authorization-header setter through the shared client.
func InternalPutMultipartForm(ctx context.Context, url string, mf MultipartFormData, expectedPtr any, authorizationInHeaderSetter AuthorizationInHeaderSetter) error {
	return InternalMultipartForm(ctx, http.MethodPut, url, mf, expectedPtr, authorizationInHeaderSetter)
}

// InternalMultipartForm sends a multipart request with an optional
// authorization-header setter through the shared client.
func InternalMultipartForm(ctx context.Context, method, url string, mf MultipartFormData, expectedPtr any, authorizationInHeaderSetter AuthorizationInHeaderSetter) error {
	return MultipartFormWithHeaders(ctx, method, url, mf, expectedPtr, legacyRequestHeaderSetter(authorizationInHeaderSetter))
}

// InternalPost sends a POST request with an optional authorization-header
// setter through the shared client.
func InternalPost(ctx context.Context, url, contentType string, body any, expectedPtr any, authorizationInHeaderSetter AuthorizationInHeaderSetter) error {
	return InternalWithMethod(ctx, url, http.MethodPost, contentType, body, expectedPtr, authorizationInHeaderSetter)
}

// InternalPut sends a PUT request with an optional authorization-header setter
// through the shared client.
func InternalPut(ctx context.Context, url, contentType string, body any, expectedPtr any, authorizationInHeaderSetter AuthorizationInHeaderSetter) error {
	return InternalWithMethod(ctx, url, http.MethodPut, contentType, body, expectedPtr, authorizationInHeaderSetter)
}

// InternalDelete sends a DELETE request with an optional authorization-header
// setter through the shared client.
func InternalDelete(ctx context.Context, url, contentType string, body any, expectedPtr any, authorizationInHeaderSetter AuthorizationInHeaderSetter) error {
	return InternalWithMethod(ctx, url, http.MethodDelete, contentType, body, expectedPtr, authorizationInHeaderSetter)
}

// InternalWithMethod sends a request with the supplied HTTP method and an
// optional authorization-header setter through the shared client.
func InternalWithMethod(ctx context.Context, url, method, contentType string, body any, expectedPtr any, authorizationInHeaderSetter AuthorizationInHeaderSetter) error {
	return SendWithHeaders(ctx, method, url, contentType, body, expectedPtr, legacyRequestHeaderSetter(authorizationInHeaderSetter))
}

// InternalGet sends a GET request with an optional authorization-header setter
// through the shared client.
func InternalGet(ctx context.Context, url string, expectedPtr any, setAuthorizationInHeader func(request *http.Request) error) error {
	return GetWithHeaders(ctx, url, expectedPtr, RequestHeaderSetter(setAuthorizationInHeader))
}

// SendWithHeaders sends a request with the supplied method after applying setHeaders.
func SendWithHeaders(ctx context.Context, method, url, contentType string, body any, expectedPtr any, setHeaders RequestHeaderSetter) error {
	r, err := getBodyReader(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, method, url, r)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", contentType)
	if err := applyRequestHeaderSetter(req, setHeaders); err != nil {
		return err
	}

	return Do(req, expectedPtr)
}

// PostWithHeaders sends a POST request after applying setHeaders.
func PostWithHeaders(ctx context.Context, url, contentType string, body any, expectedPtr any, setHeaders RequestHeaderSetter) error {
	return SendWithHeaders(ctx, http.MethodPost, url, contentType, body, expectedPtr, setHeaders)
}

// PutWithHeaders sends a PUT request after applying setHeaders.
func PutWithHeaders(ctx context.Context, url, contentType string, body any, expectedPtr any, setHeaders RequestHeaderSetter) error {
	return SendWithHeaders(ctx, http.MethodPut, url, contentType, body, expectedPtr, setHeaders)
}

// DeleteWithHeaders sends a DELETE request after applying setHeaders.
func DeleteWithHeaders(ctx context.Context, url, contentType string, body any, expectedPtr any, setHeaders RequestHeaderSetter) error {
	return SendWithHeaders(ctx, http.MethodDelete, url, contentType, body, expectedPtr, setHeaders)
}

// Post sends a POST request through the shared client.
func Post(ctx context.Context, url, contentType string, body any, expectedPtr any) error {
	return PostWithHeaders(ctx, url, contentType, body, expectedPtr, nil)
}

// Put sends a PUT request through the shared client.
func Put(ctx context.Context, url, contentType string, body any, expectedPtr any) error {
	return PutWithHeaders(ctx, url, contentType, body, expectedPtr, nil)
}

// Delete sends a DELETE request through the shared client.
func Delete(ctx context.Context, url, contentType string, body any, expectedPtr any) error {
	return DeleteWithHeaders(ctx, url, contentType, body, expectedPtr, nil)
}

// GetWithHeaders sends a GET request after applying setHeaders.
func GetWithHeaders(ctx context.Context, url string, expectedPtr any, setHeaders RequestHeaderSetter) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if err := applyRequestHeaderSetter(req, setHeaders); err != nil {
		return err
	}
	return Do(req, expectedPtr)
}

// Get sends a GET request through the shared client.
func Get(ctx context.Context, url string, expectedPtr any) error {
	return GetWithHeaders(ctx, url, expectedPtr, nil)
}

// Head sends a HEAD request through the shared client.
func Head(ctx context.Context, url string, expectedPtr any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return err
	}
	return Do(req, expectedPtr)
}

// PostMultipartForm sends a multipart POST request through the shared client.
func PostMultipartForm(ctx context.Context, url string, mf MultipartFormData, expectedPtr any) error {
	return PostMultipartFormWithHeaders(ctx, url, mf, expectedPtr, nil)
}

// PutMultipartForm sends a multipart PUT request through the shared client.
func PutMultipartForm(ctx context.Context, url string, mf MultipartFormData, expectedPtr any) error {
	return PutMultipartFormWithHeaders(ctx, url, mf, expectedPtr, nil)
}

func createMultipart(ctx context.Context, mf MultipartFormData) (*bytes.Buffer, string, error) {
	var file multipart.File
	var err error
	if mf.File != nil {
		file, err = mf.File.Open()
		if err != nil {
			slog.ErrorContext(ctx, "failed to open file", "error", err)
			return nil, "", fmt.Errorf("InternalPostForm failed to open file: %v", err)
		}
		defer file.Close()
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	encoder := form.NewEncoder()
	formData, err := encoder.Encode(mf.Form)
	if err != nil {
		slog.ErrorContext(ctx, "failed to encode struct as form", "error", err)
		return nil, "", fmt.Errorf("failed to encode struct as form: %v", err)
	}

	for key, values := range formData {
		for _, value := range values {
			err = writer.WriteField(key, value)
			if err != nil {
				slog.ErrorContext(ctx, "failed to write form field", "error", err)
				return nil, "", fmt.Errorf("failed to write form field: %v", err)
			}
		}
	}
	if mf.File != nil {
		fileFieldName := mf.FileName
		if fileFieldName == "" {
			fileFieldName = "file"
		}
		part, err := writer.CreateFormFile(fileFieldName, mf.File.Filename)
		if err != nil {
			slog.ErrorContext(ctx, "failed to create form file", "error", err)
			return nil, "", fmt.Errorf("failed to create form file: %v", err)
		}
		_, err = io.Copy(part, file)
		if err != nil {
			slog.ErrorContext(ctx, "failed to copy file content", "error", err)
			return nil, "", fmt.Errorf("failed to copy file content: %v", err)
		}
	}
	err = writer.Close()
	if err != nil {
		slog.ErrorContext(ctx, "failed to close multipart write", "error", err)
		return nil, "", fmt.Errorf("failed to close multipart writer: %v", err)
	}
	return body, writer.FormDataContentType(), nil
}

// RespHandler closes the response body and decodes it into expectedPtr.
func RespHandler(resp *http.Response, expectedPtr any) error {
	if resp == nil {
		return fmt.Errorf("response empty")
	}
	var response any
	if responsePtr, ok := expectedPtr.(*ResponsePtr); ok {
		response = responsePtr.target()
		responsePtr.StatusCode = resp.StatusCode
	} else {
		response = expectedPtr
	}
	if response == nil {
		discardRetryResponse(resp)
		return nil
	}
	if resp.Body == nil {
		return fmt.Errorf("response body empty, status: %v", resp.StatusCode)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}
	if strings.Contains(resp.Header.Get("Content-Type"), protobufContentType) {
		if message, ok := response.(proto.Message); ok {
			if err := proto.Unmarshal(raw, message); err != nil {
				return fmt.Errorf("unmarshalling proto response body: %w", err)
			}
			slog.DebugContext(resp.Request.Context(), "HTTPClient Response", "Protobuf Body", message)
			return nil
		}
	}
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, response)

}

// Do sends req through the shared client and decodes its response.
func Do(req *http.Request, expectedPtr any) error {
	var response any
	if responsePtr, ok := expectedPtr.(*ResponsePtr); ok {
		response = responsePtr.target()
	} else {
		response = expectedPtr
	}
	if _, ok := response.(proto.Message); ok {
		req.Header.Set("Accept", protobufContentType)
	}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	return RespHandler(resp, expectedPtr)
}

func getBodyReader(body any) (io.Reader, error) {
	if body == nil {
		return nil, nil
	}
	p, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(p), nil
}

func newTransportWithOptions(options ClientOptions) *Transport {
	roundTripper := options.RoundTripper
	if roundTripper == nil {
		roundTripper = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			DisableCompression:    false,
			MaxIdleConns:          0,
			MaxIdleConnsPerHost:   5000,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}
	resolver := options.ResolvePolicy
	if resolver == nil {
		resolver = DefaultRetryPolicyResolver
	}
	return &Transport{
		RoundTripper:   roundTripper,
		Skipper:        options.Skipper,
		ResolvePolicy:  resolver,
		ConfigProvider: options.ConfigProvider,
		Observer:       options.Observer,
		ResultObserver: options.ResultObserver,
	}
}

func newClientWithOptions(options ClientOptions) *http.Client {
	return &http.Client{
		Transport:     newTransportWithOptions(options),
		Timeout:       options.RequestTimeout,
		CheckRedirect: options.CheckRedirect,
		Jar:           options.Jar,
	}
}
