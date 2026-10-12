package blocklag

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yermakovsa/failnext"
)

type probeRoundTripper func(*http.Request) (*http.Response, error)

func (f probeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newProbeMonitor(t *testing.T, endpoint failnext.Endpoint, base http.RoundTripper) *Monitor {
	t.Helper()
	m, err := New(Config{Endpoints: []failnext.Endpoint{endpoint}, Base: base})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func probeHTTPResponse(status int, body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}
}

func jsonHeightResult(result string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":%s}`, result)
}

func TestProbeDirectHTTPPost(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.URL.Path != "/rpc" || r.URL.RawQuery != "key=secret" {
			t.Errorf("request destination = %q, want /rpc?key=secret", r.URL.String())
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
		}
		if string(body) != blockNumberRequest {
			t.Errorf("request body = %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, jsonHeightResult(`"0x3e8"`))
	}))
	defer server.Close()

	endpoint := failnext.Endpoint{ID: "primary", URL: server.URL + "/rpc?key=secret"}
	monitor := newProbeMonitor(t, endpoint, nil)
	height, err := monitor.probe(context.Background(), endpoint)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if height != 1000 {
		t.Fatalf("height = %d, want 1000", height)
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("request count = %d, want 1", n)
	}
}

func TestParseBlockNumberResponse(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		want   uint64
		failed bool
	}{
		{name: "zero", body: jsonHeightResult(`"0x0"`)},
		{name: "regular height", body: jsonHeightResult(`"0x3e8"`), want: 1000},
		{name: "uppercase hex digits", body: jsonHeightResult(`"0xABcd"`), want: 43981},
		{name: "max uint64", body: jsonHeightResult(`"0xffffffffffffffff"`), want: ^uint64(0)},
		{name: "null error permitted", body: `{"jsonrpc":"2.0","id":1,"result":"0x1","error":null}`, want: 1},
		{name: "malformed JSON", body: `{"jsonrpc":`, failed: true},
		{name: "trailing document", body: jsonHeightResult(`"0x1"`) + `{}`, failed: true},
		{name: "non-object", body: `[]`, failed: true},
		{name: "wrong version", body: `{"jsonrpc":"1.0","id":1,"result":"0x1"}`, failed: true},
		{name: "missing version", body: `{"id":1,"result":"0x1"}`, failed: true},
		{name: "wrong ID", body: `{"jsonrpc":"2.0","id":2,"result":"0x1"}`, failed: true},
		{name: "string ID", body: `{"jsonrpc":"2.0","id":"1","result":"0x1"}`, failed: true},
		{name: "missing ID", body: `{"jsonrpc":"2.0","result":"0x1"}`, failed: true},
		{name: "RPC error", body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"unavailable"}}`, failed: true},
		{name: "RPC error with result", body: `{"jsonrpc":"2.0","id":1,"result":"0x1","error":{"code":-32000}}`, failed: true},
		{name: "missing result", body: `{"jsonrpc":"2.0","id":1}`, failed: true},
		{name: "null result", body: jsonHeightResult(`null`), failed: true},
		{name: "numeric result", body: jsonHeightResult(`123`), failed: true},
		{name: "negative quantity", body: jsonHeightResult(`"-0x1"`), failed: true},
		{name: "decimal quantity", body: jsonHeightResult(`"1000"`), failed: true},
		{name: "missing digits", body: jsonHeightResult(`"0x"`), failed: true},
		{name: "leading zero", body: jsonHeightResult(`"0x00"`), failed: true},
		{name: "leading zero nonzero", body: jsonHeightResult(`"0x01"`), failed: true},
		{name: "invalid hex", body: jsonHeightResult(`"0xGG"`), failed: true},
		{name: "sign after prefix", body: jsonHeightResult(`"0x+1"`), failed: true},
		{name: "uppercase prefix", body: jsonHeightResult(`"0X1"`), failed: true},
		{name: "overflow", body: jsonHeightResult(`"0x10000000000000000"`), failed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBlockNumberResponse([]byte(tt.body))
			if tt.failed {
				if err == nil {
					t.Fatalf("parseBlockNumberResponse(%q) = %d, want error", tt.body, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseBlockNumberResponse: %v", err)
			}
			if got != tt.want {
				t.Fatalf("block number = %d, want %d", got, tt.want)
			}
		})
	}
}

type probeTrackingBody struct {
	reader io.Reader
	closed bool
	read   int
}

func (b *probeTrackingBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}

func (b *probeTrackingBody) Close() error {
	b.closed = true
	return nil
}

func TestProbeClosesResponsesAndLimitsBody(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{name: "success", status: http.StatusOK, body: jsonHeightResult(`"0x1"`)},
		{name: "HTTP error", status: http.StatusServiceUnavailable, body: "unavailable", wantErr: true},
		{name: "invalid JSON", status: http.StatusOK, body: "garbage", wantErr: true},
		{name: "oversized response", status: http.StatusOK, body: strings.Repeat("x", maxProbeResponseBytes+100), wantErr: true},
	}
	endpoint := failnext.Endpoint{ID: "primary", URL: "https://primary.example/rpc"}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			body := &probeTrackingBody{reader: strings.NewReader(tt.body)}
			monitor := newProbeMonitor(t, endpoint, probeRoundTripper(func(*http.Request) (*http.Response, error) {
				return probeHTTPResponse(tt.status, body), nil
			}))
			_, err := monitor.probe(context.Background(), endpoint)
			if (err != nil) != tt.wantErr {
				t.Fatalf("probe error = %v, wantErr = %t", err, tt.wantErr)
			}
			if !body.closed {
				t.Fatal("response body not closed")
			}
			if tt.name == "oversized response" {
				if !strings.Contains(err.Error(), "too large") {
					t.Fatalf("error = %q, want response too large", err)
				}
				if body.read != maxProbeResponseBytes+1 {
					t.Fatalf("read %d bytes, want %d", body.read, maxProbeResponseBytes+1)
				}
			}
		})
	}
}

func TestProbeDoesNotFollowRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		_, _ = io.WriteString(w, jsonHeightResult(`"0x10"`))
	}))
	defer target.Close()

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	endpoint := failnext.Endpoint{ID: "primary", URL: redirect.URL}
	monitor := newProbeMonitor(t, endpoint, nil)
	_, err := monitor.probe(context.Background(), endpoint)
	if err == nil || !strings.Contains(err.Error(), "307") {
		t.Fatalf("probe error = %v, want HTTP 307", err)
	}
	if calls := targetCalls.Load(); calls != 0 {
		t.Fatalf("followed redirect: target received %d request(s)", calls)
	}
}

func TestProbeTransportError(t *testing.T) {
	endpoint := failnext.Endpoint{ID: "primary", URL: "https://primary.example/rpc"}
	transportErr := errors.New("network failed")
	monitor := newProbeMonitor(t, endpoint, probeRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, transportErr
	}))
	_, err := monitor.probe(context.Background(), endpoint)
	if !errors.Is(err, transportErr) {
		t.Fatalf("probe error = %v, want wrapped transport error", err)
	}
}

type probeFailReader struct{ err error }

func (r probeFailReader) Read([]byte) (int, error) { return 0, r.err }

func TestProbeReadError(t *testing.T) {
	endpoint := failnext.Endpoint{ID: "primary", URL: "https://primary.example/rpc"}
	readErr := errors.New("read failed")
	body := &probeTrackingBody{reader: probeFailReader{err: readErr}}
	monitor := newProbeMonitor(t, endpoint, probeRoundTripper(func(*http.Request) (*http.Response, error) {
		return probeHTTPResponse(http.StatusOK, body), nil
	}))
	_, err := monitor.probe(context.Background(), endpoint)
	if !errors.Is(err, readErr) {
		t.Fatalf("probe error = %v, want wrapped read error", err)
	}
	if !body.closed {
		t.Fatal("response body not closed after read error")
	}
}

func TestProbeCancellationInterruptsHTTP(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fully consume the request before blocking the response.
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-release
	}))
	defer server.Close()
	// Release the handler even if an assertion fails, before Server.Close.
	defer close(release)

	endpoint := failnext.Endpoint{ID: "primary", URL: server.URL}
	monitor := newProbeMonitor(t, endpoint, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := monitor.probe(ctx, endpoint)
		result <- err
	}()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("probe did not reach the HTTP server")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("probe error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("probe did not honor context cancellation")
	}
}

func TestProbeCancellationTakesPrecedenceOverReturnedResponse(t *testing.T) {
	endpoint := failnext.Endpoint{ID: "primary", URL: "https://primary.example/rpc"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &probeTrackingBody{reader: strings.NewReader(jsonHeightResult(`"0x1"`))}
	monitor := newProbeMonitor(t, endpoint, probeRoundTripper(func(*http.Request) (*http.Response, error) {
		cancel()
		return probeHTTPResponse(http.StatusOK, body), nil
	}))
	_, err := monitor.probe(ctx, endpoint)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("probe error = %v, want context.Canceled", err)
	}
	if !body.closed {
		t.Fatal("response body not closed after cancellation")
	}
}
