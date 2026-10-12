package blocklag

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yermakovsa/failnext"
)

func TestNewCopiesEndpointSlice(t *testing.T) {
	endpoints := []failnext.Endpoint{{ID: "primary", URL: "https://primary.example/rpc"}}
	monitor, err := New(Config{Endpoints: endpoints, MaxLag: 3})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	endpoints[0].ID = "changed"
	endpoints[0].URL = "https://changed.example/rpc"
	if got := monitor.endpoints[0]; got.ID != "primary" || got.URL != "https://primary.example/rpc" {
		t.Fatalf("caller mutated monitor configuration: %+v", got)
	}
	if monitor.maxLag != 3 {
		t.Fatalf("MaxLag = %d, want 3", monitor.maxLag)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name      string
		endpoints []failnext.Endpoint
		want      string
	}{
		{
			name: "no endpoints",
			want: "no endpoints",
		},
		{
			name:      "empty ID",
			endpoints: []failnext.Endpoint{{ID: "", URL: "https://primary.example"}},
			want:      "empty ID",
		},
		{
			name: "duplicate ID",
			endpoints: []failnext.Endpoint{
				{ID: "primary", URL: "https://primary.example"},
				{ID: "primary", URL: "https://backup.example"},
			},
			want: "duplicate endpoint ID",
		},
		{
			name:      "relative URL",
			endpoints: []failnext.Endpoint{{ID: "primary", URL: "/rpc"}},
			want:      "must be absolute",
		},
		{
			name:      "unsupported scheme",
			endpoints: []failnext.Endpoint{{ID: "primary", URL: "ws://primary.example/rpc"}},
			want:      "must use HTTP or HTTPS",
		},
		{
			name:      "missing host",
			endpoints: []failnext.Endpoint{{ID: "primary", URL: "https:///rpc"}},
			want:      "no host",
		},
		{
			name:      "empty hostname with port",
			endpoints: []failnext.Endpoint{{ID: "primary", URL: "http://:8545/rpc"}},
			want:      "no host",
		},
		{
			name:      "nonempty fragment",
			endpoints: []failnext.Endpoint{{ID: "primary", URL: "https://primary.example/rpc#section"}},
			want:      "must not have a fragment",
		},
		{
			name:      "empty fragment",
			endpoints: []failnext.Endpoint{{ID: "primary", URL: "https://primary.example/rpc#"}},
			want:      "must not have a fragment",
		},
		{
			name:      "malformed URL",
			endpoints: []failnext.Endpoint{{ID: "primary", URL: "https://primary.example/%zz?key=secret"}},
			want:      "invalid URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor, err := New(Config{Endpoints: tt.endpoints})
			if monitor != nil || err == nil {
				t.Fatalf("New() = (%v, %v), want (nil, error)", monitor, err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New() error = %q, want substring %q", err, tt.want)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("New() error disclosed credentials: %q", err)
			}
		})
	}
}

// scriptedCheckTransport supplies one response per provider, independently of
// arrival order. Each Check finishes all its probes before test state changes.
type scriptedCheckTransport struct {
	mu       sync.Mutex
	outcomes map[string]checkOutcome
}

type checkOutcome struct {
	height uint64
	err    error
}

func (t *scriptedCheckTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	outcome, exists := t.outcomes[req.URL.String()]
	t.mu.Unlock()
	if !exists {
		return nil, fmt.Errorf("unexpected probe to %s", req.URL.Host)
	}
	if outcome.err != nil {
		return nil, outcome.err
	}
	return checkHeightResponse(outcome.height), nil
}

func (t *scriptedCheckTransport) set(outcomes map[string]checkOutcome) {
	t.mu.Lock()
	t.outcomes = outcomes
	t.mu.Unlock()
}

func checkHeightResponse(height uint64) *http.Response {
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":"0x%x"}`, height)
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func newCheckMonitor(t *testing.T, endpoints []failnext.Endpoint, maxLag uint64, base http.RoundTripper) *Monitor {
	t.Helper()
	monitor, err := New(Config{Endpoints: endpoints, MaxLag: maxLag, Base: base})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return monitor
}

func assertEligibility(t *testing.T, monitor *Monitor, want map[failnext.EndpointID]bool) {
	t.Helper()
	for id, expected := range want {
		if got := monitor.Eligible(id); got != expected {
			t.Errorf("Eligible(%q) = %v, want %v", id, got, expected)
		}
	}
}

func TestCheckLagThresholdAndInitialEligibility(t *testing.T) {
	endpoints := []failnext.Endpoint{
		{ID: "latest", URL: "https://latest.test/rpc"},
		{ID: "near", URL: "https://near.test/rpc"},
		{ID: "boundary", URL: "https://boundary.test/rpc"},
		{ID: "lagging", URL: "https://lagging.test/rpc"},
	}
	base := &scriptedCheckTransport{outcomes: map[string]checkOutcome{
		endpoints[0].URL: {height: 1000},
		endpoints[1].URL: {height: 999},
		endpoints[2].URL: {height: 997},
		endpoints[3].URL: {height: 996},
	}}
	monitor := newCheckMonitor(t, endpoints, 3, base)

	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"latest": true, "near": true, "boundary": true, "lagging": true,
		"unknown": false, "": false,
	})
	if err := monitor.Check(context.Background()); err != nil {
		t.Fatalf("Check: %v", err)
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"latest": true, "near": true, "boundary": true, "lagging": false,
		"unknown": false,
	})

	// A new usable monitoring round replaces the old snapshot, including
	// restoring eligibility for a previously lagging endpoint.
	base.set(map[string]checkOutcome{
		endpoints[0].URL: {height: 1001},
		endpoints[1].URL: {height: 1003},
		endpoints[2].URL: {height: 1002},
		endpoints[3].URL: {height: 1003},
	})
	if err := monitor.Check(context.Background()); err != nil {
		t.Fatalf("recovery Check: %v", err)
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"latest": true, "near": true, "boundary": true, "lagging": true,
	})
}

func TestCheckZeroLag(t *testing.T) {
	endpoints := []failnext.Endpoint{
		{ID: "a", URL: "https://a.test/rpc"},
		{ID: "b", URL: "https://b.test/rpc"},
		{ID: "c", URL: "https://c.test/rpc"},
	}
	base := &scriptedCheckTransport{outcomes: map[string]checkOutcome{
		endpoints[0].URL: {height: 100},
		endpoints[1].URL: {height: 100},
		endpoints[2].URL: {height: 99},
	}}
	monitor := newCheckMonitor(t, endpoints, 0, base)
	if err := monitor.Check(context.Background()); err != nil {
		t.Fatalf("Check: %v", err)
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"a": true, "b": true, "c": false,
	})
}

func TestCheckPartialFailurePublishesAndReportsError(t *testing.T) {
	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: "https://primary.test/rpc"},
		{ID: "backup", URL: "https://backup.test/rpc"},
		{ID: "third", URL: "https://third.test/rpc"},
	}
	unavailable := errors.New("RPC unavailable")
	base := &scriptedCheckTransport{outcomes: map[string]checkOutcome{
		endpoints[0].URL: {height: 500},
		endpoints[1].URL: {err: unavailable},
		endpoints[2].URL: {height: 496},
	}}
	monitor := newCheckMonitor(t, endpoints, 3, base)
	if err := monitor.Check(context.Background()); !errors.Is(err, unavailable) || !strings.Contains(err.Error(), `endpoint "backup"`) {
		t.Fatalf("Check error = %v, want identified wrapped failure", err)
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"primary": true, "backup": false, "third": false,
	})

	// Even one successful probe is sufficient to publish a replacement.
	base.set(map[string]checkOutcome{
		endpoints[0].URL: {err: unavailable},
		endpoints[1].URL: {height: 501},
		endpoints[2].URL: {err: unavailable},
	})
	if err := monitor.Check(context.Background()); !errors.Is(err, unavailable) {
		t.Fatalf("single-success Check error = %v", err)
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"primary": false, "backup": true, "third": false,
	})
}

func TestCheckAllFailedPreservesSnapshot(t *testing.T) {
	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: "https://primary.test/rpc"},
		{ID: "backup", URL: "https://backup.test/rpc"},
	}
	firstErr := errors.New("primary unavailable")
	secondErr := errors.New("backup unavailable")
	base := &scriptedCheckTransport{}
	base.set(map[string]checkOutcome{
		endpoints[0].URL: {err: firstErr},
		endpoints[1].URL: {err: secondErr},
	})
	monitor := newCheckMonitor(t, endpoints, 2, base)

	// No usable check has been published, so an all-failed check preserves
	// the initial fail-open state.
	if err := monitor.Check(context.Background()); !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("initial all-failed Check = %v", err)
	} else if strings.Index(err.Error(), `endpoint "primary"`) > strings.Index(err.Error(), `endpoint "backup"`) {
		t.Fatalf("probe errors not in endpoint order: %v", err)
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"primary": true, "backup": true,
	})

	base.set(map[string]checkOutcome{
		endpoints[0].URL: {height: 500},
		endpoints[1].URL: {height: 490},
	})
	if err := monitor.Check(context.Background()); err != nil {
		t.Fatalf("successful Check: %v", err)
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"primary": true, "backup": false,
	})

	base.set(map[string]checkOutcome{
		endpoints[0].URL: {err: firstErr},
		endpoints[1].URL: {err: secondErr},
	})
	if err := monitor.Check(context.Background()); !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("later all-failed Check = %v", err)
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"primary": true, "backup": false,
	})
}

func TestCheckCancellationPreservesSnapshot(t *testing.T) {
	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: "https://primary.test/rpc"},
		{ID: "backup", URL: "https://backup.test/rpc"},
	}
	base := &scriptedCheckTransport{outcomes: map[string]checkOutcome{
		endpoints[0].URL: {height: 800},
		endpoints[1].URL: {height: 790},
	}}
	monitor := newCheckMonitor(t, endpoints, 3, base)
	if err := monitor.Check(context.Background()); err != nil {
		t.Fatalf("initial Check: %v", err)
	}

	entered := make(chan struct{})
	monitor.base = probeRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() == endpoints[1].URL {
			close(entered)
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		return checkHeightResponse(1), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- monitor.Check(ctx) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("backup probe never started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Check cancellation error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Check did not return after cancellation")
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{
		"primary": true, "backup": false,
	})
}

func TestCheckWaitForGateRespectsCancellation(t *testing.T) {
	endpoint := failnext.Endpoint{ID: "primary", URL: "https://primary.test/rpc"}
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	base := probeRoundTripper(func(*http.Request) (*http.Response, error) {
		mu.Lock()
		calls++
		if calls == 1 {
			close(entered)
		}
		mu.Unlock()
		<-release
		return checkHeightResponse(10), nil
	})
	monitor := newCheckMonitor(t, []failnext.Endpoint{endpoint}, 0, base)
	first := make(chan error, 1)
	go func() { first <- monitor.Check(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("first check did not begin")
	}

	ctx, cancel := context.WithCancel(context.Background())
	waiting := make(chan error, 1)
	go func() { waiting <- monitor.Check(ctx) }()
	cancel()
	select {
	case err := <-waiting:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting Check = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled Check did not return while first Check was active")
	}

	// Reads of existing state remain possible while the check is blocked.
	readResult := make(chan bool, 1)
	go func() { readResult <- monitor.Eligible(endpoint.ID) }()
	select {
	case eligible := <-readResult:
		if !eligible {
			t.Fatal("published eligibility changed during in-flight Check")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Eligible blocked on in-flight network I/O")
	}
	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("probe calls = %d, want 1 before first finishes", gotCalls)
	}
	close(release)
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first Check: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first Check did not complete")
	}
}

func TestCheckProbesEndpointsConcurrently(t *testing.T) {
	endpoints := []failnext.Endpoint{
		{ID: "a", URL: "https://a.test/rpc"},
		{ID: "b", URL: "https://b.test/rpc"},
	}
	entered := make(chan string, 2)
	release := make(chan struct{})
	base := probeRoundTripper(func(req *http.Request) (*http.Response, error) {
		entered <- req.URL.String()
		<-release
		return checkHeightResponse(100), nil
	})
	monitor := newCheckMonitor(t, endpoints, 0, base)
	done := make(chan error, 1)
	go func() { done <- monitor.Check(context.Background()) }()
	for i := 0; i < len(endpoints); i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("provider probes did not run concurrently")
		}
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Check did not complete")
	}
	assertEligibility(t, monitor, map[failnext.EndpointID]bool{"a": true, "b": true})
}
