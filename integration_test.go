package blocklag

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/yermakovsa/failnext"
)

// The monitor probes each physical RPC provider directly. The same endpoint
// identities feed failnext's Eligible callback when routing application GETs.
// Both the lagging and probe-failing cases must skip the primary, and a later
// successful check must make it selectable again.
func TestFailnextCompositionSkipsLaggingAndRestoresProvider(t *testing.T) {
	type provider struct {
		name       string
		height     atomic.Uint64
		probeFails atomic.Bool
		probes     atomic.Int32
		requests   atomic.Int32
	}
	newProvider := func(p *provider) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost:
				p.probes.Add(1)
				var call struct {
					JSONRPC string `json:"jsonrpc"`
					ID      int    `json:"id"`
					Method  string `json:"method"`
				}
				if err := json.NewDecoder(r.Body).Decode(&call); err != nil ||
					call.JSONRPC != "2.0" || call.ID != 1 || call.Method != "eth_blockNumber" {
					http.Error(w, "unexpected probe", http.StatusBadRequest)
					return
				}
				if p.probeFails.Load() {
					http.Error(w, "probe unavailable", http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":"0x%x"}`, p.height.Load())
			case http.MethodGet:
				p.requests.Add(1)
				_, _ = io.WriteString(w, p.name)
			default:
				http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			}
		}))
	}

	primary := &provider{name: "primary"}
	backup := &provider{name: "backup"}
	primary.height.Store(100)
	backup.height.Store(110)
	primaryServer := newProvider(primary)
	defer primaryServer.Close()
	backupServer := newProvider(backup)
	defer backupServer.Close()

	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: primaryServer.URL + "/rpc"},
		{ID: "backup", URL: backupServer.URL + "/rpc"},
	}
	monitor, err := New(Config{Endpoints: endpoints, MaxLag: 2})
	if err != nil {
		t.Fatalf("new monitor: %v", err)
	}
	tr, err := failnext.New(failnext.Config{
		Endpoints: endpoints,
		Eligible:  monitor.Eligible,
	})
	if err != nil {
		t.Fatalf("new failnext transport: %v", err)
	}
	client := &http.Client{Transport: tr}
	defer client.CloseIdleConnections()

	get := func(want string) {
		t.Helper()
		// The logical request uses the primary URL. failnext selects the actual
		// destination using the eligibility decisions supplied by blocklag.
		resp, err := client.Get(endpoints[0].URL)
		if err != nil {
			t.Fatalf("application request: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read application response: %v", err)
		}
		if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != want {
			t.Fatalf("application response = (HTTP %d, %q), want HTTP 200 from %q", resp.StatusCode, body, want)
		}
	}

	// Before the initial check, endpoints are fail-open and priority wins.
	get("primary")
	if err := monitor.Check(context.Background()); err != nil {
		t.Fatalf("lag check: %v", err)
	}
	if primary.probes.Load() != 1 || backup.probes.Load() != 1 {
		t.Fatalf("expected one direct probe to each provider, got primary=%d backup=%d",
			primary.probes.Load(), backup.probes.Load())
	}
	get("backup")
	if got := primary.requests.Load(); got != 1 {
		t.Fatalf("lagging primary received application traffic: %d requests", got)
	}

	// An unsuccessful probe is also ineligible when another provider has
	// produced a valid height. The partially successful check reports an error.
	primary.probeFails.Store(true)
	if err := monitor.Check(context.Background()); err == nil || !strings.Contains(err.Error(), `endpoint "primary"`) {
		t.Fatalf("partial check = %v, want identified primary failure", err)
	}
	get("backup")
	if got := primary.requests.Load(); got != 1 {
		t.Fatalf("probe-failed primary received application traffic: %d requests", got)
	}

	// A fresh successful round restores primary selection in priority order.
	primary.probeFails.Store(false)
	primary.height.Store(115)
	backup.height.Store(115)
	if err := monitor.Check(context.Background()); err != nil {
		t.Fatalf("recovery check: %v", err)
	}
	get("primary")
	if got := primary.requests.Load(); got != 2 {
		t.Fatalf("recovered primary requests = %d, want 2", got)
	}
	if got := backup.requests.Load(); got != 2 {
		t.Fatalf("backup application requests = %d, want 2", got)
	}
}
