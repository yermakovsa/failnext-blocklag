// Package blocklag is a Go companion library for failnext that detects relative
// block-height lag across a fixed set of Ethereum HTTP JSON-RPC providers.
//
// Call Monitor.Check to query providers directly with eth_blockNumber, then
// pass Monitor.Eligible to failnext.Config.Eligible.
//
// blocklag determines provider eligibility from relative block-height lag;
// failnext handles endpoint selection and HTTP failover. failnext itself does
// not detect blockchain lag or stale state.
//
// Checks are initiated by the caller; no background polling occurs. Eligibility
// reflects the last published comparison, not verified canonical-chain freshness.
package blocklag

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/yermakovsa/failnext"
)

// Config configures a Monitor.
type Config struct {
	// Endpoints is the fixed set of Ethereum HTTP JSON-RPC destinations.
	// Each endpoint needs a nonempty, unique ID and an absolute HTTP or HTTPS
	// URL with a host and no fragment. Endpoints should be on the same
	// Ethereum-compatible chain.
	Endpoints []failnext.Endpoint

	// MaxLag is the largest permitted difference from the highest successfully
	// observed block in each Check. The limit is inclusive; zero is valid.
	MaxLag uint64

	// Base performs blocklag's direct HTTP probes, not failnext's application
	// requests. If nil, http.DefaultTransport is used. A custom transport must
	// preserve the requested destination, honor request cancellation, and must
	// not route probes through failnext.
	Base http.RoundTripper
}

// Monitor maintains eligibility based on the last published comparison of
// block heights for a fixed set of Ethereum RPC endpoints. Construct it with
// New; the zero value is not usable.
type Monitor struct {
	endpoints []failnext.Endpoint
	maxLag    uint64
	base      http.RoundTripper

	// A token in checkGate grants permission to run one monitoring round.
	// A channel lets a waiting Check return when its context is canceled.
	checkGate chan struct{}

	mu       sync.RWMutex
	eligible map[failnext.EndpointID]bool
}

// New validates the configuration and constructs a Monitor.
// It copies the endpoint slice so later caller modifications do not affect it.
// All configured endpoints are initially eligible.
func New(cfg Config) (*Monitor, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("blocklag: no endpoints")
	}

	initial := make(map[failnext.EndpointID]bool, len(cfg.Endpoints))
	endpoints := make([]failnext.Endpoint, len(cfg.Endpoints))
	for i, endpoint := range cfg.Endpoints {
		if endpoint.ID == "" {
			return nil, fmt.Errorf("blocklag: endpoint at index %d: empty ID", i)
		}
		if initial[endpoint.ID] {
			return nil, fmt.Errorf("blocklag: duplicate endpoint ID %q", endpoint.ID)
		}

		u, err := url.Parse(endpoint.URL)
		if err != nil {
			// url.Parse errors can include the original URL, including API keys.
			return nil, fmt.Errorf("blocklag: endpoint %q: invalid URL", endpoint.ID)
		}
		if !u.IsAbs() {
			return nil, fmt.Errorf("blocklag: endpoint %q: URL must be absolute", endpoint.ID)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("blocklag: endpoint %q: URL must use HTTP or HTTPS", endpoint.ID)
		}
		if u.Hostname() == "" {
			return nil, fmt.Errorf("blocklag: endpoint %q: URL has no host", endpoint.ID)
		}
		if strings.Contains(endpoint.URL, "#") {
			return nil, fmt.Errorf("blocklag: endpoint %q: URL must not have a fragment", endpoint.ID)
		}

		endpoints[i] = endpoint
		initial[endpoint.ID] = true
	}

	base := cfg.Base
	if base == nil {
		base = http.DefaultTransport
	}

	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &Monitor{
		endpoints: endpoints,
		maxLag:    cfg.MaxLag,
		base:      base,
		checkGate: gate,
		eligible:  initial,
	}, nil
}

// Check probes all configured providers directly with eth_blockNumber, outside
// failnext, and publishes their relative eligibility. Provider probes run
// concurrently, but overlapping calls to Check are serialized.
//
// A successful probe is eligible when its height is within MaxLag blocks of
// the highest successfully observed height. If at least one probe succeeds,
// Check publishes a complete new snapshot; failed probes are ineligible.
// Partial probe failure still returns an error, even though state is updated.
// Probe errors identify failed endpoint IDs; multiple errors are joined.
//
// If every probe fails or cancellation is observed before publication, the
// previous snapshot is retained. A waiting Check can be canceled without
// starting probes.
// The caller owns the checking schedule; published eligibility never expires.
func (m *Monitor) Check(ctx context.Context) error {
	if ctx == nil {
		return errors.New("blocklag: nil check context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.checkGate:
	}
	defer func() { m.checkGate <- struct{}{} }()

	if err := ctx.Err(); err != nil {
		return err
	}

	type probeResult struct {
		height uint64
		err    error
	}
	results := make([]probeResult, len(m.endpoints))
	var wg sync.WaitGroup
	for i, endpoint := range m.endpoints {
		wg.Add(1)
		go func(i int, endpoint failnext.Endpoint) {
			defer wg.Done()
			results[i].height, results[i].err = m.probe(ctx, endpoint)
		}(i, endpoint)
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}

	var highest uint64
	var successful int
	var failures []error
	for i, result := range results {
		if result.err != nil {
			failures = append(failures, fmt.Errorf("blocklag: endpoint %q: %w", m.endpoints[i].ID, result.err))
			continue
		}
		successful++
		if result.height > highest {
			highest = result.height
		}
	}
	if successful == 0 {
		return errors.Join(failures...)
	}

	next := make(map[failnext.EndpointID]bool, len(m.endpoints))
	for i, endpoint := range m.endpoints {
		result := results[i]
		next[endpoint.ID] = result.err == nil && highest-result.height <= m.maxLag
	}

	m.mu.Lock()
	// Check again under the publication lock to avoid committing a canceled
	// round when cancellation is already observable.
	if err := ctx.Err(); err != nil {
		m.mu.Unlock()
		return err
	}
	m.eligible = next
	m.mu.Unlock()

	return errors.Join(failures...)
}

// Eligible reports whether id is eligible according to the latest published
// snapshot. All configured endpoints are initially eligible; unknown IDs are
// always ineligible. It performs no network I/O and is safe for concurrent use.
// Separate calls may observe different snapshots if Check publishes between
// them.
// It can be used directly as failnext.Config.Eligible. It only supplies
// eligibility; failnext still performs endpoint selection and HTTP failover.
func (m *Monitor) Eligible(id failnext.EndpointID) bool {
	m.mu.RLock()
	eligible := m.eligible[id]
	m.mu.RUnlock()
	return eligible
}
