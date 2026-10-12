package blocklag

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/yermakovsa/failnext"
)

const (
	blockNumberRequest    = `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`
	maxProbeResponseBytes = 4 * 1024
)

// probe obtains the block number directly from one configured endpoint.
// It does not use failnext routing, follow redirects, or retry.
func (m *Monitor) probe(ctx context.Context, endpoint failnext.Endpoint) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL, strings.NewReader(blockNumberRequest))
	if err != nil {
		// New already validates URLs. Avoid including credentials from a URL
		// in an unexpected request construction error.
		return 0, errors.New("could not create probe request")
	}
	req.Header.Set("Content-Type", "application/json")

	// Call RoundTrip directly. Unlike http.Client.Do, it does not follow
	// redirects; a custom Base must also preserve the requested destination.
	resp, err := m.base.RoundTrip(req)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	if err != nil {
		return 0, fmt.Errorf("HTTP probe: %w", err)
	}
	if resp == nil {
		return 0, errors.New("HTTP probe returned no response")
	}
	if resp.Body == nil {
		return 0, errors.New("HTTP probe returned no response body")
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP probe returned status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeResponseBytes+1))
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	if err != nil {
		return 0, fmt.Errorf("reading probe response: %w", err)
	}
	if len(data) > maxProbeResponseBytes {
		return 0, errors.New("probe response too large")
	}

	return parseBlockNumberResponse(data)
}

func parseBlockNumberResponse(data []byte) (uint64, error) {
	var response struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return 0, fmt.Errorf("invalid JSON-RPC response: %w", err)
	}
	if response.JSONRPC != "2.0" || !bytes.Equal(bytes.TrimSpace(response.ID), []byte("1")) {
		return 0, errors.New("unexpected JSON-RPC version or ID")
	}
	if len(response.Error) != 0 && !bytes.Equal(bytes.TrimSpace(response.Error), []byte("null")) {
		return 0, errors.New("JSON-RPC error response")
	}
	if len(response.Result) == 0 || response.Result[0] != '"' {
		return 0, errors.New("missing or invalid block number result")
	}

	var quantity string
	if err := json.Unmarshal(response.Result, &quantity); err != nil {
		return 0, errors.New("invalid block number result")
	}
	if len(quantity) < 3 || !strings.HasPrefix(quantity, "0x") {
		return 0, errors.New("invalid block number quantity")
	}
	digits := quantity[2:]
	if len(digits) > 1 && digits[0] == '0' {
		return 0, errors.New("invalid block number quantity")
	}
	for _, digit := range digits {
		if !(digit >= '0' && digit <= '9' || digit >= 'a' && digit <= 'f' || digit >= 'A' && digit <= 'F') {
			return 0, errors.New("invalid block number quantity")
		}
	}
	height, err := strconv.ParseUint(digits, 16, 64)
	if err != nil {
		return 0, errors.New("block number overflows uint64")
	}
	return height, nil
}
