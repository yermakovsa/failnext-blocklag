package blocklag_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/yermakovsa/failnext"
	"github.com/yermakovsa/failnext-blocklag"
)

func ExampleMonitor() {
	// Local test servers stand in for two Ethereum HTTP RPC providers.
	provider := func(block, balance string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var request struct {
				Method string `json:"method"`
			}
			if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&request) != nil {
				http.Error(w, "invalid RPC request", http.StatusBadRequest)
				return
			}

			var result string
			switch request.Method {
			case "eth_blockNumber":
				result = block
			case "eth_getBalance":
				result = balance
			default:
				http.Error(w, "unexpected RPC method", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":%q}`, result)
		})
	}

	primary := httptest.NewServer(provider("0x64", "0x0")) // Block 100.
	defer primary.Close()
	backup := httptest.NewServer(provider("0x6e", "0x2a")) // Block 110.
	defer backup.Close()

	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: primary.URL},
		{ID: "backup", URL: backup.URL},
	}
	monitor, err := blocklag.New(blocklag.Config{
		Endpoints: endpoints,
		MaxLag:    3,
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := monitor.Check(ctx); err != nil {
		panic(err)
	}

	fmt.Println("primary eligible:", monitor.Eligible("primary"))
	fmt.Println("backup eligible:", monitor.Eligible("backup"))

	tr, err := failnext.New(failnext.Config{
		Endpoints: endpoints,
		Eligible:  monitor.Eligible,
	})
	if err != nil {
		panic(err)
	}
	client := &http.Client{Transport: tr}
	defer client.CloseIdleConnections()

	// Ethereum JSON-RPC reads use POST. Allow failover only when the
	// application knows the operation is safe to repeat.
	readCtx := failnext.WithFailoverAllowed(ctx)
	req, err := http.NewRequestWithContext(readCtx, http.MethodPost, endpoints[0].URL,
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","latest"]}`))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var rpcResult struct {
		Result string `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResult); err != nil {
		panic(err)
	}
	fmt.Println("balance:", rpcResult.Result)

	// Output:
	// primary eligible: false
	// backup eligible: true
	// balance: 0x2a
}
