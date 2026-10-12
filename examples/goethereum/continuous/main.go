// This example uses two real, same-chain Ethereum HTTP RPC providers.
// Run with ETH_RPC_PRIMARY_URL and ETH_RPC_BACKUP_URL set.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/failnext"
	"github.com/yermakovsa/failnext-blocklag"
)

const (
	maxLag        = 3
	checkInterval = 15 * time.Second
	checkTimeout  = 5 * time.Second
	readInterval  = 10 * time.Second
	readTimeout   = 10 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	primaryURL := os.Getenv("ETH_RPC_PRIMARY_URL")
	backupURL := os.Getenv("ETH_RPC_BACKUP_URL")
	if primaryURL == "" || backupURL == "" {
		return fmt.Errorf("set ETH_RPC_PRIMARY_URL and ETH_RPC_BACKUP_URL to same-chain HTTP RPC endpoints")
	}

	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: primaryURL},
		{ID: "backup", URL: backupURL},
	}
	monitor, err := blocklag.New(blocklag.Config{
		Endpoints: endpoints,
		MaxLag:    maxLag,
	})
	if err != nil {
		return fmt.Errorf("configure block-lag monitor: %w", err)
	}

	// Run a check before starting RPC work. Partial failures can publish
	// eligibility despite returning an error; all-failed checks preserve the
	// previous state (initially both endpoints are eligible).
	checkEligibility(ctx, monitor, "initial")
	if ctx.Err() != nil {
		return nil
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: endpoints,
		Eligible:  monitor.Eligible,
	})
	if err != nil {
		return fmt.Errorf("configure failnext: %w", err)
	}
	httpClient := &http.Client{Transport: tr}
	defer httpClient.CloseIdleConnections()

	rpcClient, err := rpc.DialOptions(ctx, primaryURL, rpc.WithHTTPClient(httpClient))
	if err != nil {
		return fmt.Errorf("connect Ethereum RPC client: %w", err)
	}
	defer rpcClient.Close()
	eth := ethclient.NewClient(rpcClient)

	// The application refreshes eligibility in the background; blocklag has no
	// scheduler. Reads below use the latest published decisions through failnext.
	workCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		monitorLoop(workCtx, monitor)
	}()
	defer func() {
		cancel()
		<-done
	}()

	readLatestBlock(workCtx, eth)
	reads := time.NewTicker(readInterval)
	defer reads.Stop()
	for {
		select {
		case <-workCtx.Done():
			return nil
		case <-reads.C:
			readLatestBlock(workCtx, eth)
		}
	}
}

func checkEligibility(ctx context.Context, monitor *blocklag.Monitor, label string) {
	checkCtx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()

	err := monitor.Check(checkCtx)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		// An error does not imply that eligibility stayed unchanged.
		log.Printf("%s block-lag check: %v", label, err)
	}
	log.Printf("%s eligibility: primary=%t backup=%t", label,
		monitor.Eligible("primary"), monitor.Eligible("backup"))
}

func monitorLoop(ctx context.Context, monitor *blocklag.Monitor) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkEligibility(ctx, monitor, "periodic")
		}
	}
}

func readLatestBlock(ctx context.Context, eth *ethclient.Client) {
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	// Ethereum JSON-RPC reads use POST. This specific read is safe to
	// repeat, so permit failnext to try another eligible provider.
	block, err := eth.BlockNumber(failnext.WithFailoverAllowed(readCtx))
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("eth_blockNumber: %v", err)
		}
		return
	}
	log.Printf("latest block: %d", block)
}
