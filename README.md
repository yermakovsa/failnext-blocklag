# failnext-blocklag

`failnext-blocklag` is a small Go companion library for [failnext](https://github.com/yermakovsa/failnext) that detects relative block-height lag across a fixed set of Ethereum HTTP JSON-RPC providers.

An RPC provider can return HTTP 200 while several blocks behind its peers. `blocklag` checks their reported heights directly with `eth_blockNumber` and supplies eligibility through `failnext.Config.Eligible`.

`blocklag` performs the block-height checks; `failnext` handles endpoint selection and HTTP failover. `failnext` itself does not detect blockchain lag or stale state.

## Quick start

Use the same endpoints for monitoring and failover:

```go
endpoints := []failnext.Endpoint{
    {ID: "primary", URL: "https://provider-a.example/rpc"},
    {ID: "backup", URL: "https://provider-b.example/rpc"},
}

monitor, err := blocklag.New(blocklag.Config{
    Endpoints: endpoints,
    MaxLag:    3,
})
if err != nil {
    return err
}

// The application decides when to refresh eligibility.
checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
if err := monitor.Check(checkCtx); err != nil {
    // A partially successful check may still publish new eligibility.
    log.Printf("block-lag check: %v", err)
}

tr, err := failnext.New(failnext.Config{
    Endpoints: endpoints,
    Eligible:  monitor.Eligible,
})
if err != nil {
    return err
}

httpClient := &http.Client{Transport: tr}
// Use httpClient with your HTTP / JSON-RPC application client.
```

This excerpt assumes the necessary imports, an existing `ctx`, and a surrounding function returning `error`.

Ethereum JSON-RPC reads use HTTP POST. Allow failover only for operations that are safe to repeat, using `failnext.WithFailoverAllowed(ctx)`; eligibility does not grant failover permission or make a request replayable.

## How it works

Each `Check(ctx)` probes every configured URL **directly and concurrently** with one `eth_blockNumber` HTTP POST per provider, outside failnext. A probe succeeds only with HTTP 200 and a valid JSON-RPC 2.0 response containing the expected ID and a canonical hexadecimal block number fitting `uint64`. Probes do not follow redirects or retry; response bodies are limited to 4 KiB and closed.

The reference is the **highest successfully observed block in that check**. A successful provider is eligible when:

```text
highestObservedBlock - providerBlock <= MaxLag
```

The threshold is inclusive: `MaxLag: 0` accepts only providers at the highest observed height. With at least one successful probe, `Check` publishes a complete eligibility snapshot. `Eligible(id)` reads that snapshot without network I/O.

There is no background polling loop. The application calls `Check` on its own schedule and provides a suitable context deadline.

### Failure behavior

| Check result | Eligibility state | Returned error |
| --- | --- | --- |
| All probes succeed | Publish the new comparison | `nil` |
| Some probes succeed | Publish a new snapshot; failed providers are ineligible | Error identifying failed endpoints |
| All probes fail | Retain the previous snapshot | Error |
| Cancellation observed before publication | Retain the previous snapshot | Context error |

Even one successful probe is enough to publish a snapshot; that provider is eligible. Before the first published check, **all configured endpoints are eligible** (fail-open), while unknown IDs are ineligible. A later successful check can restore previously ineligible providers.

**A `Check` error does not necessarily mean eligibility was unchanged.** Partial failures publish state and return an error. Multiple probe errors are joined, with endpoint IDs for attribution.

## Concurrency and limits

`Check` calls are serialized; a waiting call can be canceled. Probes within a check run concurrently. `Eligible` is safe for concurrent use and never waits for network probes. Separate `Eligible` calls can observe different snapshots if publication occurs between them, even during one failnext request; failnext retains each eligibility decision for that request.

Published eligibility **does not expire**. If checks stop or every subsequent check fails, the last snapshot remains in use.

**Relative, not canonical, freshness.** If all providers are equally stale, `blocklag` cannot detect it; an unusually high reported block can make others appear lagging. Configure endpoints for the same intended Ethereum-compatible chain. The library does not validate chain IDs, forks, block hashes, finality, or reported heights against an independent source.

**Eligibility is not routing.** `failnext` still applies its own endpoint priority, cooldown, exclusions, failover permission, triggers, and replay rules. An in-flight request may already have evaluated eligibility before a new snapshot was published.

`blocklag.Config.Base` is used for monitoring probes only and defaults to `http.DefaultTransport`. A custom transport must preserve the probed destination, honor the request context, and must not reroute through failnext. Its resources belong to the caller.

There is no automatic scheduling, TTL, retry/backoff policy, scoring, metrics, service discovery, or WebSocket support.

## Examples

[`ExampleMonitor`](example_test.go) is a runnable local-server example of JSON-RPC POST integration with failnext:

```sh
go test -run '^ExampleMonitor$' -v
```

For ongoing monitoring with go-ethereum, see [`examples/goethereum/continuous`](examples/goethereum/continuous/main.go). It checks eligibility every 15 seconds and reads the latest block every 10 seconds, with separate operation timeouts and signal-based shutdown.

Configure two HTTP RPC URLs on the same chain. From the repository root:

```sh
export ETH_RPC_PRIMARY_URL='https://your-primary.example/rpc'
export ETH_RPC_BACKUP_URL='https://your-backup.example/rpc'
cd examples/goethereum
go run ./continuous
```

The go-ethereum example is a separate Go 1.25+ module, so go-ethereum is not a dependency of `blocklag`. It uses a local `replace` for blocklag; run `go mod tidy` inside `examples/goethereum` to resolve module checksums.

## Installation

Requires Go 1.24 or newer.

```sh
go get github.com/yermakovsa/failnext-blocklag
```

## License

MIT. See [LICENSE](LICENSE).
