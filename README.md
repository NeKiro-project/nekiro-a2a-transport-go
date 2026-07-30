# nekiro-a2a-transport-go

Strict, bounded JSON-RPC/SSE client transport for the A2A protocol profile used
by NeKiro.

The package builds on `github.com/a2aproject/a2a-go v0.3.15`; it does not
replace or fork the A2A protocol implementation. It contributes explicit
transport hardening and a reusable caller-owned policy boundary.

## Scope

The module provides:

- `message/send`, `message/stream`, and `tasks/cancel` client operations;
- strict JSON-RPC response identity and envelope validation;
- strict single-data-line SSE framing with unique event IDs;
- explicit response/event byte bounds with no truncation;
- raw streaming result preservation alongside official decoded events;
- redirect rejection and typed transport failures;
- explicit request metadata through official call interceptors.

It does not provide discovery, endpoint selection, Agent Card resolution,
credentials, authorization, retry, fallback, persistence, Ledger behavior, or
an Agent Runtime.

## Compatibility

- A2A protocol: `0.3.0`
- Go A2A library: `github.com/a2aproject/a2a-go v0.3.15`
- Go: `1.26.0`

## Development checks

```text
go test ./...
go test -race ./...
go vet ./...
go mod tidy
git diff --check
```

Fallback delta: removed 0, retained 1, added 0, net 0. The retained behavior
is Go's documented nil `http.Client.Transport` policy. Added fallback evidence:
none.
