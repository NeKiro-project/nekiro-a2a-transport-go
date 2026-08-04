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

## Install and use

Use an immutable reviewed release:

```text
go get github.com/NeKiro-project/nekiro-a2a-transport-go@v0.1.1
```

Callers must provide an explicit endpoint and positive response/event limits.
Authentication and platform context are supplied through caller-owned official
A2A interceptors; this package never discovers endpoints or invents policy.

## Development checks

```text
go test ./...
go test -race ./...
go vet ./...
go mod tidy
go mod verify
git diff --check
```

Verification is successful only when:

- all unit tests pass without skipped protocol assertions;
- `TestClientInteroperatesWithOfficialA2AServer` passes;
- the race detector and `go vet` exit with code `0`;
- `go mod tidy` leaves `go.mod` and `go.sum` unchanged;
- both malformed input and upstream failure tests return the documented typed
  failure rather than a partial or fallback result.

CI separates reusable transport quality from official A2A server
interoperability so consumers can see which boundary failed.

## Pull requests

Transport changes must identify the affected operation (`message/send`,
`message/stream`, or `tasks/cancel`), framing or limit semantics, failure kind,
and downstream Core adapter verification. Public API changes require an
explicit compatibility statement and a new immutable release when merged.

Fallback delta: removed 0, retained 1, added 0, net 0. The retained behavior
is Go's documented nil `http.Client.Transport` policy. Added fallback evidence:
none.
