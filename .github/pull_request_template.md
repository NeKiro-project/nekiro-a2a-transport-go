## Summary

<!-- Describe the transport behavior changed and why it belongs in this module. -->

## Compatibility and downstream impact

- Affected operation or public symbol:
- A2A protocol/library compatibility:
- NeKiro Core adapter or release update:

- [ ] No public API or failure semantics changed.
- [ ] Compatible changes have interoperability coverage.
- [ ] Breaking changes include a versioning and migration decision.

## Verification

Commands run:

```text
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go mod tidy
go mod verify
git diff --check
```

Observed success signals:

<!-- Include the official A2A server interoperability result and failure-path evidence. -->

## Security and failure semantics

- [ ] Endpoints and byte limits remain explicit.
- [ ] Redirects, malformed envelopes, framing errors, and oversized data fail closed.
- [ ] No credential ownership, endpoint discovery, retry, or fallback policy moved into transport.

Fallback delta: removed 0, retained 0, added 0, net 0

Added fallback evidence: none

## Checklist

- [ ] Tests cover both the wire success path and the affected rejection path.
- [ ] README/API documentation was updated where behavior changed.
- [ ] Dependencies and GitHub Actions use immutable versions.
- [ ] The required downstream Core compatibility work is linked.
