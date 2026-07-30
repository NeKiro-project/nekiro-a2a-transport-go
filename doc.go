// Package a2atransport provides a strict, bounded JSON-RPC/SSE client
// transport for the A2A protocol profile used by NeKiro.
//
// The package intentionally owns no Agent discovery, authorization,
// credential generation, invocation lineage, retry, persistence, or Agent
// Runtime behavior. Callers provide an exact endpoint, byte limits, and any
// request metadata interceptors for every operation.
package a2atransport

const (
	// ProtocolVersion is the A2A protocol version verified by this release.
	ProtocolVersion = "0.3.0"
	// A2AGoVersion is the official Go protocol-library version verified by this release.
	A2AGoVersion = "v0.3.15"
)
