package a2atransport

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"

	"github.com/a2aproject/a2a-go/a2a"
)

// FailureKind is a stable, transport-only error classification.
type FailureKind string

const (
	FailureInvalidArgument  FailureKind = "invalid_argument"
	FailureProtocol         FailureKind = "protocol"
	FailureRemoteAgent      FailureKind = "remote_agent"
	FailureUnavailable      FailureKind = "unavailable"
	FailureDeadlineExceeded FailureKind = "deadline_exceeded"
	FailureCanceled         FailureKind = "canceled"
	FailureResponseTooLarge FailureKind = "response_too_large"
)

// Failure carries a stable kind and an inspectable cause. Error deliberately
// omits cause text so callers do not leak remote endpoints or credentials by
// formatting the transport failure.
type Failure struct {
	kind  FailureKind
	cause error
}

type interceptorFailure struct {
	cause error
}

func (failure *interceptorFailure) Error() string {
	return "A2A call interceptor failed"
}

func (failure *interceptorFailure) Unwrap() error {
	return failure.cause
}

func (failure *Failure) Error() string {
	return "A2A transport " + string(failure.kind)
}

func (failure *Failure) Unwrap() error {
	return failure.cause
}

func (failure *Failure) Kind() FailureKind {
	return failure.kind
}

// FailureKindOf returns the first transport Failure classification in err's
// unwrap chain.
func FailureKindOf(err error) (FailureKind, bool) {
	var failure *Failure
	if !errors.As(err, &failure) {
		return "", false
	}
	return failure.kind, true
}

func newFailure(kind FailureKind, cause error) error {
	if cause == nil {
		cause = errors.New(string(kind))
	}
	return &Failure{kind: kind, cause: cause}
}

func classifyFailure(err error) error {
	if err == nil {
		return nil
	}
	var failure *Failure
	if errors.As(err, &failure) {
		return failure
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return newFailure(FailureDeadlineExceeded, err)
	}
	if errors.Is(err, context.Canceled) {
		return newFailure(FailureCanceled, err)
	}
	var interceptorErr *interceptorFailure
	if errors.As(err, &interceptorErr) {
		return newFailure(FailureInvalidArgument, interceptorErr.cause)
	}
	var remoteError *a2a.Error
	if errors.As(err, &remoteError) {
		return newFailure(FailureRemoteAgent, err)
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return newFailure(FailureUnavailable, err)
	}
	var urlError *url.Error
	if errors.As(err, &urlError) {
		return newFailure(FailureUnavailable, err)
	}
	message := err.Error()
	if strings.Contains(message, "failed to send HTTP request") || strings.Contains(message, "unexpected HTTP status") {
		return newFailure(FailureUnavailable, err)
	}
	return newFailure(FailureProtocol, err)
}
