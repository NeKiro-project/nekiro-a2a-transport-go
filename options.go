package a2atransport

import (
	"errors"
	"math"
	"net/url"
	"reflect"

	"github.com/a2aproject/a2a-go/a2aclient"
)

// CallOptions contains all transport policy required for one A2A operation.
// No endpoint, byte bound, or interceptor is inferred by the package.
type CallOptions struct {
	Endpoint         string
	MaxResponseBytes int64
	MaxEventBytes    int64
	Interceptors     []a2aclient.CallInterceptor
}

func (options CallOptions) validate(stream bool) (CallOptions, error) {
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.User != nil {
		return CallOptions{}, newFailure(FailureInvalidArgument, errors.New("A2A endpoint is invalid"))
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return CallOptions{}, newFailure(FailureInvalidArgument, errors.New("A2A endpoint scheme is unsupported"))
	}
	if !validBound(options.MaxResponseBytes) {
		return CallOptions{}, newFailure(FailureInvalidArgument, errors.New("A2A response byte limit is invalid"))
	}
	if stream && !validBound(options.MaxEventBytes) {
		return CallOptions{}, newFailure(FailureInvalidArgument, errors.New("A2A event byte limit is invalid"))
	}
	interceptors := make([]a2aclient.CallInterceptor, len(options.Interceptors))
	for index, interceptor := range options.Interceptors {
		if nilInterface(interceptor) {
			return CallOptions{}, newFailure(FailureInvalidArgument, errors.New("A2A call interceptor is nil"))
		}
		interceptors[index] = interceptor
	}
	options.Interceptors = interceptors
	return options, nil
}

func validBound(value int64) bool {
	return value > 0 && value < math.MaxInt64
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
