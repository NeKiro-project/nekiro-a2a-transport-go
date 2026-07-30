package a2atransport

import (
	"context"
	"errors"
	"iter"
	"net/http"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
)

// Client executes strict JSON-RPC A2A operations with caller-owned policy.
type Client struct {
	httpClient *http.Client
}

// NewClient clones httpClient and installs a redirect-rejection policy. A nil
// *http.Client is invalid. A client whose Transport is nil retains Go's
// documented http.DefaultTransport behavior.
func NewClient(httpClient *http.Client) (*Client, error) {
	if httpClient == nil {
		return nil, newFailure(FailureInvalidArgument, errors.New("A2A HTTP client is required"))
	}
	cloned := *httpClient
	cloned.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{httpClient: &cloned}, nil
}

// SendMessage executes A2A message/send.
func (client *Client) SendMessage(ctx context.Context, options CallOptions, params *a2a.MessageSendParams) (a2a.SendMessageResult, error) {
	validated, err := options.validate(false)
	if err != nil {
		return nil, err
	}
	if params == nil || params.Message == nil {
		return nil, newFailure(FailureInvalidArgument, errors.New("A2A message/send params are required"))
	}
	a2aClient, err := client.newJSONRPCClient(ctx, validated)
	if err != nil {
		return nil, err
	}
	result, err := a2aClient.SendMessage(ctx, params)
	if err != nil {
		return nil, classifyFailure(err)
	}
	if err := validateSendMessageResult(result); err != nil {
		return nil, err
	}
	return result, nil
}

// SendStreamingMessage executes A2A message/stream and returns the decoded A2A
// event together with the exact JSON-RPC result bytes that produced it.
func (client *Client) SendStreamingMessage(ctx context.Context, options CallOptions, params *a2a.MessageSendParams) iter.Seq2[StreamItem, error] {
	return func(yield func(StreamItem, error) bool) {
		validated, err := options.validate(true)
		if err != nil {
			yield(StreamItem{}, err)
			return
		}
		if params == nil || params.Message == nil {
			yield(StreamItem{}, newFailure(FailureInvalidArgument, errors.New("A2A message/stream params are required")))
			return
		}
		streamTransport := &streamingRoundTripper{base: client.baseTransport(), maxEventBytes: validated.MaxEventBytes}
		a2aClient, err := client.newA2AClient(ctx, validated, streamTransport)
		if err != nil {
			yield(StreamItem{}, err)
			return
		}
		for event, eventErr := range a2aClient.SendStreamingMessage(ctx, params) {
			if eventErr != nil {
				yield(StreamItem{}, classifyFailure(eventErr))
				return
			}
			if err := validateStreamEventKind(event); err != nil {
				yield(StreamItem{}, err)
				return
			}
			result, err := streamTransport.lastResult()
			if err != nil {
				yield(StreamItem{}, err)
				return
			}
			if !yield(StreamItem{Event: event, Result: result}, nil) {
				return
			}
		}
	}
}

// CancelTask executes one A2A tasks/cancel operation. Callers own any timeout
// and decide whether the operation should be attempted.
func (client *Client) CancelTask(ctx context.Context, options CallOptions, taskID a2a.TaskID) (*a2a.Task, error) {
	validated, err := options.validate(false)
	if err != nil {
		return nil, err
	}
	if taskID == "" {
		return nil, newFailure(FailureInvalidArgument, errors.New("A2A task ID is required"))
	}
	a2aClient, err := client.newJSONRPCClient(ctx, validated)
	if err != nil {
		return nil, err
	}
	task, err := a2aClient.CancelTask(ctx, &a2a.TaskIDParams{ID: taskID})
	if err != nil {
		return nil, classifyFailure(err)
	}
	if task == nil {
		return nil, newFailure(FailureProtocol, errors.New("A2A tasks/cancel returned no task"))
	}
	return task, nil
}

func (client *Client) newJSONRPCClient(ctx context.Context, options CallOptions) (*a2aclient.Client, error) {
	transport := envelopeValidatingRoundTripper{base: client.baseTransport(), maxResponseBytes: options.MaxResponseBytes}
	return client.newA2AClient(ctx, options, transport)
}

func (client *Client) newA2AClient(ctx context.Context, options CallOptions, transport http.RoundTripper) (*a2aclient.Client, error) {
	httpClient := *client.httpClient
	httpClient.Transport = transport
	a2aClient, err := a2aclient.NewFromEndpoints(
		ctx,
		[]a2a.AgentInterface{{Transport: a2a.TransportProtocolJSONRPC, URL: options.Endpoint}},
		a2aclient.WithJSONRPCTransport(&httpClient),
	)
	if err != nil {
		return nil, classifyFailure(err)
	}
	for _, interceptor := range options.Interceptors {
		a2aClient.AddCallInterceptor(interceptorAdapter{delegate: interceptor})
	}
	return a2aClient, nil
}

// interceptorAdapter preserves the caller's error identity across the
// official client boundary. Transport errors are classified by this module;
// metadata or credential injection errors remain caller-owned failures.
type interceptorAdapter struct {
	delegate a2aclient.CallInterceptor
}

func (adapter interceptorAdapter) Before(ctx context.Context, request *a2aclient.Request) (context.Context, error) {
	ctx, err := adapter.delegate.Before(ctx, request)
	if err != nil {
		return ctx, &interceptorFailure{cause: err}
	}
	return ctx, nil
}

func (adapter interceptorAdapter) After(ctx context.Context, response *a2aclient.Response) error {
	if err := adapter.delegate.After(ctx, response); err != nil {
		return &interceptorFailure{cause: err}
	}
	return nil
}

func (client *Client) baseTransport() http.RoundTripper {
	if client.httpClient.Transport == nil {
		return http.DefaultTransport
	}
	return client.httpClient.Transport
}

func validateSendMessageResult(result a2a.SendMessageResult) error {
	switch result.(type) {
	case *a2a.Message, *a2a.Task:
		return nil
	default:
		return newFailure(FailureProtocol, errors.New("A2A message/send returned an unsupported result kind"))
	}
}

func validateStreamEventKind(event a2a.Event) error {
	switch event.(type) {
	case *a2a.Message, *a2a.Task, *a2a.TaskStatusUpdateEvent, *a2a.TaskArtifactUpdateEvent:
		return nil
	default:
		return newFailure(FailureProtocol, errors.New("A2A message/stream returned an unsupported event kind"))
	}
}
