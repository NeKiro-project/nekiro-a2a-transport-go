package a2atransport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
)

func TestClientSendMessageAndCancelUseExplicitInterceptors(t *testing.T) {
	var interceptorCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Test-Metadata") != "present" {
			http.Error(writer, "missing metadata", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		var result json.RawMessage
		switch envelope.Method {
		case "message/send":
			result = json.RawMessage(`{"kind":"message","messageId":"message-1","role":"agent","parts":[{"kind":"text","text":"ok"}]}`)
		case "tasks/cancel":
			result = json.RawMessage(`{"kind":"task","id":"task-1","contextId":"context-1","status":{"state":"canceled"}}`)
		default:
			t.Fatalf("unexpected method %q", envelope.Method)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"jsonrpc":"2.0","id":`+string(envelope.ID)+`,"result":`+string(result)+`}`)
	}))
	t.Cleanup(server.Close)

	interceptor := &metadataInterceptor{calls: &interceptorCalls}
	client, err := NewClient(server.Client())
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	options := CallOptions{Endpoint: server.URL, MaxResponseBytes: 4096, Interceptors: []a2aclient.CallInterceptor{interceptor}}
	result, err := client.SendMessage(t.Context(), options, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request-1", Role: a2a.MessageRoleUser, Parts: []a2a.Part{a2a.TextPart{Text: "hello"}}}})
	if err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}
	if _, ok := result.(*a2a.Message); !ok {
		t.Fatalf("SendMessage() result type = %T, want *a2a.Message", result)
	}
	if _, err := client.CancelTask(t.Context(), options, "task-1"); err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}
	if got := interceptorCalls.Load(); got != 2 {
		t.Fatalf("interceptor calls = %d, want 2", got)
	}
}

func TestClientCancelRejectsNonTaskResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"jsonrpc":"2.0","id":`+string(envelope.ID)+`,"result":{}}`)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.CancelTask(t.Context(), CallOptions{Endpoint: server.URL, MaxResponseBytes: 4096}, "task-1")
	assertFailureKind(t, err, FailureProtocol)
}

func TestClientInterceptorFailurePreservesCallerCause(t *testing.T) {
	sentinel := errors.New("caller-owned injection failure")
	interceptor := &failingInterceptor{cause: sentinel}
	client, err := NewClient(&http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(t.Context(), CallOptions{
		Endpoint:         "http://agent.example",
		MaxResponseBytes: 4096,
		Interceptors:     []a2aclient.CallInterceptor{interceptor},
	}, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request", Role: a2a.MessageRoleUser}})
	assertFailureKind(t, err, FailureInvalidArgument)
	if !errors.Is(err, sentinel) {
		t.Fatalf("interceptor cause was not preserved: %v", err)
	}
}

func TestClientSendStreamingPreservesRawResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(writer, "id: event-1\ndata: {\"jsonrpc\":\"2.0\",\"id\":"+string(envelope.ID)+",\"result\":{\"kind\":\"task\",\"id\":\"task-1\",\"contextId\":\"context-1\",\"status\":{\"state\":\"working\"}}}\n\n")
		_, _ = io.WriteString(writer, "id: event-2\ndata: {\"jsonrpc\":\"2.0\",\"id\":"+string(envelope.ID)+",\"result\":{\"kind\":\"status-update\",\"taskId\":\"task-1\",\"contextId\":\"context-1\",\"final\":true,\"status\":{\"state\":\"completed\"}}}\n\n")
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.Client())
	if err != nil {
		t.Fatal(err)
	}
	items := make([]StreamItem, 0, 2)
	for item, streamErr := range client.SendStreamingMessage(t.Context(), CallOptions{Endpoint: server.URL, MaxResponseBytes: 4096, MaxEventBytes: 4096}, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request-1", Role: a2a.MessageRoleUser, Parts: []a2a.Part{a2a.TextPart{Text: "hello"}}}}) {
		if streamErr != nil {
			t.Fatalf("stream error = %v", streamErr)
		}
		items = append(items, item)
	}
	if len(items) != 2 {
		t.Fatalf("stream item count = %d, want 2", len(items))
	}
	if string(items[0].Result) != `{"kind":"task","id":"task-1","contextId":"context-1","status":{"state":"working"}}` {
		t.Fatalf("first raw result = %s", items[0].Result)
	}
	if _, ok := items[1].Event.(*a2a.TaskStatusUpdateEvent); !ok {
		t.Fatalf("second event type = %T, want *TaskStatusUpdateEvent", items[1].Event)
	}
}

func TestClientRejectsInvalidArguments(t *testing.T) {
	client, err := NewClient(&http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		options CallOptions
		stream  bool
	}{
		{name: "missing endpoint", options: CallOptions{MaxResponseBytes: 1}},
		{name: "invalid scheme", options: CallOptions{Endpoint: "ftp://agent.example", MaxResponseBytes: 1}},
		{name: "missing response limit", options: CallOptions{Endpoint: "http://agent.example"}},
		{name: "missing event limit", options: CallOptions{Endpoint: "http://agent.example", MaxResponseBytes: 1}, stream: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.stream {
				for _, streamErr := range client.SendStreamingMessage(t.Context(), test.options, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request", Role: a2a.MessageRoleUser}}) {
					if streamErr == nil {
						t.Fatal("stream succeeded")
					}
					assertFailureKind(t, streamErr, FailureInvalidArgument)
					return
				}
				t.Fatal("stream returned no validation error")
			}
			_, err := client.SendMessage(t.Context(), test.options, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request", Role: a2a.MessageRoleUser}})
			assertFailureKind(t, err, FailureInvalidArgument)
		})
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	var targetCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetCalls.Add(1) }))
	t.Cleanup(target.Close)
	source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL)
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(source.Close)
	client, err := NewClient(source.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(t.Context(), CallOptions{Endpoint: source.URL, MaxResponseBytes: 4096}, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request", Role: a2a.MessageRoleUser}})
	assertFailureKind(t, err, FailureUnavailable)
	if targetCalls.Load() != 0 {
		t.Fatalf("redirect target calls = %d, want 0", targetCalls.Load())
	}
}

func TestFailurePreservesCauseWithoutDisplayingIt(t *testing.T) {
	client, err := NewClient(&http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	cause := context.DeadlineExceeded
	wrapped := classifyFailure(cause)
	if !errors.Is(wrapped, cause) {
		t.Fatalf("failure does not unwrap cause: %v", wrapped)
	}
	if strings.Contains(wrapped.Error(), cause.Error()) {
		t.Fatalf("failure string leaks cause: %q", wrapped.Error())
	}
	_ = client
}

type metadataInterceptor struct {
	calls *atomic.Int64
	a2aclient.PassthroughInterceptor
}

type failingInterceptor struct {
	a2aclient.PassthroughInterceptor
	cause error
}

func (interceptor *failingInterceptor) Before(ctx context.Context, _ *a2aclient.Request) (context.Context, error) {
	return ctx, interceptor.cause
}

func (interceptor *metadataInterceptor) Before(ctx context.Context, request *a2aclient.Request) (context.Context, error) {
	interceptor.calls.Add(1)
	request.Meta.Append("X-Test-Metadata", "present")
	return ctx, nil
}

func assertFailureKind(t *testing.T, err error, want FailureKind) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", want)
	}
	if got, ok := FailureKindOf(err); !ok || got != want {
		t.Fatalf("failure kind = %q, want %q (err=%v)", got, want, err)
	}
}
