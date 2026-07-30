package a2atransport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

func TestClientInteroperatesWithOfficialA2AServer(t *testing.T) {
	var interceptorCalls atomic.Int64
	official := a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(officialTestExecutor{}))
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Test-Metadata") != "present" {
			http.Error(writer, "missing metadata", http.StatusUnauthorized)
			return
		}
		// a2asrv v0.3.15 does not set the non-streaming JSON media type;
		// supply the protocol-required header while retaining its official
		// request decoding, execution, SSE, and cancellation handlers.
		writer.Header().Set("Content-Type", "application/json")
		official.ServeHTTP(writer, request)
	}))
	t.Cleanup(server.Close)

	interceptor := &metadataInterceptor{calls: &interceptorCalls}
	client, err := NewClient(server.Client())
	if err != nil {
		t.Fatal(err)
	}
	options := CallOptions{
		Endpoint:         server.URL,
		MaxResponseBytes: 1 << 20,
		MaxEventBytes:    1 << 16,
		Interceptors:     []a2aclient.CallInterceptor{interceptor},
	}
	params := &a2a.MessageSendParams{Message: &a2a.Message{
		ID: "official-request", Role: a2a.MessageRoleUser,
		Parts: []a2a.Part{a2a.TextPart{Text: "hello"}},
	}}
	result, err := client.SendMessage(t.Context(), options, params)
	if err != nil {
		var failure *Failure
		if errors.As(err, &failure) {
			t.Fatalf("official SendMessage() error = %v cause=%v", err, failure.cause)
		}
		t.Fatalf("official SendMessage() error = %v", err)
	}
	if _, ok := result.(*a2a.Message); !ok {
		t.Fatalf("official SendMessage() result type = %T, want *a2a.Message", result)
	}

	var items []StreamItem
	for item, streamErr := range client.SendStreamingMessage(t.Context(), options, params) {
		if streamErr != nil {
			t.Fatalf("official SendStreamingMessage() error = %v", streamErr)
		}
		items = append(items, item)
	}
	if len(items) != 1 {
		t.Fatalf("official stream item count = %d, want 1", len(items))
	}
	if _, ok := items[0].Event.(*a2a.Message); !ok || len(items[0].Result) == 0 {
		t.Fatalf("official stream item = %#v, want Message with raw result", items[0])
	}

	_, err = client.CancelTask(t.Context(), options, "missing-task")
	assertFailureKind(t, err, FailureRemoteAgent)
	if got := interceptorCalls.Load(); got != 3 {
		t.Fatalf("official interceptor calls = %d, want 3", got)
	}
}

type officialTestExecutor struct{}

func (officialTestExecutor) Execute(ctx context.Context, _ *a2asrv.RequestContext, queue eventqueue.Queue) error {
	return queue.Write(ctx, a2a.NewMessage(a2a.MessageRoleAgent, a2a.TextPart{Text: "official"}))
}

func (officialTestExecutor) Cancel(context.Context, *a2asrv.RequestContext, eventqueue.Queue) error {
	return errors.New("cancel unsupported")
}

var _ a2asrv.AgentExecutor = officialTestExecutor{}
