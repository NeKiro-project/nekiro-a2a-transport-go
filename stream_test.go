package a2atransport

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
)

func TestStreamingNegativeCorpus(t *testing.T) {
	tests := []struct {
		name  string
		body  func(id json.RawMessage) string
		want  FailureKind
		limit int64
	}{
		{name: "wrong media", body: func(json.RawMessage) string { return "data: {}\n\n" }, want: FailureProtocol},
		{name: "missing id", body: func(id json.RawMessage) string {
			return `data: {"jsonrpc":"2.0","id":` + string(id) + `,"result":{"kind":"task","id":"task-1","contextId":"ctx-1","status":{"state":"working"}}}` + "\n\n"
		}, want: FailureProtocol},
		{name: "empty id", body: func(id json.RawMessage) string {
			return `id: \ndata: {"jsonrpc":"2.0","id":` + string(id) + `,"result":{"kind":"task","id":"task-1","contextId":"ctx-1","status":{"state":"working"}}}` + "\n\n"
		}, want: FailureProtocol},
		{name: "unknown response member", body: func(id json.RawMessage) string {
			return "id: one\ndata: {\"jsonrpc\":\"2.0\",\"id\":" + string(id) + ",\"result\":{},\"extra\":true}\n\n"
		}, want: FailureProtocol},
		{name: "case-variant response member", body: func(id json.RawMessage) string {
			return "id: one\ndata: {\"JSONRPC\":\"2.0\",\"id\":" + string(id) + ",\"result\":{}}\n\n"
		}, want: FailureProtocol},
		{name: "duplicate event ID", body: duplicateIDStream, want: FailureProtocol},
		{name: "multiple data lines", body: func(id json.RawMessage) string { return "id: one\ndata: {}\ndata: {}\n\n" }, want: FailureProtocol},
		{name: "missing delimiter", body: func(id json.RawMessage) string {
			return `id: one\ndata: {"jsonrpc":"2.0","id":` + string(id) + `,"result":{"kind":"task","id":"task-1","contextId":"ctx-1","status":{"state":"working"}}}`
		}, want: FailureProtocol},
		{name: "mismatched response ID", body: func(json.RawMessage) string {
			return `id: one` + "\n" + `data: {"jsonrpc":"2.0","id":"other","result":{"kind":"task","id":"task-1","contextId":"ctx-1","status":{"state":"working"}}}` + "\n\n"
		}, want: FailureProtocol},
		{name: "oversized event", body: func(id json.RawMessage) string {
			return "id: one\ndata: {\"jsonrpc\":\"2.0\",\"id\":" + string(id) + ",\"result\":{\"kind\":\"task\",\"id\":\"task-1\",\"contextId\":\"ctx-1\",\"status\":{\"state\":\"working\"},\"padding\":\"0123456789\"}}\n\n"
		}, want: FailureResponseTooLarge, limit: 64},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
				if test.name == "wrong media" {
					writer.Header().Set("Content-Type", "application/json")
				} else {
					writer.Header().Set("Content-Type", "text/event-stream")
				}
				_, _ = io.WriteString(writer, test.body(envelope.ID))
			}))
			t.Cleanup(server.Close)
			client, err := NewClient(server.Client())
			if err != nil {
				t.Fatal(err)
			}
			limit := test.limit
			if limit == 0 {
				limit = 4096
			}
			seenEvent := false
			for _, streamErr := range client.SendStreamingMessage(t.Context(), CallOptions{Endpoint: server.URL, MaxResponseBytes: 4096, MaxEventBytes: limit}, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request", Role: a2a.MessageRoleUser}}) {
				if streamErr == nil {
					seenEvent = true
					continue
				}
				assertFailureKind(t, streamErr, test.want)
				return
			}
			if seenEvent {
				t.Fatal("stream accepted invalid response after emitting an event")
			}
			t.Fatal("stream returned no error")
		})
	}
}

func duplicateIDStream(id json.RawMessage) string {
	result := `{"jsonrpc":"2.0","id":` + string(id) + `,"result":{"kind":"task","id":"task-1","contextId":"ctx-1","status":{"state":"working"}}}`
	return "id: one\ndata: " + result + "\n\nid: one\ndata: " + result + "\n\n"
}
