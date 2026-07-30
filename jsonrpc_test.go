package a2atransport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
)

type testRoundTripper func(*http.Request) (*http.Response, error)

func (roundTripper testRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTripper(request)
}

type trackingBody struct {
	reader *bytes.Reader
	err    error
	closed bool
}

func (body *trackingBody) Read(destination []byte) (int, error) {
	if body.reader.Len() > 0 {
		return body.reader.Read(destination)
	}
	if body.err != nil {
		return 0, body.err
	}
	return 0, io.EOF
}

func (body *trackingBody) Close() error {
	body.closed = true
	return nil
}

func TestJSONRPCNegativeCorpus(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		media      string
		body       func(id json.RawMessage) string
		want       FailureKind
		maxBytes   int64
		targetCall bool
	}{
		{name: "wrong media", status: http.StatusOK, media: "text/plain", body: validMessageResponse, want: FailureProtocol},
		{name: "id mismatch", status: http.StatusOK, media: "application/json", body: func(json.RawMessage) string {
			return `{"jsonrpc":"2.0","id":"other","result":{"kind":"message","messageId":"message-1","role":"agent","parts":[{"kind":"text","text":"ok"}]}}`
		}, want: FailureProtocol},
		{name: "both result and error", status: http.StatusOK, media: "application/json", body: func(id json.RawMessage) string {
			return `{"jsonrpc":"2.0","id":` + string(id) + `,"result":{},"error":{}}`
		}, want: FailureProtocol},
		{name: "neither result nor error", status: http.StatusOK, media: "application/json", body: func(id json.RawMessage) string {
			return `{"jsonrpc":"2.0","id":` + string(id) + `}`
		}, want: FailureProtocol},
		{name: "request-only member", status: http.StatusOK, media: "application/json", body: func(id json.RawMessage) string {
			return `{"jsonrpc":"2.0","id":` + string(id) + `,"method":"evil","result":{}}`
		}, want: FailureProtocol},
		{name: "case-variant member", status: http.StatusOK, media: "application/json", body: func(id json.RawMessage) string {
			return `{"JSONRPC":"2.0","id":` + string(id) + `,"result":{}}`
		}, want: FailureProtocol},
		{name: "unknown member", status: http.StatusOK, media: "application/json", body: func(id json.RawMessage) string {
			return `{"jsonrpc":"2.0","id":` + string(id) + `,"result":{},"extra":true}`
		}, want: FailureProtocol},
		{name: "duplicate member", status: http.StatusOK, media: "application/json", body: func(id json.RawMessage) string {
			return `{"jsonrpc":"2.0","id":` + string(id) + `,"result":{},"result":{}}`
		}, want: FailureProtocol},
		{name: "trailing data", status: http.StatusOK, media: "application/json", body: func(id json.RawMessage) string { return `{"jsonrpc":"2.0","id":` + string(id) + `,"result":{}} {}` }, want: FailureProtocol},
		{name: "overflow", status: http.StatusOK, media: "application/json", body: validMessageResponse, want: FailureResponseTooLarge, maxBytes: 32},
		{name: "http failure", status: http.StatusBadGateway, media: "application/json", body: func(json.RawMessage) string { return "gateway" }, want: FailureUnavailable},
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
				writer.Header().Set("Content-Type", test.media)
				writer.WriteHeader(test.status)
				_, _ = io.WriteString(writer, test.body(envelope.ID))
			}))
			t.Cleanup(server.Close)
			client, err := NewClient(server.Client())
			if err != nil {
				t.Fatal(err)
			}
			limit := test.maxBytes
			if limit == 0 {
				limit = 4096
			}
			_, err = client.SendMessage(t.Context(), CallOptions{Endpoint: server.URL, MaxResponseBytes: limit}, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request", Role: a2a.MessageRoleUser}})
			assertFailureKind(t, err, test.want)
		})
	}
}

func TestJSONRPCResponseBodyIsClosed(t *testing.T) {
	body := &trackingBody{reader: bytes.NewReader(nil)}
	client, err := NewClient(&http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
		requestBody, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		var envelope struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.Unmarshal(requestBody, &envelope); err != nil {
			return nil, err
		}
		body.reader = bytes.NewReader([]byte(validMessageResponse(envelope.ID)))
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(t.Context(), CallOptions{Endpoint: "http://agent.example", MaxResponseBytes: 4096}, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request", Role: a2a.MessageRoleUser}}); err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}
	if !body.closed {
		t.Fatal("response body was not closed")
	}
}

func TestJSONRPCInterruptedResponseIsUnavailable(t *testing.T) {
	sentinel := errors.New("interrupted response")
	body := &trackingBody{reader: bytes.NewReader([]byte(`{"jsonrpc":"2.0"}`)), err: sentinel}
	client, err := NewClient(&http.Client{Transport: testRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(t.Context(), CallOptions{Endpoint: "http://agent.example", MaxResponseBytes: 4096}, &a2a.MessageSendParams{Message: &a2a.Message{ID: "request", Role: a2a.MessageRoleUser}})
	assertFailureKind(t, err, FailureUnavailable)
	if !errors.Is(err, sentinel) {
		t.Fatalf("interrupted response cause was not preserved: %v", err)
	}
	if !body.closed {
		t.Fatal("interrupted response body was not closed")
	}
}

func validMessageResponse(id json.RawMessage) string {
	return `{"jsonrpc":"2.0","id":` + string(id) + `,"result":{"kind":"message","messageId":"message-1","role":"agent","parts":[{"kind":"text","text":"ok"}]}}`
}
