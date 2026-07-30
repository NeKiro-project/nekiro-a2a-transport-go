package a2atransport_test

import (
	"context"
	"net/http"

	"github.com/NeKiro-project/nekiro-a2a-transport-go"
	"github.com/a2aproject/a2a-go/a2a"
)

func ExampleClient() {
	client, err := a2atransport.NewClient(&http.Client{})
	if err != nil {
		panic(err)
	}
	options := a2atransport.CallOptions{
		Endpoint:         "https://agent.example/a2a",
		MaxResponseBytes: 1 << 20,
		MaxEventBytes:    1 << 20,
	}
	_, _ = client.SendMessage(context.Background(), options, &a2a.MessageSendParams{})
	for _, streamErr := range client.SendStreamingMessage(context.Background(), options, &a2a.MessageSendParams{}) {
		_ = streamErr
	}
}
