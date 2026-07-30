package a2atransport

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"sync"

	"github.com/a2aproject/a2a-go/a2a"
)

// StreamItem is one decoded A2A event and the immutable raw JSON-RPC result
// that produced it.
type StreamItem struct {
	Event  a2a.Event
	Result json.RawMessage
}

type streamingRoundTripper struct {
	base          http.RoundTripper
	maxEventBytes int64
	mu            sync.Mutex
	body          *boundedSSEBody
}

func (transport *streamingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	requestBody, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	if err := request.Body.Close(); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	request.Body = io.NopCloser(bytes.NewReader(requestBody))
	expectedID, err := streamingRequestID(requestBody)
	if err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	response, err := transport.base.RoundTrip(request)
	if err != nil || response.StatusCode != http.StatusOK {
		return response, err
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/event-stream" {
		_ = response.Body.Close()
		return nil, newFailure(FailureProtocol, errors.New("A2A streaming response media type is invalid"))
	}
	body := newBoundedSSEBody(response.Body, transport.maxEventBytes, expectedID)
	transport.mu.Lock()
	transport.body = body
	transport.mu.Unlock()
	response.Body = body
	return response, nil
}

func (transport *streamingRoundTripper) lastResult() (json.RawMessage, error) {
	transport.mu.Lock()
	body := transport.body
	transport.mu.Unlock()
	if body == nil {
		return nil, newFailure(FailureProtocol, errors.New("A2A stream result payload is unavailable"))
	}
	return body.result()
}

type boundedSSEBody struct {
	body       io.ReadCloser
	reader     *bufio.Reader
	limit      int64
	expectedID json.RawMessage
	seenIDs    map[string]struct{}
	mu         sync.Mutex
	lastResult json.RawMessage
	pending    []byte
	closed     bool
}

func newBoundedSSEBody(body io.ReadCloser, limit int64, expectedID json.RawMessage) *boundedSSEBody {
	return &boundedSSEBody{
		body:       body,
		reader:     bufio.NewReader(body),
		limit:      limit,
		expectedID: append(json.RawMessage(nil), expectedID...),
		seenIDs:    make(map[string]struct{}),
	}
}

func (body *boundedSSEBody) Read(destination []byte) (int, error) {
	body.mu.Lock()
	defer body.mu.Unlock()
	if body.closed {
		return 0, io.ErrClosedPipe
	}
	if len(body.pending) == 0 {
		frame, result, err := body.readFrame()
		if err != nil {
			return 0, err
		}
		body.pending = frame
		body.lastResult = result
	}
	read := copy(destination, body.pending)
	body.pending = body.pending[read:]
	return read, nil
}

func (body *boundedSSEBody) Close() error {
	body.mu.Lock()
	defer body.mu.Unlock()
	if body.closed {
		return nil
	}
	body.closed = true
	return body.body.Close()
}

func (body *boundedSSEBody) result() (json.RawMessage, error) {
	body.mu.Lock()
	defer body.mu.Unlock()
	if len(body.lastResult) == 0 {
		return nil, newFailure(FailureProtocol, errors.New("A2A stream result payload is unavailable"))
	}
	return append(json.RawMessage(nil), body.lastResult...), nil
}

func (body *boundedSSEBody) readFrame() ([]byte, json.RawMessage, error) {
	frame := make([]byte, 0, minInt64(body.limit, 4096))
	lineBuffer := make([]byte, 0, 4096)
	dataLines := 0
	idLines := 0
	var result json.RawMessage
	for {
		line, err := body.reader.ReadSlice('\n')
		if int64(len(frame)+len(line)) > body.limit {
			return nil, nil, newFailure(FailureResponseTooLarge, errors.New("A2A streaming event exceeds the configured limit"))
		}
		frame = append(frame, line...)
		lineBuffer = append(lineBuffer, line...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(frame) == 0 {
				return nil, nil, io.EOF
			}
			return nil, nil, newFailure(FailureProtocol, errors.New("A2A SSE stream ended before an event delimiter"))
		}
		if err != nil {
			return nil, nil, newFailure(FailureUnavailable, err)
		}
		line = lineBuffer
		lineBuffer = lineBuffer[:0]
		trimmed := bytes.TrimSuffix(line, []byte("\n"))
		trimmed = bytes.TrimSuffix(trimmed, []byte("\r"))
		if len(trimmed) == 0 {
			if dataLines != 1 || idLines != 1 {
				return nil, nil, newFailure(FailureProtocol, errors.New("A2A SSE event must contain exactly one id and data line"))
			}
			return frame, result, nil
		}
		if bytes.HasPrefix(trimmed, []byte("id: ")) {
			if idLines != 0 {
				return nil, nil, newFailure(FailureProtocol, errors.New("A2A SSE event contains duplicate id lines"))
			}
			id := bytes.TrimSpace(trimmed[len("id: "):])
			if len(id) == 0 {
				return nil, nil, newFailure(FailureProtocol, errors.New("A2A SSE event id is empty"))
			}
			idValue := string(id)
			if _, exists := body.seenIDs[idValue]; exists {
				return nil, nil, newFailure(FailureProtocol, errors.New("A2A SSE event id is repeated"))
			}
			body.seenIDs[idValue] = struct{}{}
			idLines++
			continue
		}
		if dataLines != 0 || !bytes.HasPrefix(trimmed, []byte("data: ")) {
			return nil, nil, newFailure(FailureProtocol, errors.New("A2A SSE event framing is invalid"))
		}
		data := trimmed[len("data: "):]
		if len(data) == 0 || !json.Valid(data) {
			return nil, nil, newFailure(FailureProtocol, errors.New("A2A SSE data must be one JSON value"))
		}
		decodedResult, err := streamingJSONRPCResult(data, body.expectedID)
		if err != nil {
			return nil, nil, err
		}
		result = decodedResult
		dataLines++
	}
}

func streamingRequestID(data []byte) (json.RawMessage, error) {
	if err := rejectDuplicateJSONMembers(data); err != nil {
		return nil, err
	}
	if err := validateExactJSONMembers(data, map[string]struct{}{
		"jsonrpc": {}, "id": {}, "method": {}, "params": {},
	}); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var request jsonRPCRequestEnvelope
	if err := decoder.Decode(&request); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	if request.JSONRPC != "2.0" || request.Method != "message/stream" {
		return nil, errors.New("A2A streaming request envelope is invalid")
	}
	if err := validateJSONRPCID(request.ID); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), request.ID...), nil
}

func streamingJSONRPCResult(data, expectedID json.RawMessage) (json.RawMessage, error) {
	if err := rejectDuplicateJSONMembers(data); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	if err := validateExactJSONMembers(data, map[string]struct{}{
		"jsonrpc": {}, "id": {}, "result": {}, "error": {},
	}); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var response jsonRPCResponseEnvelope
	if err := decoder.Decode(&response); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	if response.JSONRPC != "2.0" {
		return nil, newFailure(FailureProtocol, errors.New("A2A JSON-RPC streaming response version is invalid"))
	}
	if err := validateJSONRPCID(response.ID); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	if !equalJSONRPCID(expectedID, response.ID) {
		return nil, newFailure(FailureProtocol, errors.New("A2A JSON-RPC streaming response id does not match the request"))
	}
	hasResult := presentJSONValue(response.Result)
	hasError := presentJSONValue(response.Error)
	if hasResult == hasError {
		return nil, newFailure(FailureProtocol, errors.New("A2A JSON-RPC streaming response must contain exactly one result or error"))
	}
	if !hasResult {
		return nil, nil
	}
	return append(json.RawMessage(nil), response.Result...), nil
}

func minInt64(left, right int64) int {
	if left < right {
		return int(left)
	}
	return int(right)
}
