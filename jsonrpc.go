package a2atransport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
)

type jsonRPCRequestEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponseEnvelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

type jsonFrame struct {
	object    bool
	expecting bool
	members   map[string]struct{}
}

type envelopeValidatingRoundTripper struct {
	base             http.RoundTripper
	maxResponseBytes int64
}

func (transport envelopeValidatingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	requestBody, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	if err := request.Body.Close(); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	request.Body = io.NopCloser(bytes.NewReader(requestBody))

	if err := rejectDuplicateJSONMembers(requestBody); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	var requestEnvelope jsonRPCRequestEnvelope
	if err := json.Unmarshal(requestBody, &requestEnvelope); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	if err := validateExactJSONMembers(requestBody, map[string]struct{}{
		"jsonrpc": {}, "id": {}, "method": {}, "params": {},
	}); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	if requestEnvelope.JSONRPC != "2.0" || requestEnvelope.Method == "" {
		return nil, newFailure(FailureProtocol, errors.New("A2A JSON-RPC request envelope is invalid"))
	}
	if err := validateJSONRPCID(requestEnvelope.ID); err != nil {
		return nil, newFailure(FailureProtocol, err)
	}
	response, err := transport.base.RoundTrip(request)
	if err != nil || response.StatusCode != http.StatusOK {
		return response, err
	}
	responseBody, err := readBounded(response.Body, transport.maxResponseBytes)
	if err != nil {
		_ = response.Body.Close()
		return nil, err
	}
	if closeErr := response.Body.Close(); closeErr != nil {
		return nil, newFailure(FailureUnavailable, closeErr)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, newFailure(FailureProtocol, errors.New("A2A JSON-RPC response media type is invalid"))
	}
	if err := validateJSONRPCResponseEnvelope(requestEnvelope, responseBody); err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(responseBody))
	return response, nil
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, newFailure(FailureUnavailable, err)
	}
	if int64(len(data)) > limit {
		return nil, newFailure(FailureResponseTooLarge, errors.New("A2A response exceeds the configured limit"))
	}
	return data, nil
}

func validateJSONRPCResponseEnvelope(request jsonRPCRequestEnvelope, responseBody []byte) error {
	if err := rejectDuplicateJSONMembers(responseBody); err != nil {
		return newFailure(FailureProtocol, err)
	}
	if err := validateExactJSONMembers(responseBody, map[string]struct{}{
		"jsonrpc": {}, "id": {}, "result": {}, "error": {},
	}); err != nil {
		return newFailure(FailureProtocol, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	var response jsonRPCResponseEnvelope
	if err := decoder.Decode(&response); err != nil {
		return newFailure(FailureProtocol, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return newFailure(FailureProtocol, err)
	}
	if response.JSONRPC != "2.0" {
		return newFailure(FailureProtocol, errors.New("A2A JSON-RPC response version is invalid"))
	}
	if err := validateJSONRPCID(response.ID); err != nil {
		return newFailure(FailureProtocol, err)
	}
	if !equalJSONRPCID(request.ID, response.ID) {
		return newFailure(FailureProtocol, errors.New("A2A JSON-RPC response id does not match the request"))
	}
	hasResult := presentJSONValue(response.Result)
	hasError := presentJSONValue(response.Error)
	if hasResult == hasError {
		return newFailure(FailureProtocol, errors.New("A2A JSON-RPC response must contain exactly one result or error"))
	}
	if request.Method == "tasks/cancel" && hasResult {
		if err := validateTaskResult(response.Result); err != nil {
			return newFailure(FailureProtocol, err)
		}
	}
	return nil
}

func validateTaskResult(result json.RawMessage) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(result, &object); err != nil {
		return errors.New("A2A tasks/cancel result is not an object")
	}
	kind, ok := object["kind"]
	if !ok {
		return errors.New("A2A tasks/cancel result kind is missing")
	}
	var value string
	if err := json.Unmarshal(kind, &value); err != nil || value != "task" {
		return errors.New("A2A tasks/cancel result kind is invalid")
	}
	return nil
}

func validateExactJSONMembers(data []byte, allowed map[string]struct{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var object map[string]json.RawMessage
	if err := decoder.Decode(&object); err != nil {
		return err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	for name := range object {
		if _, ok := allowed[name]; !ok {
			return errors.New("A2A JSON-RPC envelope contains an unknown member")
		}
	}
	return nil
}

func validateJSONRPCID(value json.RawMessage) error {
	if len(bytes.TrimSpace(value)) == 0 {
		return errors.New("A2A JSON-RPC response id is missing")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return errors.New("A2A JSON-RPC response id is invalid")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return errors.New("A2A JSON-RPC response id contains trailing data")
	}
	switch decoded.(type) {
	case nil, string, json.Number:
		return nil
	default:
		return errors.New("A2A JSON-RPC response id has unsupported JSON type")
	}
}

func equalJSONRPCID(left, right json.RawMessage) bool {
	leftValue, leftErr := decodeJSONRPCID(left)
	rightValue, rightErr := decodeJSONRPCID(right)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(leftValue, rightValue)
}

func decodeJSONRPCID(value json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return decoded, nil
}

func presentJSONValue(value json.RawMessage) bool {
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("A2A JSON-RPC envelope contains trailing data")
	}
	return err
}

func rejectDuplicateJSONMembers(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var stack []jsonFrame
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch value := token.(type) {
		case json.Delim:
			switch value {
			case '{':
				stack = append(stack, jsonFrame{object: true, expecting: true, members: map[string]struct{}{}})
			case '[':
				stack = append(stack, jsonFrame{})
			case '}', ']':
				if len(stack) == 0 {
					return errors.New("A2A JSON-RPC response has an unmatched closing delimiter")
				}
				stack = stack[:len(stack)-1]
				markValueConsumed(stack)
			}
		case string:
			if len(stack) > 0 && stack[len(stack)-1].object && stack[len(stack)-1].expecting {
				current := &stack[len(stack)-1]
				if _, exists := current.members[value]; exists {
					return errors.New("A2A JSON-RPC response contains a duplicate member")
				}
				current.members[value] = struct{}{}
				current.expecting = false
			} else {
				markValueConsumed(stack)
			}
		default:
			markValueConsumed(stack)
		}
	}
}

func markValueConsumed(stack []jsonFrame) {
	if len(stack) > 0 && stack[len(stack)-1].object {
		stack[len(stack)-1].expecting = true
	}
}
