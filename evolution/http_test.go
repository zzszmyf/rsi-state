package evolution

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPWorkflowPromotesCandidate(t *testing.T) {
	server := NewHTTPServer(NewService(NewInMemoryStore(), fixedClock()))

	head := postJSON(t, server, "/v1/workspaces", http.StatusCreated, map[string]string{
		"id": "arc-agent",
	})
	if got := int64(head["revision"].(float64)); got != 0 {
		t.Fatalf("initial revision = %d, want 0", got)
	}

	event := postJSON(t, server, "/v1/workspaces/arc-agent/events", http.StatusCreated, map[string]any{
		"parent_revision": 0,
		"kind":            EventKindCandidateProposed,
		"actor":           "evolver",
		"payload":         map[string]string{"path": "skills/frame-diff/SKILL.md"},
	})

	candidate := postJSON(t, server, "/v1/workspaces/arc-agent/candidates", http.StatusCreated, map[string]any{
		"id":             "candidate-1",
		"base_revision":  0,
		"event_id":       event["id"],
		"snapshot_ref":   "s3://rsi-state/bundles/candidate-1.bundle",
		"evaluation_ref": "s3://rsi-state/evaluations/candidate-1.json",
	})

	promoted := postJSON(t, server, "/v1/workspaces/arc-agent/candidates/candidate-1/promote?expected_revision=0", http.StatusOK, nil)
	if got := int64(promoted["revision"].(float64)); got != 1 {
		t.Fatalf("promoted revision = %d, want 1", got)
	}
	if promoted["event_id"] != candidate["event_id"] {
		t.Fatalf("promoted event = %q, want %q", promoted["event_id"], candidate["event_id"])
	}
}

func TestHTTPPromotionReportsRevisionConflict(t *testing.T) {
	server := NewHTTPServer(NewService(NewInMemoryStore(), fixedClock()))
	postJSON(t, server, "/v1/workspaces", http.StatusCreated, map[string]string{"id": "arc-agent"})

	event := postJSON(t, server, "/v1/workspaces/arc-agent/events", http.StatusCreated, map[string]any{
		"parent_revision": 0,
		"kind":            EventKindCandidateProposed,
		"actor":           "evolver",
	})
	postJSON(t, server, "/v1/workspaces/arc-agent/candidates", http.StatusCreated, map[string]any{
		"id":             "candidate-1",
		"base_revision":  0,
		"event_id":       event["id"],
		"snapshot_ref":   "s3://rsi-state/bundles/candidate-1.bundle",
		"evaluation_ref": "s3://rsi-state/evaluations/candidate-1.json",
	})
	postJSON(t, server, "/v1/workspaces/arc-agent/candidates/candidate-1/promote?expected_revision=0", http.StatusOK, nil)

	response := request(t, server, http.MethodPost, "/v1/workspaces/arc-agent/candidates/candidate-1/promote?expected_revision=0", nil)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusConflict, response.Body.String())
	}
}

func postJSON(t *testing.T, handler http.Handler, path string, wantStatus int, input any) map[string]any {
	t.Helper()
	response := request(t, handler, http.MethodPost, path, input)
	if response.Code != wantStatus {
		t.Fatalf("POST %s status = %d, want %d; body=%s", path, response.Code, wantStatus, response.Body.String())
	}
	var output map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &output); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return output
}

func request(t *testing.T, handler http.Handler, method, path string, input any) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	if input != nil {
		if err := json.NewEncoder(&body).Encode(input); err != nil {
			t.Fatalf("encode request: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &body)
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}
