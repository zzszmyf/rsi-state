package persistence

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCacheHTTPHandlerHealthAndRepair(t *testing.T) {
	handler := NewCacheHTTPHandler(WorkspaceCache{Root: t.TempDir()})
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/health", nil),
		httptest.NewRequest(http.MethodPost, "/repair", nil),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s %s = %d: %s", request.Method, request.URL, response.Code, response.Body.String())
		}
	}
}
