package evolution

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthorizeEnforcesSelectorPromotion(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := Authorize(map[string]Role{"evolver": RoleEvolver, "selector": RoleSelector}, next)
	request := httptest.NewRequest(http.MethodPost, "/v1/workspaces/arc/candidates/c1/promote", nil)
	request.Header.Set("Authorization", "Bearer evolver")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	request.Header.Set("Authorization", "Bearer selector")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}
