package evolution

import (
	"net/http"
	"strings"
)

type Role string

const (
	RoleReader   Role = "reader"
	RoleEvolver  Role = "evolver"
	RoleVerifier Role = "verifier"
	RoleSelector Role = "selector"
)

// Authorize applies bearer-token role boundaries before the state API.
func Authorize(tokens map[string]Role, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		role, ok := tokens[token]
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, "valid bearer token required")
			return
		}
		if !permits(role, r) {
			writeError(w, http.StatusForbidden, "role is not allowed for this operation")
			return
		}
		next.ServeHTTP(w, r)
	})
}
func permits(role Role, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	if strings.HasSuffix(r.URL.Path, "/promote") || strings.HasSuffix(r.URL.Path, "/reject") || r.URL.Path == "/v1/gc" {
		return role == RoleSelector
	}
	if strings.HasSuffix(r.URL.Path, "/events") {
		return role == RoleEvolver || role == RoleVerifier
	}
	return role == RoleEvolver
}
