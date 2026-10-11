package evolution

import (
	"encoding/json"
	"net/http"
)

// ReadinessCheck verifies every dependency needed to serve durable state.
type ReadinessCheck func(*http.Request) error

// HealthEndpoints provides unauthenticated process liveness and dependency
// readiness probes. API middleware must wrap only the protected API routes.
func HealthEndpoints(ready ReadinessCheck, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			writeHealth(w, http.StatusOK, "ok")
		case "/readyz":
			if ready != nil {
				if err := ready(r); err != nil {
					writeHealth(w, http.StatusServiceUnavailable, "not ready")
					return
				}
			}
			writeHealth(w, http.StatusOK, "ready")
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func writeHealth(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": value})
}
