package persistence

import (
	"encoding/json"
	"net/http"
)

// NewCacheHTTPHandler exposes local cache inspection and repair to an
// authenticated outer server. The caller owns authentication because cache
// repair deletes only local, re-hydratable files.
func NewCacheHTTPHandler(cache WorkspaceCache) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var (
			health CacheHealth
			err    error
		)
		switch r.Method + " " + r.URL.Path {
		case http.MethodGet + " /health":
			health, err = cache.Health(r.Context())
		case http.MethodPost + " /repair":
			health, err = cache.Repair(r.Context())
		default:
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, `{"error":"cache operation failed"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(health)
	})
}
