package evolution

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Instrument records request counts using an OpenTelemetry meter.
func Instrument(meter metric.Meter, next http.Handler) http.Handler {
	counter, err := meter.Int64Counter("rsi_state_http_requests_total")
	if err != nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		counter.Add(r.Context(), 1, metric.WithAttributes(attribute.String("method", r.Method)))
	})
}
