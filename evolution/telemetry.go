package evolution

import (
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Instrument emits request traces, latency, and authorization decisions.
func Instrument(meter metric.Meter, tracer trace.Tracer, next http.Handler) http.Handler {
	counter, err := meter.Int64Counter("rsi_state_http_requests_total")
	if err != nil {
		return next
	}
	latency, err := meter.Float64Histogram("rsi_state_http_request_duration_ms")
	if err != nil {
		return next
	}
	decisions, err := meter.Int64Counter("rsi_state_authorization_decisions_total")
	if err != nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "rsi-state.http "+r.Method)
		defer span.End()
		start := time.Now()
		response := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(response, r.WithContext(ctx))
		attributes := []attribute.KeyValue{
			attribute.String("method", r.Method),
			attribute.String("path", r.URL.Path),
			attribute.String("status", strconv.Itoa(response.status)),
		}
		counter.Add(ctx, 1, metric.WithAttributes(attributes...))
		latency.Record(ctx, float64(time.Since(start).Microseconds())/1000, metric.WithAttributes(attributes...))
		if response.status == http.StatusUnauthorized || response.status == http.StatusForbidden {
			decisions.Add(ctx, 1, metric.WithAttributes(
				attribute.String("decision", "denied"),
				attribute.String("status", strconv.Itoa(response.status)),
			))
		} else {
			decisions.Add(ctx, 1, metric.WithAttributes(attribute.String("decision", "allowed")))
		}
		span.SetAttributes(attributes...)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	return w.ResponseWriter.Write(b)
}
