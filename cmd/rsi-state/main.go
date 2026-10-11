package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/zzszmyf/rsi-state/evolution"
	"github.com/zzszmyf/rsi-state/persistence"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	if len(os.Args) > 1 {
		runCommand(os.Args[1:])
		return
	}
	address := os.Getenv("RSI_STATE_ADDR")
	if address == "" {
		address = ":8080"
	}
	service := evolution.NewService(evolution.NewInMemoryStore(), nil)
	log.Printf("rsi-state listening on %s", address)
	handler := http.Handler(evolution.NewHTTPServer(service))
	cacheRoot := os.Getenv("RSI_STATE_CACHE_ROOT")
	ready := func(_ *http.Request) error {
		if err := service.Ready(context.Background()); err != nil {
			return err
		}
		if cacheRoot == "" {
			return nil
		}
		_, err := (persistence.WorkspaceCache{Root: cacheRoot}).Health(context.Background())
		return err
	}
	if cacheRoot != "" {
		mux := http.NewServeMux()
		mux.Handle("/v1/cache/", http.StripPrefix("/v1/cache", persistence.NewCacheHTTPHandler(persistence.WorkspaceCache{Root: cacheRoot})))
		mux.Handle("/", handler)
		handler = mux
	}
	if os.Getenv("RSI_STATE_ENV") == "production" {
		roles, tlsConfig, certificate, key, err := productionConfig()
		if err != nil {
			log.Fatal(err)
		}
		shutdown, err := configureTelemetry(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		defer shutdown()
		service.WithTelemetry(otel.Meter("rsi-state"), otel.Tracer("rsi-state"))
		handler = evolution.IdentityAuthorize(evolution.MTLSIdentityVerifier{}, roles, handler)
		server := &http.Server{
			Addr: address, Handler: evolution.HealthEndpoints(ready,
				evolution.Instrument(otel.Meter("rsi-state"), otel.Tracer("rsi-state"), handler)),
			TLSConfig: tlsConfig,
		}

		if err := server.ListenAndServeTLS(certificate, key); err != nil {
			log.Fatal(err)
		}
		return
	}
	handler = evolution.HealthEndpoints(ready, handler)
	if err := http.ListenAndServe(address, handler); err != nil {
		log.Fatal(err)
	}
}

func configureTelemetry(ctx context.Context) (func(), error) {
	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create metric exporter: %w", err)
	}
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter))
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)))
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	return func() {
		_ = meterProvider.Shutdown(context.Background())
		_ = tracerProvider.Shutdown(context.Background())
	}, nil
}

func productionConfig() (map[string]evolution.Role, *tls.Config, string, string, error) {
	roles, err := evolution.ParseRoleMapping(os.Getenv("RSI_STATE_ROLE_MAP"))
	if err != nil {
		return nil, nil, "", "", err
	}
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) == "" {
		return nil, nil, "", "", fmt.Errorf("production requires OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	certificate, key, clientCA := os.Getenv("RSI_STATE_TLS_CERT_FILE"), os.Getenv("RSI_STATE_TLS_KEY_FILE"), os.Getenv("RSI_STATE_TLS_CLIENT_CA_FILE")
	if certificate == "" || key == "" || clientCA == "" {
		return nil, nil, "", "", fmt.Errorf("production requires RSI_STATE_TLS_CERT_FILE, RSI_STATE_TLS_KEY_FILE, and RSI_STATE_TLS_CLIENT_CA_FILE")
	}
	caBytes, err := os.ReadFile(clientCA)
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("read client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, nil, "", "", fmt.Errorf("client CA file contains no certificates")
	}
	return roles, &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}, certificate, key, nil
}
func runCommand(args []string) {
	switch args[0] {
	case "inspect":
		flags := flag.NewFlagSet("inspect", flag.ExitOnError)
		server := flags.String("server", "http://localhost:8080", "API base URL")
		workspace := flags.String("workspace", "", "workspace ID")
		flags.Parse(args[1:])
		response, err := http.Get(*server + "/v1/workspaces/" + *workspace)
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			log.Fatalf("inspect failed: %s", response.Status)
		}
		_ = json.NewEncoder(os.Stdout).Encode(json.RawMessage(mustRead(response)))
	case "diff":
		flags := flag.NewFlagSet("diff", flag.ExitOnError)
		from := flags.String("from", "", "base Git revision")
		to := flags.String("to", "HEAD", "target Git revision")
		dir := flags.String("workspace", ".", "Git workspace")
		flags.Parse(args[1:])
		command := exec.Command("git", "-C", *dir, "diff", *from, *to)
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		if err := command.Run(); err != nil {
			log.Fatal(err)
		}
	case "restore":
		flags := flag.NewFlagSet("restore", flag.ExitOnError)
		bundle := flags.String("bundle", "", "local Git bundle")
		destination := flags.String("destination", "", "empty restore directory")
		flags.Parse(args[1:])
		if *bundle == "" || *destination == "" || filepath.Clean(*destination) == "." {
			log.Fatal("bundle and destination are required")
		}
		if err := exec.Command("git", "clone", *bundle, *destination).Run(); err != nil {
			log.Fatal(err)
		}
	case "verify":
		flags := flag.NewFlagSet("verify", flag.ExitOnError)
		file := flags.String("event", "", "event envelope JSON")
		trustedKeys := flags.String("trusted-keys", "", "trusted public-key registry JSON")
		flags.Parse(args[1:])
		if *file == "" || *trustedKeys == "" {
			log.Fatal("event and trusted-keys are required")
		}
		bytes, err := os.ReadFile(*file)
		if err != nil {
			log.Fatal(err)
		}
		var envelope evolution.EventEnvelope
		if err := json.Unmarshal(bytes, &envelope); err != nil {
			log.Fatal(err)
		}
		registry, err := evolution.LoadTrustedKeyRegistry(*trustedKeys)
		if err != nil {
			log.Fatal(err)
		}
		if err := registry.Verify(envelope); err != nil {
			log.Fatalf("verification failed: %v", err)
		}
		fmt.Printf("verified event=%s content_hash=%s signer=%s\n", envelope.Event.ID, envelope.ContentHash, envelope.KeyID)
	case "cache-health", "cache-repair":
		flags := flag.NewFlagSet(args[0], flag.ExitOnError)
		root := flags.String("root", "", "local workspace cache root")
		flags.Parse(args[1:])
		if *root == "" {
			log.Fatal("cache root is required")
		}
		cache := persistence.WorkspaceCache{Root: *root}
		var health persistence.CacheHealth
		var err error
		if args[0] == "cache-repair" {
			health, err = cache.Repair(context.Background())
		} else {
			health, err = cache.Health(context.Background())
		}
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(health)
	default:
		log.Fatalf("usage: rsi-state [serve|inspect|diff|restore|verify|cache-health|cache-repair]")
	}
}
func mustRead(response *http.Response) []byte {
	bytes, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	return bytes
}
