package evolution

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIdentityAuthorizeRequiresVerifiedIdentityAndRole(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := IdentityAuthorize(StaticIdentityVerifier{Name: "worker/evolver-a"}, map[string]Role{
		"worker/evolver-a": RoleEvolver,
	}, next)

	request := httptest.NewRequest(http.MethodPost, "/v1/workspaces", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}

	handler = IdentityAuthorize(StaticIdentityVerifier{}, map[string]Role{}, next)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("missing identity status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestHealthEndpointsDoNotRequireAPIAuthentication(t *testing.T) {
	handler := HealthEndpoints(func(r *http.Request) error {
		if r.URL.Query().Get("fail") == "1" {
			return ErrNotFound
		}
		return nil
	}, http.NotFoundHandler())
	for _, tc := range []struct {
		path string
		want int
	}{
		{"/healthz", http.StatusOK},
		{"/readyz", http.StatusOK},
		{"/readyz?fail=1", http.StatusServiceUnavailable},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if response.Code != tc.want {
			t.Errorf("%s status = %d, want %d", tc.path, response.Code, tc.want)
		}
	}
}

func TestTrustedKeyRegistryVerifiesEnvelope(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{ID: "evt-1", WorkspaceID: "workspace", SignerRole: RoleEvolver}
	envelope, err := SealEvent(event, "key-1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewTrustedKeyRegistry(map[string]TrustedPublicKey{
		"key-1": {Role: RoleEvolver, PublicKey: base64.StdEncoding.EncodeToString(publicKey)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Verify(envelope); err != nil {
		t.Fatalf("verify: %v", err)
	}
	envelope.Signature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	if err := registry.Verify(envelope); err == nil {
		t.Fatal("tampered signature verified")
	}
}
