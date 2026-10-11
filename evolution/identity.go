package evolution

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// IdentityVerifier returns an identity only after the transport or identity
// provider has verified the caller's credentials.
type IdentityVerifier interface {
	Identity(*http.Request) (string, bool)
}

// StaticIdentityVerifier is useful for integration tests and for adapters
// which have already verified a workload identity.
type StaticIdentityVerifier struct{ Name string }

func (v StaticIdentityVerifier) Identity(*http.Request) (string, bool) {
	return v.Name, v.Name != ""
}

// MTLSIdentityVerifier authenticates workload identities from a verified TLS
// client certificate. The server TLS configuration must require client certs.
type MTLSIdentityVerifier struct{}

func (MTLSIdentityVerifier) Identity(r *http.Request) (string, bool) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		return "", false
	}
	cert := r.TLS.PeerCertificates[0]
	if len(cert.URIs) > 0 && cert.URIs[0].String() != "" {
		return cert.URIs[0].String(), true
	}
	return cert.Subject.CommonName, cert.Subject.CommonName != ""
}

// IdentityAuthorize maps an already authenticated workload identity to its
// least-privilege service role.
func IdentityAuthorize(verifier IdentityVerifier, roles map[string]Role, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, ok := verifier.Identity(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "verified workload identity required")
			return
		}
		role, ok := roles[identity]
		if !ok {
			writeError(w, http.StatusForbidden, "identity has no assigned role")
			return
		}
		if !permits(role, r) {
			writeError(w, http.StatusForbidden, "role is not allowed for this operation")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ParseRoleMapping parses RSI_STATE_ROLE_MAP JSON (identity to role).
func ParseRoleMapping(value string) (map[string]Role, error) {
	var roles map[string]Role
	if err := json.Unmarshal([]byte(value), &roles); err != nil || len(roles) == 0 {
		return nil, fmt.Errorf("RSI_STATE_ROLE_MAP must be a non-empty JSON identity-to-role map")
	}
	for identity, role := range roles {
		if strings.TrimSpace(identity) == "" || (role != RoleReader && role != RoleEvolver && role != RoleVerifier && role != RoleSelector) {
			return nil, fmt.Errorf("RSI_STATE_ROLE_MAP contains an invalid identity or role")
		}
	}
	return roles, nil
}

// TrustedPublicKey is the public-only, serializable form of a signing key.
type TrustedPublicKey struct {
	Role      Role   `json:"role"`
	PublicKey string `json:"public_key"`
}

// TrustedKeyRegistry verifies envelopes against an explicit public-key
// registry, never against key metadata supplied by the envelope itself.
type TrustedKeyRegistry struct{ keys map[string]SigningKey }

func NewTrustedKeyRegistry(entries map[string]TrustedPublicKey) (*TrustedKeyRegistry, error) {
	registry := &TrustedKeyRegistry{keys: make(map[string]SigningKey, len(entries))}
	for id, entry := range entries {
		publicKey, err := base64.StdEncoding.DecodeString(entry.PublicKey)
		if err != nil || len(publicKey) != ed25519.PublicKeySize || id == "" {
			return nil, fmt.Errorf("trusted key %q is invalid", id)
		}
		if entry.Role != RoleReader && entry.Role != RoleEvolver && entry.Role != RoleVerifier && entry.Role != RoleSelector {
			return nil, fmt.Errorf("trusted key %q has invalid role", id)
		}
		registry.keys[id] = SigningKey{ID: id, Role: entry.Role, PublicKey: ed25519.PublicKey(publicKey)}
	}
	if len(registry.keys) == 0 {
		return nil, fmt.Errorf("trusted key registry is empty")
	}
	return registry, nil
}

func LoadTrustedKeyRegistry(filename string) (*TrustedKeyRegistry, error) {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var entries map[string]TrustedPublicKey
	if err := json.Unmarshal(bytes, &entries); err != nil {
		return nil, fmt.Errorf("decode trusted key registry: %w", err)
	}
	return NewTrustedKeyRegistry(entries)
}

func (r *TrustedKeyRegistry) Verify(envelope EventEnvelope) error {
	key, ok := r.keys[envelope.KeyID]
	if !ok || key.Role != envelope.Event.SignerRole {
		return fmt.Errorf("%w: unknown or unauthorized signing key", ErrInvalidInput)
	}
	return envelope.Verify(key.PublicKey)
}

// VerifyClientCertificate is exported for deployment adapters which need to
// validate that a configured CA actually parses before serving traffic.
func VerifyClientCertificate(certificate []byte) error {
	_, err := x509.ParseCertificate(certificate)
	return err
}
