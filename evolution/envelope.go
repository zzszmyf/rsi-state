package evolution

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
)

// EventEnvelope binds an immutable event to a content hash and signer.
type EventEnvelope struct {
	Event            Event  `json:"event"`
	ContentHash      string `json:"content_hash"`
	KeyID            string `json:"key_id"`
	SigningAlgorithm string `json:"signing_algorithm"`
	Signature        string `json:"signature"`
}

func SealEvent(event Event, keyID string, privateKey ed25519.PrivateKey) (EventEnvelope, error) {
	if len(privateKey) != ed25519.PrivateKeySize || keyID == "" {
		return EventEnvelope{}, fmt.Errorf("%w: signing key and key ID are required", ErrInvalidInput)
	}
	event.KeyID = keyID
	event.SigningAlgorithm = "Ed25519"
	bytes, err := canonicalEvent(event)
	if err != nil {
		return EventEnvelope{}, err
	}
	hash := sha256.Sum256(bytes)
	return EventEnvelope{
		Event: event, ContentHash: hex.EncodeToString(hash[:]), KeyID: keyID, SigningAlgorithm: "Ed25519",
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, hash[:])),
	}, nil
}

func (e EventEnvelope) Verify(publicKey ed25519.PublicKey) error {
	if e.SigningAlgorithm != "Ed25519" || e.Event.SigningAlgorithm != e.SigningAlgorithm || e.Event.KeyID != e.KeyID {
		return fmt.Errorf("%w: unsupported or inconsistent signing metadata", ErrInvalidInput)
	}
	bytes, err := canonicalEvent(e.Event)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(bytes)
	if hex.EncodeToString(hash[:]) != e.ContentHash {
		return fmt.Errorf("%w: content hash mismatch", ErrInvalidInput)
	}
	signature, err := base64.StdEncoding.DecodeString(e.Signature)
	if err != nil || !ed25519.Verify(publicKey, hash[:], signature) {
		return fmt.Errorf("%w: signature verification failed", ErrInvalidInput)
	}
	return nil
}

// EventLedger seals new events and verifies historical envelopes. DurableStore
// requires this interface so production WAL records cannot be unsigned.
type EventLedger interface {
	Seal(Event) (EventEnvelope, error)
	Verify(EventEnvelope) error
}

// SigningKey identifies an Ed25519 key and the role authorized to use it.
type SigningKey struct {
	ID         string
	Role       Role
	PrivateKey ed25519.PrivateKey
	PublicKey  ed25519.PublicKey
	Revoked    bool
}

// Keyring supports active-key rotation while retaining public keys for
// historical verification. Revoked keys may verify events created before their
// revocation, but may never sign new events.
type Keyring struct {
	mu   sync.RWMutex
	keys map[string]SigningKey
}

func NewKeyring(keys ...SigningKey) (*Keyring, error) {
	k := &Keyring{keys: make(map[string]SigningKey)}
	for _, key := range keys {
		if err := k.Add(key); err != nil {
			return nil, err
		}
	}
	return k, nil
}

func (k *Keyring) Add(key SigningKey) error {
	if key.ID == "" || key.Role == "" || len(key.PublicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: key ID, role, and public key are required", ErrInvalidInput)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, exists := k.keys[key.ID]; exists {
		return fmt.Errorf("%w: signing key %q", ErrAlreadyExists, key.ID)
	}
	k.keys[key.ID] = SigningKey{ID: key.ID, Role: key.Role, PrivateKey: append(ed25519.PrivateKey(nil), key.PrivateKey...), PublicKey: append(ed25519.PublicKey(nil), key.PublicKey...), Revoked: key.Revoked}
	return nil
}

func (k *Keyring) Revoke(keyID string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	key, ok := k.keys[keyID]
	if !ok {
		return fmt.Errorf("%w: key %q", ErrNotFound, keyID)
	}
	key.Revoked = true
	k.keys[keyID] = key
	return nil
}

func (k *Keyring) Seal(event Event) (EventEnvelope, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	for _, key := range k.keys {
		if key.Role == event.SignerRole && !key.Revoked && len(key.PrivateKey) == ed25519.PrivateKeySize {
			return SealEvent(event, key.ID, key.PrivateKey)
		}
	}
	return EventEnvelope{}, fmt.Errorf("%w: no active signing key for role %q", ErrInvalidInput, event.SignerRole)
}

func (k *Keyring) Verify(envelope EventEnvelope) error {
	k.mu.RLock()
	key, ok := k.keys[envelope.KeyID]
	k.mu.RUnlock()
	if !ok || key.Role != envelope.Event.SignerRole {
		return fmt.Errorf("%w: unknown or unauthorized signing key", ErrInvalidInput)
	}
	return envelope.Verify(key.PublicKey)
}

func canonicalEvent(event Event) ([]byte, error) {
	return json.Marshal(event)
}
