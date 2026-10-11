package evolution

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// EventEnvelope binds an immutable event to a content hash and signer.
type EventEnvelope struct {
	Event       Event  `json:"event"`
	ContentHash string `json:"content_hash"`
	KeyID       string `json:"key_id"`
	Signature   string `json:"signature"`
}

func SealEvent(event Event, keyID string, privateKey ed25519.PrivateKey) (EventEnvelope, error) {
	if len(privateKey) != ed25519.PrivateKeySize || keyID == "" {
		return EventEnvelope{}, fmt.Errorf("%w: signing key and key ID are required", ErrInvalidInput)
	}
	bytes, err := canonicalEvent(event)
	if err != nil {
		return EventEnvelope{}, err
	}
	hash := sha256.Sum256(bytes)
	return EventEnvelope{
		Event: event, ContentHash: hex.EncodeToString(hash[:]), KeyID: keyID,
		Signature: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, hash[:])),
	}, nil
}

func (e EventEnvelope) Verify(publicKey ed25519.PublicKey) error {
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

func canonicalEvent(event Event) ([]byte, error) {
	return json.Marshal(event)
}
