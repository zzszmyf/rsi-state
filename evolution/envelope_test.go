package evolution

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

func TestEventEnvelopeDetectsTampering(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := SealEvent(Event{ID: "evt-1", WorkspaceID: "arc", Kind: EventKindCandidateProposed, Actor: "evolver"}, "key-1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := envelope.Verify(publicKey); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	envelope.Event.Actor = "attacker"
	if err := envelope.Verify(publicKey); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Verify() error = %v, want invalid input", err)
	}
}

func TestRejectAndCollectCandidate(t *testing.T) {
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	service := NewService(NewInMemoryStore(), func() time.Time { return now })
	mustCreateWorkspace(t, service, "arc")
	candidate := mustCreateCandidate(t, service, "arc", "candidate-1", 0)
	rejected, err := service.RejectCandidate(context.Background(), candidate.ID, "holdout regression")
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Status != CandidateRejected {
		t.Fatalf("status = %q", rejected.Status)
	}
	if removed := service.CollectRejectedBefore(context.Background(), now.Add(time.Second)); removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, err := service.GetCandidate(context.Background(), candidate.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetCandidate() error = %v", err)
	}
}
