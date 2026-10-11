package persistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zzszmyf/rsi-state/evolution"
)

type failingObjects struct {
	ObjectStore
	putErr  error
	listErr error
}

func (f failingObjects) Put(ctx context.Context, key string, body []byte, contentType string) error {
	if f.putErr != nil {
		return f.putErr
	}
	return f.ObjectStore.Put(ctx, key, body, contentType)
}

func (f failingObjects) List(ctx context.Context, prefix string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.ObjectStore.List(ctx, prefix)
}

func TestFaultInjectionPermissionDeniedWriteLeavesNoEvent(t *testing.T) {
	objects := NewMemoryObjects()
	store := DurableStore{
		Objects: failingObjects{ObjectStore: objects, putErr: errors.New("permission denied")},
		Heads:   newMemoryHeads(), Ledger: testLedger(t),
	}
	now := time.Now().UTC()
	if _, err := store.CreateWorkspace("arc", now); err != nil {
		t.Fatal(err)
	}
	_, err := store.AppendEvent(evolution.EventInput{
		WorkspaceID: "arc", ParentRevision: 0, Kind: evolution.EventKindCandidateProposed, Actor: "evolver",
	}, now)
	if err == nil {
		t.Fatal("permission-denied write succeeded")
	}
	if len(objects.Values) != 0 {
		t.Fatalf("partial WAL object persisted: %#v", objects.Values)
	}
}

func TestFaultInjectionReadinessRejectsUnavailableDurableStore(t *testing.T) {
	store := DurableStore{
		Objects: failingObjects{ObjectStore: NewMemoryObjects(), listErr: errors.New("timeout")},
		Heads:   newMemoryHeads(),
	}
	if err := store.Ready(context.Background()); err == nil {
		t.Fatal("unavailable object store reported ready")
	}
}
