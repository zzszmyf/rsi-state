package persistence

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zzszmyf/rsi-state/evolution"
)

type memoryHeads struct {
	mu    sync.Mutex
	heads map[string]evolution.AcceptedHead
}

func newMemoryHeads() *memoryHeads {
	return &memoryHeads{heads: make(map[string]evolution.AcceptedHead)}
}
func (m *memoryHeads) Create(_ context.Context, head evolution.AcceptedHead) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.heads[head.WorkspaceID]; ok {
		return evolution.ErrRevisionConflict
	}
	m.heads[head.WorkspaceID] = head
	return nil
}
func (m *memoryHeads) CompareAndSwap(_ context.Context, head evolution.AcceptedHead, expected int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.heads[head.WorkspaceID]
	if !ok || current.Revision != expected {
		return evolution.ErrRevisionConflict
	}
	m.heads[head.WorkspaceID] = head
	return nil
}
func (m *memoryHeads) Get(_ context.Context, workspace string) (evolution.AcceptedHead, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	head, ok := m.heads[workspace]
	if !ok {
		return evolution.AcceptedHead{}, evolution.ErrNotFound
	}
	return head, nil
}

func TestDurableStoreRestartsFromWALAndHead(t *testing.T) {
	objects, heads := NewMemoryObjects(), newMemoryHeads()
	store := DurableStore{Objects: objects, Heads: heads, Prefix: "rsi"}
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	if _, err := store.CreateWorkspace("arc", now); err != nil {
		t.Fatal(err)
	}
	candidate := createDurableCandidate(t, store, objects, "arc", "first", 0, now)
	if _, err := store.PromoteCandidate("arc", candidate.ID, 0, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	// A new store instance represents a process restart: it has no local state.
	restarted := DurableStore{Objects: objects, Heads: heads, Prefix: "rsi"}
	head, err := restarted.GetHead("arc")
	if err != nil {
		t.Fatal(err)
	}
	if head.Revision != 1 || head.EventID != candidate.EventID {
		t.Fatalf("head after restart = %#v", head)
	}
	stored, err := restarted.GetCandidate(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != evolution.CandidateAccepted {
		t.Fatalf("candidate after restart = %q", stored.Status)
	}
	events, err := restarted.ListEvents("arc")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || !hasEventKind(events, evolution.EventKindCandidatePromoted) {
		t.Fatalf("WAL events = %#v", events)
	}
}

func TestDurableStoreCompetingPromotionsOnlyOneWins(t *testing.T) {
	objects, heads := NewMemoryObjects(), newMemoryHeads()
	store := DurableStore{Objects: objects, Heads: heads}
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	if _, err := store.CreateWorkspace("arc", now); err != nil {
		t.Fatal(err)
	}
	first := createDurableCandidate(t, store, objects, "arc", "first", 0, now)
	second := createDurableCandidate(t, store, objects, "arc", "second", 0, now)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, candidate := range []evolution.Candidate{first, second} {
		go func(candidate evolution.Candidate) {
			<-start
			_, err := store.PromoteCandidate("arc", candidate.ID, 0, now)
			errs <- err
		}(candidate)
	}
	close(start)
	var successes, conflicts int
	for range 2 {
		err := <-errs
		if err == nil {
			successes++
		} else if errors.Is(err, evolution.ErrRevisionConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected promotion error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	head, err := store.GetHead("arc")
	if err != nil || head.Revision != 1 {
		t.Fatalf("head = %#v, %v", head, err)
	}
}

func TestDurableStoreRejectsHashMismatchWithoutAdvancingHead(t *testing.T) {
	objects, heads := NewMemoryObjects(), newMemoryHeads()
	store := DurableStore{Objects: objects, Heads: heads}
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	if _, err := store.CreateWorkspace("arc", now); err != nil {
		t.Fatal(err)
	}
	candidate := createDurableCandidate(t, store, objects, "arc", "first", 0, now)
	if err := objects.Put(context.Background(), "bucket/snapshots/first", []byte("tampered"), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PromoteCandidate("arc", candidate.ID, 0, now); !errors.Is(err, evolution.ErrInvalidInput) {
		t.Fatalf("PromoteCandidate() error = %v", err)
	}
	head, err := store.GetHead("arc")
	if err != nil || head.Revision != 0 {
		t.Fatalf("head = %#v, %v", head, err)
	}
	stored, err := store.GetCandidate(candidate.ID)
	if err != nil || stored.Status != evolution.CandidatePending {
		t.Fatalf("candidate = %#v, %v", stored, err)
	}
}

func createDurableCandidate(t *testing.T, store DurableStore, objects *MemoryObjects, workspace, id string, base int64, now time.Time) evolution.Candidate {
	t.Helper()
	snapshot, evaluation := "s3://bucket/snapshots/"+id, "s3://bucket/evaluations/"+id
	if err := objects.Put(context.Background(), "bucket/snapshots/"+id, []byte("snapshot-"+id), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(context.Background(), "bucket/evaluations/"+id, []byte("evaluation-"+id), "application/json"); err != nil {
		t.Fatal(err)
	}
	event, err := store.AppendEvent(evolution.EventInput{WorkspaceID: workspace, ParentRevision: base, Kind: evolution.EventKindCandidateProposed, Actor: "evolver", Payload: map[string]string{"candidate_id": id}}, now)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := store.CreateCandidate(evolution.CandidateInput{ID: id, WorkspaceID: workspace, BaseRevision: base, EventID: event.ID, SnapshotRef: snapshot, EvaluationRef: evaluation}, now)
	if err != nil {
		t.Fatal(err)
	}
	return candidate
}

func TestDurableStoreDoesNotExposeCorruptAcceptedHead(t *testing.T) {
	objects, heads := NewMemoryObjects(), newMemoryHeads()
	store := DurableStore{Objects: objects, Heads: heads}
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	if _, err := store.CreateWorkspace("arc", now); err != nil {
		t.Fatal(err)
	}
	candidate := createDurableCandidate(t, store, objects, "arc", "first", 0, now)
	if _, err := store.PromoteCandidate("arc", candidate.ID, 0, now); err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(context.Background(), "bucket/evaluations/first", []byte("tampered"), "application/json"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetHead("arc"); !errors.Is(err, evolution.ErrInvalidInput) {
		t.Fatalf("GetHead() error = %v, want hash failure", err)
	}
}

func TestDurableStoreRejectionIsRecordedInWAL(t *testing.T) {
	objects, heads := NewMemoryObjects(), newMemoryHeads()
	store := DurableStore{Objects: objects, Heads: heads}
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	if _, err := store.CreateWorkspace("arc", now); err != nil {
		t.Fatal(err)
	}
	candidate := createDurableCandidate(t, store, objects, "arc", "first", 0, now)
	if _, err := store.RejectCandidate(candidate.ID, "holdout regression", now); err != nil {
		t.Fatal(err)
	}
	events, err := store.ListEvents("arc")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || !hasEventKind(events, evolution.EventKindCandidateRejected) {
		t.Fatalf("WAL events = %#v", events)
	}
}

func hasEventKind(events []evolution.Event, want evolution.EventKind) bool {
	for _, event := range events {
		if event.Kind == want {
			return true
		}
	}
	return false
}
