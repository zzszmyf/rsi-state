package evolution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPromoteCandidateAdvancesAcceptedHead(t *testing.T) {
	service := NewService(NewInMemoryStore(), fixedClock())
	ctx := context.Background()

	if _, err := service.CreateWorkspace(ctx, "arc-agent"); err != nil {
		t.Fatalf("CreateWorkspace() error = %v", err)
	}

	event, err := service.AppendEvent(ctx, EventInput{
		WorkspaceID:    "arc-agent",
		ParentRevision: 0,
		Kind:           EventKindCandidateProposed,
		Actor:          "evolver",
		Payload:        map[string]string{"changed_path": "skills/informative-exploration/SKILL.md"},
	})
	if err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}

	candidate, err := service.CreateCandidate(ctx, CandidateInput{
		ID:            "candidate-1",
		WorkspaceID:   "arc-agent",
		BaseRevision:  0,
		EventID:       event.ID,
		SnapshotRef:   "s3://rsi-state/bundles/candidate-1.bundle",
		EvaluationRef: "s3://rsi-state/evaluations/candidate-1.json",
	})
	if err != nil {
		t.Fatalf("CreateCandidate() error = %v", err)
	}

	head, err := service.PromoteCandidate(ctx, "arc-agent", candidate.ID, 0)
	if err != nil {
		t.Fatalf("PromoteCandidate() error = %v", err)
	}

	if head.Revision != 1 {
		t.Fatalf("head revision = %d, want 1", head.Revision)
	}
	if head.EventID != event.ID {
		t.Fatalf("head event ID = %q, want %q", head.EventID, event.ID)
	}
	if head.SnapshotRef != candidate.SnapshotRef {
		t.Fatalf("head snapshot = %q, want %q", head.SnapshotRef, candidate.SnapshotRef)
	}

	storedCandidate, err := service.GetCandidate(ctx, candidate.ID)
	if err != nil {
		t.Fatalf("GetCandidate() error = %v", err)
	}
	if storedCandidate.Status != CandidateAccepted {
		t.Fatalf("candidate status = %q, want %q", storedCandidate.Status, CandidateAccepted)
	}
}

func TestPromoteCandidateRejectsStaleHead(t *testing.T) {
	service := NewService(NewInMemoryStore(), fixedClock())
	ctx := context.Background()
	mustCreateWorkspace(t, service, "arc-agent")

	first := mustCreateCandidate(t, service, "arc-agent", "candidate-1", 0)
	if _, err := service.PromoteCandidate(ctx, "arc-agent", first.ID, 0); err != nil {
		t.Fatalf("first PromoteCandidate() error = %v", err)
	}

	second := mustCreateCandidate(t, service, "arc-agent", "candidate-2", 1)
	if _, err := service.PromoteCandidate(ctx, "arc-agent", second.ID, 0); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("PromoteCandidate() error = %v, want ErrRevisionConflict", err)
	}

	storedCandidate, err := service.GetCandidate(ctx, second.ID)
	if err != nil {
		t.Fatalf("GetCandidate() error = %v", err)
	}
	if storedCandidate.Status != CandidatePending {
		t.Fatalf("candidate status = %q, want %q", storedCandidate.Status, CandidatePending)
	}
}

func TestCandidateMustReferenceCurrentBaseRevision(t *testing.T) {
	service := NewService(NewInMemoryStore(), fixedClock())
	ctx := context.Background()
	mustCreateWorkspace(t, service, "arc-agent")

	candidate := mustCreateCandidate(t, service, "arc-agent", "candidate-1", 0)
	if _, err := service.PromoteCandidate(ctx, "arc-agent", candidate.ID, 0); err != nil {
		t.Fatalf("PromoteCandidate() error = %v", err)
	}

	stale := mustCreateCandidate(t, service, "arc-agent", "candidate-stale", 0)
	if _, err := service.PromoteCandidate(ctx, "arc-agent", stale.ID, 1); !errors.Is(err, ErrCandidateBaseStale) {
		t.Fatalf("PromoteCandidate() error = %v, want ErrCandidateBaseStale", err)
	}
}

func TestAppendEventCopiesPayloadForImmutableAuditRecord(t *testing.T) {
	service := NewService(NewInMemoryStore(), fixedClock())
	ctx := context.Background()
	mustCreateWorkspace(t, service, "arc-agent")

	payload := map[string]string{"score": "0.72"}
	event, err := service.AppendEvent(ctx, EventInput{
		WorkspaceID:    "arc-agent",
		ParentRevision: 0,
		Kind:           EventKindEvaluationCompleted,
		Actor:          "verifier",
		Payload:        payload,
	})
	if err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	payload["score"] = "0.01"

	events, err := service.ListEvents(ctx, "arc-agent")
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Payload["score"] != "0.72" {
		t.Fatalf("stored payload = %q, want immutable copy", events[0].Payload["score"])
	}
	if events[0].ID != event.ID {
		t.Fatalf("event ID = %q, want %q", events[0].ID, event.ID)
	}
}

func fixedClock() func() time.Time {
	return func() time.Time {
		return time.Date(2026, time.October, 11, 0, 0, 0, 0, time.UTC)
	}
}

func mustCreateWorkspace(t *testing.T, service *Service, workspaceID string) {
	t.Helper()
	if _, err := service.CreateWorkspace(context.Background(), workspaceID); err != nil {
		t.Fatalf("CreateWorkspace() error = %v", err)
	}
}

func mustCreateCandidate(t *testing.T, service *Service, workspaceID, candidateID string, baseRevision int64) Candidate {
	t.Helper()
	event, err := service.AppendEvent(context.Background(), EventInput{
		WorkspaceID:    workspaceID,
		ParentRevision: baseRevision,
		Kind:           EventKindCandidateProposed,
		Actor:          "evolver",
		Payload:        map[string]string{"candidate_id": candidateID},
	})
	if err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	candidate, err := service.CreateCandidate(context.Background(), CandidateInput{
		ID:            candidateID,
		WorkspaceID:   workspaceID,
		BaseRevision:  baseRevision,
		EventID:       event.ID,
		SnapshotRef:   "s3://rsi-state/bundles/" + candidateID + ".bundle",
		EvaluationRef: "s3://rsi-state/evaluations/" + candidateID + ".json",
	})
	if err != nil {
		t.Fatalf("CreateCandidate() error = %v", err)
	}
	return candidate
}
