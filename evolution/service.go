// Package evolution implements the control-plane state machine for agent
// workspace evolution. It keeps accepted workspace heads separate from
// untrusted candidates and only advances a head through compare-and-swap.
package evolution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrAlreadyExists      = errors.New("evolution state already exists")
	ErrNotFound           = errors.New("evolution state not found")
	ErrRevisionConflict   = errors.New("accepted head revision conflict")
	ErrCandidateBaseStale = errors.New("candidate base revision is stale")
	ErrInvalidInput       = errors.New("invalid evolution state input")
	ErrCandidateFinalized = errors.New("candidate has already been finalized")
)

type EventKind string

const (
	EventKindCandidateProposed   EventKind = "candidate.proposed"
	EventKindEvaluationCompleted EventKind = "evaluation.completed"
)

type CandidateStatus string

const (
	CandidatePending  CandidateStatus = "pending"
	CandidateAccepted CandidateStatus = "accepted"
	CandidateRejected CandidateStatus = "rejected"
)

type AcceptedHead struct {
	WorkspaceID string    `json:"workspace_id"`
	Revision    int64     `json:"revision"`
	EventID     string    `json:"event_id,omitempty"`
	SnapshotRef string    `json:"snapshot_ref,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type Event struct {
	ID             string            `json:"id"`
	WorkspaceID    string            `json:"workspace_id"`
	ParentRevision int64             `json:"parent_revision"`
	Kind           EventKind         `json:"kind"`
	Actor          string            `json:"actor"`
	Payload        map[string]string `json:"payload,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
}

type EventInput struct {
	WorkspaceID    string            `json:"workspace_id"`
	ParentRevision int64             `json:"parent_revision"`
	Kind           EventKind         `json:"kind"`
	Actor          string            `json:"actor"`
	Payload        map[string]string `json:"payload,omitempty"`
}

type Candidate struct {
	ID            string          `json:"id"`
	WorkspaceID   string          `json:"workspace_id"`
	BaseRevision  int64           `json:"base_revision"`
	EventID       string          `json:"event_id"`
	SnapshotRef   string          `json:"snapshot_ref"`
	EvaluationRef string          `json:"evaluation_ref"`
	Status        CandidateStatus `json:"status"`
	CreatedAt     time.Time       `json:"created_at"`
	AcceptedAt    *time.Time      `json:"accepted_at,omitempty"`
}

type CandidateInput struct {
	ID            string `json:"id"`
	WorkspaceID   string `json:"workspace_id"`
	BaseRevision  int64  `json:"base_revision"`
	EventID       string `json:"event_id"`
	SnapshotRef   string `json:"snapshot_ref"`
	EvaluationRef string `json:"evaluation_ref"`
}

// Service applies the evolution state-machine rules to a backing store.
type Service struct {
	store *InMemoryStore
	now   func() time.Time
}

func NewService(store *InMemoryStore, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, now: now}
}

func (s *Service) CreateWorkspace(_ context.Context, workspaceID string) (AcceptedHead, error) {
	if strings.TrimSpace(workspaceID) == "" {
		return AcceptedHead{}, fmt.Errorf("%w: workspace ID is required", ErrInvalidInput)
	}
	return s.store.createWorkspace(workspaceID, s.now().UTC())
}

func (s *Service) AppendEvent(_ context.Context, input EventInput) (Event, error) {
	if strings.TrimSpace(input.WorkspaceID) == "" || input.ParentRevision < 0 ||
		input.Kind == "" || strings.TrimSpace(input.Actor) == "" {
		return Event{}, fmt.Errorf("%w: workspace ID, non-negative parent revision, kind, and actor are required", ErrInvalidInput)
	}
	return s.store.appendEvent(input, s.now().UTC())
}

func (s *Service) ListEvents(_ context.Context, workspaceID string) ([]Event, error) {
	return s.store.listEvents(workspaceID)
}

func (s *Service) CreateCandidate(_ context.Context, input CandidateInput) (Candidate, error) {
	if strings.TrimSpace(input.ID) == "" || strings.TrimSpace(input.WorkspaceID) == "" ||
		input.BaseRevision < 0 || strings.TrimSpace(input.EventID) == "" ||
		strings.TrimSpace(input.SnapshotRef) == "" || strings.TrimSpace(input.EvaluationRef) == "" {
		return Candidate{}, fmt.Errorf("%w: candidate ID, workspace ID, event, snapshot, evaluation, and non-negative base revision are required", ErrInvalidInput)
	}
	return s.store.createCandidate(input, s.now().UTC())
}

func (s *Service) GetCandidate(_ context.Context, candidateID string) (Candidate, error) {
	return s.store.getCandidate(candidateID)
}

// PromoteCandidate performs the accepted-head compare-and-swap. The candidate
// stays pending whenever the caller has a stale head or candidate base.
func (s *Service) PromoteCandidate(_ context.Context, workspaceID, candidateID string, expectedRevision int64) (AcceptedHead, error) {
	if expectedRevision < 0 {
		return AcceptedHead{}, fmt.Errorf("%w: expected revision must be non-negative", ErrInvalidInput)
	}
	return s.store.promoteCandidate(workspaceID, candidateID, expectedRevision, s.now().UTC())
}

type workspaceState struct {
	head       AcceptedHead
	events     []Event
	eventCount int64
}

// InMemoryStore is a concurrency-safe reference backend. Production stores
// can preserve the same Service contract while persisting events and CAS heads
// in object storage and a conditional-write metadata service.
type InMemoryStore struct {
	mu         sync.RWMutex
	workspaces map[string]*workspaceState
	candidates map[string]Candidate
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		workspaces: make(map[string]*workspaceState),
		candidates: make(map[string]Candidate),
	}
}

func (s *InMemoryStore) createWorkspace(workspaceID string, now time.Time) (AcceptedHead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.workspaces[workspaceID]; exists {
		return AcceptedHead{}, fmt.Errorf("%w: workspace %q", ErrAlreadyExists, workspaceID)
	}
	head := AcceptedHead{WorkspaceID: workspaceID, Revision: 0, UpdatedAt: now}
	s.workspaces[workspaceID] = &workspaceState{head: head}
	return head, nil
}

func (s *InMemoryStore) appendEvent(input EventInput, now time.Time) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	workspace, ok := s.workspaces[input.WorkspaceID]
	if !ok {
		return Event{}, fmt.Errorf("%w: workspace %q", ErrNotFound, input.WorkspaceID)
	}
	workspace.eventCount++
	event := Event{
		ID:             fmt.Sprintf("evt-%s-%012d", input.WorkspaceID, workspace.eventCount),
		WorkspaceID:    input.WorkspaceID,
		ParentRevision: input.ParentRevision,
		Kind:           input.Kind,
		Actor:          input.Actor,
		Payload:        copyMap(input.Payload),
		CreatedAt:      now,
	}
	workspace.events = append(workspace.events, event)
	return copyEvent(event), nil
}

func (s *InMemoryStore) listEvents(workspaceID string) ([]Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workspace, ok := s.workspaces[workspaceID]
	if !ok {
		return nil, fmt.Errorf("%w: workspace %q", ErrNotFound, workspaceID)
	}
	events := make([]Event, len(workspace.events))
	for i, event := range workspace.events {
		events[i] = copyEvent(event)
	}
	return events, nil
}

func (s *InMemoryStore) createCandidate(input CandidateInput, now time.Time) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.candidates[input.ID]; exists {
		return Candidate{}, fmt.Errorf("%w: candidate %q", ErrAlreadyExists, input.ID)
	}
	workspace, ok := s.workspaces[input.WorkspaceID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w: workspace %q", ErrNotFound, input.WorkspaceID)
	}
	if !workspace.containsEvent(input.EventID) {
		return Candidate{}, fmt.Errorf("%w: event %q", ErrNotFound, input.EventID)
	}
	candidate := Candidate{
		ID: input.ID, WorkspaceID: input.WorkspaceID, BaseRevision: input.BaseRevision,
		EventID: input.EventID, SnapshotRef: input.SnapshotRef, EvaluationRef: input.EvaluationRef,
		Status: CandidatePending, CreatedAt: now,
	}
	s.candidates[candidate.ID] = candidate
	return candidate, nil
}

func (s *InMemoryStore) getCandidate(candidateID string) (Candidate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w: candidate %q", ErrNotFound, candidateID)
	}
	return copyCandidate(candidate), nil
}

func (s *InMemoryStore) promoteCandidate(workspaceID, candidateID string, expectedRevision int64, now time.Time) (AcceptedHead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	workspace, ok := s.workspaces[workspaceID]
	if !ok {
		return AcceptedHead{}, fmt.Errorf("%w: workspace %q", ErrNotFound, workspaceID)
	}
	if workspace.head.Revision != expectedRevision {
		return AcceptedHead{}, fmt.Errorf("%w: expected %d, current %d", ErrRevisionConflict, expectedRevision, workspace.head.Revision)
	}
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return AcceptedHead{}, fmt.Errorf("%w: candidate %q", ErrNotFound, candidateID)
	}
	if candidate.WorkspaceID != workspaceID {
		return AcceptedHead{}, fmt.Errorf("%w: candidate belongs to %q", ErrInvalidInput, candidate.WorkspaceID)
	}
	if candidate.Status != CandidatePending {
		return AcceptedHead{}, fmt.Errorf("%w: candidate %q is %q", ErrCandidateFinalized, candidateID, candidate.Status)
	}
	if candidate.BaseRevision != workspace.head.Revision {
		return AcceptedHead{}, fmt.Errorf("%w: candidate base %d, current %d", ErrCandidateBaseStale, candidate.BaseRevision, workspace.head.Revision)
	}

	head := AcceptedHead{
		WorkspaceID: workspaceID,
		Revision:    workspace.head.Revision + 1,
		EventID:     candidate.EventID,
		SnapshotRef: candidate.SnapshotRef,
		UpdatedAt:   now,
	}
	workspace.head = head
	candidate.Status = CandidateAccepted
	candidate.AcceptedAt = &now
	s.candidates[candidate.ID] = candidate
	return head, nil
}

func (s *workspaceState) containsEvent(eventID string) bool {
	for _, event := range s.events {
		if event.ID == eventID {
			return true
		}
	}
	return false
}

func copyEvent(event Event) Event {
	event.Payload = copyMap(event.Payload)
	return event
}

func copyCandidate(candidate Candidate) Candidate {
	if candidate.AcceptedAt != nil {
		acceptedAt := *candidate.AcceptedAt
		candidate.AcceptedAt = &acceptedAt
	}
	return candidate
}

func copyMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
