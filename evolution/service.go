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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
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
	EventKindCandidatePromoted   EventKind = "candidate.promoted"
	EventKindCandidateRejected   EventKind = "candidate.rejected"
	EventKindCandidateRolledBack EventKind = "candidate.rolled_back"
	EventKindHistoryCompacted    EventKind = "history.compacted"
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
	ID               string            `json:"id"`
	WorkspaceID      string            `json:"workspace_id"`
	ParentRevision   int64             `json:"parent_revision"`
	Kind             EventKind         `json:"kind"`
	Actor            string            `json:"actor"`
	Payload          map[string]string `json:"payload,omitempty"`
	KeyID            string            `json:"key_id,omitempty"`
	SigningAlgorithm string            `json:"signing_algorithm,omitempty"`
	SignerRole       Role              `json:"signer_role,omitempty"`
	ParentHash       string            `json:"parent_hash,omitempty"`
	ArtifactRefs     []string          `json:"artifact_refs,omitempty"`
	SelectorDecision string            `json:"selector_decision,omitempty"`
	CreatedAt        time.Time         `json:"created_at"`
}

type EventInput struct {
	WorkspaceID      string            `json:"workspace_id"`
	ParentRevision   int64             `json:"parent_revision"`
	Kind             EventKind         `json:"kind"`
	Actor            string            `json:"actor"`
	Payload          map[string]string `json:"payload,omitempty"`
	SignerRole       Role              `json:"signer_role,omitempty"`
	ArtifactRefs     []string          `json:"artifact_refs,omitempty"`
	SelectorDecision string            `json:"selector_decision,omitempty"`
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
	RejectedAt    *time.Time      `json:"rejected_at,omitempty"`
	RejectReason  string          `json:"reject_reason,omitempty"`
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
	store  StateStore
	now    func() time.Time
	meter  metric.Meter
	tracer trace.Tracer
}

type StateStore interface {
	CreateWorkspace(string, time.Time) (AcceptedHead, error)
	AppendEvent(EventInput, time.Time) (Event, error)
	ListEvents(string) ([]Event, error)
	CreateCandidate(CandidateInput, time.Time) (Candidate, error)
	GetCandidate(string) (Candidate, error)
	GetHead(string) (AcceptedHead, error)
	RejectCandidate(string, string, time.Time) (Candidate, error)
	CollectRejectedBefore(time.Time) int
	PromoteCandidate(string, string, int64, time.Time) (AcceptedHead, error)
}

// ReadyStore is implemented by backends that can prove their durable
// dependencies are reachable without mutating state.
type ReadyStore interface {
	Ready(context.Context) error
}

func NewService(store StateStore, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{store: store, now: now, meter: otel.Meter("github.com/zzszmyf/rsi-state"), tracer: otel.Tracer("github.com/zzszmyf/rsi-state")}
}

// WithTelemetry attaches the application telemetry provider to a service.
func (s *Service) WithTelemetry(meter metric.Meter, tracer trace.Tracer) *Service {
	if meter != nil {
		s.meter = meter
	}
	if tracer != nil {
		s.tracer = tracer
	}
	return s
}

func (s *Service) CreateWorkspace(_ context.Context, workspaceID string) (AcceptedHead, error) {
	if strings.TrimSpace(workspaceID) == "" {
		return AcceptedHead{}, fmt.Errorf("%w: workspace ID is required", ErrInvalidInput)
	}
	return s.store.CreateWorkspace(workspaceID, s.now().UTC())
}

func (s *Service) AppendEvent(ctx context.Context, input EventInput) (event Event, err error) {
	ctx, span := s.tracer.Start(ctx, "rsi-state.wal.write")
	defer span.End()
	if strings.TrimSpace(input.WorkspaceID) == "" || input.ParentRevision < 0 ||
		input.Kind == "" || strings.TrimSpace(input.Actor) == "" {
		return Event{}, fmt.Errorf("%w: workspace ID, non-negative parent revision, kind, and actor are required", ErrInvalidInput)
	}
	event, err = s.store.AppendEvent(input, s.now().UTC())
	counter, counterErr := s.meter.Int64Counter("rsi_state_wal_writes_total")
	if counterErr == nil {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		counter.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	}
	if err != nil {
		s.recordIntegrityFailure(ctx, err)
	}
	return event, err
}

func (s *Service) ListEvents(_ context.Context, workspaceID string) ([]Event, error) {
	return s.store.ListEvents(workspaceID)
}

func (s *Service) CreateCandidate(_ context.Context, input CandidateInput) (Candidate, error) {
	if strings.TrimSpace(input.ID) == "" || strings.TrimSpace(input.WorkspaceID) == "" ||
		input.BaseRevision < 0 || strings.TrimSpace(input.EventID) == "" ||
		strings.TrimSpace(input.SnapshotRef) == "" || strings.TrimSpace(input.EvaluationRef) == "" {
		return Candidate{}, fmt.Errorf("%w: candidate ID, workspace ID, event, snapshot, evaluation, and non-negative base revision are required", ErrInvalidInput)
	}
	return s.store.CreateCandidate(input, s.now().UTC())
}

func (s *Service) GetCandidate(_ context.Context, candidateID string) (Candidate, error) {
	return s.store.GetCandidate(candidateID)
}

func (s *Service) GetHead(ctx context.Context, workspaceID string) (head AcceptedHead, err error) {
	ctx, span := s.tracer.Start(ctx, "rsi-state.restore")
	defer span.End()
	start := time.Now()
	head, err = s.store.GetHead(workspaceID)
	histogram, histogramErr := s.meter.Float64Histogram("rsi_state_restore_duration_ms")
	if histogramErr == nil {
		histogram.Record(ctx, float64(time.Since(start).Microseconds())/1000)
	}
	if err != nil {
		s.recordIntegrityFailure(ctx, err)
	}
	return head, err
}

// Ready checks the backing durable store when it exposes a readiness probe.
func (s *Service) Ready(ctx context.Context) error {
	if ready, ok := s.store.(ReadyStore); ok {
		return ready.Ready(ctx)
	}
	return nil
}

func (s *Service) RejectCandidate(_ context.Context, candidateID, reason string) (Candidate, error) {
	if strings.TrimSpace(reason) == "" {
		return Candidate{}, fmt.Errorf("%w: rejection reason is required", ErrInvalidInput)
	}
	return s.store.RejectCandidate(candidateID, reason, s.now().UTC())
}

func (s *Service) CollectRejectedBefore(_ context.Context, cutoff time.Time) int {
	return s.store.CollectRejectedBefore(cutoff.UTC())
}

// PromoteCandidate performs the accepted-head compare-and-swap. The candidate
// stays pending whenever the caller has a stale head or candidate base.
func (s *Service) PromoteCandidate(ctx context.Context, workspaceID, candidateID string, expectedRevision int64) (head AcceptedHead, err error) {
	ctx, span := s.tracer.Start(ctx, "rsi-state.head.compare_and_swap")
	defer span.End()
	if expectedRevision < 0 {
		return AcceptedHead{}, fmt.Errorf("%w: expected revision must be non-negative", ErrInvalidInput)
	}
	head, err = s.store.PromoteCandidate(workspaceID, candidateID, expectedRevision, s.now().UTC())
	if errors.Is(err, ErrRevisionConflict) {
		counter, counterErr := s.meter.Int64Counter("rsi_state_cas_conflicts_total")
		if counterErr == nil {
			counter.Add(ctx, 1)
		}
	}
	if err != nil {
		s.recordIntegrityFailure(ctx, err)
	}
	return head, err
}

func (s *Service) recordIntegrityFailure(ctx context.Context, err error) {
	if !errors.Is(err, ErrInvalidInput) {
		return
	}
	counter, counterErr := s.meter.Int64Counter("rsi_state_integrity_failures_total")
	if counterErr == nil {
		counter.Add(ctx, 1)
	}
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

func (s *InMemoryStore) CreateWorkspace(workspaceID string, now time.Time) (AcceptedHead, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.workspaces[workspaceID]; exists {
		return AcceptedHead{}, fmt.Errorf("%w: workspace %q", ErrAlreadyExists, workspaceID)
	}

	head := AcceptedHead{WorkspaceID: workspaceID, Revision: 0, UpdatedAt: now}
	s.workspaces[workspaceID] = &workspaceState{head: head}
	return head, nil
}

func (*InMemoryStore) Ready(context.Context) error { return nil }

func (s *InMemoryStore) AppendEvent(input EventInput, now time.Time) (Event, error) {
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

func (s *InMemoryStore) ListEvents(workspaceID string) ([]Event, error) {
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

func (s *InMemoryStore) CreateCandidate(input CandidateInput, now time.Time) (Candidate, error) {
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

func (s *InMemoryStore) GetCandidate(candidateID string) (Candidate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w: candidate %q", ErrNotFound, candidateID)
	}
	return copyCandidate(candidate), nil
}

func (s *InMemoryStore) GetHead(workspaceID string) (AcceptedHead, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	workspace, ok := s.workspaces[workspaceID]
	if !ok {
		return AcceptedHead{}, fmt.Errorf("%w: workspace %q", ErrNotFound, workspaceID)
	}
	return workspace.head, nil
}

func (s *InMemoryStore) RejectCandidate(candidateID, reason string, now time.Time) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w: candidate %q", ErrNotFound, candidateID)
	}
	if candidate.Status != CandidatePending {
		return Candidate{}, fmt.Errorf("%w: candidate %q is %q", ErrCandidateFinalized, candidateID, candidate.Status)
	}
	candidate.Status, candidate.RejectReason = CandidateRejected, reason
	candidate.RejectedAt = &now
	s.candidates[candidateID] = candidate
	return copyCandidate(candidate), nil
}

func (s *InMemoryStore) CollectRejectedBefore(cutoff time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, candidate := range s.candidates {
		if candidate.Status == CandidateRejected && candidate.RejectedAt != nil && candidate.RejectedAt.Before(cutoff) {
			delete(s.candidates, id)
			removed++
		}
	}
	return removed
}

func (s *InMemoryStore) PromoteCandidate(workspaceID, candidateID string, expectedRevision int64, now time.Time) (AcceptedHead, error) {
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
	event.ArtifactRefs = append([]string(nil), event.ArtifactRefs...)
	return event
}

func copyCandidate(candidate Candidate) Candidate {
	if candidate.AcceptedAt != nil {
		acceptedAt := *candidate.AcceptedAt
		candidate.AcceptedAt = &acceptedAt
	}
	if candidate.RejectedAt != nil {
		rejectedAt := *candidate.RejectedAt
		candidate.RejectedAt = &rejectedAt
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
