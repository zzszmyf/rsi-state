package persistence

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zzszmyf/rsi-state/evolution"
)

// ObjectStore is the durable object-storage subset used by DurableStore. Put
// must make the object readable before returning. Objects are addressed by key.
type ObjectStore interface {
	Put(context.Context, string, []byte, string) error
	Get(context.Context, string) ([]byte, error)
	List(context.Context, string) ([]string, error)
}

// HeadStore is the conditional accepted-head pointer used by DurableStore.
type HeadStore interface {
	Create(context.Context, evolution.AcceptedHead) error
	CompareAndSwap(context.Context, evolution.AcceptedHead, int64) error
	Get(context.Context, string) (evolution.AcceptedHead, error)
}

// DurableStore composes immutable object storage with a conditional head store.
// WAL records are persisted and hash-verified before a DynamoDB-backed head can
// reference a candidate's proposal, snapshot, and evaluation.
type DurableStore struct {
	Objects ObjectStore
	Heads   HeadStore
	Prefix  string
}

func (s DurableStore) CreateWorkspace(workspaceID string, now time.Time) (evolution.AcceptedHead, error) {
	head := evolution.AcceptedHead{WorkspaceID: workspaceID, Revision: 0, UpdatedAt: now.UTC()}
	if err := s.Heads.Create(context.Background(), head); err != nil {
		if errors.Is(err, evolution.ErrRevisionConflict) {
			return evolution.AcceptedHead{}, fmt.Errorf("%w: workspace %q", evolution.ErrAlreadyExists, workspaceID)
		}
		return evolution.AcceptedHead{}, err
	}
	return head, nil
}

func (s DurableStore) AppendEvent(input evolution.EventInput, now time.Time) (evolution.Event, error) {
	if _, err := s.Heads.Get(context.Background(), input.WorkspaceID); err != nil {
		return evolution.Event{}, err
	}
	event := evolution.Event{ID: newID(), WorkspaceID: input.WorkspaceID, ParentRevision: input.ParentRevision, Kind: input.Kind, Actor: input.Actor, Payload: copyMap(input.Payload), CreatedAt: now.UTC()}
	if err := s.putEvent(event); err != nil {
		return evolution.Event{}, err
	}
	return event, nil
}

func (s DurableStore) ListEvents(workspaceID string) ([]evolution.Event, error) {
	if _, err := s.Heads.Get(context.Background(), workspaceID); err != nil {
		return nil, err
	}
	keys, err := s.Objects.List(context.Background(), s.key("wal", workspaceID)+"/")
	if err != nil {
		return nil, fmt.Errorf("list WAL records: %w", err)
	}
	events := make([]evolution.Event, 0, len(keys))
	for _, key := range keys {
		event, err := s.getEventKey(key)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].CreatedAt.Equal(events[j].CreatedAt) {
			return events[i].ID < events[j].ID
		}
		return events[i].CreatedAt.Before(events[j].CreatedAt)
	})
	return events, nil
}

func (s DurableStore) CreateCandidate(input evolution.CandidateInput, now time.Time) (evolution.Candidate, error) {
	if _, err := s.Heads.Get(context.Background(), input.WorkspaceID); err != nil {
		return evolution.Candidate{}, err
	}
	if _, err := s.getEvent(input.WorkspaceID, input.EventID); err != nil {
		return evolution.Candidate{}, err
	}
	if _, err := s.getCandidate(input.ID); err == nil {
		return evolution.Candidate{}, fmt.Errorf("%w: candidate %q", evolution.ErrAlreadyExists, input.ID)
	} else if !errors.Is(err, evolution.ErrNotFound) {
		return evolution.Candidate{}, err
	}
	snapshotHash, err := s.objectHash(input.SnapshotRef)
	if err != nil {
		return evolution.Candidate{}, fmt.Errorf("verify snapshot: %w", err)
	}
	evaluationHash, err := s.objectHash(input.EvaluationRef)
	if err != nil {
		return evolution.Candidate{}, fmt.Errorf("verify evaluation: %w", err)
	}
	candidate := evolution.Candidate{ID: input.ID, WorkspaceID: input.WorkspaceID, BaseRevision: input.BaseRevision, EventID: input.EventID, SnapshotRef: input.SnapshotRef, EvaluationRef: input.EvaluationRef, Status: evolution.CandidatePending, CreatedAt: now.UTC()}
	record := candidateRecord{Candidate: candidate, SnapshotSHA256: snapshotHash, EvaluationSHA256: evaluationHash}
	if err := s.putJSON(s.candidateKey(input.ID), record); err != nil {
		return evolution.Candidate{}, err
	}
	return candidate, nil
}

func (s DurableStore) GetCandidate(candidateID string) (evolution.Candidate, error) {
	record, err := s.getCandidate(candidateID)
	if err != nil {
		return evolution.Candidate{}, err
	}
	// A successful CAS is authoritative even if a process dies before it writes
	// the denormalized candidate status.
	head, err := s.Heads.Get(context.Background(), record.WorkspaceID)
	if err == nil && head.EventID == record.EventID && head.SnapshotRef == record.SnapshotRef && head.Revision > record.BaseRevision {
		record.Status = evolution.CandidateAccepted
	}
	return record.Candidate, nil
}

func (s DurableStore) GetHead(workspaceID string) (evolution.AcceptedHead, error) {
	head, err := s.Heads.Get(context.Background(), workspaceID)
	if err != nil || head.Revision == 0 {
		return head, err
	}
	if _, err := s.getEvent(workspaceID, head.EventID); err != nil {
		return evolution.AcceptedHead{}, fmt.Errorf("accepted head WAL verification: %w", err)
	}
	record, err := s.candidateForHead(head)
	if err != nil {
		return evolution.AcceptedHead{}, fmt.Errorf("accepted head candidate verification: %w", err)
	}
	if err := s.verifyObjectHash(record.SnapshotRef, record.SnapshotSHA256); err != nil {
		return evolution.AcceptedHead{}, fmt.Errorf("accepted head snapshot verification: %w", err)
	}
	if err := s.verifyObjectHash(record.EvaluationRef, record.EvaluationSHA256); err != nil {
		return evolution.AcceptedHead{}, fmt.Errorf("accepted head evaluation verification: %w", err)
	}
	return head, nil
}

func (s DurableStore) RejectCandidate(candidateID, reason string, now time.Time) (evolution.Candidate, error) {
	record, err := s.getCandidate(candidateID)
	if err != nil {
		return evolution.Candidate{}, err
	}
	if record.Status != evolution.CandidatePending {
		return evolution.Candidate{}, fmt.Errorf("%w: candidate %q is %q", evolution.ErrCandidateFinalized, candidateID, record.Status)
	}
	if err := s.appendSystemEvent(record.WorkspaceID, record.BaseRevision, evolution.EventKindCandidateRejected, "selector", map[string]string{"candidate_id": candidateID, "reason": reason}, now); err != nil {
		return evolution.Candidate{}, err
	}
	record.Status, record.RejectReason = evolution.CandidateRejected, reason
	record.RejectedAt = ptr(now.UTC())
	if err := s.putJSON(s.candidateKey(candidateID), record); err != nil {
		return evolution.Candidate{}, err
	}
	return record.Candidate, nil
}

func (s DurableStore) CollectRejectedBefore(cutoff time.Time) int { return 0 }

func (s DurableStore) PromoteCandidate(workspaceID, candidateID string, expectedRevision int64, now time.Time) (evolution.AcceptedHead, error) {
	head, err := s.Heads.Get(context.Background(), workspaceID)
	if err != nil {
		return evolution.AcceptedHead{}, err
	}
	if head.Revision != expectedRevision {
		return evolution.AcceptedHead{}, fmt.Errorf("%w: expected %d, current %d", evolution.ErrRevisionConflict, expectedRevision, head.Revision)
	}
	record, err := s.getCandidate(candidateID)
	if err != nil {
		return evolution.AcceptedHead{}, err
	}
	if record.WorkspaceID != workspaceID {
		return evolution.AcceptedHead{}, fmt.Errorf("%w: candidate belongs to %q", evolution.ErrInvalidInput, record.WorkspaceID)
	}
	if record.Status != evolution.CandidatePending {
		return evolution.AcceptedHead{}, fmt.Errorf("%w: candidate %q is %q", evolution.ErrCandidateFinalized, candidateID, record.Status)
	}
	if record.BaseRevision != head.Revision {
		return evolution.AcceptedHead{}, fmt.Errorf("%w: candidate base %d, current %d", evolution.ErrCandidateBaseStale, record.BaseRevision, head.Revision)
	}
	if _, err := s.getEvent(workspaceID, record.EventID); err != nil {
		return evolution.AcceptedHead{}, err
	}
	if err := s.verifyObjectHash(record.SnapshotRef, record.SnapshotSHA256); err != nil {
		return evolution.AcceptedHead{}, fmt.Errorf("verify snapshot: %w", err)
	}
	if err := s.verifyObjectHash(record.EvaluationRef, record.EvaluationSHA256); err != nil {
		return evolution.AcceptedHead{}, fmt.Errorf("verify evaluation: %w", err)
	}
	if err := s.appendSystemEvent(workspaceID, head.Revision, evolution.EventKindCandidatePromoted, "selector", map[string]string{"candidate_id": candidateID, "event_id": record.EventID}, now); err != nil {
		return evolution.AcceptedHead{}, err
	}
	next := evolution.AcceptedHead{WorkspaceID: workspaceID, Revision: head.Revision + 1, EventID: record.EventID, SnapshotRef: record.SnapshotRef, UpdatedAt: now.UTC()}
	if err := s.Heads.CompareAndSwap(context.Background(), next, expectedRevision); err != nil {
		return evolution.AcceptedHead{}, err
	}
	record.Status, record.AcceptedAt = evolution.CandidateAccepted, ptr(now.UTC())
	if err := s.putJSON(s.candidateKey(candidateID), record); err != nil {
		return evolution.AcceptedHead{}, err
	}
	return next, nil
}

type walRecord struct {
	Event  evolution.Event `json:"event"`
	SHA256 string          `json:"sha256"`
}
type candidateRecord struct {
	evolution.Candidate
	SnapshotSHA256   string `json:"snapshot_sha256"`
	EvaluationSHA256 string `json:"evaluation_sha256"`
}

func (s DurableStore) appendSystemEvent(workspace string, revision int64, kind evolution.EventKind, actor string, payload map[string]string, now time.Time) error {
	_, err := s.AppendEvent(evolution.EventInput{WorkspaceID: workspace, ParentRevision: revision, Kind: kind, Actor: actor, Payload: payload}, now)
	return err
}
func (s DurableStore) putEvent(event evolution.Event) error {
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	return s.putJSON(s.key("wal", event.WorkspaceID, event.ID+".json"), walRecord{Event: event, SHA256: hex.EncodeToString(sum[:])})
}
func (s DurableStore) getEvent(workspace, id string) (evolution.Event, error) {
	return s.getEventKey(s.key("wal", workspace, id+".json"))
}
func (s DurableStore) getEventKey(key string) (evolution.Event, error) {
	var r walRecord
	if err := s.getJSON(key, &r); err != nil {
		return evolution.Event{}, err
	}
	b, _ := json.Marshal(r.Event)
	sum := sha256.Sum256(b)
	if r.SHA256 != hex.EncodeToString(sum[:]) {
		return evolution.Event{}, fmt.Errorf("%w: WAL event hash mismatch", evolution.ErrInvalidInput)
	}
	return r.Event, nil
}
func (s DurableStore) candidateForHead(head evolution.AcceptedHead) (candidateRecord, error) {
	keys, err := s.Objects.List(context.Background(), s.key("candidates")+"/")
	if err != nil {
		return candidateRecord{}, err
	}
	for _, key := range keys {
		var record candidateRecord
		if err := s.getJSON(key, &record); err != nil {
			return candidateRecord{}, err
		}
		if record.WorkspaceID == head.WorkspaceID && record.EventID == head.EventID && record.SnapshotRef == head.SnapshotRef {
			return record, nil
		}
	}
	return candidateRecord{}, fmt.Errorf("%w: candidate for accepted head", evolution.ErrNotFound)
}

func (s DurableStore) getCandidate(id string) (candidateRecord, error) {
	var r candidateRecord
	if err := s.getJSON(s.candidateKey(id), &r); err != nil {
		return r, err
	}
	return r, nil
}
func (s DurableStore) objectHash(ref string) (string, error) {
	b, err := s.Objects.Get(context.Background(), s.objectKey(ref))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
func (s DurableStore) verifyObjectHash(ref, want string) error {
	got, err := s.objectHash(ref)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: object hash mismatch", evolution.ErrInvalidInput)
	}
	return nil
}
func (s DurableStore) candidateKey(id string) string { return s.key("candidates", id+".json") }
func (s DurableStore) key(parts ...string) string {
	all := make([]string, 0, len(parts)+1)
	if strings.Trim(s.Prefix, "/") != "" {
		all = append(all, strings.Trim(s.Prefix, "/"))
	}
	for _, p := range parts {
		all = append(all, path.Clean("/" + p)[1:])
	}
	return path.Join(all...)
}
func (s DurableStore) objectKey(ref string) string { return strings.TrimPrefix(ref, "s3://") }
func (s DurableStore) putJSON(key string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err := s.Objects.Put(context.Background(), key, b, "application/json"); err != nil {
		return fmt.Errorf("write durable record: %w", err)
	}
	return nil
}
func (s DurableStore) getJSON(key string, target any) error {
	b, err := s.Objects.Get(context.Background(), key)
	if err != nil {
		return fmt.Errorf("%w: %s", evolution.ErrNotFound, key)
	}
	if err := json.Unmarshal(b, target); err != nil {
		return fmt.Errorf("%w: decode durable record: %v", evolution.ErrInvalidInput, err)
	}
	return nil
}
func ptr(t time.Time) *time.Time { return &t }
func copyMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// MemoryObjects is a deterministic ObjectStore useful for integration tests.
type MemoryObjects struct {
	mu     sync.RWMutex
	Values map[string][]byte
}

func NewMemoryObjects() *MemoryObjects { return &MemoryObjects{Values: map[string][]byte{}} }
func (m *MemoryObjects) Put(_ context.Context, key string, body []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Values[key] = append([]byte(nil), body...)
	return nil
}
func (m *MemoryObjects) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.Values[key]
	if !ok {
		return nil, evolution.ErrNotFound
	}
	return append([]byte(nil), b...), nil
}
func (m *MemoryObjects) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.Values {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}
