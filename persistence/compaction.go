package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zzszmyf/rsi-state/evolution"
	"go.opentelemetry.io/otel"
)

const compactSnapshotVersion = 1

// CompactSnapshot is a checksummed, immutable recovery checkpoint. It keeps
// accepted-head history and artifact references even after preceding WAL
// records are compacted.
type CompactSnapshot struct {
	SchemaVersion     int                      `json:"schema_version"`
	WorkspaceID       string                   `json:"workspace_id"`
	CreatedAt         time.Time                `json:"created_at"`
	ObjectRef         string                   `json:"object_ref"`
	ThroughHash       string                   `json:"through_hash"`
	Head              evolution.AcceptedHead   `json:"head"`
	AcceptedHeads     []evolution.AcceptedHead `json:"accepted_heads"`
	Candidates        []candidateRecord        `json:"candidates"`
	AuditArtifactRefs []string                 `json:"audit_artifact_refs"`
	StateSHA256       string                   `json:"state_sha256"`
}

// RecoveryState is the state obtained by replaying a verified compact
// snapshot followed by the remaining WAL.
type RecoveryState struct {
	Head         evolution.AcceptedHead
	Candidates   []evolution.Candidate
	ReplayDepth  int
	SnapshotAge  time.Duration
	SnapshotUsed bool
}

// Compact creates a checkpoint, records a signed history.compacted WAL event,
// then removes WAL records represented by the checkpoint when deletion is
// available. A failed deletion leaves redundant WAL, never an invalid state.
func (s DurableStore) Compact(workspaceID string, now time.Time) (CompactSnapshot, error) {
	ctx, span := otel.Tracer("rsi-state").Start(context.Background(), "rsi-state.compaction")
	defer span.End()
	if _, err := s.Heads.Get(context.Background(), workspaceID); err != nil {
		return CompactSnapshot{}, err
	}
	envelopes, previous, err := s.recoveryEnvelopes(workspaceID)
	if err != nil {
		return CompactSnapshot{}, err
	}
	if len(envelopes) == 0 {
		return CompactSnapshot{}, fmt.Errorf("%w: no WAL records to compact", evolution.ErrInvalidInput)
	}
	tip, err := s.chainTip(workspaceID)
	if err != nil {
		return CompactSnapshot{}, err
	}
	head, err := s.GetHead(workspaceID)
	if err != nil {
		return CompactSnapshot{}, err
	}
	records, err := s.workspaceCandidates(workspaceID, previous)
	if err != nil {
		return CompactSnapshot{}, err
	}
	applyCandidateEvents(records, envelopes)
	refs := artifactRefs(envelopes)
	if previous != nil {
		refs = append(refs, previous.AuditArtifactRefs...)
	}
	snapshot := CompactSnapshot{
		SchemaVersion: compactSnapshotVersion, WorkspaceID: workspaceID, CreatedAt: now.UTC(), ThroughHash: tip,
		Head: head, Candidates: records, AuditArtifactRefs: uniqueStrings(refs),
	}
	snapshot.ObjectRef = s.key("compactions", workspaceID, snapshot.CreatedAt.Format("20060102T150405.000000000Z")+"-"+newID()+".json")
	if previous != nil {
		snapshot.AcceptedHeads = append(snapshot.AcceptedHeads, previous.AcceptedHeads...)
	}
	snapshot.AcceptedHeads = append(snapshot.AcceptedHeads, acceptedHeads(records, envelopes)...)
	snapshot.AcceptedHeads = append(snapshot.AcceptedHeads, head)
	snapshot.AcceptedHeads = uniqueHeads(snapshot.AcceptedHeads)
	hash, err := compactChecksum(snapshot)
	if err != nil {
		return CompactSnapshot{}, err
	}
	snapshot.StateSHA256 = hash
	key := snapshot.ObjectRef
	if err := s.putJSON(key, snapshot); err != nil {
		return CompactSnapshot{}, err
	}
	if _, err := s.AppendEvent(evolution.EventInput{WorkspaceID: workspaceID, ParentRevision: head.Revision,
		Kind: evolution.EventKindHistoryCompacted, Actor: "selector", SignerRole: evolution.RoleSelector,
		Payload: map[string]string{"snapshot_ref": key, "through_hash": tip}, ArtifactRefs: []string{key}}, now); err != nil {
		return CompactSnapshot{}, err
	}
	if deleter, ok := s.Objects.(ObjectDeleter); ok {
		keys, err := s.Objects.List(context.Background(), s.key("wal", workspaceID)+"/")
		if err != nil {
			return CompactSnapshot{}, err
		}
		for _, key := range keys {
			event, err := s.getEventKey(key)
			if err != nil {
				return CompactSnapshot{}, err
			}
			if event.Kind != evolution.EventKindHistoryCompacted && event.CreatedAt.Before(now.UTC().Add(time.Nanosecond)) {
				if err := deleter.Delete(context.Background(), key); err != nil {
					return CompactSnapshot{}, err
				}
			}
		}
	}
	counter, err := otel.Meter("rsi-state").Int64Counter("rsi_state_compactions_total")
	if err == nil {
		counter.Add(ctx, 1)
	}
	return snapshot, nil
}

// Restore verifies the newest usable checkpoint and replays only the WAL
// records after it. It is safe to call after a process restart.
func (s DurableStore) Restore(workspaceID string, now time.Time) (RecoveryState, error) {
	head, err := s.GetHead(workspaceID)
	if err != nil {
		return RecoveryState{}, err
	}
	envelopes, snapshot, err := s.recoveryEnvelopes(workspaceID)
	if err != nil {
		return RecoveryState{}, err
	}
	records, err := s.workspaceCandidates(workspaceID, snapshot)
	if err != nil {
		return RecoveryState{}, err
	}
	applyCandidateEvents(records, envelopes)
	state := RecoveryState{Head: head, ReplayDepth: len(envelopes)}
	for _, record := range records {
		state.Candidates = append(state.Candidates, record.Candidate)
	}
	if snapshot != nil {
		state.SnapshotUsed = true
		state.SnapshotAge = now.UTC().Sub(snapshot.CreatedAt)
	}
	return state, nil
}

func (s DurableStore) recoveryEnvelopes(workspace string) ([]evolution.EventEnvelope, *CompactSnapshot, error) {
	keys, err := s.Objects.List(context.Background(), s.key("wal", workspace)+"/")
	if err != nil {
		return nil, nil, fmt.Errorf("list WAL records: %w", err)
	}
	all := make([]evolution.EventEnvelope, 0, len(keys))
	for _, key := range keys {
		envelope, err := s.getEnvelopeKey(key)
		if err != nil {
			return nil, nil, err
		}
		all = append(all, envelope)
	}
	snapshot, err := s.latestCompactSnapshot(workspace, all)
	if err != nil {
		return nil, nil, err
	}
	if snapshot == nil {
		if err := verifyChain(all); err != nil {
			return nil, nil, err
		}
		return all, nil, nil
	}
	after := make([]evolution.EventEnvelope, 0, len(all))
	for _, envelope := range all {
		if envelope.Event.ParentHash == snapshot.ThroughHash || envelope.Event.CreatedAt.After(snapshot.CreatedAt) {
			after = append(after, envelope)
		}
	}
	if err := verifyChainFrom(after, snapshot.ThroughHash); err != nil {
		return nil, nil, err
	}
	return after, snapshot, nil
}

func (s DurableStore) latestCompactSnapshot(workspace string, envelopes []evolution.EventEnvelope) (*CompactSnapshot, error) {
	if envelopes == nil {
		walKeys, err := s.Objects.List(context.Background(), s.key("wal", workspace)+"/")
		if err != nil {
			return nil, err
		}
		envelopes = make([]evolution.EventEnvelope, 0, len(walKeys))
		for _, key := range walKeys {
			envelope, err := s.getEnvelopeKey(key)
			if err != nil {
				return nil, err
			}
			envelopes = append(envelopes, envelope)
		}
	}
	keys, err := s.Objects.List(context.Background(), s.key("compactions", workspace)+"/")
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	for _, key := range keys {
		var snapshot CompactSnapshot
		if err := s.getJSON(key, &snapshot); err != nil || validateCompactSnapshot(snapshot) != nil {
			continue // An interrupted or corrupt checkpoint is never selected.
		}
		for _, envelope := range envelopes {
			if envelope.Event.Kind == evolution.EventKindHistoryCompacted &&
				containsRefs(envelope.Event.ArtifactRefs, key) &&
				envelope.Event.ParentHash == snapshot.ThroughHash {
				return &snapshot, nil
			}
		}
	}
	return nil, nil
}

func (s DurableStore) workspaceCandidates(workspace string, snapshot *CompactSnapshot) ([]candidateRecord, error) {
	byID := map[string]candidateRecord{}
	if snapshot != nil {
		for _, candidate := range snapshot.Candidates {
			byID[candidate.ID] = candidate
		}
	}
	keys, err := s.Objects.List(context.Background(), s.key("candidates")+"/")
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		var candidate candidateRecord
		if err := s.getJSON(key, &candidate); err != nil {
			return nil, err
		}
		if candidate.WorkspaceID == workspace {
			if _, compacted := byID[candidate.ID]; !compacted {
				byID[candidate.ID] = candidate
			}
		}
	}
	out := make([]candidateRecord, 0, len(byID))
	for _, candidate := range byID {
		out = append(out, candidate)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func validateCompactSnapshot(snapshot CompactSnapshot) error {
	if snapshot.SchemaVersion != compactSnapshotVersion || snapshot.WorkspaceID == "" || snapshot.ThroughHash == "" || snapshot.ObjectRef == "" {
		return fmt.Errorf("%w: incomplete compact snapshot", evolution.ErrInvalidInput)
	}
	hash, err := compactChecksum(snapshot)
	if err != nil || hash != snapshot.StateSHA256 {
		return fmt.Errorf("%w: compact snapshot checksum mismatch", evolution.ErrInvalidInput)
	}
	return nil
}
func compactChecksum(snapshot CompactSnapshot) (string, error) {
	snapshot.StateSHA256 = ""
	body, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	return checksum(body), nil
}
func artifactRefs(envelopes []evolution.EventEnvelope) []string {
	var refs []string
	for _, envelope := range envelopes {
		refs = append(refs, envelope.Event.ArtifactRefs...)
	}
	return uniqueStrings(refs)
}
func acceptedHeads(records []candidateRecord, envelopes []evolution.EventEnvelope) []evolution.AcceptedHead {
	candidates := make(map[string]candidateRecord, len(records))
	for _, record := range records {
		candidates[record.ID] = record
	}
	var heads []evolution.AcceptedHead
	for _, envelope := range envelopes {
		if envelope.Event.Kind != evolution.EventKindCandidatePromoted {
			continue
		}
		candidate, ok := candidates[envelope.Event.Payload["candidate_id"]]
		if !ok {
			continue
		}
		heads = append(heads, evolution.AcceptedHead{
			WorkspaceID: candidate.WorkspaceID, Revision: envelope.Event.ParentRevision + 1,
			EventID: candidate.EventID, SnapshotRef: candidate.SnapshotRef, UpdatedAt: envelope.Event.CreatedAt,
		})
	}
	return heads
}
func uniqueHeads(heads []evolution.AcceptedHead) []evolution.AcceptedHead {
	seen := map[int64]bool{}
	out := make([]evolution.AcceptedHead, 0, len(heads))
	for _, head := range heads {
		if !seen[head.Revision] {
			seen[head.Revision], out = true, append(out, head)
		}
	}
	return out
}
func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" && !seen[value] {
			seen[value], out = true, append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func applyCandidateEvents(records []candidateRecord, envelopes []evolution.EventEnvelope) {
	for _, event := range envelopes {
		for i := range records {
			if records[i].ID != event.Event.Payload["candidate_id"] {
				continue
			}
			switch event.Event.Kind {
			case evolution.EventKindCandidatePromoted:
				records[i].Status = evolution.CandidateAccepted
				at := event.Event.CreatedAt
				records[i].AcceptedAt = &at
			case evolution.EventKindCandidateRejected:
				records[i].Status, records[i].RejectReason = evolution.CandidateRejected, event.Event.Payload["reason"]
				at := event.Event.CreatedAt
				records[i].RejectedAt = &at
			}
		}
	}
}

func verifyChainFrom(envelopes []evolution.EventEnvelope, baseHash string) error {
	if len(envelopes) == 0 {
		return nil
	}
	if err := verifyChain(envelopes); err == nil && baseHash == "" {
		return nil
	}
	byParent := map[string]evolution.EventEnvelope{}
	for _, envelope := range envelopes {
		if _, exists := byParent[envelope.Event.ParentHash]; exists {
			return fmt.Errorf("%w: ledger fork/replay", evolution.ErrInvalidInput)
		}
		byParent[envelope.Event.ParentHash] = envelope
	}
	current := baseHash
	for visited := 0; visited < len(envelopes); visited++ {
		next, ok := byParent[current]
		if !ok {
			return fmt.Errorf("%w: disconnected compacted ledger", evolution.ErrInvalidInput)
		}
		current = next.ContentHash
	}
	return nil
}
