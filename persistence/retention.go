package persistence

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zzszmyf/rsi-state/evolution"
)

// RetentionClass documents the lifecycle contract for durable objects.
type RetentionClass string

const (
	RetentionAcceptedRevision  RetentionClass = "accepted_revision" // retained indefinitely
	RetentionRejectedCandidate RetentionClass = "rejected_candidate"
	RetentionTrajectory        RetentionClass = "trajectory"
	RetentionLog               RetentionClass = "log"
	RetentionRawVisual         RetentionClass = "raw_visual"
	RetentionAuditEvent        RetentionClass = "audit_event" // retained by a compact checkpoint
	RetentionUnknown           RetentionClass = "unknown"
)

// RetentionPolicy uses zero TTL to retain a class forever. DryRun reports all
// eligible objects without deleting anything.
type RetentionPolicy struct {
	RejectedCandidateTTL time.Duration
	TrajectoryTTL        time.Duration
	LogTTL               time.Duration
	RawVisualTTL         time.Duration
	DryRun               bool
}

// RetentionPolicyFromEnv reads duration values from the RSI_STATE_RETENTION_*
// environment variables. Empty values retain that class indefinitely.
func RetentionPolicyFromEnv() (RetentionPolicy, error) {
	var policy RetentionPolicy
	for _, setting := range []struct {
		name string
		set  func(time.Duration)
	}{
		{"RSI_STATE_RETENTION_REJECTED_CANDIDATE_TTL", func(v time.Duration) { policy.RejectedCandidateTTL = v }},
		{"RSI_STATE_RETENTION_TRAJECTORY_TTL", func(v time.Duration) { policy.TrajectoryTTL = v }},
		{"RSI_STATE_RETENTION_LOG_TTL", func(v time.Duration) { policy.LogTTL = v }},
		{"RSI_STATE_RETENTION_RAW_VISUAL_TTL", func(v time.Duration) { policy.RawVisualTTL = v }},
	} {
		if value := os.Getenv(setting.name); value != "" {
			duration, err := time.ParseDuration(value)
			if err != nil || duration < 0 {
				return RetentionPolicy{}, fmt.Errorf("%s must be a non-negative Go duration", setting.name)
			}
			setting.set(duration)
		}
	}
	policy.DryRun = strings.EqualFold(os.Getenv("RSI_STATE_RETENTION_DRY_RUN"), "true")
	return policy, nil
}

type GCObject struct {
	Key   string         `json:"key"`
	Class RetentionClass `json:"class"`
	Bytes int64          `json:"bytes"`
}
type RetentionObject struct {
	Key       string
	Class     RetentionClass
	CreatedAt time.Time
}
type GCReport struct {
	DryRun           bool       `json:"dry_run"`
	Eligible         []GCObject `json:"eligible"`
	Deleted          []GCObject `json:"deleted"`
	BytesReclaimable int64      `json:"bytes_reclaimable"`
	BytesReclaimed   int64      `json:"bytes_reclaimed"`
}

// ClassifyRetention assigns lifecycle classes from immutable key namespaces.
func ClassifyRetention(key string) RetentionClass {
	key = strings.ToLower(strings.TrimPrefix(key, "s3://"))
	switch {
	case strings.Contains(key, "/wal/"):
		return RetentionAuditEvent
	case strings.Contains(key, "/candidates/"):
		return RetentionRejectedCandidate
	case strings.Contains(key, "trajectory"):
		return RetentionTrajectory
	case strings.Contains(key, "/logs/") || strings.Contains(key, "/log/"):
		return RetentionLog
	case strings.Contains(key, "visual") || strings.Contains(key, "image") || strings.Contains(key, "frame"):
		return RetentionRawVisual
	case strings.Contains(key, "snapshot") || strings.Contains(key, "bundle") || strings.Contains(key, "manifest"):
		return RetentionAcceptedRevision
	default:
		return RetentionUnknown
	}
}

// GarbageCollect removes expired rejected candidate records only after proving
// they are not referenced by the accepted head, compact manifests, or retained
// audit records. Artifact namespaces are classified for lifecycle reporting;
// callers must supply immutable timestamps before deleting those opaque blobs.
func (s DurableStore) GarbageCollect(workspaceID string, policy RetentionPolicy, now time.Time) (GCReport, error) {
	report := GCReport{DryRun: policy.DryRun}
	if _, err := s.Heads.Get(context.Background(), workspaceID); err != nil {
		return report, err
	}

	if policy.RejectedCandidateTTL <= 0 {
		return report, nil
	}
	protected, err := s.retainedReferences(workspaceID)
	if err != nil {
		return report, err
	}
	keys, err := s.Objects.List(context.Background(), s.key("candidates")+"/")
	if err != nil {
		return report, err
	}
	deleter, canDelete := s.Objects.(ObjectDeleter)
	for _, key := range keys {
		var record candidateRecord
		if err := s.getJSON(key, &record); err != nil {
			return report, err
		}
		if record.WorkspaceID != workspaceID {
			continue
		}
		candidate, err := s.GetCandidate(record.ID)
		if err != nil {
			return report, err
		}
		if candidate.Status != evolution.CandidateRejected || candidate.RejectedAt == nil ||
			now.UTC().Sub(*candidate.RejectedAt) < policy.RejectedCandidateTTL ||
			protected[key] || protected[candidate.SnapshotRef] || protected[candidate.EvaluationRef] {
			continue
		}
		body, err := s.Objects.Get(context.Background(), key)
		if err != nil {
			return report, err
		}
		object := GCObject{Key: key, Class: RetentionRejectedCandidate, Bytes: int64(len(body))}
		report.Eligible = append(report.Eligible, object)
		report.BytesReclaimable += object.Bytes
		if !policy.DryRun {
			if !canDelete {
				return report, fmt.Errorf("object store does not support retention deletion")
			}
			if err := deleter.Delete(context.Background(), key); err != nil {
				return report, err
			}
			report.Deleted = append(report.Deleted, object)
			report.BytesReclaimed += object.Bytes
		}
	}
	return report, nil
}

// GarbageCollectObjects applies configured artifact TTLs to objects whose
// creation timestamps came from an immutable manifest or inventory. Referenced
// objects are always retained, including in dry-run mode.
func (s DurableStore) GarbageCollectObjects(workspaceID string, objects []RetentionObject, policy RetentionPolicy, now time.Time) (GCReport, error) {
	report := GCReport{DryRun: policy.DryRun}
	protected, err := s.retainedReferences(workspaceID)
	if err != nil {
		return report, err
	}
	deleter, canDelete := s.Objects.(ObjectDeleter)
	for _, candidate := range objects {
		class := candidate.Class
		if class == "" {
			class = ClassifyRetention(candidate.Key)
		}
		ttl := policy.ttl(class)
		if ttl <= 0 || candidate.CreatedAt.IsZero() || now.UTC().Sub(candidate.CreatedAt.UTC()) < ttl ||
			protected[candidate.Key] || protected[s.objectKey(candidate.Key)] {
			continue
		}
		body, err := s.Objects.Get(context.Background(), s.objectKey(candidate.Key))
		if err != nil {
			return report, err
		}
		object := GCObject{Key: candidate.Key, Class: class, Bytes: int64(len(body))}
		report.Eligible = append(report.Eligible, object)
		report.BytesReclaimable += object.Bytes
		if !policy.DryRun {
			if !canDelete {
				return report, fmt.Errorf("object store does not support retention deletion")
			}
			if err := deleter.Delete(context.Background(), s.objectKey(candidate.Key)); err != nil {
				return report, err
			}
			report.Deleted = append(report.Deleted, object)
			report.BytesReclaimed += object.Bytes
		}
	}
	return report, nil
}

func (p RetentionPolicy) ttl(class RetentionClass) time.Duration {
	switch class {
	case RetentionRejectedCandidate:
		return p.RejectedCandidateTTL
	case RetentionTrajectory:
		return p.TrajectoryTTL
	case RetentionLog:
		return p.LogTTL
	case RetentionRawVisual:
		return p.RawVisualTTL
	default:
		return 0
	}
}

func (s DurableStore) retainedReferences(workspace string) (map[string]bool, error) {
	refs := map[string]bool{}
	head, err := s.Heads.Get(context.Background(), workspace)
	if err != nil {
		return nil, err
	}
	refs[head.SnapshotRef], refs[s.objectKey(head.SnapshotRef)] = true, true
	snapshot, err := s.latestCompactSnapshot(workspace, nil)
	if err != nil {
		return nil, err
	}
	if snapshot != nil {
		refs[snapshot.ObjectRef], refs[s.objectKey(snapshot.ObjectRef)] = true, true
		for _, ref := range snapshot.AuditArtifactRefs {
			refs[ref], refs[s.objectKey(ref)] = true, true
		}
		for _, accepted := range snapshot.AcceptedHeads {
			refs[accepted.SnapshotRef], refs[s.objectKey(accepted.SnapshotRef)] = true, true
		}
	}
	events, err := s.ListEvents(workspace)
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		for _, ref := range event.ArtifactRefs {
			refs[ref], refs[s.objectKey(ref)] = true, true
		}
	}
	return refs, nil
}

// Metrics gives pull-based operational measurements suitable for an OTel
// callback or a lightweight status endpoint.
type Metrics struct {
	ReplayDepth    int           `json:"replay_depth"`
	SnapshotAge    time.Duration `json:"snapshot_age"`
	StorageBytes   int64         `json:"storage_bytes"`
	BytesReclaimed int64         `json:"bytes_reclaimed"`
}

func (s DurableStore) Metrics(workspaceID string, now time.Time, bytesReclaimed int64) (Metrics, error) {
	envelopes, snapshot, err := s.recoveryEnvelopes(workspaceID)
	if err != nil {
		return Metrics{}, err
	}
	keys, err := s.Objects.List(context.Background(), s.key(""))
	if err != nil {
		return Metrics{}, err
	}
	metrics := Metrics{ReplayDepth: len(envelopes), BytesReclaimed: bytesReclaimed}
	if snapshot != nil {
		metrics.SnapshotAge = now.UTC().Sub(snapshot.CreatedAt)
	}
	for _, key := range keys {
		body, err := s.Objects.Get(context.Background(), key)
		if err != nil {
			return Metrics{}, err
		}
		metrics.StorageBytes += int64(len(body))
	}
	return metrics, nil
}
