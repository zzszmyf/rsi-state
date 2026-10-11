# rsi-state

`rsi-state` is a Go control plane for durable, auditable Agent RSI
(recursive self-improvement) workspace evolution.

It keeps **accepted workspace state** separate from untrusted candidate
changes. A candidate can only advance the accepted head after its evidence
artifacts have been recorded and a selector performs a compare-and-swap (CAS)
promotion.

## Why it exists

An Evolver can modify an Agent workspace after analyzing trajectories and
feedback. Without durable state, those edits are hard to reproduce, compare,
audit, or roll back.

```text
trajectory + rubric feedback
  -> evolver proposal
  -> candidate workspace snapshot + evaluation
  -> selector promotion through CAS
  -> new accepted workspace revision
```

This service owns the state transition, not the model call or benchmark:

```text
Evolver / Verifier / Selector
              |
              v
          rsi-state
    events, candidates,
    accepted-head revisions
              |
              v
   S3-compatible immutable artifacts
   (planned production backend)
```

## Current MVP

- Creates a workspace with accepted revision `0`.
- Appends immutable audit events with defensive payload copies.
- Creates candidates that reference an event, workspace snapshot, and
  evaluation artifact.
- Atomically promotes a pending candidate only when the expected accepted
  revision matches its base revision.
- Exposes a standard-library JSON HTTP API.
- Includes TDD coverage for promotion, stale writes, stale candidates,
  immutable event payloads, and the HTTP workflow.
- Provides `persistence.DurableStore`, which writes hash-bound immutable WAL
  records and verifies the proposal, snapshot, and evaluation before an
  accepted-head CAS. It reconstructs state after restart from S3 and DynamoDB
  and maps conditional failures to `evolution.ErrRevisionConflict`.

The default local server uses process-local memory. Production callers compose
`persistence.DurableStore{Objects: persistence.S3ObjectStore{...}, Heads:
persistence.DynamoHeads{...}}`; its `StateStore` interface is the same one
used by the in-memory test backend.

## Run

```bash
go test ./...
go run ./cmd/rsi-state
```

The server listens on `:8080` by default. Set `RSI_STATE_ADDR` to override it.

## API walkthrough

Create a workspace:

```bash
curl -sS -X POST http://localhost:8080/v1/workspaces \
  -H 'Content-Type: application/json' \
  -d '{"id":"arc-agent"}'
```

Append an immutable proposal event:

```bash
curl -sS -X POST http://localhost:8080/v1/workspaces/arc-agent/events \
  -H 'Content-Type: application/json' \
  -d '{
    "parent_revision": 0,
    "kind": "candidate.proposed",
    "actor": "evolver",
    "payload": {"changed_path": "skills/frame-diff/SKILL.md"}
  }'
```

Create a candidate using the returned event ID:

```bash
curl -sS -X POST http://localhost:8080/v1/workspaces/arc-agent/candidates \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "candidate-1",
    "base_revision": 0,
    "event_id": "evt-arc-agent-000000000001",
    "snapshot_ref": "s3://rsi-state/bundles/candidate-1.bundle",
    "evaluation_ref": "s3://rsi-state/evaluations/candidate-1.json"
  }'
```

Promote only if revision `0` is still current:

```bash
curl -sS -X POST \
  'http://localhost:8080/v1/workspaces/arc-agent/candidates/candidate-1/promote?expected_revision=0'
```

The accepted head advances to revision `1`. If another selector promoted a
candidate first, this request returns `409 Conflict`; callers must reload the
head and re-evaluate their candidate against the new base.

## State invariants

1. Events are append-only audit records.
2. Candidate artifacts must reference an event, snapshot, and evaluation.
3. A candidate can only be promoted from `pending`.
4. `expected_revision` and `candidate.base_revision` must both match the
   accepted head before promotion.
5. A failed CAS never changes a candidate's status or accepted head.

## Production storage design

Each worker uses a local Git worktree on NVMe/EBS. Do not put active Git
worktrees on an S3 mount or shared NFS filesystem. Persist durable artifacts
separately:

```text
s3://<bucket>/rsi/<workspace-id>/
├── events/<sequence>-<uuid>.json      # immutable WAL event
├── bundles/<sha>.bundle               # restorable Git bundle/snapshot
├── diffs/<sha>.patch                  # human-readable candidate change
├── evaluations/<sha>.json             # rubric and holdout evidence
├── artifacts/<sha>/...                # trajectories and logs
└── state/accepted-head.json           # CAS-protected head pointer
```

The production promotion sequence is:

1. Create and validate a candidate in an isolated local worktree.
2. Upload the immutable bundle, patch, evaluation, and event.
3. Verify object hashes and artifact references.
4. CAS-update the accepted head with DynamoDB's conditional write.
5. Treat the revision as accepted only after the CAS succeeds. Reads re-verify
   the referenced WAL event, snapshot, and evaluation hashes before returning
   a nonzero accepted head.

## Implemented operations

- `GET /v1/workspaces/{workspace}` reads an accepted head.
- `GET /v1/workspaces/{workspace}/events` reads immutable audit events.
- `GET /v1/workspaces/{workspace}/candidates/{candidate}` inspects a candidate.
- `POST .../reject` rejects a pending candidate with an auditable reason.
- `POST /v1/gc?before=<RFC3339>` garbage-collects expired rejected candidates.
- `rsi-state inspect`, `diff`, `restore`, and `verify` provide operator CLI
  entry points. `restore` clones a local Git bundle into a destination.
- `persistence.SnapshotStore` writes versioned, SHA-256-bound bundle manifests.
  `persistence.WorkspaceCache` hydrates them only after verification into
  isolated `workspaces/<workspace>/revisions/<revision>-<hash>` paths, rejects
  stale or unsupported manifests, and bounds local storage by entry count.
  `rsi-state cache-health --root <path>` inspects the cache and
  `cache-repair` removes corrupt entries. Set `RSI_STATE_CACHE_ROOT` to mount
  `GET /v1/cache/health` and `POST /v1/cache/repair` on the server.

`evolution.SealEvent` produces an Ed25519-signed event envelope containing a
SHA-256 content hash. `evolution.Authorize` supplies bearer-token roles:
Evolver can propose state, Verifier can append evidence, Selector can promote,
reject, or run retention cleanup, and all authenticated roles can read.
`evolution.Instrument` exposes a standard OpenTelemetry request counter
middleware.

The `persistence` package provides production AWS adapters: content-addressed
S3 artifact upload/download and a DynamoDB conditional accepted-head update.
They use small injected AWS client interfaces and offline tests; deployers
must supply AWS credentials, bucket/table configuration, retention policies,
and server middleware wiring.
