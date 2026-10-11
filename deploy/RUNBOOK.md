# Production runbook

## Deploy

Copy `production.env.example` into the deployment's secret/config system; do
not commit real certificates, role maps, or key registries. The server refuses
production startup unless mTLS server/client-CA files, a non-empty identity
role map, and `OTEL_EXPORTER_OTLP_ENDPOINT` are present. Terminate neither TLS
nor client-certificate validation before this process. The supplied IAM policy
is a boundary: scope bucket, prefix, region, account, and table before use.

Map distinct workload identities to the minimum role: Evolver creates
proposals, Verifier appends evaluation evidence, Selector promotes/rejects/GC,
and Reader only reads. Rotate client CAs and signing public-key registry files
through the secret manager; retain old public keys until all signed history is
past retention.

## Probes and observability

`GET /healthz` proves the process is live. `GET /readyz` additionally checks
the configured local cache; durable-store credentials/connectivity must be
checked by the production storage adapter before traffic is admitted. Alert on
failed readiness, `rsi_state_integrity_failures_total`,
`rsi_state_cas_conflicts_total`, WAL-write errors, restore latency, denied
authorization decisions, and compaction failures. Correlate traces by the
`rsi-state.http`, `rsi-state.wal.write`, `rsi-state.restore`, and
`rsi-state.head.compare_and_swap` spans.

## Recovery

1. Stop writers or remove their Selector/Evolver role mapping.
2. Inspect immutable WAL and the newest valid compaction snapshot; never
   overwrite an object while investigating.
3. Run `rsi-state verify --event EVENT.json --trusted-keys registry.json` for
   each suspect envelope. Verification requires a trusted public key whose
   registered role equals the signed event role.
4. Restore only into a new, empty directory:
   `rsi-state restore --bundle candidate.bundle --destination /safe/new-tree`.
   Inspect the Git revision and run the benchmark before any promotion.
5. `rsi-state diff --from ACCEPTED_REVISION --to CANDIDATE_REVISION
   --workspace /safe/new-tree` requires both revisions to exist in that local
   Git repository; it does not fetch missing objects.
6. Re-enable roles only after durable hashes, event chain, and accepted-head
   references verify.
