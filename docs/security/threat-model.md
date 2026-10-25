# Threat model refresh — go-sequent (Phase 4)

Dated **2026-10-26** revision for production hardening. Aligns with locked Q1–Q10.

## Assets

| Asset | Notes |
|-------|-------|
| Event store (`events` / `commands`) | Source of truth; immutable append-only in normal ops |
| Unique keys / idempotency keys | Correctness under retries |
| Projection tables | Disposable; rebuildable |
| Outbox | At-least-once delivery buffer |
| Snapshots | Performance cache; versioned JSON (Q8) |
| PII in payloads | Encrypt via app `PayloadProtector`; GDPR = **tombstone only** (Q5) |

## Trust boundaries

1. **Client → app**: authentication happens in the host app; go-sequent sees `Meta.AuthContext` / `UserID` only as opaque claims — **never trust client-set UserID without verified authn**.
2. **App → CommandService**: AuthZ hooks (`security.Authorizer`) run as Filter/Middleware before handlers.
3. **CommandService → PostgreSQL**: single-writer primary; DB ACLs + backups (Q3 — **no** event signatures / hash chain in framework).
4. **SyncInTxn projectors**: same write txn as append (Q1 for ledgers).
5. **Outbox pollers**: at-least-once; handlers must be idempotent (Q6 polling).

## Threats & mitigations

| Threat | Mitigation |
|--------|------------|
| API replay / double submit | Tenant-scoped idempotency keys; defensive handlers |
| Privilege escalation via forged commands | AuthZ middleware; deny-by-default for sensitive types |
| Event store tampering | DB controls, WORM backups (Q3 — no in-framework signing) |
| Cross-tenant access | `tenant_id` on meta/rows; `tenancy.MatchAggregateTenant`; UniqueScope |
| PII in logs/traces | Redaction; PayloadProtector; never log secrets |
| Projector SQL injection | Parameterized persistor only |
| Workflow / outbox double-exec | Idempotent commands + outbox ack/dedupe |
| Snapshot poison | Versioned JSON; regenerate from events |
| GDPR erasure | Tombstone events + projector scrub (Q5); **not** crypto-shredding productization |
| Ruby Sequent dump import | **Never** (Q9) |

## Explicit non-goals (locked)

- Event hash chains / signatures (Q3)
- Crypto-shredding as product GDPR path (Q5)
- gRPC/HTTP server in this repo (Q7)
- Public open-source publication for now (Q10)

## Review checklist for apps

- [ ] Authorizer installed for all mutating command types
- [ ] Tenant always set on commands; partition keys tenant-scoped
- [ ] Money fields use `types.Money` (no float64)
- [ ] SyncInTxn for ledger balances; outbox only for async fans
- [ ] Backup restore drill completed (see ops/backup-restore)
- [ ] Upcasters registered for any renamed event payloads

Companion: [event-evolution.md](./event-evolution.md).
