# Threat model refresh — go-sequent (Phase 4)

Dated **2026-10-26** revision for production hardening. Aligns with the locked decision.

## Assets

| Asset | Notes |
|-------|-------|
| Event store (`events` / `commands`) | Source of truth; immutable append-only in normal ops |
| Unique keys / idempotency keys | Correctness under retries |
| Projection tables | Disposable; rebuildable |
| Outbox | At-least-once delivery buffer |
| Snapshots | Performance cache; versioned JSON |
| PII in payloads | Encrypt via app `PayloadProtector`; GDPR = **tombstone only** |

## Trust boundaries

1. **Client → app**: authentication happens in the host app; go-sequent sees `Meta.AuthContext` / `UserID` only as opaque claims — **never trust client-set UserID without verified authn**.
2. **App → CommandService**: AuthZ hooks (`security.Authorizer`) run as Filter/Middleware before handlers.
3. **CommandService → PostgreSQL**: single-writer primary; hash chain on events + DB ACLs/backups (**no** event signatures).
4. **SyncInTxn projectors**: same write txn as append (for ledgers).
5. **Outbox pollers**: at-least-once; handlers must be idempotent (polling).

## Threats & mitigations

| Threat | Mitigation |
|--------|------------|
| API replay / double submit | Tenant-scoped idempotency keys; defensive handlers |
| Privilege escalation via forged commands | AuthZ middleware; deny-by-default for sensitive types |
| Event store tampering | Hash chain verify + DB controls / WORM (no signatures) |
| Cross-tenant access | `tenant_id` on meta/rows; `tenancy.MatchAggregateTenant`; UniqueScope |
| PII in logs/traces | Redaction; PayloadProtector; never log secrets |
| Projector SQL injection | Parameterized persistor only |
| Workflow / outbox double-exec | Idempotent commands + outbox ack/dedupe |
| Snapshot poison | Versioned JSON; regenerate from events |
| GDPR erasure | Tombstone events + projector scrub ; **not** crypto-shredding productization |
| Ruby Sequent dump import | **Never** |

## Explicit non-goals (locked)

- Event signatures ; hash chain is in-framework
- Crypto-shredding as product GDPR path
- gRPC/HTTP server in this repo
- Public open-source publication for now

## Review checklist for apps

- [ ] Authorizer installed for all mutating command types
- [ ] Tenant always set on commands; partition keys tenant-scoped
- [ ] Money fields use `types.Money` (no float64)
- [ ] SyncInTxn for ledger balances; outbox only for async fans
- [ ] Backup restore drill completed (see ops/backup-restore)
- [ ] Upcasters registered for any renamed event payloads

Companion: [event-evolution.md](./event-evolution.md).
