# Benchmarks

Package `bench/` exercises in-memory (always) and Postgres (optional) append throughput and optimistic concurrency conflict storms.

## Run

```bash
# Memory benches (default CI-friendly)
go test ./bench/ -bench=. -benchmem

# Filter
go test ./bench/ -bench=BenchmarkMemoryOptimisticConflicts -benchmem -benchtime=2s

# Postgres (requires migrated DB)
export GO_SEQUENT_PG_DSN='postgres://user:pass@localhost:5432/go_sequent?sslmode=disable'
go test ./bench/ -bench=BenchmarkPostgres -benchmem
```

## What is measured

| Benchmark | Intent |
|-----------|--------|
| `BenchmarkMemoryAppendThroughput` | Sequential appends, new stream each op |
| `BenchmarkMemoryAppendSameStream` | Hot aggregate sequential append |
| `BenchmarkMemoryOptimisticConflicts` | Parallel stale `ExpectedVersion` → conflicts |
| `BenchmarkMemoryConflictStormWithProgress` | Contended correct advances + conflict count |
| `BenchmarkPostgresAppendThroughput` | PG sequential append (skipped without DSN) |
| `BenchmarkPostgresOptimisticConflicts` | PG conflict storm (skipped without DSN) |

Unit tests remain `go test ./...` (benches are opt-in via `-bench`).
