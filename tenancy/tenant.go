// Package tenancy provides shared-schema + tenant_id helpers (Q4).
package tenancy

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/wenweih/go-sequent/command"
)

type ctxKey struct{}

// WithTenant attaches a tenant id to context.
func WithTenant(ctx context.Context, tenantID uuid.UUID) context.Context {
	return context.WithValue(ctx, ctxKey{}, tenantID)
}

// FromContext returns the tenant id if set.
func FromContext(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(ctxKey{}).(uuid.UUID)
	return id, ok && id != uuid.Nil
}

// Require returns the tenant id or an error.
func Require(ctx context.Context) (uuid.UUID, error) {
	id, ok := FromContext(ctx)
	if !ok {
		return uuid.Nil, fmt.Errorf("tenancy: tenant required in context")
	}
	return id, nil
}

// Middleware ensures command Meta.TenantID is set from context when missing,
// and rejects mismatched tenant ids (defense in depth).
func Middleware(ctx context.Context, cmd command.Command, next command.HandlerFunc) error {
	meta := command.EnsureMeta(cmd.Meta())
	if tid, ok := FromContext(ctx); ok {
		if meta.TenantID == uuid.Nil {
			meta.TenantID = tid
		} else if meta.TenantID != tid {
			return fmt.Errorf("tenancy: command tenant %s != context tenant %s", meta.TenantID, tid)
		}
	}
	return next(ctx, cmd)
}

// PartitionKey returns a default partition key for a tenant (empty if nil).
func PartitionKey(tenantID uuid.UUID) string {
	if tenantID == uuid.Nil {
		return ""
	}
	return tenantID.String()
}

// PartitionKeyHashed returns tenantID + optional suffix for finer partitioning
// within the shared schema (still one logical tenant).
func PartitionKeyHashed(tenantID uuid.UUID, shard string) string {
	if tenantID == uuid.Nil {
		return shard
	}
	if shard == "" {
		return tenantID.String()
	}
	return tenantID.String() + ":" + shard
}

// UniqueScope prefixes a unique-key scope with tenant (recommended for Q4).
// Example: UniqueScope(tid, "email") => "tenant:{uuid}:email".
func UniqueScope(tenantID uuid.UUID, scope string) string {
	if tenantID == uuid.Nil {
		return scope
	}
	return "tenant:" + tenantID.String() + ":" + scope
}

// MatchAggregateTenant returns an error if the aggregate's partition/tenant
// does not match the command tenant (defense in depth on load).
func MatchAggregateTenant(cmdTenant uuid.UUID, aggregatePartitionKey string) error {
	if cmdTenant == uuid.Nil {
		return nil
	}
	want := PartitionKey(cmdTenant)
	if aggregatePartitionKey != "" && aggregatePartitionKey != want &&
		!hasTenantPrefix(aggregatePartitionKey, want) {
		return fmt.Errorf("tenancy: aggregate partition %q does not match tenant %s", aggregatePartitionKey, cmdTenant)
	}
	return nil
}

func hasTenantPrefix(partition, tenantKey string) bool {
	if partition == tenantKey {
		return true
	}
	return len(partition) > len(tenantKey) && partition[:len(tenantKey)] == tenantKey && partition[len(tenantKey)] == ':'
}
