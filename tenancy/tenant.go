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
