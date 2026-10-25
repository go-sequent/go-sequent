package tenancy_test

import (
	"testing"

	"github.com/go-sequent/go-sequent/tenancy"
	"github.com/google/uuid"
)

func TestUniqueScopeAndPartition(t *testing.T) {
	tid := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	if got := tenancy.UniqueScope(tid, "email"); got != "tenant:11111111-1111-1111-1111-111111111111:email" {
		t.Fatalf("%s", got)
	}
	if got := tenancy.PartitionKeyHashed(tid, "a"); got != tid.String()+":a" {
		t.Fatalf("%s", got)
	}
	if err := tenancy.MatchAggregateTenant(tid, tid.String()); err != nil {
		t.Fatal(err)
	}
	if err := tenancy.MatchAggregateTenant(tid, "other"); err == nil {
		t.Fatal("expected mismatch")
	}
}
