package tombstone_test

import (
	"testing"

	"github.com/wenweih/go-sequent/tombstone"
)

func TestNewTombstone(t *testing.T) {
	e := tombstone.NewSubjectTombstoned("user-1", "erasure request")
	if e.EventType() != tombstone.EventType {
		t.Fatal(e.EventType())
	}
	if tombstone.Guide == "" {
		t.Fatal("guide empty")
	}
}
