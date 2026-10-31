package statestore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestWorkflowMCPClientNamePersistenceAndAuthority(t *testing.T) {
	s := testStore(t)
	spec := workflowSpec()
	spec.Mode, spec.WorktreePath, spec.NormalTokens = "EXTERNAL", "/fixture/worktree", 0
	before := mutationDatabaseSnapshot(t, s)
	for _, name := range []string{"two names", "control\x00name", "line\nname", strings.Repeat("x", 257), string([]byte{0xff})} {
		spec.MCPClientName = name
		if err := s.CreateWorkflow(accountingContext, spec); err == nil {
			t.Fatalf("invalid name accepted: %q", name)
		}
	}
	spec.MCPClientName = "Team_Control.日本"
	if err := s.CreateWorkflow(context.Background(), spec); !errors.Is(err, ErrMutationAuthority) {
		t.Fatal("routing name supplied mutation authority", err)
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("rejected creation changed state")
	}
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	w, err := s.GetWorkflow(accountingContext, spec.RunID)
	if err != nil || w.Spec.MCPClientName != spec.MCPClientName {
		t.Fatal("name not persisted", w.Spec, err)
	}
	before = mutationDatabaseSnapshot(t, s)
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal("exact replay failed", err)
	}
	spec.MCPClientName = "different"
	if err := s.CreateWorkflow(accountingContext, spec); err == nil {
		t.Fatal("changed immutable name accepted")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("replay changed state")
	}
}

func TestWorkflowMCPClientNameLegacyEncoding(t *testing.T) {
	// This is the field layout before routing metadata. Its exact JSON bytes are
	// part of immutable creation replay, including for deterministic fake Runs.
	type legacySpec struct {
		RunID, Goal, ContentIdentity, StartedAt, HardDeadline string
		Mode, WorktreePath                                    string
		NormalTokens                                          int64
		Confirmation                                          HumanBudgetApproval
	}
	spec := workflowSpec()
	old, err := json.Marshal(legacySpec{spec.RunID, spec.Goal, spec.ContentIdentity, spec.StartedAt, spec.HardDeadline, spec.Mode, spec.WorktreePath, spec.NormalTokens, spec.Confirmation})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(spec)
	if err != nil || string(raw) != string(old) {
		t.Fatalf("legacy immutable encoding changed: %s != %s (%v)", raw, old, err)
	}
	var restored WorkflowSpec
	if err := json.Unmarshal(old, &restored); err != nil || !reflect.DeepEqual(restored, spec) {
		t.Fatal("legacy decoding changed", restored, err)
	}
	s := testStore(t)
	if err := s.CreateWorkflow(accountingContext, restored); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal("legacy replay failed", err)
	}
}
