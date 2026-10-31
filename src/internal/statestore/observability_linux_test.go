package statestore

import (
	"reflect"
	"testing"
)

func TestOBSWorkflowAncestryAndRejectedTransitions(t *testing.T) {
	s := testStore(t)
	spec := workflowSpec()
	spec.Mode, spec.WorktreePath, spec.NormalTokens = "EXTERNAL", "/disposable-fixture", 0
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	w, err := s.GetWorkflow(accountingContext, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	before := mutationDatabaseSnapshot(t, s)
	stale := w.Owner
	stale.ActorRevision++
	if _, err := s.BeginWorkflowStep(accountingContext, spec.RunID, "", stale, semanticTimestamp); err == nil {
		t.Fatal("stale start accepted")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("rejected start changed state or audit")
	}
	step, err := s.BeginWorkflowStep(accountingContext, spec.RunID, "", w.Owner, semanticTimestamp)
	if err != nil {
		t.Fatal(err)
	}
	before = mutationDatabaseSnapshot(t, s)
	for _, run := range []string{spec.RunID, "foreign-run"} {
		if err := s.FinishWorkflowStep(accountingContext, run, step.InvocationID, semanticTimestamp); err == nil {
			t.Fatal("unresolved or foreign invocation finish accepted", run)
		}
		if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
			t.Fatal("rejected finish changed state or audit", run)
		}
	}
	events, err := s.ListEvents(accountingContext, spec.RunID, 0, MaxJournalPageSize)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range events {
		if e.EventType == "workflow_invocation_finished" {
			t.Fatal("rejected finish appended an event")
		}
		if e.EventType != "workflow_invocation_started" {
			continue
		}
		count++
		if e.Cycle == nil || e.ExecutionID == nil || e.AttemptID == nil || e.InvocationID == nil || *e.Cycle != step.Cycle || *e.ExecutionID != step.ExecutionID || *e.AttemptID != step.AttemptID || *e.InvocationID != step.InvocationID {
			t.Fatalf("start event lacks validated invocation ancestry: %+v", e)
		}
	}
	if count != 1 {
		t.Fatal("start event count", count)
	}
}
