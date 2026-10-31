package orchestrator

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"
	"time"

	"ai-control-plane/src/internal/statestore"
)

func TestExternalMCPClientNameLegacyRestartAndReplay(t *testing.T) {
	ctx, s, r, tree, _ := protectedWorktreeFixture(t, false)
	start := ExternalStart{RunID: "legacy-run", Goal: "Verify task.txt.", WorktreePath: tree, ApprovalResponse: "Approved."}
	at := r.now().UTC()
	spec := statestore.WorkflowSpec{RunID: start.RunID, Mode: "EXTERNAL", Goal: start.Goal, WorktreePath: tree, ContentIdentity: fmt.Sprintf("external-content-%x", sha256.Sum256([]byte(start.RunID))), StartedAt: at.Format(time.RFC3339Nano), HardDeadline: at.Add(time.Hour).Format(time.RFC3339Nano), Confirmation: statestore.HumanBudgetApproval{ApprovalID: "legacy-approval", InteractionID: "legacy-interaction", Prompt: "Start the legacy fixture?", Response: start.ApprovalResponse, Decision: "APPROVE"}}
	if err := s.CreateWorkflow(ctx, spec); err != nil {
		t.Fatal(err)
	}
	s, r = restart(t, ctx, s)
	v, err := r.ExternalStart(ctx, start)
	if err != nil || v.MCPClientName != "" || v.Instructions != "" || v.Step == nil {
		t.Fatal("legacy workflow could not resume", v, err)
	}
	before := externalDatabase(t, s)
	s, r = restart(t, ctx, s)
	replay, err := r.ExternalStart(ctx, start)
	if err != nil || !reflect.DeepEqual(v, replay) {
		t.Fatal("legacy replay changed view", replay, err)
	}
	start.MCPClientName = "new-route"
	if _, err := r.ExternalStart(ctx, start); err == nil {
		t.Fatal("legacy immutable input silently backfilled")
	}
	if !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("legacy replay or rejected name changed state")
	}
	input := externalInput(v)
	input.Decision = "DEVELOPER"
	next, err := r.ExternalAdvance(ctx, v.RunID, &input)
	if err != nil || !next.StepCompleted || next.MCPClientName != "" || next.Instructions != "" {
		t.Fatal("legacy unnamed workflow could not advance", next, err)
	}
}
