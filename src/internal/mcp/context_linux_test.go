package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"ai-control-plane/src/internal/orchestrator"
)

func TestMCPContextHistoryAndHealth(t *testing.T) {
	c, _, tree := mcpWorktreeFixture(t)
	v, bad := c.tool(t, "workflow_start", map[string]any{"run_id": "context-history", "goal": "Change task.txt and verify it independently.", "worktree_path": tree, "mcp_client_name": "fixture-control", "approval_response": "I approve this task."})
	if bad || v.ContextHealth == nil || v.History != nil || v.ContextHealth.InputTokens != nil {
		t.Fatal("start omitted measured context or invented tokens", v)
	}
	inspect := func(history any) (orchestrator.ExternalView, bool) {
		return c.tool(t, "workflow_inspect", map[string]any{"run_id": v.RunID, "history": history})
	}
	for _, args := range []any{map[string]any{}, map[string]any{"limit": 0}, map[string]any{"limit": 5}, map[string]any{"limit": 1, "before_step": -1}, map[string]any{"limit": 1, "unknown": true}} {
		if _, bad := inspect(args); !bad {
			t.Fatal("invalid retrieval bound accepted", args)
		}
	}
	page, bad := inspect(map[string]any{"limit": 1})
	if bad || page.History == nil || len(page.History.Entries) != 0 {
		t.Fatal("empty Lead history", page)
	}
	for _, role := range []string{"DEVELOPER", "QA", "REVIEWER"} {
		in := orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Decision: role, Summary: "Verify exact task.txt bytes without changing unrelated files."}
		v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": in})
		if bad {
			t.Fatal("Lead submission failed")
		}
		v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID})
		if bad || v.Step.Role != role || !v.ContextHealth.FreshContextRequired {
			t.Fatal("dispatch lost role context", v)
		}
		if _, bad := inspect(map[string]any{"limit": 1}); !bad {
			t.Fatal("Worker received historical context", role)
		}
		if role == "QA" && len(v.AcceptedResults) != 0 {
			t.Fatal("QA received Developer claims")
		}
		if role == "DEVELOPER" {
			if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		in = orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision,
			Outcome: map[string]string{"DEVELOPER": "completed", "QA": "passed", "REVIEWER": "approved"}[role], Summary: role + " verified exact task.txt bytes.", Evidence: "Observed task.txt bytes: after followed by newline."}
		if role == "QA" {
			in.Checks = []orchestrator.ExternalCheck{{CheckID: "exact-bytes", Required: true, Outcome: "passed", Summary: "Independently read task.txt."}}
		}
		evidenceID := v.EvidenceID
		v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": in})
		if bad || v.ContextHealth.FreshContextRequired {
			t.Fatal("accepted result lost phase boundary", v)
		}
		v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID})
		if bad || v.Step.Role != "LEAD" {
			t.Fatal("Lead not issued")
		}
		page, bad = inspect(map[string]any{"limit": 1})
		if bad || len(page.History.Entries) != 1 || page.History.Entries[0].Evidence != in.Evidence || page.History.Entries[0].EvidenceRefs[0].EvidenceID != evidenceID {
			t.Fatal("history lost exact accepted evidence", page)
		}
		if page.ContextHealth.HistoryEntries != 1 || page.History.NextBeforeStep == 0 {
			t.Fatal("missing history observations or cursor", page)
		}
		raw, err := json.Marshal(page.History)
		if err != nil || strings.Contains(string(raw), "Worktree") {
			t.Fatal("history included source snapshots")
		}
	}
	defaultView, bad := c.tool(t, "workflow_inspect", map[string]any{"run_id": v.RunID})
	if bad || defaultView.History != nil || defaultView.ContextHealth.HistoryEntries != 0 || len(defaultView.AcceptedResults) != 3 {
		t.Fatal("opt-in history contaminated default view", defaultView)
	}
}
