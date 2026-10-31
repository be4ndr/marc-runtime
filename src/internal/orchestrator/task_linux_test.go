package orchestrator

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestExternalTaskGuidanceAndRoleIsolation(t *testing.T) {
	ctx, s, r, tree, v := externalFixture(t)
	developerClaim := "Developer-only semantic claim: exact bytes verified."
	for _, role := range []string{"DEVELOPER", "QA", "REVIEWER"} {
		if v.Step.Role != "LEAD" || !strings.Contains(v.TaskInstructions, "verifiable acceptance criteria") {
			t.Fatal("Lead lacks task formulation guidance", v)
		}
		in := externalInput(v)
		in.Decision = role
		in.Summary = "Goal: Verify task.txt. Context: Current worktree. Acceptance: task.txt contains exactly after followed by a newline; unrelated files remain unchanged."
		task := in.Summary
		var err error
		v, err = r.ExternalAdvance(ctx, v.RunID, &in)
		if err != nil {
			t.Fatal(err)
		}
		if v.TaskInstructions != "" {
			t.Fatal("completed Lead still has an actionable task")
		}
		v = advanceExternal(t, ctx, r, v)
		if v.Step.Role != role || v.LeadIntent != task {
			t.Fatal("accepted task intent changed", v)
		}
		for _, required := range []string{"Goal:", "Context:", "Acceptance:", "Never forward prompts or transcripts", "returned fences", "Workers cannot allocate resources or declare workflow success"} {
			if !strings.Contains(v.TaskInstructions, required) {
				t.Fatalf("%s task missing %q", role, required)
			}
		}
		before := externalDatabase(t, s)
		s, r = restart(t, ctx, s)
		restored, err := r.ExternalInspect(ctx, v.RunID)
		if err != nil || !reflect.DeepEqual(v, restored) {
			t.Fatal("restart changed task projection", err, restored)
		}
		if !reflect.DeepEqual(before, externalDatabase(t, s)) {
			t.Fatal("task inspection mutated authoritative state")
		}
		if role == "QA" && (len(v.AcceptedResults) != 0 || strings.Contains(string(marshal(v)), developerClaim)) {
			t.Fatal("QA task leaked Developer claims")
		}
		if role == "REVIEWER" {
			if len(v.AcceptedResults) != 2 || v.AcceptedResults[0].Summary != developerClaim || v.AcceptedResults[1].Role != "QA" {
				t.Fatal("Reviewer lost implementation or QA evidence", v.AcceptedResults)
			}
		}
		if role == "DEVELOPER" {
			if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600); err != nil {
				t.Fatal(err)
			}
			in = externalInput(v)
			in.Summary, in.Evidence, in.Outcome = developerClaim, "Observed task.txt bytes: after followed by newline.", "completed"
			v, err = r.ExternalAdvance(ctx, v.RunID, &in)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			v = submitExternal(t, ctx, r, v, "", map[string]string{"QA": "passed", "REVIEWER": "approved"}[role])
		}
		if v.TaskInstructions != "" {
			t.Fatal("completed Worker still has an actionable task")
		}
		v = advanceExternal(t, ctx, r, v)
	}
	v = submitExternal(t, ctx, r, v, "APPROVE", "")
	v, err := r.ExternalComplete(ctx, v.RunID)
	if err != nil || v.Status != "READY_FOR_HUMAN_MERGE" || v.TaskInstructions != "" {
		t.Fatal("terminal task projection", err, v)
	}
	if strings.Contains(string(marshal(v)), "task_instructions") {
		t.Fatal("terminal response retained task instructions")
	}
}

func TestExternalBlockedTaskGuidance(t *testing.T) {
	ctx, _, r, _, v := externalFixture(t)
	v = submitExternal(t, ctx, r, v, "BLOCK", "")
	v = advanceExternal(t, ctx, r, v)
	if v.Status != "BLOCKED" || v.TaskInstructions != "" {
		t.Fatal("blocked workflow retained actionable guidance", v)
	}
}

func TestExternalRemediationTaskIntent(t *testing.T) {
	ctx, _, r, _, v := externalReviewFixture(t, "QA")
	in := externalNegative(v)
	var err error
	v, err = r.ExternalAdvance(ctx, v.RunID, &in)
	if err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	in = externalInput(v)
	in.Decision = "REMEDIATE"
	in.Summary = "Repair the confirmed blocker."
	in.EscalationReason = "Goal: Repair task.txt. Context: QA confirmed fnd-000001 remains open. Acceptance: exact expected bytes and the failing boundary check pass. Prior implementation missed that boundary."
	in.FindingIDs = []string{"fnd-000001"}
	v, err = r.ExternalAdvance(ctx, v.RunID, &in)
	if err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	if v.Step.Role != "DEVELOPER" || v.Step.Purpose != "REMEDIATE" || v.LeadIntent != in.EscalationReason || !reflect.DeepEqual(v.FindingIDs, in.FindingIDs) {
		t.Fatal("remediation changed accepted task or targets", v)
	}
	if !strings.Contains(v.TaskInstructions, "remediation addresses those open findings") {
		t.Fatal("remediation lacks bounded Developer guidance")
	}
}
