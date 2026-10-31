package orchestrator

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestContextFiveSyntheticCycles(t *testing.T) {
	firstBytes := 0
	for cycles := 1; cycles <= 5; cycles++ {
		w := contextHistoryFixture(cycles)
		v := ExternalView{RunID: "context-run", Status: "RUNNING", ContentIdentity: w.ContentIdentity, Step: &w.Current.CurrentInvocation}
		if err := externalContextFacts(&v, w); err != nil {
			t.Fatal(err)
		}
		if len(v.AcceptedResults) != 2 || len(v.Findings) != 1 || v.Findings[0].Definition.FindingID != "fnd-000001" || v.Findings[0].ResolutionEvidenceID != "" {
			t.Fatal("lost current results or unresolved stable finding", v)
		}
		for _, result := range v.AcceptedResults {
			if !strings.HasPrefix(result.Summary, fmt.Sprintf("candidate-%d-", cycles)) {
				t.Fatal("old candidate narrative loaded by default", result)
			}
		}
		v, err := observeExternalContext(v)
		if err != nil {
			t.Fatal(err)
		}
		if cycles == 1 {
			firstBytes = v.ContextHealth.ProjectionBytes
		}
		if v.ContextHealth.ProjectionBytes > firstBytes+128 {
			t.Fatal("history-driven projection growth", cycles, firstBytes, v.ContextHealth.ProjectionBytes)
		}
		page, err := externalHistory(w, ExternalHistoryRequest{Limit: MaxExternalHistoryEntries})
		if err != nil || len(page.Entries) > MaxExternalHistoryEntries || len(marshal(page)) > 128<<10 {
			t.Fatal("unbounded history page", err)
		}
		t.Logf("cycles=%d projection_bytes=%d accepted_results=%d findings=%d history_page_bytes=%d", cycles, v.ContextHealth.ProjectionBytes, len(v.AcceptedResults), len(v.Findings), len(marshal(page)))
	}
}

func TestContextHistoryRestartIsolationAndAuthority(t *testing.T) {
	ctx, s, r, tree, v := externalReviewFixture(t, "QA")
	before := externalDatabase(t, s)
	if _, err := r.ExternalInspectHistory(ctx, v.RunID, ExternalHistoryRequest{Limit: 4}); err == nil {
		t.Fatal("QA retrieved Developer/Lead history")
	}
	if !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("history rejection mutated state")
	}
	oldEvidence := v.EvidenceID
	negative := externalNegative(v)
	negative.Summary = "Old candidate failed the independent boundary check."
	var err error
	v, err = r.ExternalAdvance(ctx, v.RunID, &negative)
	if err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	lead := externalInput(v)
	lead.Decision, lead.Summary, lead.EscalationReason = "REMEDIATE", "Old Lead decision: repair the boundary.", "The accepted QA evidence confirms the boundary defect."
	v, err = r.ExternalAdvance(ctx, v.RunID, &lead)
	if err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	if _, err = r.ExternalInspectHistory(ctx, v.RunID, ExternalHistoryRequest{Limit: 1}); err == nil {
		t.Fatal("Developer retrieved history")
	}
	v = submitExternal(t, ctx, r, v, "", "completed")
	v = advanceExternal(t, ctx, r, v)
	if strings.Contains(string(marshal(v.AcceptedResults)), negative.Summary) || v.History != nil {
		t.Fatal("old narrative loaded by default")
	}
	before = externalDatabase(t, s)
	for _, req := range []ExternalHistoryRequest{{}, {Limit: -1}, {Limit: 5}, {Limit: 1, BeforeStep: -1}} {
		if _, err := r.ExternalInspectHistory(ctx, v.RunID, req); err == nil {
			t.Fatal("invalid history bound accepted", req)
		}
	}
	seen := map[int]bool{}
	foundQA, foundLead := false, false
	request := ExternalHistoryRequest{Limit: 2}
	var first ExternalView
	for {
		page, err := r.ExternalInspectHistory(ctx, v.RunID, request)
		if err != nil || page.History == nil || len(page.History.Entries) > request.Limit {
			t.Fatal("history request", err, page)
		}
		if request.BeforeStep == 0 {
			first = page
		}
		for _, e := range page.History.Entries {
			if seen[e.StepNo] {
				t.Fatal("history cursor repeated a step")
			}
			seen[e.StepNo] = true
			if e.Summary == negative.Summary {
				foundQA = e.Evidence == negative.Evidence && len(e.EvidenceRefs) == 1 && e.EvidenceRefs[0].EvidenceID == oldEvidence
			}
			if e.Summary == lead.Summary {
				foundLead = e.Decision == "REMEDIATE" && e.EscalationReason == lead.EscalationReason
			}
		}
		if page.History.NextBeforeStep == 0 {
			break
		}
		request.BeforeStep = page.History.NextBeforeStep
	}
	if !foundQA || !foundLead {
		t.Fatal("selective history lost old decision or evidence", foundQA, foundLead)
	}
	if !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("history reads changed authoritative state")
	}
	s, r = restart(t, ctx, s)
	restored, err := r.ExternalInspectHistory(ctx, v.RunID, ExternalHistoryRequest{Limit: 2})
	if err != nil || !reflect.DeepEqual(first, restored) || !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("history/health changed on restart", err)
	}
	if _, err := r.ExternalInspectHistory(ctx, "unknown-run", ExternalHistoryRequest{Limit: 1}); err == nil {
		t.Fatal("history escaped Run scope")
	}
	stale := externalInput(v)
	stale.Decision, stale.ActorRevision = "QA", stale.ActorRevision+1
	if _, err := r.ExternalAdvance(ctx, v.RunID, &stale); err == nil {
		t.Fatal("history read authorized stale mutation")
	}
	if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("changed after freeze\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ExternalInspectHistory(ctx, v.RunID, ExternalHistoryRequest{Limit: 1}); err == nil {
		t.Fatal("history bypassed frozen candidate verification")
	}
	if !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("rejected history/submission changed authoritative state")
	}
}

func TestContextHealthReproducibleAndUnknown(t *testing.T) {
	ctx, _, r, _, v := externalFixture(t)
	h := v.ContextHealth
	if h == nil || !h.FreshContextRequired || h.InputTokens != nil || h.ContextWindowTokens != nil || h.ContextGrowthBytes != nil || h.RepeatedContextBytes != nil || h.ToolResultVolumeBytes != nil || h.MaxPromptBytes != nil || h.ContextResetCount != nil {
		t.Fatal("fabricated external context telemetry", h)
	}
	v.ContextHealth = nil
	raw := marshal(v)
	if len(raw) != h.ProjectionBytes || fmt.Sprintf("%x", sha256.Sum256(raw)) != h.ProjectionSHA256 {
		t.Fatal("projection measurement cannot be reproduced", h)
	}
	view, err := r.ExternalInspect(ctx, v.RunID)
	if err != nil || !reflect.DeepEqual(h, view.ContextHealth) {
		t.Fatal("identical inspection changed measurement", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(marshal(h), &wire); err != nil {
		t.Fatal(err)
	}
	_, inputPresent := wire["input_tokens"]
	_, resetsPresent := wire["context_reset_count"]
	if !inputPresent || !resetsPresent || wire["input_tokens"] != nil || wire["context_reset_count"] != nil {
		t.Fatal("unknown metrics omitted or encoded as zero", wire, err)
	}
}

func TestContextHistoryWorstCasePage(t *testing.T) {
	w := contextHistoryFixture(5)
	for i := range w.Steps {
		out := externalSemanticOutput{Submission: ExternalSubmission{
			Summary: strings.Repeat("\x00", 1024), Evidence: strings.Repeat("\x00", 1024), EscalationReason: strings.Repeat("\x00", 1024),
		}}
		w.Steps[i].Output = marshal(out)
	}
	page, err := externalHistory(w, ExternalHistoryRequest{Limit: MaxExternalHistoryEntries})
	if err != nil || len(page.Entries) != MaxExternalHistoryEntries || len(marshal(page)) > 128<<10 {
		t.Fatal("worst-case escaped prose exceeded history page bound", err)
	}
	if page.Entries[0].Summary != strings.Repeat("\x00", 1024) {
		t.Fatal("history silently truncated accepted text")
	}
}

func TestContextResultProjectionRetainsWireIdentity(t *testing.T) {
	ctx, s, _, _, v := externalReviewFixture(t, "REVIEWER")
	w, err := s.GetWorkflow(ctx, v.RunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range w.Steps {
		if step.ResultID == "" {
			continue
		}
		accepted, err := s.WorkflowResult(ctx, v.RunID, step.InvocationID)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, fact := range v.AcceptedResults {
			if fact.ResultID == accepted.ResultID {
				found = fact.Role == accepted.Role && fact.Outcome == accepted.Outcome && fact.ContentIdentity == accepted.ContentIdentity
			}
		}
		if !found {
			t.Fatal("projection changed accepted wire identity or facts", accepted, v.AcceptedResults)
		}
	}
}
