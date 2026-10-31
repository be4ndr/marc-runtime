package orchestrator

import (
	"encoding/json"
	"testing"
)

// This test also runs unchanged against the before snapshot. Live admission
// retains its three-candidate bound; the five-cycle test is synthetic only.
func TestContextThreeAdmittedCycles(t *testing.T) {
	ctx, _, r, _, v := externalReviewFixture(t, "QA")
	for cycle := 1; cycle <= 3; cycle++ {
		in := externalNegative(v)
		if cycle > 1 {
			in.FindingDefinitions = nil
		}
		var err error
		v, err = r.ExternalAdvance(ctx, v.RunID, &in)
		if err != nil {
			t.Fatal(err)
		}
		v = advanceExternal(t, ctx, r, v)
		if len(v.AcceptedResults) != 2 || len(v.Findings) != 1 {
			t.Fatal("history changed default role context cardinality", v)
		}
		raw := marshal(v)
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		delete(fields, "context_health")
		projection, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("cycle=%d projection_bytes=%d response_bytes=%d accepted_results=%d findings=%d", cycle, len(projection), len(raw), len(v.AcceptedResults), len(v.Findings))
		if cycle == 3 {
			if v.NextDecision != "BLOCK" {
				t.Fatal("context management bypassed original cycle bound")
			}
			return
		}
		in = externalInput(v)
		in.Decision, in.Summary, in.EscalationReason = "REMEDIATE", "Repair the unresolved boundary.", "Accepted evidence still confirms the boundary defect."
		v, err = r.ExternalAdvance(ctx, v.RunID, &in)
		if err != nil {
			t.Fatal(err)
		}
		v = advanceExternal(t, ctx, r, v)
		v = submitExternal(t, ctx, r, v, "", "completed")
		v = advanceExternal(t, ctx, r, v)
		in = externalInput(v)
		in.Decision, in.Summary, in.EscalationReason = "QA", "Verify the repaired boundary.", "Independently verify the next candidate against the accepted blocker."
		v, err = r.ExternalAdvance(ctx, v.RunID, &in)
		if err != nil {
			t.Fatal(err)
		}
		v = advanceExternal(t, ctx, r, v)
	}
}
