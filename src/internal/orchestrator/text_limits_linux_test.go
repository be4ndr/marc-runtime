package orchestrator

import (
	"reflect"
	"strings"
	"testing"
)

func TestExternalDescriptionRejectionPreservesState(t *testing.T) {
	ctx, s, r, _, v := externalFixture(t)
	before, err := s.GetWorkflow(ctx, v.RunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Repeat("😀", 1025), string([]byte{255})} {
		for _, field := range []string{"summary", "escalation_reason", "evidence", "blocker", "remaining_work", "check_summary"} {
			in := externalInput(v)
			in.Decision = "DEVELOPER"
			switch field {
			case "summary":
				in.Summary = bad
			case "escalation_reason":
				in.EscalationReason = bad
			case "evidence":
				in.Evidence = bad
			case "blocker":
				in.Blocker = bad
			case "remaining_work":
				in.RemainingWork = []string{bad}
			case "check_summary":
				in.Checks = []ExternalCheck{{CheckID: "check", Summary: bad, Outcome: "passed"}}
			}
			if _, err = r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
				t.Fatalf("invalid typed %s accepted", field)
			}
			after, err := s.GetWorkflow(ctx, v.RunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("rejection changed workflow", err)
			}
		}
	}
}
