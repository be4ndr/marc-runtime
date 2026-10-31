package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"ai-control-plane/src/internal/roleresult"
	"ai-control-plane/src/internal/statestore"
)

// Synthetic history can exceed the live three-candidate limit without granting
// additional workflow authority. Keep this fixture usable by the before build.
func contextHistoryFixture(cycles int) statestore.WorkflowSnapshot {
	w := statestore.WorkflowSnapshot{Status: "RUNNING", ContentIdentity: fmt.Sprintf("candidate-%d", cycles)}
	for cycle := 1; cycle <= cycles; cycle++ {
		content := fmt.Sprintf("candidate-%d", cycle)
		for _, role := range []string{"DEVELOPER", "QA", "LEAD"} {
			n := len(w.Steps) + 1
			s := statestore.WorkflowStep{CurrentInvocation: statestore.CurrentInvocation{RunID: "context-run", Cycle: cycle, InvocationID: fmt.Sprintf("inv-%03d", n), Role: role, ContentIdentity: &content}, StepNo: n, CompletedAt: "2026-10-29T00:00:00Z"}
			in := ExternalSubmission{Summary: fmt.Sprintf("candidate-%d-%s-summary", cycle, role), Outcome: "completed", Evidence: fmt.Sprintf("candidate-%d-evidence", cycle)}
			if role == "LEAD" {
				in.Outcome, in.Decision = "", "REMEDIATE"
			} else {
				s.ResultID = roleresult.ResultID(cycle, n)
			}
			if role == "QA" {
				in.Outcome = "failed"
				if cycle == 1 {
					in.FindingDefinitions = []roleresult.Finding{{FindingID: "fnd-000001", FindingOrdinal: 1, OriginCycle: 1, OriginResultID: s.ResultID, Blocking: true, Summary: "Stable unresolved boundary."}}
				}
				in.FindingOccurrences = []roleresult.Occurrence{{FindingID: "fnd-000001"}}
			}
			out := externalOutput{Submission: in, Worktree: worktreeSnapshot{Files: []worktreeFile{{Path: "source.txt", Chunks: [][]byte{[]byte(strings.Repeat("source\n", 32768))}}}}}
			s.Output, _ = json.Marshal(out)
			w.Steps = append(w.Steps, s)
		}
	}
	w.Current = &statestore.WorkflowStep{CurrentInvocation: statestore.CurrentInvocation{Role: "LEAD", Cycle: cycles}, StepNo: len(w.Steps) + 1}
	return w
}

func BenchmarkContextHistoryProcessing(b *testing.B) {
	for _, cycles := range []int{1, 5} {
		b.Run(fmt.Sprintf("cycles-%d", cycles), func(b *testing.B) {
			w := contextHistoryFixture(cycles)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := externalFindings(w, nil); err != nil {
					b.Fatal(err)
				}
				want := "REMEDIATE"
				if cycles >= 3 {
					want = "BLOCK"
				}
				if externalNext(w) != want {
					b.Fatal("lost current blocker or existing cycle limit")
				}
			}
		})
	}
}
