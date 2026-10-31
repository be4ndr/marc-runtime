package statestore

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"ai-control-plane/src/internal/roleresult"
)

func TestCumulativeAgentHardDispositionLateUsageAndRestart(t *testing.T) {
	for _, decision := range []string{"ALLOW_TERMINAL_EVALUATION", "DENY_TERMINAL_EVALUATION"} {
		t.Run(decision, func(t *testing.T) {
			s := testStore(t)
			var err error
			seedAdmissionRun(t, s)
			pools := admissionPools(t, s, "normal")
			if err := s.ConfigureRunAdmission(accountingContext, admissionPolicy(100), accountingOptions()); err != nil {
				t.Fatal(err)
			}
			a := admissionFor(t, s, seedAdmissionInvocation(t, s, "late-agent", "late-execution"), pools, 10)
			a.SoftLimit = counter(5)
			admit(t, s, a)
			first, second := tokenClaim(a, "first-late", 10), tokenClaim(a, "second-late", 10)
			for _, d := range []AdmissionDispatchClaim{first, second} {
				claim(t, s, d)
				if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
					t.Fatal(err)
				}
			}
			// Model accepted old evidence before the corrected enforcement refresh.
			r1, r2 := admissionReceipt(first, "first-six", "final", counter(6)), admissionReceipt(second, "second-six", "final", counter(6))
			historicalUsageWithoutStopRefresh(t, s, r2)
			historicalUsageWithoutStopRefresh(t, s, r1)
			var original []RunHardStop
			{
				if err := s.numericMutation(accountingContext, "historical fixture", a.RunID, nil, func(c *sql.Conn) error {
					if err := persistHardStop(accountingContext, c, a.RunID, "WORK", "BUDGET_EXCEEDED", "agent:"+a.AdmissionID, counter(12), counter(10), semanticTimestamp); err != nil {
						return err
					}
					return resolveHardStop(accountingContext, c, stopIdentity(a.RunID, "BUDGET_EXCEEDED", "agent:"+a.AdmissionID), "BOUNDED_TERMINAL_OVERRUN", semanticTimestamp)
				}); err != nil {
					t.Fatal(err)
				}
				original, err = s.ListRunHardStops(accountingContext, a.RunID)
				if err != nil {
					t.Fatal(err)
				}
				p := AuthorityProof{RunID: a.RunID, ContentIdentity: a.Binding.ContentIdentity, Terminal: &TerminalProof{}}
				if err := terminalStopDispositions(accountingContext, s.db, p); err == nil {
					t.Fatal("historical bounded agent stop waived human decision")
				}
			}

			evidenceBefore := resultCount(t, s, "usage_receipts")
			s = restartAgentStore(t, s)
			requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 5, HardLimit: 10, Used: 12, BudgetExceeded: true})
			requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 12, 88, 10, 0, 10, 110})
			stops, err := s.ListRunHardStops(accountingContext, a.RunID)
			if err != nil {
				t.Fatal(err)
			}
			var actionable RunHardStop
			for _, stop := range stops {
				if stop.Classification == "OBSERVED_DISPATCH_BOUND_EXCEEDED" {
					actionable = stop
				}
				if len(original) > 0 && stop.StopID == original[0].StopID && !reflect.DeepEqual(stop, original[0]) {
					t.Fatal("historical stop rewritten")
				}
			}
			if actionable.StopID == "" || *actionable.ObservedAmount != 12 || *actionable.ApplicableLimit != 10 {
				t.Fatal("no corrected cumulative stop", stops)
			}
			acceptUsage(t, s, r1)
			acceptUsage(t, s, r2)
			if resultCount(t, s, "usage_receipts") != evidenceBefore {
				t.Fatal("replay duplicated receipts")
			}
			p := AuthorityProof{RunID: a.RunID, ContentIdentity: a.Binding.ContentIdentity, Terminal: &TerminalProof{}}
			if err := terminalStopDispositions(accountingContext, s.db, p); err == nil {
				t.Fatal("missing disposition accepted")
			}
			w := &authorityWorkflow{s: s, worker: roleresult.Result{RunID: a.RunID, Cycle: a.Binding.Cycle, ContentIdentity: a.Binding.ContentIdentity}, lead: a.Binding}
			w.bindLead(t, "disposition-lead", nil, nil, false)
			h := humanContract(t, w, actionable, "hard-response", decision)
			before := mutationDatabaseSnapshot(t, s)
			stale := h
			stale.Boundary.Owner.ActorRevision++
			if err := s.RecordHumanReadinessDisposition(accountingContext, stale, accountingOptions()); err == nil {
				t.Fatal("stale owner accepted")
			}
			for _, ctx := range []context.Context{context.Background(), WithMutationActor(context.Background(), "LEAD")} {
				if err := s.RecordHumanReadinessDisposition(ctx, h, accountingOptions()); !errors.Is(err, ErrMutationAuthority) {
					t.Fatal("unauthorized disposition", err)
				}
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("rejected disposition changed state")
			}
			if err := s.RecordHumanReadinessDisposition(accountingContext, h, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			after := mutationDatabaseSnapshot(t, s)
			if err := s.RecordHumanReadinessDisposition(accountingContext, h, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("replayed disposition changed state")
			}
			p.Terminal.HumanDispositions = []string{h.DispositionID}
			if err := terminalStopDispositions(accountingContext, s.db, p); (err == nil) != (decision == "ALLOW_TERMINAL_EVALUATION") {
				t.Fatal("wrong disposition gate", err)
			}
			s = restartAgentStore(t, s)
			if !reflect.DeepEqual(after, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("restart changed corrected state")
			}
			if err := terminalStopDispositions(accountingContext, s.db, p); (err == nil) != (decision == "ALLOW_TERMINAL_EVALUATION") {
				t.Fatal("restart changed disposition", err)
			}
			if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "no-human-retry", 1), accountingOptions()); created || err == nil {
				t.Fatal("human authorized consuming work")
			}
			conflicting := h
			conflicting.Response = "different response"
			if err := s.RecordHumanReadinessDisposition(accountingContext, conflicting, accountingOptions()); err == nil {
				t.Fatal("conflicting disposition replay accepted")
			}
		})
	}
}

func TestRunOnlyBoundedTerminalOverrunNeedsNoHumanDisposition(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	a.Resources[0].Amount = 100
	admit(t, s, a)
	first := tokenClaim(a, "first-agent-call", 100)
	claim(t, s, first)
	if err := s.FinishProviderCall(accountingContext, a.RunID, first.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	b := admissionFor(t, s, seedAdmissionInvocation(t, s, "second-agent", "second-agent-execution"), []ResourceAccount{a.Resources[0].Account}, 100)
	admit(t, s, b)
	second := tokenClaim(b, "second-agent-call", 100)
	claim(t, s, second)
	if err := s.FinishProviderCall(accountingContext, b.RunID, second.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	acceptUsage(t, s, admissionReceipt(second, "second-agent-sixty", "final", counter(60)))
	acceptUsage(t, s, admissionReceipt(first, "first-agent-forty-five", "final", counter(45)))
	stops, err := s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil || len(stops) != 1 || stops[0].SourceID != "bank:normal" || stops[0].Classification != "BOUNDED_TERMINAL_OVERRUN" {
		t.Fatal("legitimate Run overrun changed", stops, err)
	}
	p := AuthorityProof{RunID: a.RunID, ContentIdentity: a.Binding.ContentIdentity, Terminal: &TerminalProof{}}
	if err := terminalStopDispositions(accountingContext, s.db, p); err != nil {
		t.Fatal("Run-only overrun needs human", err)
	}
	s = restartAgentStore(t, s)
	if err := terminalStopDispositions(accountingContext, s.db, p); err != nil {
		t.Fatal("restart changed Run-only handling", err)
	}
}

func TestAgentHardStopPreservesOriginalViolationThroughLateFinal(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	a.Resources[0].Amount, a.SoftLimit = 10, counter(5)
	admit(t, s, a)
	first, second := tokenClaim(a, "partial-first", 10), tokenClaim(a, "partial-second", 10)
	for _, d := range []AdmissionDispatchClaim{first, second} {
		claim(t, s, d)
		if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	}
	acceptUsage(t, s, admissionReceipt(first, "first-final-six", "final", counter(6)))
	acceptUsage(t, s, admissionReceipt(second, "second-partial-six", "partial", counter(6)))
	before, err := s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil || len(before) != 1 || before[0].Classification != "OBSERVED_DISPATCH_BOUND_EXCEEDED" {
		t.Fatal(before, err)
	}
	s = restartAgentStore(t, s)
	final := admissionReceipt(second, "second-late-final-seven", "final", counter(7))
	acceptUsage(t, s, final)
	acceptUsage(t, s, final)
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 5, HardLimit: 10, Used: 13, BudgetExceeded: true})
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 13, 87, 10, 0, 10, 110})
	after, err := s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("late FINAL rewrote original reason/evidence", after, err)
	}
	var known int64
	if err := s.db.QueryRow(`SELECT SUM(work_amount) FROM provider_call_usage`).Scan(&known); err != nil || known != 13 {
		t.Fatal("actual usage hidden", known, err)
	}
	s = restartAgentStore(t, s)
	if err := terminalStopDispositions(accountingContext, s.db, AuthorityProof{RunID: a.RunID, ContentIdentity: a.Binding.ContentIdentity, Terminal: &TerminalProof{}}); err == nil {
		t.Fatal("late FINAL waived human decision")
	}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "late-final-retry", 1), accountingOptions()); created || err == nil {
		t.Fatal("late FINAL restored work")
	}
}
