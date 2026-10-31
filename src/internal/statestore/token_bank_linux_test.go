package statestore

import (
	"path/filepath"
	"testing"
)

func requireTokenBank(t *testing.T, s *StateStore, run string, want RunTokenBalance) {
	t.Helper()
	got, err := s.GetRunTokenBalance(accountingContext, run)
	if err != nil || got != want {
		t.Fatalf("bank=%+v want=%+v err=%v", got, want, err)
	}
	internal, err := runTokenBalance(accountingContext, s.db, run)
	if err != nil || internal != got {
		t.Fatalf("checked bank disagrees with SQL view: %+v %v", internal, err)
	}
}

func TestTokenBankGrantsAreLimitsAndUnknownUsageAllowsSequentialWork(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, 0, 10, 110})
	first := tokenClaim(a, "unknown-first", 30)
	claim(t, s, first)
	acceptUsage(t, s, admissionReceipt(first, "unknown-evidence", "partial", nil))
	if err := s.FinishProviderCall(accountingContext, a.RunID, first.ProviderCall.ProviderCallID, 7, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	second := tokenClaim(a, "known-second", 20)
	claim(t, s, second)
	acceptUsage(t, s, admissionReceipt(second, "second-final", "final", counter(20)))
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 20, 80, 10, 0, 10, 110})
	var work *int64
	if err := s.db.QueryRow(`SELECT work_amount FROM provider_call_usage WHERE provider_call_id=?`, first.ProviderCall.ProviderCallID).Scan(&work); err != nil || work != nil {
		t.Fatal("unknown evidence changed", work, err)
	}
	late := admissionReceipt(first, "first-late", "final", counter(25))
	acceptUsage(t, s, late)
	acceptUsage(t, s, late)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 45, 55, 10, 0, 10, 110})
	if tokenAllocations(t, s) != 1 || countSchemaObject(t, s, "resource_reservations") != 0 {
		t.Fatal("calls created new grants or reservations")
	}
	dir := filepath.Dir(s.Info().Path)
	s.Close()
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 45, 55, 10, 0, 10, 110})
	acceptUsage(t, s, late)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 45, 55, 10, 0, 10, 110})

}

func lateOverrunFixture(t *testing.T) (*StateStore, InvocationAdmission, AdmissionDispatchClaim) {
	t.Helper()
	s, a := configuredAdmissionFixture(t, 100)
	a.Resources[0].Amount = 100
	admit(t, s, a)
	first := tokenClaim(a, "late-call", 100)
	claim(t, s, first)
	if err := s.FinishProviderCall(accountingContext, a.RunID, first.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	second := tokenClaim(a, "ordinary-call", 50)
	claim(t, s, second)
	acceptUsage(t, s, admissionReceipt(second, "ordinary-final", "final", counter(50)))
	if err := s.FinishProviderCall(accountingContext, a.RunID, second.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	acceptUsage(t, s, admissionReceipt(first, "late-final", "final", counter(55)))
	return s, a, first
}

func TestTokenBankLateOverrunAndExplicitMinimalTerminalContingency(t *testing.T) {
	s, a, first := lateOverrunFixture(t)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 105, -5, 10, 5, 5, 110})
	requireStopClass(t, s, "BUDGET_EXCEEDED", "BOUNDED_TERMINAL_OVERRUN")
	g := ResourceGrant{Account: a.Resources[0].Account, AllocationID: "ordinary-contingency-topup", AdmissionID: a.AdmissionID, Source: "request", Amount: 1}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
		t.Fatal("ordinary topup used contingency")
	}
	if _, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "after-exhaustion", 1), accountingOptions()); err == nil {
		t.Fatal("normal dispatch passed exhausted bank")
	}
	b := seedControllerInvocation(t, s, "close-run", "close-execution")
	terminal := admissionFor(t, s, b, admissionPools(t, s, "controller"), 5)
	terminal.Kind = "CONTAINMENT"
	if _, err := s.AdmitInvocation(accountingContext, terminal, accountingOptions()); err == nil {
		t.Fatal("RECOVER purpose implicitly accessed contingency")
	}
	terminal.ContingencyReason = "Minimal terminal work to durably close this Run"
	if _, err := s.AdmitInvocation(accountingContext, terminal, accountingOptions()); err == nil {
		t.Fatal("future terminal work received tokens while a primary agent was bound")
	}
	if err := s.ClearActiveInvocation(accountingContext, a.RunID, OwnershipOptions{Expected: a.Fence.Owner, ChangedAt: semanticTimestamp}); err != nil {
		t.Fatal(err)
	}
	terminal = admissionFor(t, s, b, admissionPools(t, s, "controller"), 5)
	terminal.Kind = "CONTAINMENT"
	terminal.ContingencyReason = "Minimal terminal work to durably close this Run"
	admit(t, s, terminal)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 105, -5, 10, 5, 5, 110})
	d := tokenClaim(terminal, "terminal-call", 5)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "terminal-final", "final", counter(3)))
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 105, -5, 10, 8, 2, 110})
	g.AdmissionID, g.AllocationID = "close-run-admission", "terminal-topup"
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
		t.Fatal("terminal authorization became an ordinary topup pool")
	}
	dir := filepath.Dir(s.Info().Path)
	s.Close()
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 105, -5, 10, 8, 2, 110})
	acceptUsage(t, s, admissionReceipt(first, "late-final", "final", counter(55)))
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 105, -5, 10, 8, 2, 110})
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err != nil {
		t.Fatal("restart replay reauthorized terminal call", created, err)
	}
}

func TestTokenBankTruthfulUsageBeyondAbsoluteCeilingIsNeverClamped(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "absolute-overrun", 50)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "absolute-actual", "final", counter(120)))
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 120, -20, 10, 20, -10, 110})
	var actual int64
	if err := s.db.QueryRow(`SELECT work_amount FROM provider_call_usage`).Scan(&actual); err != nil || actual != 120 {
		t.Fatal("actual was clamped", actual, err)
	}
	if lenStops, err := s.ListRunHardStops(accountingContext, a.RunID); err != nil || len(lenStops) < 2 {
		t.Fatal("absolute ceiling failed to stop", lenStops, err)
	}
}

func TestTokenBankCannotOverlapAgentsAcrossInvocations(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "running-agent", 20)
	claim(t, s, d)
	b := admissionFor(t, s, seedAdmissionInvocation(t, s, "next-agent", "next-execution"), admissionPools(t, s, "next"), 80)
	if _, err := s.AdmitInvocation(accountingContext, b, accountingOptions()); err == nil {
		t.Fatal("second active agent admitted while first call unresolved")
	}
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	admit(t, s, b)
	claim(t, s, tokenClaim(b, "now-active", 80))
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, 0, 10, 110})
}
