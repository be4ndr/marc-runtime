package statestore

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestZeroLimitActiveAdmissionReceivesFirstGrant(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	a.Resources[0].Amount = 0
	admit(t, s, a)
	if tokenAllocations(t, s) != 0 {
		t.Fatal("zero limits allocated tokens")
	}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "unfunded", 1), accountingOptions()); created || err == nil {
		t.Fatal("zero limit authorized consuming dispatch", created, err)
	}
	g := ResourceGrant{Account: a.Resources[0].Account, AdmissionID: a.AdmissionID, AllocationID: "first-positive-grant", Source: "Orchestrator initial limits", Amount: 20}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err != nil {
		t.Fatal("active zero admission cannot receive its first grant", err)
	}
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 20, HardLimit: 20})
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, 0, 10, 110})
	if created, err := s.AdmitInvocation(accountingContext, a, accountingOptions()); created || err != nil {
		t.Fatal("zero admission replay allocated twice", created, err)
	}
	changed := a
	changed.Resources = append([]AdmissionResource(nil), a.Resources...)
	changed.Resources[0].Amount = 20
	if created, err := s.AdmitInvocation(accountingContext, changed, accountingOptions()); created || err == nil {
		t.Fatal("changed admission replay succeeded", created, err)
	}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
		t.Fatal("duplicate allocation replay accepted")
	}
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 20, HardLimit: 20})
	s = restartAgentStore(t, s)
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 20, HardLimit: 20})
	finishAgentUsage(t, s, a, "first-positive-call", 10, 10, false)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 10, 90, 10, 0, 10, 110})
}

func requireAgentLimits(t *testing.T, s *StateStore, a InvocationAdmission, want AgentTokenLimits) {
	t.Helper()
	got, err := s.GetAgentTokenLimits(accountingContext, a.RunID, a.AdmissionID)
	if err != nil || got != want {
		t.Fatalf("limits=%+v want=%+v err=%v", got, want, err)
	}
}

func restartAgentStore(t *testing.T, s *StateStore) *StateStore {
	t.Helper()
	dir := filepath.Dir(s.Info().Path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func finishAgentUsage(t *testing.T, s *StateStore, a InvocationAdmission, id string, bound, used int64, completion bool) AdmissionDispatchClaim {
	t.Helper()
	d := tokenClaim(a, id, bound)
	d.CompletionOnly = completion
	if completion {
		d.CompletionCapabilities = &CompletionCapabilities{FinalResponseOnly: true}
	}
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, id+"-usage", "final", counter(used)))
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestAgentRoleProfileSoftHardApprovalDenialAndRestart(t *testing.T) {
	s := testStore(t)
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	p := admissionPolicy(100)
	p.AgentTokenProfiles = []AgentTokenProfile{{Role: "LEAD", SoftPercent: counter(20), HardPercent: counter(30)}, {Role: "QA", SoftLimit: 4, HardLimit: 8}}
	if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if tokenAllocations(t, s) != 0 {
		t.Fatal("role profiles preallocated tokens")
	}
	a := admissionFor(t, s, seedAdmissionInvocation(t, s, "profile-agent", "profile-execution"), pools, 0)
	admit(t, s, a)
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 20, HardLimit: 30})
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, 0, 10, 110})
	if created, err := s.AdmitInvocation(accountingContext, a, accountingOptions()); created || err != nil {
		t.Fatal("profile admission replay", created, err)
	}
	// The first call can cross Soft within Hard; no per-call top-up is needed.
	finishAgentUsage(t, s, a, "cross-soft", 25, 25, false)
	next := tokenClaim(a, "paused-at-soft", 1)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, next, accountingOptions()); created || err == nil || !strings.Contains(err.Error(), "Soft") {
		t.Fatal("ordinary work passed Soft without decision", created, err)
	}
	next.CompletionOnly = true
	next.CompletionCapabilities = &CompletionCapabilities{FinalResponseOnly: true}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, next, accountingOptions()); created || err == nil {
		t.Fatal("completion flag bypassed pending decision", created, err)
	}
	s = restartAgentStore(t, s)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "restart-soft", 1), accountingOptions()); created || err == nil {
		t.Fatal("restart cleared Soft pause", created, err)
	}
	g := ResourceGrant{Account: pools[0], AdmissionID: a.AdmissionID, AllocationID: "approved-profile-topup", Source: "Orchestrator approved", Amount: 10}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 30, HardLimit: 40, Used: 25})
	finishAgentUsage(t, s, a, "approved-ordinary", 5, 5, false)
	if created, err := s.DenyTokenTopUp(accountingContext, a.RunID, a.AdmissionID, "deny-topup", "finish within existing Hard", accountingOptions()); !created || err != nil {
		t.Fatal("denial", created, err)
	}
	if created, err := s.DenyTokenTopUp(accountingContext, a.RunID, a.AdmissionID, "deny-topup", "finish within existing Hard", accountingOptions()); created || err != nil {
		t.Fatal("denial replay", created, err)
	}
	if created, err := s.DenyTokenTopUp(accountingContext, a.RunID, a.AdmissionID, "deny-topup", "changed reason", accountingOptions()); created || err == nil {
		t.Fatal("conflicting denial replay", created, err)
	}
	if tokenAllocations(t, s) != 2 {
		t.Fatal("denial created a grant")
	}
	s = restartAgentStore(t, s)
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 30, HardLimit: 40, Used: 30, CompletionOnly: true})
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "denied-exploration", 1), accountingOptions()); created || err == nil {
		t.Fatal("denial permitted exploration", created, err)
	}
	d := finishAgentUsage(t, s, a, "completion-within-hard", 10, 10, true)
	var marked bool
	if err := s.db.QueryRow(`SELECT completion_only FROM admission_dispatch_claims WHERE claim_id=?`, d.ClaimID).Scan(&marked); err != nil || !marked {
		t.Fatal("completion restriction not durable", marked, err)
	}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err != nil {
		t.Fatal("completion dispatch replay", created, err)
	}
	d.CompletionOnly = false
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err == nil {
		t.Fatal("completion restriction changed on replay", created, err)
	}
	s = restartAgentStore(t, s)
	next = tokenClaim(a, "hard-exhausted", 1)
	next.CompletionOnly = true
	next.CompletionCapabilities = &CompletionCapabilities{FinalResponseOnly: true}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, next, accountingOptions()); created || err == nil {
		t.Fatal("completion bypassed Hard", created, err)
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 40, 60, 10, 0, 10, 110})
}

func TestAgentSoftHardOverrunIsTruthfulAndRequiresHumanDecision(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	a.Resources[0].Amount, a.SoftLimit = 30, counter(20)
	admit(t, s, a)
	// A still-running call reports more than both limits. Evidence must commit.
	d := tokenClaim(a, "both-limits-crossed", 30)
	claim(t, s, d)
	r := admissionReceipt(d, "truthful-overrun", "partial", counter(35))
	acceptUsage(t, s, r)
	acceptUsage(t, s, r)
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 20, HardLimit: 30, Used: 35, BudgetExceeded: true})
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 35, 65, 10, 0, 10, 110})
	stops, err := s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, stop := range stops {
		if stop.SourceID == "agent:"+a.AdmissionID && *stop.ObservedAmount == 35 && *stop.ApplicableLimit == 30 {
			found = true
		}
	}
	if !found {
		t.Fatal("no durable agent Hard stop", stops)
	}
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	s = restartAgentStore(t, s)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "no-auto-retry", 1), accountingOptions()); created || err == nil || !strings.Contains(err.Error(), "human decision") {
		t.Fatal("overrun did not return clear human decision result", created, err)
	}
	g := ResourceGrant{Account: a.Resources[0].Account, AdmissionID: a.AdmissionID, AllocationID: "no-auto-increase", Source: "overrun", Amount: 10}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
		t.Fatal("top-up erased an exceeded Hard limit")
	}
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 20, HardLimit: 30, Used: 35, BudgetExceeded: true})
}

func TestZeroAgentGrantOwnershipAndAtomicRollback(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	a.Resources[0].Amount = 0
	admit(t, s, a)
	g := ResourceGrant{Account: a.Resources[0].Account, AdmissionID: a.AdmissionID, AllocationID: "valid-first", Source: "controller", Amount: 10}
	bad := g
	bad.AllocationID, bad.AdmissionID = "invalid-second", "missing-admission"
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g, bad}, accountingOptions()); err == nil {
		t.Fatal("invalid batch committed")
	}
	if tokenAllocations(t, s) != 0 {
		t.Fatal("rollback leaked first allocation")
	}
	requireAgentLimits(t, s, a, AgentTokenLimits{})
	s = restartAgentStore(t, s)
	requireAgentLimits(t, s, a, AgentTokenLimits{})
	b := admissionFor(t, s, seedAdmissionInvocation(t, s, "new-owner", "new-owner-execution"), admissionPools(t, s, "normal"), 0)
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
		t.Fatal("inactive zero admission received first grant")
	}
	if created, err := s.DenyTokenTopUp(accountingContext, a.RunID, a.AdmissionID, "stale-denial", "old agent", accountingOptions()); created || err == nil {
		t.Fatal("stale owner recorded denial", created, err)
	}
	admit(t, s, b)
	g.AdmissionID = b.AdmissionID
	if err := s.GrantResources(accountingContext, b.RunID, []ResourceGrant{g}, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	requireAgentLimits(t, s, b, AgentTokenLimits{SoftLimit: 10, HardLimit: 10})
}

func TestAgentLateUsageAcrossCallsStopsCumulativeHardOverrun(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	a.Resources[0].Amount, a.SoftLimit = 10, counter(5)
	admit(t, s, a)
	first, second := tokenClaim(a, "late-first", 10), tokenClaim(a, "late-second", 10)
	for _, d := range []AdmissionDispatchClaim{first, second} {
		claim(t, s, d)
		if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	}
	// NULL counts as zero provisionally; each actual stays within its call bound.
	acceptUsage(t, s, admissionReceipt(first, "late-first-six", "final", counter(6)))
	acceptUsage(t, s, admissionReceipt(second, "late-second-six", "final", counter(6)))
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 5, HardLimit: 10, Used: 12, BudgetExceeded: true})
	stops, err := s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil || len(stops) != 1 || stops[0].SourceID != "agent:"+a.AdmissionID || *stops[0].ObservedAmount != 12 || *stops[0].ApplicableLimit != 10 {
		t.Fatal("cumulative overrun hidden by per-call bounds", stops, err)
	}
	if stops[0].Classification != "OBSERVED_DISPATCH_BOUND_EXCEEDED" {
		t.Errorf("agent Hard violation misclassified: %s", stops[0].Classification)
	}
	p := AuthorityProof{RunID: a.RunID, ContentIdentity: a.Binding.ContentIdentity, Terminal: &TerminalProof{}}
	if err := terminalStopDispositions(accountingContext, s.db, p); err == nil {
		t.Error("cumulative agent Hard overrun bypassed human disposition")
	}
	s = restartAgentStore(t, s)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "no-late-retry", 1), accountingOptions()); created || err == nil {
		t.Fatal("late cumulative overrun allowed new work", created, err)
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 12, 88, 10, 0, 10, 110})
}

func TestAgentRoleProfileValidation(t *testing.T) {
	for n, profile := range []AgentTokenProfile{
		{Role: "ORCHESTRATOR", SoftLimit: 1, HardLimit: 2},
		{Role: "LEAD", SoftLimit: 3, HardLimit: 2},
		{Role: "LEAD", SoftLimit: 10, HardLimit: 101},
		{Role: "LEAD", SoftPercent: counter(70), HardPercent: counter(50)},
		{Role: "LEAD", SoftPercent: counter(50)},
		{Role: "LEAD", SoftPercent: counter(50), HardPercent: counter(110)},
		{Role: "LEAD", SoftLimit: 1, SoftPercent: counter(10), HardPercent: counter(20)},
	} {
		t.Run(fmt.Sprintf("invalid-%d", n), func(t *testing.T) {
			s := testStore(t)
			seedAdmissionRun(t, s)
			p := admissionPolicy(100)
			p.AgentTokenProfiles = []AgentTokenProfile{profile}
			if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err == nil {
				t.Fatal("invalid profile accepted", profile)
			}
			if resultCount(t, s, "run_admission_policy") != 0 {
				t.Fatal("invalid profile partially committed")
			}
		})
	}
}

func TestAgentCompletionOnlyCannotSpendContingencyAfterNormalExhaustion(t *testing.T) {
	s, previous := configuredAdmissionFixture(t, 100)
	previous.Resources[0].Amount = 100
	admit(t, s, previous)
	late := tokenClaim(previous, "previous-unknown", 100)
	claim(t, s, late)
	if err := s.FinishProviderCall(accountingContext, previous.RunID, late.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	a := admissionFor(t, s, seedAdmissionInvocation(t, s, "completion-agent", "completion-execution"), admissionPools(t, s, "normal"), 20)
	a.SoftLimit = counter(10)
	admit(t, s, a)
	finishAgentUsage(t, s, a, "reach-soft", 10, 10, false)
	if created, err := s.DenyTokenTopUp(accountingContext, a.RunID, a.AdmissionID, "deny-completion", "only finish", accountingOptions()); !created || err != nil {
		t.Fatal(created, err)
	}
	acceptUsage(t, s, admissionReceipt(late, "late-normal-exhaustion", "final", counter(90)))
	s = restartAgentStore(t, s)
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 10, HardLimit: 20, Used: 10, CompletionOnly: true})
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 100, 0, 10, 0, 10, 110})
	d := tokenClaim(a, "no-contingency-completion", 1)
	d.CompletionOnly = true
	d.CompletionCapabilities = &CompletionCapabilities{FinalResponseOnly: true}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err == nil {
		t.Fatal("remaining agent Hard capacity spent contingency", created, err)
	}
	g := ResourceGrant{Account: a.Resources[0].Account, AdmissionID: a.AdmissionID, AllocationID: "no-contingency-topup", Source: "Orchestrator", Amount: 1}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
		t.Fatal("top-up spent contingency after normal exhaustion")
	}
}

func TestAgentProfilesApplyOnlyAfterMatchingRoleActivation(t *testing.T) {
	s := testStore(t)
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	p := admissionPolicy(100)
	p.AgentTokenProfiles = []AgentTokenProfile{{Role: "LEAD", SoftLimit: 5, HardLimit: 10}, {Role: "QA", SoftLimit: 4, HardLimit: 8}}
	if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	a := admissionFor(t, s, seedAdmissionInvocation(t, s, "profile-lead", "profile-lead-execution"), pools, 0)
	admit(t, s, a)
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 5, HardLimit: 10})
	b := seedFutureWork(t, s, "profile-qa", "QA", "QA")
	future := admissionFor(t, s, b, pools, 0)
	if created, err := s.AdmitInvocation(accountingContext, future, accountingOptions()); created || err == nil {
		t.Fatal("future role profile allocated work", created, err)
	}
	if tokenAllocations(t, s) != 1 || resultCount(t, s, "admission_agent_token_limits") != 1 {
		t.Fatal("future role profile leaked grants/limits")
	}
	execSQL(t, s.db, `UPDATE invocations SET ownership_epoch=2 WHERE invocation_id=?`, b.InvocationID)
	if err := s.TransferSemanticOwnership(accountingContext, a.RunID, "QA", &b, OwnershipOptions{Expected: a.Fence.Owner, ChangedAt: semanticTimestamp}); err != nil {
		t.Fatal(err)
	}
	qa := admissionFor(t, s, b, pools, 0)
	admit(t, s, qa)
	requireAgentLimits(t, s, qa, AgentTokenLimits{SoftLimit: 4, HardLimit: 8})
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, 0, 10, 110})
	claim(t, s, tokenClaim(qa, "profile-qa-call", 4))
}
