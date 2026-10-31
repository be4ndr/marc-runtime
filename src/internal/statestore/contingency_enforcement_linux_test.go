package statestore

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestContingencyExhaustionCreatesCorrectStopBeforeAbsoluteCeiling(t *testing.T) {
	s := testStore(t)
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	if err := s.ConfigureRunAdmission(accountingContext, admissionPolicy(100), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	a := admissionFor(t, s, seedControllerInvocation(t, s, "emergency", "emergency-execution"), pools, 10)
	a.Kind, a.ContingencyReason = "CONTAINMENT", "Orchestrator emergency completion"
	admit(t, s, a)
	if tokenAllocations(t, s) != 0 || resultCount(t, s, "admission_grants") != 0 {
		t.Fatal("controller emergency bound allocated an agent token grant")
	}
	g := ResourceGrant{Account: pools[0], AdmissionID: a.AdmissionID, AllocationID: "emergency-topup", Source: "extend agent", Amount: 1}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
		t.Fatal("emergency authorization accepted an agent topup")
	}
	first, second := tokenClaim(a, "emergency-first", 10), tokenClaim(a, "emergency-second", 10)
	for _, d := range []AdmissionDispatchClaim{first, second} {
		claim(t, s, d)
		if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	}
	acceptUsage(t, s, admissionReceipt(first, "first-late-six", "final", counter(6)))
	acceptUsage(t, s, admissionReceipt(second, "second-late-six", "final", counter(6)))
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, 12, -2, 110})
	stops, err := s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil || len(stops) != 1 || stops[0].SourceID != "bank:contingency" || *stops[0].ObservedAmount != 12 || *stops[0].ApplicableLimit != 10 {
		t.Fatal("inconsistent contingency/absolute stop", stops, err)
	}
	b := admissionFor(t, s, seedAdmissionInvocation(t, s, "ordinary", "ordinary-execution"), pools, 100)
	if created, err := s.AdmitInvocation(accountingContext, b, accountingOptions()); created || err == nil {
		t.Fatal("agent admitted after known contingency exhaustion", created, err)
	}
	dir := filepath.Dir(s.Info().Path)
	s.Close()
	s, err = openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if created, err := s.AdmitInvocation(accountingContext, b, accountingOptions()); created || err == nil {
		t.Fatal("restart lost contingency stop", created, err)
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, 12, -2, 110})
}

// Build the old QA state without running today's corrected hard-stop refresh.
// Primitive/current usage is advanced by the normal evidence implementation.
func historicalUsageWithoutStopRefresh(t *testing.T, s *StateStore, r UsageReceipt) {
	t.Helper()
	r, work, spend, err := normalizeReceipt(r)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.db.Conn(accountingContext)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.ExecContext(accountingContext, `INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,completeness,input_tokens,cached_input_tokens,output_tokens,work_tokens,spend_tokens,reported_at,accepted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, r.ReceiptID, r.InvocationID, r.ProviderCallID, r.Completeness, r.InputTokens, r.CachedInputTokens, r.OutputTokens, work, spend, r.ReportedAt, semanticTimestamp); err != nil {
		t.Fatal(err)
	}
	if err := advanceProviderCallUsage(accountingContext, c, r); err != nil {
		t.Fatal(err)
	}
}

func TestAgentFinalizationUsesOnlyItsNormalHardLimit(t *testing.T) {
	s, lead := configuredAdmissionFixture(t, 100)
	b := seedFutureWork(t, s, "agent-finalize", "DEVELOPER", "FINALIZE_IMPLEMENTATION")
	execSQL(t, s.db, `UPDATE invocations SET ownership_epoch=2 WHERE invocation_id=?`, b.InvocationID)
	if err := s.TransferSemanticOwnership(accountingContext, lead.RunID, "DEVELOPER", &b, OwnershipOptions{Expected: lead.Fence.Owner, ChangedAt: semanticTimestamp}); err != nil {
		t.Fatal(err)
	}
	a := admissionFor(t, s, b, admissionPools(t, s, "normal"), 10)
	bad := a
	bad.ContingencyReason = "finish agent result"
	if created, err := s.AdmitInvocation(accountingContext, bad, accountingOptions()); created || err == nil {
		t.Fatal("agent finalization received contingency", created, err)
	}
	admit(t, s, a)
	for n, amount := range []int64{8, 2} {
		d := tokenClaim(a, fmt.Sprintf("finalization-%d", n), amount)
		claim(t, s, d)
		acceptUsage(t, s, admissionReceipt(d, fmt.Sprintf("finalization-usage-%d", n), "final", counter(amount)))
		if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 10, 90, 10, 0, 10, 110})
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "finalize-past-hard", 1), accountingOptions()); created || err == nil {
		t.Fatal("agent Hard limit bypassed by terminal purpose", created, err)
	}
	dir := filepath.Dir(s.Info().Path)
	s.Close()
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "finalize-restarted", 1), accountingOptions()); created || err == nil {
		t.Fatal("restart bypassed agent Hard limit", created, err)
	}
}

func TestNormalBankExhaustionRejectsExistingAgentGrantAfterLateUsageAndRestart(t *testing.T) {
	for _, total := range []int64{99, 100, 103, 111} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			s, first := configuredAdmissionFixture(t, 100)
			first.Resources[0].Amount = 100
			admit(t, s, first)
			d := tokenClaim(first, "late-first", 100)
			claim(t, s, d)
			if err := s.FinishProviderCall(accountingContext, first.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			current := admissionFor(t, s, seedAdmissionInvocation(t, s, "current", "current-execution"), admissionPools(t, s, "normal"), 100)
			admit(t, s, current)
			known := tokenClaim(current, "known-current", 50)
			claim(t, s, known)
			acceptUsage(t, s, admissionReceipt(known, "known-50", "final", counter(50)))
			if err := s.FinishProviderCall(accountingContext, current.RunID, known.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			late := admissionReceipt(d, "late-first-usage", "final", counter(total-50))
			acceptUsage(t, s, late)
			dir := filepath.Dir(s.Info().Path)
			for restart := 0; restart < 2; restart++ {
				if restart != 0 {
					s.Close()
					var err error
					s, err = openTest(accountingContext, dir)
					if err != nil {
						t.Fatal(err)
					}
				}
				acceptUsage(t, s, late)
				overrun := int64(0)
				if total > 100 {
					overrun = total - 100
				}
				requireTokenBank(t, s, current.RunID, RunTokenBalance{100, total, 100 - total, 10, overrun, 10 - overrun, 110})
				// The current agent still has 50 of its own Hard limit left.
				// Exhaustion must be enforced by the Run bank, not that grant.
				probe := tokenClaim(current, fmt.Sprintf("boundary-%d", restart), 1)
				created, err := s.ClaimAdmissionDispatch(accountingContext, probe, accountingOptions())
				if total < 100 {
					if !created || err != nil {
						t.Fatal("remaining normal token denied", created, err)
					}
					if err := s.FinishProviderCall(accountingContext, current.RunID, probe.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
						t.Fatal(err)
					}
				} else {
					if created || err == nil {
						t.Fatal("old grant bypassed exhausted normal bank", created, err)
					}
					g := ResourceGrant{Account: current.Resources[0].Account, AdmissionID: current.AdmissionID, AllocationID: fmt.Sprintf("topup-%d", restart), Source: "insufficient", Amount: 1}
					if err := s.GrantResources(accountingContext, current.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
						t.Fatal("ordinary topup used contingency")
					}
				}
			}
			next := admissionFor(t, s, seedAdmissionInvocation(t, s, "next", "next-execution"), admissionPools(t, s, "normal"), 1)
			created, err := s.AdmitInvocation(accountingContext, next, accountingOptions())
			if total < 100 && (!created || err != nil) || total >= 100 && (created || err == nil) {
				t.Fatal("fresh grant ignored normal-bank boundary", created, err)
			}
			s.Close()
		})
	}
}
