package statestore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestExactlyExhaustedContingencyRejectsFreshAgentAfterRestart(t *testing.T) {
	s := testStore(t)
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	if err := s.ConfigureRunAdmission(accountingContext, admissionPolicy(100), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	a := admissionFor(t, s, seedControllerInvocation(t, s, "emergency-zero", "emergency-zero-execution"), pools, 10)
	a.Kind, a.ContingencyReason = "CONTAINMENT", "Explicit Orchestrator minimal terminal decision"
	admit(t, s, a)
	d := tokenClaim(a, "emergency-ten", 10)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "emergency-ten-final", "final", counter(10)))
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	s = restartAgentStore(t, s)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, 10, 0, 110})
	b := admissionFor(t, s, seedAdmissionInvocation(t, s, "ordinary-zero", "ordinary-zero-execution"), pools, 1)
	if created, err := s.AdmitInvocation(accountingContext, b, accountingOptions()); created || err == nil {
		t.Error("fresh consuming grant accepted after exact contingency exhaustion", created, err)
		if dispatched, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(b, "ordinary-after-zero", 1), accountingOptions()); dispatched || err == nil {
			t.Error("consuming dispatch accepted after exact contingency exhaustion", dispatched, err)
		}
	}
}

func TestContingencyCapacityBoundaryExistingGrantsAndRestart(t *testing.T) {
	for _, version := range []int{1} {
		for _, used := range []int64{9, 10, 11} {
			t.Run(fmt.Sprintf("schema-%d/used-%d", version, used), func(t *testing.T) {
				migrations, err := loadMigrations()
				if err != nil {
					t.Fatal(err)
				}
				s, err := openTestMigrations(accountingContext, privateTempDir(t), migrations[:version])
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				seedAdmissionRun(t, s)
				pools := admissionPools(t, s, "normal")
				if err := s.ConfigureRunAdmission(accountingContext, admissionPolicy(100), accountingOptions()); err != nil {
					t.Fatal(err)
				}
				emergency := admissionFor(t, s, seedControllerInvocation(t, s, "historical-emergency", "historical-emergency-execution"), pools, 10)
				emergency.Kind, emergency.ContingencyReason = "CONTAINMENT", "Historical explicit Orchestrator emergency contract"
				admit(t, s, emergency)
				d := tokenClaim(emergency, "historical-emergency-call", 10)
				claim(t, s, d)
				if err := s.FinishProviderCall(accountingContext, emergency.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
					t.Fatal(err)
				}
				a := admissionFor(t, s, seedAdmissionInvocation(t, s, "already-granted", "already-granted-execution"), pools, 10)
				admit(t, s, a)
				// Accepted old usage without relying on today's stop refresh.
				receipt := admissionReceipt(d, "historical-emergency-final", "final", counter(used))
				historicalUsageWithoutStopRefresh(t, s, receipt)
				for restart := 0; restart < 2; restart++ {
					s = restartAgentStore(t, s)
					requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 0, 100, 10, used, 10 - used, 110})
					before := mutationDatabaseSnapshot(t, s)
					acceptUsage(t, s, receipt)
					if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err != nil {
						t.Fatal("accepted dispatch replay changed authority", created, err)
					}
					if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
						t.Fatal("replay changed evidence")
					}
					opts := accountingOptions()
					if used <= 10 {
						opts.Audit = accountingAudit()
					}
					probe := tokenClaim(a, fmt.Sprintf("ordinary-probe-%d", restart), 1)
					for _, ctx := range []context.Context{context.Background(), WithMutationActor(context.Background(), "DEVELOPER")} {
						if created, err := s.ClaimAdmissionDispatch(ctx, probe, opts); created || !errors.Is(err, ErrMutationAuthority) {
							t.Fatal("unauthorized dispatch", created, err)
						}
					}
					created, err := s.ClaimAdmissionDispatch(accountingContext, probe, opts)
					if (created && err == nil) != (used < 10) {
						t.Fatal("wrong ordinary capacity boundary", created, err)
					}
					if used < 10 {
						if err := s.FinishProviderCall(accountingContext, a.RunID, probe.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
							t.Fatal(err)
						}
					} else if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
						t.Fatal("denied dispatch changed state or audit")
					}
					before = mutationDatabaseSnapshot(t, s)
					g := ResourceGrant{Account: pools[0], AdmissionID: a.AdmissionID, AllocationID: fmt.Sprintf("capacity-topup-%d", restart), Source: "explicit Orchestrator topup", Amount: 1}
					err = s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, opts)
					if (err == nil) != (used < 10) {
						t.Fatal("wrong topup capacity boundary", err)
					}
					if used >= 10 && !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
						t.Fatal("denied topup changed state or audit")
					}
				}
				b := admissionFor(t, s, seedAdmissionInvocation(t, s, "fresh-agent", "fresh-agent-execution"), pools, 1)
				before := mutationDatabaseSnapshot(t, s)
				created, err := s.AdmitInvocation(accountingContext, b, accountingOptions())
				if (created && err == nil) != (used < 10) {
					t.Fatal("wrong fresh grant boundary", created, err)
				}
				if used >= 10 && !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
					t.Fatal("denied admission changed state")
				}
				if resultCount(t, s, "usage_receipts") != 1 {
					t.Fatal("historical receipt lost or duplicated")
				}
			})
		}
	}
}

func TestContingencyExhaustionAfterNormalExhaustionBlocksControllerWork(t *testing.T) {
	for _, used := range []int64{10, 11} {
		t.Run(fmt.Sprint(used), func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 100)
			a.Resources[0].Amount = 100
			admit(t, s, a)
			normal := tokenClaim(a, "normal-exhaustion", 100)
			claim(t, s, normal)
			acceptUsage(t, s, admissionReceipt(normal, "normal-hundred", "final", counter(100)))
			if err := s.FinishProviderCall(accountingContext, a.RunID, normal.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			if err := s.ClearActiveInvocation(accountingContext, a.RunID, OwnershipOptions{Expected: a.Fence.Owner, ChangedAt: semanticTimestamp}); err != nil {
				t.Fatal(err)
			}
			pools := admissionPools(t, s, "controller")
			b := admissionFor(t, s, seedControllerInvocation(t, s, "valid-emergency", "valid-emergency-execution"), pools, 10)
			b.Kind, b.ContingencyReason = "CONTAINMENT", "Normal bank exhausted: minimal terminal completion"
			admit(t, s, b)
			emergency := tokenClaim(b, "valid-emergency-call", 10)
			claim(t, s, emergency)
			acceptUsage(t, s, admissionReceipt(emergency, "valid-emergency-final", "final", counter(used)))
			if err := s.FinishProviderCall(accountingContext, b.RunID, emergency.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			s = restartAgentStore(t, s)
			requireTokenBank(t, s, b.RunID, RunTokenBalance{100, 100, 0, 10, used, 10 - used, 110})
			if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(b, "emergency-after-exhaustion", 1), accountingOptions()); created || err == nil {
				t.Fatal("controller consuming dispatch bypassed exhaustion", created, err)
			}
			next := admissionFor(t, s, seedControllerInvocation(t, s, "next-emergency", "next-emergency-execution"), pools, 1)
			next.Kind, next.ContingencyReason = b.Kind, b.ContingencyReason
			if created, err := s.AdmitInvocation(accountingContext, next, accountingOptions()); created || err == nil {
				t.Fatal("controller admission bypassed exhaustion", created, err)
			}
		})
	}
}

func TestZeroWholeTokenContingencyDoesNotBlockUnusedNormalBank(t *testing.T) {
	for _, budget := range []int64{1, 9} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, budget)
			a.Resources[0].Amount = budget
			admit(t, s, a)
			d := tokenClaim(a, "small-normal", 1)
			claim(t, s, d)
			acceptUsage(t, s, admissionReceipt(d, "small-null", "partial", nil))
			if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			s = restartAgentStore(t, s)
			requireTokenBank(t, s, a.RunID, RunTokenBalance{budget, 0, budget, 0, 0, 0, budget})
			claim(t, s, tokenClaim(a, "small-normal-after-null", 1))
			var evidence *int64
			if err := s.db.QueryRow(`SELECT work_amount FROM provider_call_usage WHERE provider_call_id=?`, d.ProviderCall.ProviderCallID).Scan(&evidence); err != nil || evidence != nil {
				t.Fatal("NULL usage changed", evidence, err)
			}
		})
	}
}
