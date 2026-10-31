package statestore

import (
	"testing"
	"time"
)

func TestOnlyTokensCanBeBudgeted(t *testing.T) {
	for _, retired := range []string{"ACTIVE_TIME", "TOOL_CALLS"} {
		t.Run(retired, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 100)
			account := ResourceAccount{PoolID: "retired", RunID: a.RunID, Scope: "RUN", ResourceType: retired}
			if err := s.CreateResourcePool(accountingContext, ResourcePool{account, primaryTokenEligibility()}, accountingOptions()); err == nil {
				t.Fatal("retired resource pool accepted")
			}
			if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{{Account: account, AllocationID: "retired", Source: "fixture", Amount: 1}}, accountingOptions()); err == nil {
				t.Fatal("retired grant accepted")
			}

			if _, err := s.GetResourceBalance(accountingContext, account); err == nil {
				t.Fatal("retired balance exposed")
			}
			invalid := a
			invalid.Resources = append(append([]AdmissionResource(nil), a.Resources...), AdmissionResource{Account: account, Amount: 1})
			if created, err := s.AdmitInvocation(accountingContext, invalid, accountingOptions()); created || err == nil {
				t.Fatal("retired admission commitment accepted", created, err)
			}
			admit(t, s, a)
			d := tokenClaim(a, "retired-bound", 10)
			d.Bounds = append(d.Bounds, DispatchBound{retired, 1})
			if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err == nil {
				t.Fatal("retired dispatch bound accepted", created, err)
			}
			claim(t, s, tokenClaim(a, "valid", 10))
			e := ControllerReleaseEvidence{RunID: a.RunID, ClaimID: "valid", EvidenceID: "retired-release", ReleasedAt: semanticTimestamp, SubmittedBounds: []DispatchBound{{retired, 1}}}
			if created, err := s.RecordControllerReleaseEvidence(accountingContext, e, accountingOptions()); created || err == nil {
				t.Fatal("retired submitted bound accepted", created, err)
			}
			if resultCount(t, s, "resource_allocations") != 1 || countSchemaObject(t, s, "resource_reservations") != 0 || resultCount(t, s, "run_resource_budgets") != 1 || resultCount(t, s, "run_hard_stops") != 0 {
				t.Fatal("rejected resource mutation leaked budget state")
			}
		})
	}
	s := testStore(t)
	seedAdmissionRun(t, s)
	for _, retired := range []string{"ACTIVE_TIME", "TOOL_CALLS"} {
		policy := admissionPolicy(100)
		policy.Budgets[0].ResourceType = retired
		if err := s.ConfigureRunAdmission(accountingContext, policy, accountingOptions()); err == nil {
			t.Fatal("retired budget configured", retired)
		}
	}
	if resultCount(t, s, "run_admission_policy") != 0 {
		t.Fatal("invalid policy partially committed")
	}
}

func TestTimingCannotConsumeGrantOrCauseResourceStop(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "long-running", 10)
	claim(t, s, d)
	duration := int64(100*24*time.Hour + time.Nanosecond)
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, duration, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	var got int64
	var started string
	if err := s.db.QueryRow(`SELECT started_at,duration_ns FROM provider_calls WHERE provider_call_id=?`, d.ProviderCall.ProviderCallID).Scan(&started, &got); err != nil || got != duration || started != semanticTimestamp {
		t.Fatal(started, got, err)
	}
	for _, resource := range budgetResources {
		charge, err := admissionCharge(accountingContext, s.db, a.RunID, resource)
		if err != nil || charge.Total != 0 || charge.Known != 0 {
			t.Fatal("timing changed token grant/usage", charge, err)
		}
	}
	if resultCount(t, s, "run_hard_stops") != 0 || countSchemaObject(t, s, "reservation_actual_observations") != 0 {
		t.Fatal("timing created resource evidence")
	}
	claim(t, s, tokenClaim(a, "unknown-usage", 10))
	if err := s.FinishProviderCall(accountingContext, a.RunID, "unknown-usage-call", 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	acceptUsage(t, s, admissionReceipt(d, "final", "final", counter(10)))
	claim(t, s, tokenClaim(a, "after-final", 10))
}

func TestResourceRequestsRejectTimeAndToolCounts(t *testing.T) {
	for _, key := range []string{"active_time_sec", "tool_calls"} {
		s := testStore(t)
		state, opts := semanticFixture(t, s)
		if err := s.AcceptLeadState(accountingContext, wire(t, state), opts); err != nil {
			t.Fatal(err)
		}
		opts.ExpectedSemanticRevision = 1
		doc := requestFixture(state)
		doc["requested"].(map[string]any)[key] = 1
		if err := s.AcceptResourceRequest(accountingContext, wire(t, doc), opts); err == nil {
			t.Fatal("retired request accepted", key)
		}
		if resultCount(t, s, "resource_requests") != 0 {
			t.Fatal("rejected request changed state")
		}
	}
}
