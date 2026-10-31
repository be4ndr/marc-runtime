package statestore

import (
	"path/filepath"
	"strings"
	"testing"
)

func tokenAllocations(t *testing.T, s *StateStore) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM resource_allocations WHERE resource_type IN ('WORK','SPEND')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func tokenClaim(a InvocationAdmission, id string, amount int64) AdmissionDispatchClaim {
	d := claimFor(a, id, amount)
	return d
}

func seedFutureWork(t *testing.T, s *StateStore, id, role, purpose string) InvocationBinding {
	t.Helper()
	var content *string
	if role != "ORCHESTRATOR" {
		content = stringPointer("admission-content")
	}
	execSQL(t, s.db, `INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES(?,?,1,?,?,'RUNNING',?)`, id+"-execution", admissionRun, role, purpose, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES(?,?,?,1,1,'RUNNING',?)`, id+"-attempt", id+"-execution", admissionRun, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch,content_identity) VALUES(?,?,?,?,1,1,?,?,'fake','fake-model','high','RUNNING',?,1,?)`, id, id+"-attempt", id+"-execution", admissionRun, role, purpose, semanticTimestamp, content)
	b := InvocationBinding{InvocationID: id, ExecutionID: id + "-execution", AttemptID: id + "-attempt", Cycle: 1, Purpose: purpose}
	if content != nil {
		b.ContentIdentity = *content
	}
	return b
}

func TestTokenGrantOnlyAtActiveAdmission(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	if tokenAllocations(t, s) != 0 {
		t.Fatal("configuration preallocated normal tokens")
	}
	for _, rolePurpose := range []ResourceEligibility{{"QA", "QA"}, {"REVIEWER", "REVIEW"}, {"DEVELOPER", "FINALIZE_IMPLEMENTATION"}, {"ORCHESTRATOR", "RECOVER"}} {
		id := "future-" + rolePurpose.Role
		b := seedFutureWork(t, s, id, rolePurpose.Role, rolePurpose.Purpose)
		future := admissionFor(t, s, b, admissionPools(t, s, id), 20)
		if rolePurpose.Role == "ORCHESTRATOR" {
			future.Kind = "CONTAINMENT"
		}
		if created, err := s.AdmitInvocation(accountingContext, future, accountingOptions()); created || err == nil {
			t.Fatal("future work received tokens", rolePurpose, created, err)
		}
	}
	pool := a.Resources[0].Account // WORK, in canonical sorted resource order
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{{Account: pool, AllocationID: "preallocate", Source: "future", Amount: 20}}, accountingOptions()); err == nil {
		t.Fatal("unbound pool grant accepted")
	}
	if tokenAllocations(t, s) != 0 || resultCount(t, s, "invocation_admissions") != 0 {
		t.Fatal("rejected future allocation leaked state")
	}
	admit(t, s, a)
	if tokenAllocations(t, s) != 1 {
		t.Fatal("active admission did not allocate exactly its two token accounting dimensions")
	}
	for _, resource := range []string{"WORK"} {
		got, err := activeTokenGrant(accountingContext, s.db, a.AdmissionID, resource)
		if err != nil || got != 50 {
			t.Fatal(got, err)
		}
	}
}

func TestFutureFinalizationGetsFreshGrantOnlyAfterActivation(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	b := seedFutureWork(t, s, "later-finalization", "DEVELOPER", "FINALIZE_IMPLEMENTATION")
	pools := admissionPools(t, s, "later-finalization")
	future := admissionFor(t, s, b, pools, 20)
	if created, err := s.AdmitInvocation(accountingContext, future, accountingOptions()); created || err == nil {
		t.Fatal("inactive finalization received a grant", created, err)
	}
	execSQL(t, s.db, `UPDATE invocations SET ownership_epoch=2 WHERE invocation_id=?`, b.InvocationID)
	options := OwnershipOptions{Expected: a.Fence.Owner, ChangedAt: semanticTimestamp}
	if err := s.TransferSemanticOwnership(accountingContext, a.RunID, "DEVELOPER", &b, options); err != nil {
		t.Fatal(err)
	}
	if tokenAllocations(t, s) != 1 {
		t.Fatal("activation itself preallocated tokens")
	}
	fresh := admissionFor(t, s, b, pools, 20)
	admit(t, s, fresh)
	claim(t, s, tokenClaim(fresh, "active-finalization", 10))
	for _, resource := range []string{"WORK"} {
		charge, err := admissionCharge(accountingContext, s.db, a.RunID, resource)
		if err != nil || charge.Total != 0 {
			t.Fatal("fresh grant did not use current bank or refunded prior grant", charge, err)
		}
	}
}

func TestTokenGrantReusedThenExplicitTopUp(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	for _, id := range []string{"first-call", "second-call"} {
		d := tokenClaim(a, id, 20)
		claim(t, s, d)
		acceptUsage(t, s, admissionReceipt(d, id+"-usage", "final", counter(20)))
		if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	}
	if tokenAllocations(t, s) != 1 || countSchemaObject(t, s, "resource_reservations") != 0 {
		t.Fatal("per-call token allocation or reservation was created")
	}
	next := tokenClaim(a, "third-call", 20)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, next, accountingOptions()); created || err == nil {
		t.Fatal("insufficient grant continued", created, err)
	}
	grants := []ResourceGrant{}
	for _, r := range a.Resources {
		if tokenResource(r.Account.ResourceType) {
			grants = append(grants, ResourceGrant{Account: r.Account, AllocationID: "topup-" + strings.ToLower(r.Account.ResourceType), AdmissionID: a.AdmissionID, Source: "controller-request-insufficient-grant", Amount: 20})
		}
	}
	if err := s.GrantResources(accountingContext, a.RunID, grants, accountingOptions()); err != nil {
		t.Fatal(err)
	}

	claim(t, s, next)
	acceptUsage(t, s, admissionReceipt(next, "third-usage", "final", counter(20)))
	// The normal bank cannot spend the documented contingency on an ordinary top-up.
	grants[0].AllocationID, grants[0].Amount = "oversized-topup", 31
	if err := s.GrantResources(accountingContext, a.RunID, grants[:1], accountingOptions()); err == nil {
		t.Fatal("top-up exceeded current normal bank")
	}
	charge, err := admissionCharge(accountingContext, s.db, a.RunID, "WORK")
	if err != nil || charge.Total != 60 {
		t.Fatal("consumed grant was automatically refunded", charge, err)
	}
}

func TestActiveTokenGrantAtomicCommitAndRestart(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	bad := accountingOptions()
	bad.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp}}
	if _, err := s.AdmitInvocation(accountingContext, a, bad); err == nil {
		t.Fatal("faulted transaction committed")
	}
	dir := filepath.Dir(s.info.Path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	if tokenAllocations(t, s) != 0 || resultCount(t, s, "invocation_admissions") != 0 {
		t.Fatal("restart invented rolled-back grant")
	}
	admit(t, s, a)
	grants := []ResourceGrant{}
	for _, r := range a.Resources {
		if tokenResource(r.Account.ResourceType) {
			grants = append(grants, ResourceGrant{Account: r.Account, AllocationID: "durable-topup-" + strings.ToLower(r.Account.ResourceType), AdmissionID: a.AdmissionID, Source: "insufficient", Amount: 10})
		}
	}
	if err := s.GrantResources(accountingContext, a.RunID, grants, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, resource := range []string{"WORK"} {
		amount, err := activeTokenGrant(accountingContext, s.db, a.AdmissionID, resource)
		if err != nil || amount != 60 {
			t.Fatal("restart lost active grant", amount, err)
		}
	}
	claim(t, s, tokenClaim(a, "after-restart", 20))
}

func TestTokenPurposePoolsAndProtectedCapacityRejected(t *testing.T) {
	s := testStore(t)
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	p := admissionPolicy(100)
	if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO run_protected_capacity VALUES(?,?,?,10)`, admissionRun, pools[0].ResourceType, pools[0].PoolID); err == nil {
		t.Fatal("protected token capacity configured")
	}
	if countSchemaObject(t, s, "run_protected_capacity") != 0 {
		t.Fatal("rejected protection partially committed")
	}
	a := admissionFor(t, s, seedAdmissionInvocation(t, s, "active", "active-execution"), pools, 20)
	dedicated := ResourceAccount{PoolID: "future-finalization", RunID: a.RunID, Scope: "RUN", ResourceType: "WORK"}
	if err := s.CreateResourcePool(accountingContext, ResourcePool{dedicated, []ResourceEligibility{{"DEVELOPER", "FINALIZE_IMPLEMENTATION"}}}, accountingOptions()); err == nil {
		t.Fatal("purpose pool created")
	}
}

func TestTokenTopUpRequiresStillActiveAgent(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	b := admissionFor(t, s, seedAdmissionInvocation(t, s, "now-active", "now-active-execution"), admissionPools(t, s, "now-active"), 20)
	for _, r := range a.Resources {
		if tokenResource(r.Account.ResourceType) {
			g := ResourceGrant{Account: r.Account, AllocationID: "stale-topup-" + strings.ToLower(r.Account.ResourceType), AdmissionID: a.AdmissionID, Source: "insufficient", Amount: 10}
			if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
				t.Fatal("inactive agent got top-up")
			}
		}
	}
	if _, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "stale-agent", 10), accountingOptions()); err == nil {
		t.Fatal("inactive agent dispatched")
	}
	if tokenAllocations(t, s) != 1 {
		t.Fatal("stale top-up allocated tokens")
	}
	admit(t, s, b)
	charge, err := admissionCharge(accountingContext, s.db, a.RunID, "WORK")
	if err != nil || charge.Total != 0 {
		t.Fatal("ownership change refunded unresolved tokens", charge, err)
	}
}
