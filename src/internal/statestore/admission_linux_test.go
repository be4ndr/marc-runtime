package statestore

import (
	"database/sql"
	"errors"

	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"ai-control-plane/internal/numeric"
)

func TestAdmissionSubmittedBoundEvidenceDoesNotRewriteNeutralHistory(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := claimFor(a, "submitted-bound", 50)
	setDispatchBound(&d, "INPUT_TOKENS", 49)
	setDispatchBound(&d, "OUTPUT_TOKENS", 1)
	claim(t, s, d)
	r := admissionReceipt(d, "observed-output", "partial", counter(1))
	r.OutputTokens = counter(2)
	if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	requireStopClass(t, s, "DISPATCH_BOUND_EXCEEDED", "OBSERVED_DISPATCH_BOUND_EXCEEDED")
	e := ControllerReleaseEvidence{RunID: admissionRun, ClaimID: d.ClaimID, EvidenceID: "submitted-output", ReleasedAt: semanticTimestamp, SubmittedBounds: []DispatchBound{{"OUTPUT_TOKENS", 2}}}
	if created, err := s.RecordControllerReleaseEvidence(accountingContext, e, accountingOptions()); !created || err != nil {
		t.Fatal(created, err)
	}
	requireStopClass(t, s, "CONTROLLER_RELEASE_VIOLATION", "CONTROLLER_ADMISSION_BUG")
	requireStopClass(t, s, "DISPATCH_BOUND_EXCEEDED", "OBSERVED_DISPATCH_BOUND_EXCEEDED")
	before := resultCount(t, s, "hard_stop_decisions")
	if created, err := s.RecordControllerReleaseEvidence(accountingContext, e, accountingOptions()); created || err != nil {
		t.Fatal(created, err)
	}
	e.SubmittedBounds[0].Amount = 3
	if _, err := s.RecordControllerReleaseEvidence(accountingContext, e, accountingOptions()); err == nil {
		t.Fatal("conflicting submitted limit accepted")
	}
	if resultCount(t, s, "hard_stop_decisions") != before {
		t.Fatal("replay changed stop history")
	}
}

func TestAdmissionKnownMinimumProvesBoundViolationWithoutInventingUsage(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := claimFor(a, "minimum-bound", 20)
	claim(t, s, d)
	r := admissionReceipt(d, "incomplete-input", "partial", counter(30))
	r.CachedInputTokens, r.OutputTokens = nil, nil
	if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	requireStopClass(t, s, "DISPATCH_BOUND_EXCEEDED", "OBSERVED_DISPATCH_BOUND_EXCEEDED")
	var work, spend *int64
	if err := s.db.QueryRow(`SELECT work_amount,spend_amount FROM provider_call_usage`).Scan(&work, &spend); err != nil || work != nil || spend != nil {
		t.Fatal("safety minimum fabricated usage", work, spend, err)
	}
	if _, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(a, "blocked-minimum", 0), accountingOptions()); err == nil {
		t.Fatal("known bound violation did not block normal continuation")
	}
}

func TestAdmissionDispatchVersusSealedProofRace(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	other, err := openTest(accountingContext, filepath.Dir(s.info.Path))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(a, "proof-race", 50), accountingOptions())
		errs <- err
	}()
	go func() {
		<-start
		_, err := other.SealAdmissionNonDispatch(accountingContext, admissionRun, a.AdmissionID, "race-proof", "NEVER_RELEASED", accountingOptions())
		errs <- err
	}()
	close(start)
	wins := 0
	for i := 0; i < 2; i++ {
		if <-errs == nil {
			wins++
		}
	}
	if wins != 1 || resultCount(t, s, "admission_dispatch_claims")+resultCount(t, s, "admission_non_dispatch_proofs") != 1 {
		t.Fatal("contradictory claim and proof both committed")
	}
	if countSchemaObject(t, s, "resource_reservations") != 0 {
		t.Fatal("race created ghost holds")
	}

}

func TestAdmissionExpiredClaimPersistsPendingStop(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	opts := accountingOptions()
	opts.At = admissionDeadline
	if created, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(a, "expired", 10), opts); created || err == nil {
		t.Fatal("expired release authorized", created, err)
	}
	requireStopClass(t, s, "UNPROVEN_CONTINUATION", "PENDING")
	if resultCount(t, s, "provider_calls") != 0 || countSchemaObject(t, s, "resource_reservations") != 0 {
		t.Fatal("deadline denial leaked release or lost holds")
	}
}

func TestAdmissionWorkerRequiresCurrentBoundContent(t *testing.T) {
	s := testStore(t)
	_, r, b, ownership := ownershipFixture(t, s, "developer-completed")
	if err := s.TransferSemanticOwnership(accountingContext, r.RunID, r.Role, &b, ownership); err != nil {
		t.Fatal(err)
	}
	p := admissionPolicy(100)
	p.RunID = r.RunID
	a := InvocationAdmission{AdmissionID: "worker-admission", RunID: r.RunID, Binding: b, HardDeadline: admissionDeadline,
		Fence: AdmissionFence{Owner: SemanticFence{Role: r.Role, InvocationID: &r.InvocationID, ContentIdentity: &r.ContentIdentity, OwnershipEpoch: 11, ActorRevision: 6}}}
	for _, resource := range budgetResources {
		account := ResourceAccount{PoolID: "worker-" + strings.ToLower(resource), RunID: r.RunID, Scope: "RUN", ResourceType: resource}
		if err := s.CreateResourcePool(accountingContext, ResourcePool{account, primaryTokenEligibility()}, accountingOptions()); err != nil {
			t.Fatal(err)
		}
		a.Resources = append(a.Resources, AdmissionResource{Account: account, Amount: 50})
	}
	if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	wrong := a
	wrong.Binding.ContentIdentity = "other-content"
	if _, err := s.AdmitInvocation(accountingContext, wrong, accountingOptions()); err == nil {
		t.Fatal("wrong target content admitted")
	}
	wrong = a
	wrong.Fence.Owner.ContentIdentity = nil
	if _, err := s.AdmitInvocation(accountingContext, wrong, accountingOptions()); err == nil {
		t.Fatal("bound owner admitted without content fence")
	}
	admit(t, s, a)
	// Immutable admission fences are rechecked at the physical release boundary.
	if err := s.ClearActiveInvocation(accountingContext, r.RunID, ownerOptions(ActiveActor{Role: r.Role, InvocationID: &r.InvocationID, OwnershipEpoch: 11, Revision: 6})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(a, "stale-worker-claim", 50), accountingOptions()); err == nil {
		t.Fatal("stale owner released work")
	}
	if resultCount(t, s, "provider_calls") != 0 {
		t.Fatal("stale release persisted physical call")
	}
}

func TestAdmissionEvidenceAndProofAuditFaultsRollBack(t *testing.T) {
	for _, operation := range []string{"release evidence", "non-dispatch"} {
		t.Run(operation, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 100)
			admit(t, s, a)
			bad := accountingOptions()
			bad.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp}}
			var created bool
			var err error
			if operation == "non-dispatch" {
				created, err = s.SealAdmissionNonDispatch(accountingContext, admissionRun, a.AdmissionID, "bad-proof", "NEVER_RELEASED", bad)
			} else {
				d := claimFor(a, "evidence-fault", 50)
				claim(t, s, d)
				created, err = s.RecordControllerReleaseEvidence(accountingContext, ControllerReleaseEvidence{RunID: admissionRun, ClaimID: d.ClaimID, EvidenceID: "bad-release", ReleasedAt: admissionDeadline}, bad)
			}
			if created || err == nil {
				t.Fatal("failed mutation reported new evidence", created, err)
			}
			for _, table := range []string{"admission_controller_release_evidence", "admission_non_dispatch_proofs", "run_hard_stops", "hard_stop_decisions"} {
				if resultCount(t, s, table) != 0 {
					t.Fatal("audit failure leaked", table)
				}
			}

		})
	}
}

func TestAdmissionNewCounterExhaustionRollsBack(t *testing.T) {
	t.Run("dispatch ordinal", func(t *testing.T) {
		s, a := configuredAdmissionFixture(t, 100)
		admit(t, s, a)
		d := claimFor(a, "ordinal-first", 1)
		claim(t, s, d)
		if err := s.FinishProviderCall(accountingContext, admissionRun, d.ProviderCall.ProviderCallID, 0, accountingOptions()); err != nil {
			t.Fatal(err)
		}
		seedRevision(t, s, "admission_dispatch_claims_no_update", `UPDATE admission_dispatch_claims SET claim_ordinal=?`)
		_, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(a, "ordinal-next", 1), accountingOptions())
		requireNumeric(t, err, "dispatch_claim_ordinal")
		if resultCount(t, s, "provider_calls") != 1 || resultCount(t, s, "admission_dispatch_claims") != 1 {
			t.Fatal("ordinal exhaustion leaked physical registration")
		}
	})
}

func TestAdmissionProjectionFailurePreservesAdmissionAndStop(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	dir := privateTempDir(t)
	if err := s.EnableMetricsProjection(accountingContext, admissionRun, dir); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(dir, ".metrics.lock")
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lock, 0700); err != nil {
		t.Fatal(err)
	}
	var projection *MetricsProjectionError
	var number *numeric.Error
	if _, err := s.AdmitInvocation(accountingContext, a, accountingOptions()); !errors.As(err, &projection) || errors.As(err, &number) {
		t.Fatal("wrong postcommit classification", err)
	}
	if resultCount(t, s, "invocation_admissions") != 1 || countSchemaObject(t, s, "resource_reservations") != 0 {
		t.Fatal("projection failure rolled back admission")
	}
	if ok, err := s.AdmitInvocation(accountingContext, a, accountingOptions()); ok || err != nil {
		t.Fatal("committed replay", ok, err)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	d := claimFor(a, "projection-call", 50)
	claim(t, s, d)
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lock, 0700); err != nil {
		t.Fatal(err)
	}
	err := s.AcceptUsageReceipt(accountingContext, admissionReceipt(d, "projection-usage", "partial", counter(120)), accountingOptions())
	if !errors.As(err, &projection) || errors.As(err, &number) {
		t.Fatal("projection error lost", err)
	}
	if resultCount(t, s, "usage_receipts") != 1 {
		t.Fatal("projection failure rolled back truthful receipt")
	}
	requireStopClass(t, s, "BUDGET_EXCEEDED", "OBSERVED_DISPATCH_BOUND_EXCEEDED")
}

const admissionRun = "admission-run"
const admissionDeadline = "2027-01-01T00:00:00Z"

func seedAdmissionRun(t *testing.T, s *StateStore) {
	t.Helper()
	execSQL(t, s.db, `INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES (?,'RUNNING',1,?,?)`, admissionRun, semanticTimestamp, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES (?,1,'RUNNING',?)`, admissionRun, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO run_active_actor VALUES(?,'LEAD',NULL,1,?,0)`, admissionRun, semanticTimestamp)
}
func seedAdmissionInvocation(t *testing.T, s *StateStore, id, execution string) InvocationBinding {
	t.Helper()
	execSQL(t, s.db, `INSERT OR IGNORE INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES(?,?,1,'LEAD','LEAD','RUNNING',?)`, execution, admissionRun, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES(?,?,?,1,?,'RUNNING',?)`, id+"-attempt", execution, admissionRun, resultCount(t, s, "attempts")+1, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch) VALUES(?,?,?,?,1,1,'LEAD','LEAD','fake','fake-model','high','RUNNING',?,1)`, id, id+"-attempt", execution, admissionRun, semanticTimestamp)
	execSQL(t, s.db, `UPDATE invocations SET content_identity='admission-content' WHERE invocation_id=?`, id)
	execSQL(t, s.db, `UPDATE run_active_actor SET active_invocation_id=?,revision=revision+1 WHERE run_id=?`, id, admissionRun)
	return InvocationBinding{InvocationID: id, ExecutionID: execution, AttemptID: id + "-attempt", Cycle: 1, Purpose: "LEAD", ContentIdentity: "admission-content"}
}
func seedControllerInvocation(t *testing.T, s *StateStore, id, execution string) InvocationBinding {
	t.Helper()
	execSQL(t, s.db, `INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES(?,?,1,'ORCHESTRATOR','RECOVER','RUNNING',?)`, execution, admissionRun, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES(?,?,?,1,1,'RUNNING',?)`, id+"-attempt", execution, admissionRun, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch) VALUES(?,?,?,?,1,1,'ORCHESTRATOR','RECOVER','fake','fake-model','high','RUNNING',?,1)`, id, id+"-attempt", execution, admissionRun, semanticTimestamp)
	return InvocationBinding{InvocationID: id, ExecutionID: execution, AttemptID: id + "-attempt", Cycle: 1, Purpose: "RECOVER"}
}
func primaryTokenEligibility() []ResourceEligibility {
	return []ResourceEligibility{{"LEAD", "LEAD"}, {"DEVELOPER", "IMPLEMENT"}, {"DEVELOPER", "REMEDIATE"}, {"DEVELOPER", "FINALIZE_IMPLEMENTATION"}, {"QA", "QA"}, {"REVIEWER", "REVIEW"}, {"REVIEWER", "FINALIZE_REVIEW"}, {"ORCHESTRATOR", "RECOVER"}}
}
func admissionPools(t *testing.T, s *StateStore, prefix string) []ResourceAccount {
	t.Helper()
	out := []ResourceAccount{}
	for _, r := range budgetResources {
		a := ResourceAccount{PoolID: prefix + "-" + r, RunID: admissionRun, Scope: "RUN", ResourceType: r}
		if tokenResource(r) {
			a.PoolID = "normal-" + strings.ToLower(r)
			var exists int
			if err := s.db.QueryRow(`SELECT count(*) FROM resource_pools WHERE pool_id=?`, a.PoolID).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if exists == 0 {
				if err := s.CreateResourcePool(accountingContext, ResourcePool{a, primaryTokenEligibility()}, accountingOptions()); err != nil {
					t.Fatal(err)
				}
			}
		}
		out = append(out, a)
	}
	return out
}
func admissionPolicy(hard int64) RunAdmissionPolicy {
	p := RunAdmissionPolicy{Confirmation: approvedBudget("fixture-budget-confirmation"), RunID: admissionRun, PolicyID: "configured-run-policy", HardDeadline: admissionDeadline}
	for _, r := range budgetResources {
		p.Budgets = append(p.Budgets, RunResourceBudget{ResourceType: r, HardLimit: hard, SoftLimit: counter(hard / 2)})
	}
	return p
}
func admissionFor(t *testing.T, s *StateStore, b InvocationBinding, pools []ResourceAccount, amount int64) InvocationAdmission {
	t.Helper()
	actor, err := s.GetActiveActor(accountingContext, admissionRun)
	if err != nil {
		t.Fatal(err)
	}
	fence := SemanticFence{Role: actor.Role, InvocationID: actor.InvocationID, OwnershipEpoch: actor.OwnershipEpoch, ActorRevision: actor.Revision}
	if actor.InvocationID != nil {
		var content string
		if err := s.db.QueryRow(`SELECT content_identity FROM invocations WHERE invocation_id=?`, *actor.InvocationID).Scan(&content); err != nil {
			t.Fatal(err)
		}
		fence.ContentIdentity = &content
	}
	a := InvocationAdmission{AdmissionID: b.InvocationID + "-admission", RunID: admissionRun, Binding: b, HardDeadline: admissionDeadline, Fence: AdmissionFence{Owner: fence}}
	for _, p := range pools {
		r := AdmissionResource{Account: p, Amount: amount}
		a.Resources = append(a.Resources, r)
	}
	return a
}
func admit(t *testing.T, s *StateStore, a InvocationAdmission) {
	t.Helper()
	created, err := s.AdmitInvocation(accountingContext, a, accountingOptions())
	if err != nil || !created {
		t.Fatal(created, err)
	}
}
func claimFor(a InvocationAdmission, id string, bound int64) AdmissionDispatchClaim {
	call := &ProviderCall{ProviderCallID: id + "-call", RunID: a.RunID, InvocationID: a.Binding.InvocationID, Provider: "fake", Model: "fake-model"}
	contextWindow := bound * 2
	if contextWindow <= 0 {
		contextWindow = 1
	}
	maxOutput := bound
	if maxOutput <= 0 {
		maxOutput = 1
	}
	d := AdmissionDispatchClaim{ClaimID: id, AdmissionID: a.AdmissionID, RunID: a.RunID, ProviderCall: call, Capabilities: &ProviderModelCapabilities{Provider: call.Provider, Model: call.Model, ContextWindowTokens: contextWindow, MaxOutputTokens: maxOutput}}
	for _, r := range budgetResources {
		d.Bounds = append(d.Bounds, DispatchBound{r, bound})
	}
	d.Bounds = append(d.Bounds, DispatchBound{"INPUT_TOKENS", max(bound-1, 0)}, DispatchBound{"OUTPUT_TOKENS", 1})
	return d
}

func setDispatchBound(d *AdmissionDispatchClaim, dimension string, amount int64) {
	for i := range d.Bounds {
		if d.Bounds[i].Dimension == dimension {
			d.Bounds[i].Amount = amount
			return
		}
	}
	d.Bounds = append(d.Bounds, DispatchBound{dimension, amount})
}
func claim(t *testing.T, s *StateStore, d AdmissionDispatchClaim) {
	t.Helper()
	created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions())
	if err != nil || !created {
		t.Fatal(created, err)
	}
	if created, err := s.AuthorizeAdmissionRelease(accountingContext, d.RunID, d.ClaimID, accountingOptions()); err != nil || !created {
		t.Fatal("pre-release gate", created, err)
	}
}
func admissionReceipt(d AdmissionDispatchClaim, id, state string, amount *int64) UsageReceipt {
	r := UsageReceipt{ReceiptID: id, RunID: d.RunID, InvocationID: d.ProviderCall.InvocationID, ProviderCallID: d.ProviderCall.ProviderCallID, Completeness: state, ReportedAt: semanticTimestamp, InputTokens: amount}
	if amount != nil {
		if *amount > 0 {
			r.InputTokens = counter(*amount - 1)
			r.OutputTokens = counter(1)
		} else {
			r.OutputTokens = counter(0)
		}
		r.CachedInputTokens = counter(0)
	}
	return r
}
func configuredAdmissionFixture(t *testing.T, hard int64) (*StateStore, InvocationAdmission) {
	s := testStore(t)
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	if err := s.ConfigureRunAdmission(accountingContext, admissionPolicy(hard), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	return s, admissionFor(t, s, seedAdmissionInvocation(t, s, "first", "execution-first"), pools, hard/2)
}
func requireStopClass(t *testing.T, s *StateStore, cause, class string) {
	t.Helper()
	stops, err := s.ListRunHardStops(accountingContext, admissionRun)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range stops {
		if v.Cause == cause && v.Classification == class {
			return
		}
	}
	t.Fatalf("missing %s/%s in %+v", cause, class, stops)
}

func TestAdmissionObservedBoundExceededIsNeutralAndDimensionSpecific(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := claimFor(a, "observed", 100)
	setDispatchBound(&d, "INPUT_TOKENS", 98)
	setDispatchBound(&d, "OUTPUT_TOKENS", 2)
	d.Capabilities.ContextWindowTokens = 100
	d.Capabilities.MaxOutputTokens = 2
	claim(t, s, d)
	r := admissionReceipt(d, "actual", "final", counter(120))
	r.OutputTokens = counter(3)
	acceptUsage(t, s, r)
	requireStopClass(t, s, "DISPATCH_BOUND_EXCEEDED", "OBSERVED_DISPATCH_BOUND_EXCEEDED")
	stops, _ := s.ListRunHardStops(accountingContext, admissionRun)
	for _, v := range stops {
		if v.Classification == "CONTROLLER_ADMISSION_BUG" {
			t.Fatal("guessed controller blame")
		}
	}
	// A large input with output within its bound does not violate that output bound.
	other, b := configuredAdmissionFixture(t, 1000)
	admit(t, other, b)
	e := claimFor(b, "dimensions", 500)
	setDispatchBound(&e, "INPUT_TOKENS", 498)
	setDispatchBound(&e, "OUTPUT_TOKENS", 2)
	e.Capabilities.ContextWindowTokens = 500
	e.Capabilities.MaxOutputTokens = 2
	claim(t, other, e)
	acceptUsage(t, other, admissionReceipt(e, "larger-input", "final", counter(300)))
	if got, _ := other.ListRunHardStops(accountingContext, admissionRun); len(got) != 0 {
		t.Fatal("compared WORK with OUTPUT bound", got)
	}
}

func TestAdmissionControllerReleaseViolationAndDeadlineReplay(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := claimFor(a, "released", 50)
	claim(t, s, d)
	if ok, err := s.RecordControllerReleaseEvidence(accountingContext, ControllerReleaseEvidence{RunID: admissionRun, ClaimID: d.ClaimID, EvidenceID: "release-proof", ReleasedAt: admissionDeadline}, accountingOptions()); !ok || err != nil {
		t.Fatal(ok, err)
	}
	requireStopClass(t, s, "CONTROLLER_RELEASE_VIOLATION", "CONTROLLER_ADMISSION_BUG")
	if ok, err := s.RecordControllerReleaseEvidence(accountingContext, ControllerReleaseEvidence{RunID: admissionRun, ClaimID: d.ClaimID, EvidenceID: "release-proof", ReleasedAt: admissionDeadline}, accountingOptions()); ok || err != nil {
		t.Fatal("release evidence replay", ok, err)
	}
	if _, err := s.RecordControllerReleaseEvidence(accountingContext, ControllerReleaseEvidence{RunID: admissionRun, ClaimID: d.ClaimID, EvidenceID: "release-proof", ReleasedAt: semanticTimestamp}, accountingOptions()); err == nil {
		t.Fatal("rewrote violation")
	}
	acceptUsage(t, s, admissionReceipt(d, "late-usage", "partial", nil))
}

func TestAdmissionNonDispatchProofIsExplicitAtomicAndSealed(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	if _, err := s.SealAdmissionNonDispatch(accountingContext, admissionRun, a.AdmissionID, "proof", "", accountingOptions()); err == nil {
		t.Fatal("absence manufactured proof")
	}

	if created, err := s.SealAdmissionNonDispatch(accountingContext, admissionRun, a.AdmissionID, "proof", "NEVER_RELEASED", accountingOptions()); !created || err != nil {
		t.Fatal(created, err)
	}

	if created, err := s.SealAdmissionNonDispatch(accountingContext, admissionRun, a.AdmissionID, "proof", "NEVER_RELEASED", accountingOptions()); created || err != nil {
		t.Fatal(created, err)
	}
	if _, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(a, "after-proof", 10), accountingOptions()); err == nil {
		t.Fatal("sealed admission dispatched")
	}
	// A claim is durable evidence of ambiguous dispatch even with all-NULL usage.
	other, b := configuredAdmissionFixture(t, 100)
	admit(t, other, b)
	d := claimFor(b, "claimed", 50)
	claim(t, other, d)
	acceptUsage(t, other, admissionReceipt(d, "no-counters", "partial", nil))
	if _, err := other.SealAdmissionNonDispatch(accountingContext, admissionRun, b.AdmissionID, "bad-proof", "NEVER_RELEASED", accountingOptions()); err == nil {
		t.Fatal("NULL treated as non-dispatch")
	}
}

func TestAdmissionCurrentActualAndCommitmentNotDoubleCounted(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := claimFor(a, "first", 50)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "known-partial", "partial", counter(40)))
	charge, err := admissionCharge(accountingContext, s.db, admissionRun, "WORK")
	if err != nil || charge.Total != 40 || charge.Known != 40 {
		t.Fatal("double charge or unknown-as-zero", charge, err)
	}

}

func TestAdmissionSerialRetryUsesOriginalCommitmentAndDeadline(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 200)
	admit(t, s, a)
	d := claimFor(a, "original", 30)
	claim(t, s, d)
	if _, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(a, "simultaneous", 10), accountingOptions()); err == nil {
		t.Fatal("parallel unresolved ProviderCall")
	}
	acceptUsage(t, s, admissionReceipt(d, "original-final", "final", counter(20)))
	if err := s.FinishProviderCall(accountingContext, admissionRun, d.ProviderCall.ProviderCallID, 0, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	next := claimFor(a, "fallback", 30)
	claim(t, s, next)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, next, AccountingOptions{At: "2028-01-01T00:00:00Z"}); created || err != nil {
		t.Fatal("replay redelivered", created, err)
	}
	if got := resultCount(t, s, "provider_calls"); got != 2 {
		t.Fatal(got)
	}
	if got := countSchemaObject(t, s, "resource_reservations"); got != 0 {
		t.Fatal("retry created budget/holds", got)
	}
	var deadline string
	s.db.QueryRow(`SELECT hard_deadline FROM execution_admission_deadlines`).Scan(&deadline)
	if deadline != admissionDeadline {
		t.Fatal("deadline renewed", deadline)
	}
}

func TestAdmissionConcurrentGlobalReservationsAndIndependentControllerClaims(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	pools := admissionPools(t, s, "other")
	b := admissionFor(t, s, seedAdmissionInvocation(t, s, "other", "other-execution"), pools, 60)
	for n := range a.Resources {
		a.Resources[n].Amount = 60
	}
	second, err := openTest(accountingContext, filepath.Dir(s.info.Path))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	start := make(chan struct{})
	results := make(chan error, 2)
	for n, ad := range []InvocationAdmission{a, b} {
		store := []*StateStore{s, second}[n]
		go func() {
			<-start
			_, err := store.AdmitInvocation(accountingContext, ad, accountingOptions())
			results <- err
		}()
	}
	close(start)
	success := 0
	for n := 0; n < 2; n++ {
		if err := <-results; err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatal("global race admitted", success)
	}
	if countSchemaObject(t, s, "resource_reservations") != 0 {
		t.Fatal("race leaked holds")
	}
	// Independent controller invocations can claim without consuming token grants.
	other, _ := configuredAdmissionFixture(t, 100)
	c := admissionFor(t, other, seedControllerInvocation(t, other, "controller-first", "controller-first-execution"), admissionPools(t, other, "controller-first"), 0)
	d := admissionFor(t, other, seedControllerInvocation(t, other, "concurrent", "concurrent-execution"), admissionPools(t, other, "concurrent"), 0)
	admit(t, other, c)
	admit(t, other, d)
	var wg sync.WaitGroup
	results = make(chan error, 2)
	for _, ad := range []InvocationAdmission{c, d} {
		wg.Add(1)
		go func(a InvocationAdmission) {
			defer wg.Done()
			claim := claimFor(a, a.AdmissionID, 0)
			claim.ProviderCall = nil
			claim.Capabilities = nil
			for n := range claim.Bounds {
				claim.Bounds[n].Amount = 0
			}
			_, err := other.ClaimAdmissionDispatch(accountingContext, claim, accountingOptions())
			results <- err
		}(ad)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if resultCount(t, other, "admission_dispatch_claims") != 2 || resultCount(t, other, "provider_calls") != 0 {
		t.Fatal("independent controller claims lost or invented provider calls")
	}
}

func TestAdmissionAtomicFaultAndNumericRollback(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	bad := accountingOptions()
	bad.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp}}
	if _, err := s.AdmitInvocation(accountingContext, a, bad); err == nil {
		t.Fatal("invalid audit committed")
	}
	if resultCount(t, s, "invocation_admissions") != 0 || countSchemaObject(t, s, "resource_reservations") != 0 || resultCount(t, s, "execution_admission_deadlines") != 0 {
		t.Fatal("fault leaked admission/deadline/holds")
	}
	admit(t, s, a)
	d := claimFor(a, "numeric", 50)
	claim(t, s, d)
	r := admissionReceipt(d, "overflow", "partial", counter(math.MaxInt64))
	r.InputTokens = counter(math.MaxInt64)
	r.OutputTokens = counter(1)
	var n *numeric.Error
	if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); !errors.As(err, &n) {
		t.Fatal("overflow accepted", err)
	}
	if resultCount(t, s, "usage_receipts") != 0 || resultCount(t, s, "run_hard_stops") != 0 {
		t.Fatal("invalid numeric mutation partially committed")
	}
	// Valid over-budget evidence plus its hard stop/audit must also roll back on fault.
	r = admissionReceipt(d, "would-stop", "final", counter(130))
	if err := s.AcceptUsageReceipt(accountingContext, r, bad); err == nil {
		t.Fatal("audit fault accepted")
	}
	if resultCount(t, s, "usage_receipts") != 0 || resultCount(t, s, "run_hard_stops") != 0 || resultCount(t, s, "hard_stop_decisions") != 0 {
		t.Fatal("hard stop or receipt leaked from failed transaction")
	}
}

func TestAdmissionFencingEnvelopeAndDeadlineFailures(t *testing.T) {
	for _, name := range []string{"owner", "semantic", "run", "execution", "attempt", "invocation", "ancestry", "pool", "deadline"} {
		t.Run(name, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 100)
			switch name {
			case "owner":
				a.Fence.Owner.OwnershipEpoch++
			case "semantic":
				a.Fence.SemanticRevision++
			case "run":
				a.Fence.RunRevision++
			case "execution":
				a.Fence.ExecutionRevision++
			case "attempt":
				a.Fence.AttemptRevision++
			case "invocation":
				a.Fence.InvocationRevision++
			case "ancestry":
				a.Binding.AttemptID = "other"
			case "pool":
				a.Resources[0].Account.Scope = "ATTEMPT"
			case "deadline":
				a.HardDeadline = "2028-01-01T00:00:00Z"
			}
			if _, err := s.AdmitInvocation(accountingContext, a, accountingOptions()); err == nil {
				t.Fatal("invalid binding accepted")
			}
			if countSchemaObject(t, s, "resource_reservations") != 0 {
				t.Fatal("invalid admission leaked hold")
			}
		})
	}
}

func TestAdmissionUnknownUsageKeepsHold(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := claimFor(a, "unknown", 50)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "unknown-partial", "partial", nil))
	var amount sql.NullInt64
	if err := s.db.QueryRow(`SELECT work_amount FROM provider_call_usage`).Scan(&amount); err != nil || amount.Valid {
		t.Fatal("NULL became zero", amount, err)
	}
	charge, err := admissionCharge(accountingContext, s.db, admissionRun, "WORK")
	if err != nil || charge.Total != 0 {
		t.Fatal("unknown became consumption", charge, err)
	}
	if _, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(a, "unknown-retry", 1), accountingOptions()); err == nil {
		t.Fatal("unknown authorized continuation")
	}
}

func TestAdmissionFinishVersusClaimSerialization(t *testing.T) {
	for _, order := range []string{"claim-first", "finish-first", "race"} {
		t.Run(order, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 200)
			admit(t, s, a)
			d := claimFor(a, "first", 30)
			claim(t, s, d)
			acceptUsage(t, s, admissionReceipt(d, "first-final", "final", counter(20)))
			next := claimFor(a, "next", 30)
			finish := func() error {
				return s.FinishProviderCall(accountingContext, admissionRun, d.ProviderCall.ProviderCallID, 1, accountingOptions())
			}
			register := func() error {
				_, err := s.ClaimAdmissionDispatch(accountingContext, next, accountingOptions())
				return err
			}
			switch order {
			case "claim-first":
				if register() == nil {
					t.Fatal("unresolved call admitted successor")
				}
				if err := finish(); err != nil {
					t.Fatal(err)
				}
				claim(t, s, next)
			case "finish-first":
				if err := finish(); err != nil {
					t.Fatal(err)
				}
				claim(t, s, next)
			case "race":
				start := make(chan struct{})
				ended := make(chan error, 1)
				registered := make(chan error, 1)
				go func() { <-start; ended <- finish() }()
				go func() { <-start; registered <- register() }()
				close(start)
				if err := <-ended; err != nil {
					t.Fatal(err)
				}
				if err := <-registered; err != nil {
					claim(t, s, next)
				}
			}
			var unresolved int
			if err := s.db.QueryRow(`SELECT count(*) FROM provider_calls WHERE duration_ns IS NULL`).Scan(&unresolved); err != nil || unresolved != 1 {
				t.Fatal(unresolved, err)
			}
			if resultCount(t, s, "admission_dispatch_claims") != 2 || resultCount(t, s, "provider_calls") != 2 {
				t.Fatal("serialization duplicated claim/call")
			}
		})
	}
}

func TestAdmissionFreshContinuationCannotRenewDeadlineOrBudget(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 200)
	admit(t, s, a)
	if _, err := s.SealAdmissionNonDispatch(accountingContext, admissionRun, a.AdmissionID, "cancelled", "NEVER_RELEASED", accountingOptions()); err != nil {
		t.Fatal(err)
	}
	b := admissionFor(t, s, seedAdmissionInvocation(t, s, "continuation", a.Binding.ExecutionID), admissionPools(t, s, "continuation"), 50)
	b.HardDeadline = "2026-12-31T00:00:00Z" // even shortening changes the recorded execution contract
	if _, err := s.AdmitInvocation(accountingContext, b, accountingOptions()); err == nil {
		t.Fatal("fresh continuation changed original deadline")
	}
	b.HardDeadline = admissionDeadline
	admit(t, s, b)
	if err := s.ConfigureRunAdmission(accountingContext, admissionPolicy(400), accountingOptions()); err == nil {
		t.Fatal("configuration replay increased finite budget")
	}
	var hard int64
	if err := s.db.QueryRow(`SELECT hard_limit FROM run_resource_budgets WHERE resource_type='WORK'`).Scan(&hard); err != nil || hard != 200 {
		t.Fatal(hard, err)
	}
}
