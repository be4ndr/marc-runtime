package statestore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"ai-control-plane/internal/identity"
	"ai-control-plane/src/internal/roleresult"
)

func TestB2AdmissionRejectsMalformedPoolIDsAtomically(t *testing.T) {
	for _, id := range []string{"Uppercase", "under_score", "bad--id", strings.Repeat("a", 257), ""} {
		t.Run(fmt.Sprintf("id-%q", id), func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 100)
			a.Resources[0].Account.PoolID = id
			before := mutationDatabaseSnapshot(t, s)
			if created, err := s.AdmitInvocation(accountingContext, a, accountingOptions()); created || err == nil {
				t.Fatal("malformed accounting pool accepted", created, err)
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("rejection changed authoritative state")
			}
		})
	}
	s, a := configuredAdmissionFixture(t, 100)
	a.AdmissionID = strings.Repeat("a", 256)
	admit(t, s, a)
}

func TestB2RequestTopUpUsesRunQualifiedIdentity(t *testing.T) {
	for _, requested := range []int64{100, 1} {
		t.Run(fmt.Sprintf("requested-%d", requested), func(t *testing.T) {
			w := proofWorkflowFixture(t)
			s := w.s
			request := requestFixture(w.state)
			request["requested"] = map[string]any{"work_tokens": requested}
			request["target"].(map[string]any)["role"] = "DEVELOPER"
			request["target"].(map[string]any)["purpose"] = "IMPLEMENT"
			b := authorityBoundary(t, s, w.worker.RunID, w.lead)
			if err := s.AcceptResourceRequest(accountingContext, wire(t, request), MutationOptions{Fence: ResultFence{b.Owner.OwnershipEpoch, b.Owner.ActorRevision}, ExpectedSemanticRevision: b.SemanticRevision}); err != nil {
				t.Fatal(err)
			}
			actor, err := s.GetActiveActor(accountingContext, w.worker.RunID)
			if err != nil {
				t.Fatal(err)
			}
			execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch)
 SELECT 'continuation',attempt_id,execution_id,run_id,cycle_no,2,role,purpose,provider,model,reasoning_effort,'RUNNING',started_at,content_identity,? FROM invocations WHERE invocation_id=?`, actor.OwnershipEpoch+1, w.worker.InvocationID)
			bound := w.workerAdmission.Binding
			bound.InvocationID = "continuation"
			if err := s.TransferSemanticOwnership(accountingContext, w.worker.RunID, "DEVELOPER", &bound, ownerOptions(actor)); err != nil {
				t.Fatal(err)
			}
			b = authorityBoundary(t, s, w.worker.RunID, bound)
			a := InvocationAdmission{AdmissionID: "continuation-admission", RunID: w.worker.RunID, Binding: bound, HardDeadline: admissionDeadline, Fence: AdmissionFence{Owner: b.Owner, SemanticRevision: b.SemanticRevision}}
			a.Resources = []AdmissionResource{{Account: w.pools[0], Amount: 50}}
			admit(t, s, a)
			id := request["request_id"].(string)
			g := ResourceGrant{Account: w.pools[0], AllocationID: "accepted-request-topup", Amount: 5, Source: "lead-request", RequestID: &id, AdmissionID: a.AdmissionID}
			// A previously created allocation must also lose request authority
			// when another grant closes the request.
			execSQL(t, s.db, `INSERT INTO resource_allocations(allocation_id,run_id,resource_type,amount,source,created_at,pool_id,request_id)
 VALUES('pending-unlinked-allocation',?,'WORK',5,'lead-request',?,?,?)`, a.RunID, semanticTimestamp, g.Account.PoolID, requestStorageKey(a.RunID, id))
			if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err != nil {
				t.Fatal("accepted logical request could not fund its active target", err)
			}
			var status string
			if err := s.db.QueryRow(`SELECT status FROM resource_requests WHERE request_id=?`, requestStorageKey(a.RunID, id)).Scan(&status); err != nil || status != "GRANTED" {
				t.Fatal("request remained undecided", status, err)
			}
			decided := mutationDatabaseSnapshot(t, s)
			repeat := g
			repeat.AllocationID = "duplicate-request-grant"
			if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{repeat}, accountingOptions()); err == nil {
				t.Fatal("decided request funded another grant")
			}
			if !reflect.DeepEqual(decided, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("duplicate decision changed history")
			}
			for _, stmt := range []string{
				`INSERT INTO resource_allocations(allocation_id,run_id,resource_type,amount,source,created_at,pool_id,request_id)
 SELECT 'sql-repeat-allocation',run_id,resource_type,amount,source,created_at,pool_id,request_id FROM resource_allocations WHERE allocation_id='accepted-request-topup'`,
				`INSERT INTO admission_grants VALUES('continuation-admission','WORK','pending-unlinked-allocation')`,
			} {
				if _, err := s.db.Exec(stmt); err == nil {
					t.Fatal("SQL reused GRANTED request", stmt)
				}
				if !reflect.DeepEqual(decided, mutationDatabaseSnapshot(t, s)) {
					t.Fatal("SQL rejection changed history")
				}
			}
			s = restartAgentStore(t, s)
			if _, err := s.db.Exec(`INSERT INTO admission_grants VALUES('continuation-admission','WORK','pending-unlinked-allocation')`); err == nil {
				t.Fatal("restart restored GRANTED request authority")
			}
			var stored string
			if err := s.db.QueryRow(`SELECT request_id FROM resource_allocations WHERE allocation_id=?`, g.AllocationID).Scan(&stored); err != nil || stored != requestStorageKey(a.RunID, id) {
				t.Fatal("grant lost qualified request provenance", stored, err)
			}
			limits, err := s.GetAgentTokenLimits(accountingContext, a.RunID, a.AdmissionID)
			if err != nil || limits.HardLimit != 55 {
				t.Fatal("top-up did not increase the active grant", limits, err)
			}
			// A request's presence in another Run cannot supply this target's authority.
			execSQL(t, s.db, `INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES('foreign','RUNNING',1,'now','now')`)
			before := mutationDatabaseSnapshot(t, s)
			g.AllocationID = "foreign-request-topup"
			if err := s.GrantResources(accountingContext, "foreign", []ResourceGrant{g}, accountingOptions()); err == nil {
				t.Fatal("foreign Run accepted the request/grant")
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("foreign rejection changed state")
			}
		})
	}
}

func TestB2SealedArtifactIDsRemainReferencable(t *testing.T) {
	s, a := recoveryFixture(t)
	for _, id := range []string{strings.Repeat("a", 256), strings.Repeat("a", 255) + "b"} {
		p := recoveryContract(t, s, a, "support-"+id[len(id)-1:])
		p.ProofID = id
		ref := sealProof(t, s, p)
		var eid string
		if err := s.db.QueryRow(`SELECT evidence_id FROM authority_proofs WHERE proof_id=?`, id).Scan(&eid); err != nil {
			t.Fatal(err)
		}
		if !identity.ValidID(eid) {
			t.Fatal("derived proof evidence ID violates its reference contract", eid)
		}
		if err := verifyAuthorityArtifact(accountingContext, s.db, ProofArtifact{eid, "authority_proof", ref.SHA256}); err != nil {
			t.Fatal("proof artifact cannot be referenced", err)
		}
		if _, err := s.SealAuthorityProof(accountingContext, p, accountingOptions()); err != nil {
			t.Fatal("max-length proof replay", err)
		}
	}
	w := proofWorkflowFixture(t, "neutral")
	stops, err := w.s.ListRunHardStops(accountingContext, w.worker.RunID)
	if err != nil || len(stops) == 0 {
		t.Fatal("missing hard-stop fixture", err)
	}
	h := humanContract(t, w, stops[0], "short-support", "ALLOW_TERMINAL_EVALUATION")
	h.DispositionID = strings.Repeat("h", 256)
	if err := w.s.RecordHumanReadinessDisposition(accountingContext, h, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	var eid, digest string
	if err := w.s.db.QueryRow(`SELECT h.evidence_id,e.sha256 FROM human_readiness_dispositions h JOIN evidence_artifacts e USING(evidence_id) WHERE disposition_id=?`, h.DispositionID).Scan(&eid, &digest); err != nil {
		t.Fatal(err)
	}
	if !identity.ValidID(eid) || verifyAuthorityArtifact(accountingContext, w.s.db, ProofArtifact{eid, "human_readiness_disposition", digest}) != nil {
		t.Fatal("derived human-response artifact cannot be referenced", eid)
	}
	w.s = restartAgentStore(t, w.s)
	if _, err := loadHumanDisposition(accountingContext, w.s.db, h.DispositionID); err != nil {
		t.Fatal("max-length human response lost on restart", err)
	}
}

func TestB2HardStopReplayRejectsChangedFactsAndDecisions(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	persist := func(resource string, amount, limit *int64, at string) error {
		return s.numericMutation(accountingContext, "stop-fixture", a.RunID, nil, func(c *sql.Conn) error {
			return persistHardStop(accountingContext, c, a.RunID, resource, "DISPATCH_BOUND_EXCEEDED", "immutable-fixture", amount, limit, at)
		})
	}
	if err := persist("WORK", counter(110), counter(100), semanticTimestamp); err != nil {
		t.Fatal(err)
	}
	t.Run("facts", func(t *testing.T) {
		before := mutationDatabaseSnapshot(t, s)
		if err := persist("WORK", counter(110), counter(100), semanticTimestamp); err != nil {
			t.Fatal("identical stop replay rejected", err)
		}
		for _, changed := range []struct {
			resource      string
			amount, limit *int64
			at            string
		}{{"WORK", counter(111), counter(100), semanticTimestamp}, {"WORK", nil, counter(100), semanticTimestamp}, {"WORK", counter(110), counter(99), semanticTimestamp}, {"", counter(110), counter(100), semanticTimestamp}, {"WORK", counter(110), counter(100), "2026-10-05T12:00:01Z"}} {
			if err := persist(changed.resource, changed.amount, changed.limit, changed.at); err == nil {
				t.Fatal("contradictory stop replay accepted")
			}
		}
		if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
			t.Fatal("stop replay altered state/audit")
		}
	})
	stop := stopIdentity(a.RunID, "DISPATCH_BOUND_EXCEEDED", "immutable-fixture")
	resolve := func(class, at string) error {
		return s.numericMutation(accountingContext, "decision-fixture", a.RunID, nil, func(c *sql.Conn) error {
			return resolveHardStop(accountingContext, c, stop, class, at)
		})
	}
	t.Run("decisions", func(t *testing.T) {
		// Reconciliation can resolve a supported stop automatically. Replay its
		// actual immutable decision rather than replacing it with test assumptions.
		var class, at string
		if err := s.db.QueryRow(`SELECT classification,decided_at FROM hard_stop_decisions WHERE stop_id=? AND ordinal=1`, stop).Scan(&class, &at); errors.Is(err, sql.ErrNoRows) {
			class, at = "OBSERVED_DISPATCH_BOUND_EXCEEDED", semanticTimestamp
			if err := resolve(class, at); err != nil {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		before := mutationDatabaseSnapshot(t, s)
		if err := resolve(class, at); err != nil {
			t.Fatal("identical decision replay rejected", err)
		}
		other := "CONTROLLER_ADMISSION_BUG"
		if class == other {
			other = "OBSERVED_DISPATCH_BOUND_EXCEEDED"
		}
		if err := resolve(other, at); err == nil {
			t.Fatal("contradictory classification accepted")
		}
		if err := resolve(class, "2026-10-05T12:00:01Z"); err == nil {
			t.Fatal("changed decision time accepted")
		}
		if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
			t.Fatal("decision replay altered history")
		}
	})
}

func TestB2ResolutionRequiresAcceptedRunEvidence(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(fmt.Sprintf("foreign-%t", foreign), func(t *testing.T) {
			s := testStore(t)
			doc, opts := semanticFixture(t, s)
			ref := authorityArtifact(t, s, "unrelated-proof", "test_result", []byte("unrelated"))
			if foreign {
				seedInvocation(t, s)
				seedResultSQL := `INSERT INTO role_results(result_id,run_id,cycle_no,result_ordinal,execution_id,attempt_id,invocation_id,role,outcome,summary,content_identity,produced_at) VALUES('foreign-result','r1',1,1,'e1','a1','i1','DEVELOPER','completed','fixture','content1','now')`
				execSQL(t, s.db, seedResultSQL)
				execSQL(t, s.db, `INSERT INTO role_result_evidence(result_id,evidence_id) VALUES('foreign-result',?)`, ref.EvidenceID)
			}
			f := doc["open_findings"].([]any)[0].(map[string]any)
			f["resolution_ref"] = map[string]any{"evidence_id": ref.EvidenceID}
			doc["open_findings"], doc["resolved_findings"] = []any{}, []any{f}
			before := mutationDatabaseSnapshot(t, s)
			if err := s.AcceptLeadState(accountingContext, wire(t, doc), opts); err == nil {
				t.Fatal("unrelated resolution evidence accepted")
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("resolution rejection changed authority/audit")
			}
			// Same-Run accepted evidence supplies provenance, while Lead still
			// owns the semantic determination that it resolves the finding.
			f["resolution_ref"] = map[string]any{"evidence_id": "proof"}
			if err := s.AcceptLeadState(accountingContext, wire(t, doc), opts); err != nil {
				t.Fatal("accepted Run evidence rejected", err)
			}
		})
	}
}

func TestB2LeadStatusRequiresAcceptedResult(t *testing.T) {
	for _, role := range []string{"qa", "review"} {
		for _, status := range map[string][]string{"qa": {"passed", "failed", "inconclusive", "not_run"}, "review": {"approved", "changes_required", "inconclusive"}}[role] {
			t.Run(role+"-"+status, func(t *testing.T) {
				s := testStore(t)
				doc, opts := semanticFixture(t, s)
				doc[role] = map[string]any{"status": status}
				before := mutationDatabaseSnapshot(t, s)
				if err := s.AcceptLeadState(accountingContext, wire(t, doc), opts); err == nil {
					t.Fatal("unsupported semantic status accepted")
				}
				if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
					t.Fatal("status rejection changed authority/audit")
				}
			})
		}
	}
}

func TestB2GrantRejectsForeignRunAndPoolProvenance(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	execSQL(t, s.db, `INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES('foreign','RUNNING',1,'now','now');
 INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES('foreign',1,'RUNNING','now')`)
	foreign := ResourceAccount{PoolID: "foreign-work", RunID: "foreign", Scope: "RUN", ResourceType: "WORK"}
	if err := s.CreateResourcePool(accountingContext, ResourcePool{foreign, primaryTokenEligibility()}, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	policy := admissionPolicy(100)
	policy.RunID, policy.PolicyID = "foreign", "foreign-run-policy"
	policy.Confirmation = approvedBudget("foreign-confirmation")
	if err := s.ConfigureRunAdmission(accountingContext, policy, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	other := foreign
	other.RunID = a.RunID
	for _, account := range []ResourceAccount{foreign, other} {
		before := mutationDatabaseSnapshot(t, s)
		g := ResourceGrant{Account: account, AdmissionID: a.AdmissionID, AllocationID: "invalid-provenance", Amount: 1, Source: "fixture"}
		if err := s.GrantResources(accountingContext, account.RunID, []ResourceGrant{g}, accountingOptions()); err == nil {
			t.Fatal("foreign grant relationship accepted")
		}
		if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
			t.Fatal("provenance rejection changed authority/audit")
		}
	}
}

func TestB2LeadRejectsSupersededQAAndReviewerVerdicts(t *testing.T) {
	for _, role := range []string{"qa", "review"} {
		t.Run(role, func(t *testing.T) {
			w := proofWorkflowFixture(t)
			positive, inconclusive := "qa-passed", "qa-inconclusive"
			if role == "review" {
				positive, inconclusive = "reviewer-approved", "reviewer-inconclusive"
			}
			old, _ := w.addWorker(t, positive, "first-verdict", 4)
			w.accept(t, old)
			later, _ := w.addWorker(t, inconclusive, "later-verdict", 5)
			w.accept(t, later)
			var qa, review *roleresult.Result
			if role == "qa" {
				qa = &later
			} else {
				review = &later
			}
			w.bindLead(t, "current-lead", qa, review, false)
			doc := cloneDoc(t, w.state)
			doc["lead_state_id"] = "stale-verdict-lead"
			doc[role] = map[string]any{"status": old.Outcome, "result_ref": map[string]any{"result_id": old.ResultID}}
			b := authorityBoundary(t, w.s, old.RunID, w.lead)
			opts := MutationOptions{Fence: ResultFence{b.Owner.OwnershipEpoch, b.Owner.ActorRevision}, ExpectedSemanticRevision: b.SemanticRevision, Audit: fixtureAudit()}
			before := mutationDatabaseSnapshot(t, w.s)
			if err := w.s.AcceptLeadState(accountingContext, wire(t, doc), opts); err == nil {
				t.Fatal("older positive verdict replaced later inconclusive")
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, w.s)) {
				t.Fatal("stale verdict changed state/audit")
			}
			doc[role] = map[string]any{"status": later.Outcome, "result_ref": map[string]any{"result_id": later.ResultID}}
			if err := w.s.AcceptLeadState(accountingContext, wire(t, doc), opts); err != nil {
				t.Fatal("latest inconclusive rejected", err)
			}
			w.s = restartAgentStore(t, w.s)
			current, err := w.s.GetCurrentSemanticState(accountingContext, old.RunID)
			if err != nil || current.LeadStateID == nil || *current.LeadStateID != "stale-verdict-lead" {
				t.Fatal("latest decision lost on restart", current, err)
			}
		})
	}
}

func TestB2BackwardsResultOrdinalCannotHideLaterVerdict(t *testing.T) {
	w := proofWorkflowFixture(t)
	old, _ := w.addWorker(t, "qa-passed", "first-verdict", 5)
	w.accept(t, old)
	later, _ := w.addWorker(t, "qa-inconclusive", "backwards-verdict", 4)
	before := mutationDatabaseSnapshot(t, w.s)
	actor, err := w.s.GetActiveActor(accountingContext, later.RunID)
	if err != nil {
		t.Fatal(err)
	}
	raw := map[string]any{}
	// Preserve the real inconclusive wire body while replacing only identity.
	if err := json.Unmarshal(resultFixture(t, "qa-inconclusive"), &raw); err != nil {
		t.Fatal(err)
	}
	raw["result_id"], raw["result_ordinal"] = later.ResultID, later.ResultOrdinal
	raw["invocation_id"], raw["execution_id"], raw["attempt_id"] = later.InvocationID, later.ExecutionID, later.AttemptID
	if err := w.s.AcceptRoleResult(accountingContext, wire(t, raw), nil, ResultFence{actor.OwnershipEpoch, actor.Revision}); err == nil {
		t.Fatal("backwards result accepted")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, w.s)) {
		t.Fatal("backwards rejection changed state/ownership")
	}
}

func TestB2TerminalCannotResealOlderPositiveRoleHistory(t *testing.T) {
	for _, role := range []string{"qa", "reviewer"} {
		t.Run(role, func(t *testing.T) {
			w, p := terminalFixture(t)
			later, _ := w.addWorker(t, role+"-inconclusive", "later-verdict", 6)
			w.accept(t, later)
			// Retain the old selected approval, but make every controller proof
			// and ownership fence current. Fresh sealing must not make its old
			// positive QA/review references authoritative again.
			actor, err := w.s.GetActiveActor(accountingContext, p.RunID)
			if err != nil {
				t.Fatal(err)
			}
			execSQL(t, w.s.db, `UPDATE invocations SET ownership_epoch=? WHERE invocation_id=?`, actor.OwnershipEpoch, w.lead.InvocationID)
			if err := w.s.BindActiveInvocation(accountingContext, p.RunID, w.lead, ownerOptions(actor)); err != nil {
				t.Fatal(err)
			}
			for n, ref := range []*ProofReference{&p.Terminal.Completion, &p.Terminal.QAValidation, &p.Terminal.ReviewValidation, &p.Terminal.Containment} {
				_, original, err := loadAuthorityProof(accountingContext, w.s.db, ref.ProofID)
				if err != nil {
					t.Fatal(err)
				}
				request := original.Request
				request.ProofID = fmt.Sprintf("fresh-controller-%d", n)
				request.Boundary = authorityBoundary(t, w.s, p.RunID, request.Boundary.Binding)
				*ref = sealProof(t, w.s, request)
			}
			p.ProofID = "old-verdict-new-boundary"
			p.Boundary = authorityBoundary(t, w.s, p.RunID, w.lead)
			before := mutationDatabaseSnapshot(t, w.s)
			if _, err := w.s.SealAuthorityProof(accountingContext, p, accountingOptions()); err == nil || !strings.Contains(err.Error(), "freshness") {
				t.Fatal("terminal accepted old verdicts or failed for another reason", err)
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, w.s)) {
				t.Fatal("stale terminal attempt changed authority/audit")
			}
		})
	}
}

func breakB2Projection(t *testing.T, s *StateStore, run string) string {
	t.Helper()
	dir := privateTempDir(t)
	if err := s.EnableMetricsProjection(accountingContext, run, dir); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(dir, ".metrics.lock")
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lock, 0700); err != nil {
		t.Fatal(err)
	}
	return lock
}

func TestB2CommittedProjectionFailureRetainsCreatedOutcome(t *testing.T) {
	for _, operation := range []string{"admit", "claim", "non-dispatch", "release"} {
		t.Run(operation, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 100)
			if operation != "admit" {
				admit(t, s, a)
			}
			d := tokenClaim(a, "projection-claim", 10)
			if operation == "release" {
				claim(t, s, d)
			}
			lock := breakB2Projection(t, s, a.RunID)
			invoke := func() (bool, error) {
				switch operation {
				case "admit":
					return s.AdmitInvocation(accountingContext, a, accountingOptions())
				case "claim":
					return s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions())
				case "non-dispatch":
					return s.SealAdmissionNonDispatch(accountingContext, a.RunID, a.AdmissionID, "never-released", "NEVER_RELEASED", accountingOptions())
				default:
					return s.RecordControllerReleaseEvidence(accountingContext, ControllerReleaseEvidence{RunID: a.RunID, ClaimID: d.ClaimID, EvidenceID: "release-observation", ReleasedAt: semanticTimestamp}, accountingOptions())
				}
			}
			created, err := invoke()
			var projection *MetricsProjectionError
			if !created || !errors.As(err, &projection) {
				t.Fatal("committed creation reported as absent/rolled back", created, err)
			}
			if _, ok := err.(*MetricsProjectionError); !ok {
				t.Fatal("plain projection failure lost its existing concrete error type", err)
			}
			before := mutationDatabaseSnapshot(t, s)
			if created, err := invoke(); created || err != nil {
				t.Fatal("replay recreated authority", created, err)
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("replay changed state/audit")
			}
			if err := os.Remove(lock); err != nil {
				t.Fatal(err)
			}
			if err := s.RefreshMetrics(accountingContext, a.RunID); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("projection refresh changed authority")
			}
		})
	}
}

func TestB2ProjectionFailureCannotHideDenialOrRollback(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	breakB2Projection(t, s, a.RunID)
	opts := accountingOptions()
	opts.At = admissionDeadline
	created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "expired-with-projection", 10), opts)
	var projection *MetricsProjectionError
	var denied *AdmissionDeniedError
	if created || !errors.As(err, &projection) || !errors.As(err, &denied) {
		t.Fatal("projection failure hid committed denial", created, err)
	}
	if resultCount(t, s, "admission_dispatch_claims") != 0 || resultCount(t, s, "run_hard_stops") != 1 {
		t.Fatal("denial created dispatch or lost deadline evidence")
	}

	s, a = configuredAdmissionFixture(t, 100)
	breakB2Projection(t, s, a.RunID)
	opts = accountingOptions()
	opts.Audit = []AuditRow{{EventType: "fixture", ActorRole: "ORCHESTRATOR", OccurredAt: "invalid"}}
	before := mutationDatabaseSnapshot(t, s)
	created, err = s.AdmitInvocation(accountingContext, a, opts)
	projection = nil
	if created || err == nil || errors.As(err, &projection) {
		t.Fatal("rolled-back creation reported committed", created, err)
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("audit failure did not roll back creation")
	}
}

func TestB2TokenBankGoSQLAgreementAcrossUsageBoundaries(t *testing.T) {
	for _, budget := range []int64{1, 9, 10, 99, 100, 999, math.MaxInt64 / 11 * 10} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, budget)
			a.Resources[0].Amount = budget
			admit(t, s, a)
			d := tokenClaim(a, "property-call", max(budget/2, 1))
			claim(t, s, d)
			for n, used := range []*int64{nil, counter(0), counter(budget / 2), counter(budget - 1), counter(budget), counter(budget + 1), counter(budget + budget/10 + 1)} {
				r := admissionReceipt(d, fmt.Sprintf("property-%d", n), "partial", used)
				acceptUsage(t, s, r)
				amount := int64(0)
				if used != nil {
					amount = *used
				}
				overrun := int64(0)
				if amount > budget {
					overrun = amount - budget
				}
				requireTokenBank(t, s, a.RunID, RunTokenBalance{budget, amount, budget - amount, budget / 10, overrun, budget/10 - overrun, budget + budget/10})
			}
		})
	}
}

func TestB2TokenBankGoSQLAgreementWithLateContingencyUsage(t *testing.T) {
	s := testStore(t)
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	if err := s.ConfigureRunAdmission(accountingContext, admissionPolicy(100), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	emergency := admissionFor(t, s, seedControllerInvocation(t, s, "emergency", "emergency-execution"), pools, 10)
	emergency.Kind, emergency.ContingencyReason = "CONTAINMENT", "Actual Orchestrator emergency completion"
	admit(t, s, emergency)
	d := tokenClaim(emergency, "emergency-observed", 10)
	claim(t, s, d)
	if err := s.FinishProviderCall(accountingContext, emergency.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	a := admissionFor(t, s, seedAdmissionInvocation(t, s, "normal", "normal-execution"), pools, 50)
	admit(t, s, a)
	normal := tokenClaim(a, "normal-observed", 50)
	claim(t, s, normal)
	acceptUsage(t, s, admissionReceipt(normal, "normal-late", "final", counter(105)))
	for n, used := range []int64{0, 3, 5, 10, 11, 20} {
		acceptUsage(t, s, admissionReceipt(d, fmt.Sprintf("emergency-late-%d", n), "partial", counter(used)))
		requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 105, -5, 10, 5 + used, 5 - used, 110})
	}
	s = restartAgentStore(t, s)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 105, -5, 10, 25, -15, 110})
}
