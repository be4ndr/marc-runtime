package statestore

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"ai-control-plane/src/internal/roleresult"
)

func authorityArtifact(t *testing.T, s *StateStore, id, kind string, raw []byte) ProofArtifact {
	t.Helper()
	sha := fmt.Sprintf("%x", sha256.Sum256(raw))
	execSQL(t, s.db, `INSERT INTO evidence_artifacts VALUES(?,?,'sqlite',?,?,?,NULL,?)`, id, kind, sha, int64(len(raw)), raw, semanticTimestamp)
	return ProofArtifact{id, kind, sha}
}
func authorityBoundary(t *testing.T, s *StateStore, run string, b InvocationBinding) ProofBoundary {
	t.Helper()
	a, err := s.GetActiveActor(accountingContext, run)
	if err != nil {
		t.Fatal(err)
	}
	f := SemanticFence{Role: a.Role, InvocationID: a.InvocationID, OwnershipEpoch: a.OwnershipEpoch, ActorRevision: a.Revision}
	if a.InvocationID != nil {
		var content string
		if err = s.db.QueryRow(`SELECT content_identity FROM invocations WHERE invocation_id=?`, *a.InvocationID).Scan(&content); err != nil {
			t.Fatal(err)
		}
		f.ContentIdentity = &content
	}
	current, err := s.GetCurrentSemanticState(accountingContext, run)
	if err != nil {
		t.Fatal(err)
	}
	return ProofBoundary{b, f, current.Revision}
}
func recoveryContract(t *testing.T, s *StateStore, a InvocationAdmission, id string) AuthorityProof {
	t.Helper()
	ref := authorityArtifact(t, s, id+"-containment", "controller_containment", []byte(`{"observation":"owned local execution has been contained"}`))
	return AuthorityProof{ProofID: id, RunID: a.RunID, Kind: "RECOVERY", ContentIdentity: a.Binding.ContentIdentity, Verdict: "RECOVERABLE", Reason: "confirmed containment; accounting retained", Boundary: authorityBoundary(t, s, a.RunID, a.Binding), Recovery: &RecoveryProof{Containment: "CONFIRMED", ContainmentEvidence: []ProofArtifact{ref}}}
}
func sealProof(t *testing.T, s *StateStore, p AuthorityProof) ProofReference {
	t.Helper()
	r, err := s.SealAuthorityProof(accountingContext, p, accountingOptions())
	if err != nil {
		t.Fatal(err)
	}
	return ProofReference{r.ProofID, r.SHA256}
}
func recoveryFixture(t *testing.T) (*StateStore, InvocationAdmission) {
	t.Helper()
	s, a := configuredAdmissionFixture(t, 100)
	a.Binding.ContentIdentity = "known-content"
	a.Fence.Owner.ContentIdentity = &a.Binding.ContentIdentity
	execSQL(t, s.db, `UPDATE invocations SET content_identity=? WHERE invocation_id=?`, a.Binding.ContentIdentity, a.Binding.InvocationID)
	admit(t, s, a)
	return s, a
}
func TestAuthorityRecoverySealingReplayRestartAndRetention(t *testing.T) {
	s, a := recoveryFixture(t)
	d := claimFor(a, "interrupted", 50)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "all-null", "partial", nil))
	p := recoveryContract(t, s, a, "recover-1")
	bad := p
	bad.ProofID = "recover_1"
	if _, err := s.SealAuthorityProof(accountingContext, bad, accountingOptions()); err == nil {
		t.Fatal("malformed typed proof identity accepted")
	}
	if resultCount(t, s, "authority_proofs") != 0 || resultCount(t, s, "events") != 0 {
		t.Fatal("malformed typed proof identity left durable evidence")
	}
	ref := sealProof(t, s, p)
	before, err := s.AuthorityProofRetention(accountingContext, a.RunID, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Artifacts) < 2 || len(before.Dependencies) < 8 {
		t.Fatal("incomplete dependency graph", before)
	}
	for _, statement := range []string{`DELETE FROM evidence_artifacts WHERE evidence_id='recover-1-containment'`, `UPDATE evidence_artifacts SET kind='other' WHERE evidence_id='recover-1-containment'`, `DELETE FROM invocations WHERE invocation_id='first'`, `DELETE FROM provider_calls`, `DELETE FROM usage_receipts`, `DELETE FROM authority_proofs`, `UPDATE authority_proofs SET verdict='UNCONFIRMED'`} {
		if _, err = s.db.Exec(statement); err == nil {
			t.Fatal("sealed dependency mutation accepted", statement)
		}
	}
	if _, err = s.SealAuthorityProof(accountingContext, p, accountingOptions()); err != nil {
		t.Fatal("identical replay", err)
	}
	p.Reason = "different assertion"
	if _, err = s.SealAuthorityProof(accountingContext, p, accountingOptions()); err == nil {
		t.Fatal("conflicting replay")
	}
	if resultCount(t, s, "role_results") != 0 || resultCount(t, s, "authority_proofs") != 1 {
		t.Fatal("recovery created semantic completion or duplicate proof")
	}

	var duration, work *int64
	if err = s.db.QueryRow(`SELECT p.duration_ns,u.work_amount FROM provider_calls p JOIN provider_call_usage u USING(provider_call_id)`).Scan(&duration, &work); err != nil || duration != nil || work != nil {
		t.Fatal("recovery invented completion/usage", duration, work, err)
	}
	dir := filepath.Dir(s.info.Path)
	s.Close()
	restarted, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err = restarted.ValidateCurrentAuthorityProof(accountingContext, a.RunID, ref); err != nil {
		t.Fatal("restart invalidated same snapshot", err)
	}
	after, err := restarted.AuthorityProofRetention(accountingContext, a.RunID, ref)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("retention changed", err)
	}
	acceptUsage(t, restarted, admissionReceipt(d, "late-final", "final", counter(20)))
	if _, err = restarted.ValidateCurrentAuthorityProof(accountingContext, a.RunID, ref); err == nil {
		t.Fatal("old proof authorized changed current usage")
	}
	if _, err = restarted.AuthorityProofRetention(accountingContext, a.RunID, ref); err != nil {
		t.Fatal("stale proof lost historical retention", err)
	}
}
func TestAuthorityRecoveryInvalidAndAtomic(t *testing.T) {
	for _, failure := range []string{"ancestry", "content", "owner", "semantic", "containment", "digest", "bytes", "body", "audit"} {
		t.Run(failure, func(t *testing.T) {
			s, a := recoveryFixture(t)
			p := recoveryContract(t, s, a, "invalid")
			opts := accountingOptions()
			switch failure {
			case "ancestry":
				p.Boundary.Binding.AttemptID = "different"
			case "content":
				p.ContentIdentity = "different"
			case "owner":
				p.Boundary.Owner.ActorRevision++
			case "semantic":
				p.Boundary.SemanticRevision++
			case "containment":
				p.Recovery.Containment = "UNKNOWN"
			case "digest":
				p.Recovery.ContainmentEvidence[0].SHA256 = fmt.Sprintf("%064d", 1)
			case "bytes":
				execSQL(t, s.db, `UPDATE evidence_artifacts SET inline_blob=zeroblob(size_bytes) WHERE evidence_id='invalid-containment'`)
			case "body":
				p.Terminal = &TerminalProof{}
			case "audit":
				opts.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp}}
			}
			if _, err := s.SealAuthorityProof(accountingContext, p, opts); err == nil {
				t.Fatal("invalid proof accepted")
			}
			if resultCount(t, s, "authority_proofs") != 0 || resultCount(t, s, "authority_proof_dependencies") != 0 {
				t.Fatal("failed proof partially committed")
			}
			var count int
			s.db.QueryRow(`SELECT count(*) FROM evidence_artifacts WHERE kind='authority_proof'`).Scan(&count)
			if count != 0 {
				t.Fatal("failed seal leaked artifact")
			}
		})
	}
}
func TestAuthorityFileEvidenceReverified(t *testing.T) {
	s, a := recoveryFixture(t)
	p := recoveryContract(t, s, a, "file-proof")
	path := filepath.Join(privateTempDir(t), "contained.json")
	raw := []byte(`{"contained":true}`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	sha := fmt.Sprintf("%x", sha256.Sum256(raw))
	execSQL(t, s.db, `INSERT INTO evidence_artifacts VALUES('file-containment','controller_containment','file',?,?,NULL,?,?)`, sha, int64(len(raw)), path, semanticTimestamp)
	p.Recovery.ContainmentEvidence = []ProofArtifact{{"file-containment", "controller_containment", sha}}
	ref := sealProof(t, s, p)
	if _, err := s.ValidateCurrentAuthorityProof(accountingContext, a.RunID, ref); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"contained":false}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ValidateCurrentAuthorityProof(accountingContext, a.RunID, ref); err == nil {
		t.Fatal("changed file bytes retained authority")
	}
}

type authorityWorkflow struct {
	s               *StateStore
	worker          roleresult.Result
	workerAdmission InvocationAdmission
	pools           []ResourceAccount
	lead            InvocationBinding
	state           map[string]any
}

func proofWorkflowFixture(t *testing.T, mode ...string) *authorityWorkflow {
	t.Helper()
	s := testStore(t)
	raw, r, b, opts := ownershipFixture(t, s, "developer-completed")
	if err := s.TransferSemanticOwnership(accountingContext, r.RunID, r.Role, &b, opts); err != nil {
		t.Fatal(err)
	}
	pools := []ResourceAccount{}
	for _, resource := range budgetResources {
		a := ResourceAccount{PoolID: "authority-" + strings.ToLower(resource), RunID: r.RunID, Scope: "RUN", ResourceType: resource}
		if err := s.CreateResourcePool(accountingContext, ResourcePool{a, primaryTokenEligibility()}, accountingOptions()); err != nil {
			t.Fatal(err)
		}
		pools = append(pools, a)
	}
	policy := admissionPolicy(1000)
	policy.RunID = r.RunID
	if err := s.ConfigureRunAdmission(accountingContext, policy, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	ad := InvocationAdmission{AdmissionID: "worker-admission", RunID: r.RunID, Binding: b, HardDeadline: admissionDeadline, Fence: AdmissionFence{Owner: authorityBoundary(t, s, r.RunID, b).Owner}}
	for _, pool := range pools {
		ad.Resources = append(ad.Resources, AdmissionResource{Account: pool, Amount: 1000})
	}
	admit(t, s, ad)
	bound := int64(100)
	usage := int64(20)
	state := "final"
	if len(mode) > 0 {
		switch mode[0] {
		case "neutral":
			bound = 10
		case "pending":
			bound = 100
			state = "partial"
		case "bounded":
			bound = 1000
			usage = 600
		}
	}
	d := claimFor(ad, "worker-dispatch", bound)
	claim(t, s, d)
	if len(mode) > 0 && mode[0] == "bounded" {
		if err := s.FinishProviderCall(accountingContext, r.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
			t.Fatal(err)
		}
		next := claimFor(ad, "worker-second-dispatch", 600)
		claim(t, s, next)
		acceptUsage(t, s, admissionReceipt(next, "worker-second-usage", "final", counter(600)))
		if err := s.FinishProviderCall(accountingContext, r.RunID, next.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	}
	reported := counter(usage)
	if len(mode) > 0 && mode[0] == "unknown" {
		reported = nil
	}
	acceptUsage(t, s, admissionReceipt(d, "worker-usage", state, reported))
	if err := s.FinishProviderCall(accountingContext, r.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if len(mode) > 0 && mode[0] == "pending" {
		expired := accountingOptions()
		expired.At = admissionDeadline
		if _, err := s.ClaimAdmissionDispatch(accountingContext, claimFor(ad, "expired-continuation", 1), expired); err == nil {
			t.Fatal("deadline did not create unresolved stop")
		}
	}
	if err := s.AcceptRoleResult(accountingContext, raw, nil, ResultFence{11, 6}); err != nil {
		t.Fatal(err)
	}
	w := &authorityWorkflow{s: s, worker: r, workerAdmission: ad, pools: pools}
	w.bindLead(t, "lead-boundary-1", nil, nil, false)
	return w
}
func (w *authorityWorkflow) bindLead(t *testing.T, id string, qa, review *roleresult.Result, approved bool) {
	t.Helper()
	s := w.s
	a, err := s.GetActiveActor(accountingContext, w.worker.RunID)
	if err != nil {
		t.Fatal(err)
	}
	run, cycle, content := w.worker.RunID, w.worker.Cycle, w.worker.ContentIdentity
	execSQL(t, s.db, `INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES(?,?,?,'LEAD','LEAD','RUNNING',?)`, id+"-ex", run, cycle, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES(?,?,?,?,1,'RUNNING',?)`, id+"-at", id+"-ex", run, cycle, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch) VALUES(?,?,?,?,?,1,'LEAD','LEAD','fake','fake','high','RUNNING',?,?,?)`, id, id+"-at", id+"-ex", run, cycle, semanticTimestamp, content, a.OwnershipEpoch)
	b := InvocationBinding{id, id + "-ex", id + "-at", cycle, "LEAD", content}
	if err = s.BindActiveInvocation(accountingContext, run, b, ownerOptions(a)); err != nil {
		t.Fatal(err)
	}
	w.lead = b
	q := map[string]any{"status": "not_run"}
	v := map[string]any{"status": "not_run"}
	if qa != nil {
		q = map[string]any{"status": qa.Outcome, "result_ref": map[string]any{"result_id": qa.ResultID}}
	}
	if review != nil {
		v = map[string]any{"status": review.Outcome, "result_ref": map[string]any{"result_id": review.ResultID}}
	}
	status := "active"
	if approved {
		status = "approved"
	}
	state := map[string]any{"schema_version": 1, "lead_state_id": id + "-state", "run_id": run, "cycle": cycle, "producer": map[string]any{"execution_id": b.ExecutionID, "attempt": 1, "invocation_id": id, "invocation": 1}, "content_identity": content, "goal": "Complete the requested work.", "acceptance_criteria": []string{"Verified exact content."}, "open_findings": []any{}, "resolved_findings": []any{}, "qa": q, "review": v, "semantic_status": status, "created_at": semanticTimestamp}
	boundary := authorityBoundary(t, s, run, b)
	if err = s.AcceptLeadState(accountingContext, wire(t, state), MutationOptions{Fence: ResultFence{boundary.Owner.OwnershipEpoch, boundary.Owner.ActorRevision}, ExpectedSemanticRevision: boundary.SemanticRevision}); err != nil {
		t.Fatal(err)
	}
	w.state = state
}
func validationContract(t *testing.T, w *authorityWorkflow, r roleresult.Result, id string) AuthorityProof {
	t.Helper()
	s := w.s
	var purpose string
	if err := s.db.QueryRow(`SELECT purpose FROM invocations WHERE invocation_id=?`, r.InvocationID).Scan(&purpose); err != nil {
		t.Fatal(err)
	}
	b := InvocationBinding{r.InvocationID, r.ExecutionID, r.AttemptID, r.Cycle, purpose, r.ContentIdentity}
	ref := authorityArtifact(t, s, id+"-check", "controller_validation", []byte(`{"observation":"controller independently validated exact structured content and evidence"}`))
	return AuthorityProof{ProofID: id, RunID: r.RunID, Kind: "CONTROLLER_VALIDATION", ContentIdentity: r.ContentIdentity, Verdict: "VALIDATED", Reason: "independent controller validation", Boundary: authorityBoundary(t, s, r.RunID, b), Validation: &ControllerValidationProof{ResultID: r.ResultID, ValidationEvidence: []ProofArtifact{ref}}}
}
func TestAuthorityControllerValidationAndReviewBinding(t *testing.T) {
	w := proofWorkflowFixture(t)
	p := validationContract(t, w, w.worker, "verified-worker")
	ref := sealProof(t, w.s, p)
	if _, err := w.s.ValidateCurrentAuthorityProof(accountingContext, w.worker.RunID, ref); err != nil {
		t.Fatal(err)
	}
	p.ProofID = "self-validated"
	p.Validation.ValidationEvidence = []ProofArtifact{{"proof", "test_result", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}}
	if _, err := w.s.SealAuthorityProof(accountingContext, p, accountingOptions()); err == nil {
		t.Fatal("worker evidence substituted for controller validation")
	}
	p = validationContract(t, w, w.worker, "wrong-result")
	p.Validation.ResultID = "res-c002-r999"
	if _, err := w.s.SealAuthorityProof(accountingContext, p, accountingOptions()); err == nil {
		t.Fatal("unaccepted result reference accepted")
	}
}

func (w *authorityWorkflow) addWorker(t *testing.T, name, id string, ordinal int) (roleresult.Result, InvocationAdmission) {
	t.Helper()
	s := w.s
	var r roleresult.Result
	if err := json.Unmarshal(resultFixture(t, name), &r); err != nil {
		t.Fatal(err)
	}
	r.ResultOrdinal = ordinal
	r.ResultID = roleresult.ResultID(r.Cycle, ordinal)
	r.InvocationID = id
	r.ExecutionID = id + "-ex"
	r.AttemptID = id + "-at"
	actor, err := s.GetActiveActor(accountingContext, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	seedResultInvocation(t, s, r, int(actor.OwnershipEpoch+1))
	purpose := map[string]string{"QA": "QA", "REVIEWER": "REVIEW"}[r.Role]
	b := InvocationBinding{r.InvocationID, r.ExecutionID, r.AttemptID, r.Cycle, purpose, r.ContentIdentity}
	if err = s.TransferSemanticOwnership(accountingContext, r.RunID, r.Role, &b, ownerOptions(actor)); err != nil {
		t.Fatal(err)
	}
	boundary := authorityBoundary(t, s, r.RunID, b)
	ad := InvocationAdmission{AdmissionID: id + "-admission", RunID: r.RunID, Binding: b, HardDeadline: admissionDeadline, Fence: AdmissionFence{Owner: boundary.Owner, SemanticRevision: boundary.SemanticRevision}}
	for _, pool := range w.pools {
		ad.Resources = append(ad.Resources, AdmissionResource{Account: pool, Amount: 10})
	}
	admit(t, s, ad)
	return r, ad
}
func (w *authorityWorkflow) accept(t *testing.T, r roleresult.Result) {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if r.Role == "QA" || r.Role == "REVIEWER" {
		doc["finding_occurrences"] = r.FindingOccurrences
		if r.FindingOccurrences == nil {
			doc["finding_occurrences"] = []any{}
		}
	}
	raw = wire(t, doc)
	a, err := w.s.GetActiveActor(accountingContext, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.s.AcceptRoleResult(accountingContext, raw, nil, ResultFence{a.OwnershipEpoch, a.Revision}); err != nil {
		t.Fatal(err)
	}
}
func terminalFixture(t *testing.T) (*authorityWorkflow, AuthorityProof) {
	t.Helper()
	w := proofWorkflowFixture(t)
	qa, _ := w.addWorker(t, "qa-passed", "qa-verified", 4)
	w.accept(t, qa)
	w.bindLead(t, "lead-boundary-2", &qa, nil, false)
	review, ad := w.addWorker(t, "reviewer-approved", "review-verified", 5)
	completion := sealProof(t, w.s, validationContract(t, w, w.worker, "review-completion"))
	recovery := sealProof(t, w.s, recoveryContract(t, w.s, w.workerAdmission, "review-containment"))
	rp := AuthorityProof{ProofID: "review-boundary", RunID: review.RunID, Kind: "REVIEW_ADMISSION", ContentIdentity: review.ContentIdentity, Verdict: "BLOCKED", Reason: "prior accounting is not settled", Boundary: authorityBoundary(t, w.s, review.RunID, ad.Binding), Review: &ReviewAdmissionProof{Completion: completion, Containment: recovery, AdmissionID: ad.AdmissionID}}
	reviewRef := sealProof(t, w.s, rp)
	// This contract test records later semantic evidence; it does not execute work
	// from the BLOCKED review proof or manufacture a dispatch authorization.
	w.accept(t, review)
	w.bindLead(t, "lead-boundary-3", &qa, &review, true)
	cp := sealProof(t, w.s, validationContract(t, w, w.worker, "terminal-completion"))
	qp := sealProof(t, w.s, validationContract(t, w, qa, "terminal-qa"))
	vp := sealProof(t, w.s, validationContract(t, w, review, "terminal-review"))
	containment := sealProof(t, w.s, recoveryContract(t, w.s, w.workerAdmission, "terminal-containment"))
	p := AuthorityProof{ProofID: "terminal-boundary", RunID: w.worker.RunID, Kind: "TERMINAL", ContentIdentity: w.worker.ContentIdentity, Verdict: "BLOCKED", Reason: "required token settlement controller fact is absent", Boundary: authorityBoundary(t, w.s, w.worker.RunID, w.lead), Terminal: &TerminalProof{Completion: cp, QAValidation: qp, ReviewValidation: vp, ReviewAdmission: reviewRef, Containment: containment, LeadStateID: w.state["lead_state_id"].(string)}}
	return w, p
}
func TestAuthorityTerminalBindsCompleteChainWithoutBypassingHolds(t *testing.T) {
	w, p := terminalFixture(t)
	ref := sealProof(t, w.s, p)
	if _, err := w.s.ValidateCurrentAuthorityProof(accountingContext, p.RunID, ref); err != nil {
		t.Fatal(err)
	}
	p.ProofID = "forbidden-ready"
	p.Verdict = "SUPPORTS_READINESS"
	if _, err := w.s.SealAuthorityProof(accountingContext, p, accountingOptions()); err == nil {
		t.Fatal("terminal proof bypassed unresolved accounting")
	}
	var status string
	if err := w.s.db.QueryRow(`SELECT status FROM runs WHERE run_id=?`, p.RunID).Scan(&status); err != nil || status != "RUNNING" {
		t.Fatal("proof implemented readiness transition", status, err)
	}

	inventory, err := w.s.AuthorityProofRetention(accountingContext, p.RunID, ref)
	if err != nil {
		t.Fatal(err)
	}
	roots := map[string]bool{}
	for _, d := range inventory.Dependencies {
		if d.Kind == "proof" {
			roots[d.ID] = true
		}
	}
	for _, id := range []string{"terminal-boundary", "terminal-completion", "terminal-qa", "terminal-review", "review-boundary", "review-completion", "review-containment", "terminal-containment"} {
		if !roots[id] {
			t.Fatal("authority chain not retained", id)
		}
	}
	for _, query := range []string{`DELETE FROM role_result_evidence`, `UPDATE qa_checks SET outcome='failed'`, `DELETE FROM lead_state_bindings`, `UPDATE lead_states SET semantic_status='active'`, `DELETE FROM evidence_artifacts WHERE evidence_id='terminal-completion-check'`} {
		if _, err = w.s.db.Exec(query); err == nil {
			t.Fatal("cleanup/substitution severed terminal chain", query)
		}
	}
}
func TestAuthorityTerminalSubstitutionAndStaleness(t *testing.T) {
	w, p := terminalFixture(t)
	for _, which := range []string{"qa-as-completion", "digest", "lead", "content", "review-invocation"} {
		t.Run(which, func(t *testing.T) {
			bad := p
			body := *p.Terminal
			bad.Terminal = &body
			bad.ProofID = "bad-" + which
			switch which {
			case "qa-as-completion":
				body.Completion = body.QAValidation
			case "digest":
				body.QAValidation.SHA256 = fmt.Sprintf("%064d", 1)
			case "lead":
				body.LeadStateID = "missing"
			case "content":
				bad.ContentIdentity = "other"
			case "review-invocation":
				body.ReviewAdmission = body.ReviewValidation
			}
			if _, err := w.s.SealAuthorityProof(accountingContext, bad, accountingOptions()); err == nil {
				t.Fatal("terminal substitution accepted")
			}
		})
	}
	old := p.Terminal.Completion
	if err := w.s.ClearActiveInvocation(accountingContext, p.RunID, ownerOptions(ActiveActor{Role: p.Boundary.Owner.Role, InvocationID: p.Boundary.Owner.InvocationID, OwnershipEpoch: p.Boundary.Owner.OwnershipEpoch, Revision: p.Boundary.Owner.ActorRevision})); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.ValidateCurrentAuthorityProof(accountingContext, p.RunID, old); err == nil {
		t.Fatal("old authority survived changed owner")
	}
	if _, err := w.s.SealAuthorityProof(accountingContext, p, accountingOptions()); err == nil {
		t.Fatal("stale terminal boundary accepted")
	}
}
func humanContract(t *testing.T, w *authorityWorkflow, stop RunHardStop, id, decision string) HumanReadinessDisposition {
	t.Helper()
	ref := authorityArtifact(t, w.s, id+"-support", "human_disposition_support", []byte(`{"facts":"exact hard-stop evidence presented to human"}`))
	return HumanReadinessDisposition{DispositionID: id, RunID: w.worker.RunID, StopID: stop.StopID, Classification: stop.Classification, ContentIdentity: w.worker.ContentIdentity, InteractionID: id + "-interaction", Prompt: "May terminal readiness evaluation proceed for this exact Run, stop and content?", Response: decision, Decision: decision, Boundary: authorityBoundary(t, w.s, w.worker.RunID, w.lead), Evidence: []ProofArtifact{ref}}
}
func TestAuthorityHumanDispositionClassesReplayRestart(t *testing.T) {
	for _, mode := range []string{"neutral", "pending", "bounded", "controller"} {
		t.Run(mode, func(t *testing.T) {
			w := proofWorkflowFixture(t, mode)
			if mode == "controller" {
				if _, err := w.s.RecordControllerReleaseEvidence(accountingContext, ControllerReleaseEvidence{RunID: w.worker.RunID, ClaimID: "worker-dispatch", EvidenceID: "late-release", ReleasedAt: admissionDeadline}, accountingOptions()); err != nil {
					t.Fatal(err)
				}
			}
			stops, err := w.s.ListRunHardStops(accountingContext, w.worker.RunID)
			if err != nil || len(stops) == 0 {
				t.Fatal("missing stop", err)
			}
			// This fixture exceeds both Run and agent limits. Select the Run stop
			// when checking the bounded class's disposition eligibility.
			if mode == "bounded" {
				for _, stop := range stops {
					if stop.SourceID == "bank:normal" {
						stops[0] = stop
						break
					}
				}
			}
			h := humanContract(t, w, stops[0], "human-"+mode, "ALLOW_TERMINAL_EVALUATION")
			err = w.s.RecordHumanReadinessDisposition(accountingContext, h, accountingOptions())
			if mode == "pending" || mode == "bounded" {
				if err == nil {
					t.Fatal("disposition accepted for non-applicable class")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			before := resultCount(t, w.s, "events")
			replay := accountingOptions()
			replay.Audit = accountingAudit()
			if err = w.s.RecordHumanReadinessDisposition(accountingContext, h, replay); err != nil {
				t.Fatal(err)
			}
			if resultCount(t, w.s, "events") != before {
				t.Fatal("replay appended audit")
			}
			h.Response = "different reply"
			if err = w.s.RecordHumanReadinessDisposition(accountingContext, h, accountingOptions()); err == nil {
				t.Fatal("human response overwritten")
			}
			after, _ := w.s.ListRunHardStops(accountingContext, w.worker.RunID)
			if !reflect.DeepEqual(stops, after) {
				t.Fatal("human changed violation classification")
			}
			old := filepath.Dir(w.s.info.Path)
			w.s.Close()
			restarted, err := openTest(accountingContext, old)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			h.Response = h.Decision
			loaded, err := loadHumanDisposition(accountingContext, restarted.db, h.DispositionID)
			if err != nil || !reflect.DeepEqual(h, loaded) {
				t.Fatal("human decision lost on restart", err)
			}

		})
	}
}
func TestAuthorityHumanDispositionFaultAndSubstitution(t *testing.T) {
	for _, failure := range []string{"stop", "class", "content", "owner", "evidence", "response", "audit"} {
		t.Run(failure, func(t *testing.T) {
			w := proofWorkflowFixture(t, "neutral")
			stops, _ := w.s.ListRunHardStops(accountingContext, w.worker.RunID)
			h := humanContract(t, w, stops[0], "invalid-human", "DENY_TERMINAL_EVALUATION")
			opts := accountingOptions()
			switch failure {
			case "stop":
				h.StopID = "other"
			case "class":
				h.Classification = "CONTROLLER_ADMISSION_BUG"
			case "content":
				h.ContentIdentity = "other"
			case "owner":
				h.Boundary.Owner.ActorRevision++
			case "evidence":
				h.Evidence[0].SHA256 = fmt.Sprintf("%064d", 1)
			case "response":
				h.Response = ""
			case "audit":
				opts.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp}}
			}
			if err := w.s.RecordHumanReadinessDisposition(accountingContext, h, opts); err == nil {
				t.Fatal("invalid human evidence accepted")
			}
			if resultCount(t, w.s, "human_readiness_dispositions") != 0 || resultCount(t, w.s, "human_disposition_dependencies") != 0 {
				t.Fatal("failed human evidence partly committed")
			}
		})
	}
}

func TestAuthorityHumanTerminalPolicyAndNoExecutionPermission(t *testing.T) {
	for _, mode := range []string{"neutral", "controller", "bounded", "pending"} {
		for _, decision := range []string{"ALLOW_TERMINAL_EVALUATION", "DENY_TERMINAL_EVALUATION"} {
			t.Run(mode+"/"+decision, func(t *testing.T) {
				w := proofWorkflowFixture(t, mode)
				if mode == "controller" {
					if _, err := w.s.RecordControllerReleaseEvidence(accountingContext, ControllerReleaseEvidence{RunID: w.worker.RunID, ClaimID: "worker-dispatch", EvidenceID: "late-release", ReleasedAt: admissionDeadline}, accountingOptions()); err != nil {
						t.Fatal(err)
					}
				}
				stops, err := w.s.ListRunHardStops(accountingContext, w.worker.RunID)
				if err != nil {
					t.Fatal(err)
				}
				p := AuthorityProof{RunID: w.worker.RunID, ContentIdentity: w.worker.ContentIdentity, Terminal: &TerminalProof{}}
				if mode == "neutral" || mode == "controller" || mode == "bounded" {
					if err := terminalStopDispositions(accountingContext, w.s.db, p); err == nil {
						t.Fatal("missing human decision allowed")
					}
					for i, stop := range stops {
						if stop.Classification == "BOUNDED_TERMINAL_OVERRUN" {
							continue
						}
						h := humanContract(t, w, stop, fmt.Sprintf("policy-response-%d", i), decision)
						if err := w.s.RecordHumanReadinessDisposition(accountingContext, h, accountingOptions()); err != nil {
							t.Fatal(err)
						}
						p.Terminal.HumanDispositions = append(p.Terminal.HumanDispositions, h.DispositionID)
					}
				}
				err = terminalStopDispositions(accountingContext, w.s.db, p)
				allowed := mode != "pending" && decision == "ALLOW_TERMINAL_EVALUATION"
				if (err == nil) != allowed {
					t.Fatal("wrong terminal disposition eligibility", err)
				}
				// Allowing evaluation must still leave ordinary dispatch and token settlement blocked.
				if _, err := w.s.ClaimAdmissionDispatch(accountingContext, claimFor(w.workerAdmission, "later-retry", 50), accountingOptions()); err == nil {
					t.Fatal("human decision authorized retry")
				}
				if err := proofResourcesSettled(accountingContext, w.s.db, w.worker.RunID, ""); err == nil {
					t.Fatal("human decision settled tokens")
				}

			})
		}
	}
}
func TestAuthorityProjectionFailureDoesNotUndoSeal(t *testing.T) {
	s, a := recoveryFixture(t)
	dir := privateTempDir(t)
	if err := s.EnableMetricsProjection(accountingContext, a.RunID, dir); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(dir, ".metrics.lock")
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lock, 0700); err != nil {
		t.Fatal(err)
	}
	p := recoveryContract(t, s, a, "projection-failed")
	r, err := s.SealAuthorityProof(accountingContext, p, accountingOptions())
	var projection *MetricsProjectionError
	if !errors.As(err, &projection) {
		t.Fatal("missing projection failure", err)
	}
	if resultCount(t, s, "authority_proofs") != 1 {
		t.Fatal("projection rolled back seal")
	}
	ref := ProofReference{r.ProofID, r.SHA256}
	if _, err := s.ValidateCurrentAuthorityProof(accountingContext, a.RunID, ref); err != nil {
		t.Fatal(err)
	}
	if replay, err := s.SealAuthorityProof(accountingContext, p, accountingOptions()); err != nil || replay != r {
		t.Fatal("committed failure cannot replay", err)
	}
}
func TestAuthoritySealAndUsageConcurrentSnapshot(t *testing.T) {
	s, a := recoveryFixture(t)
	d := claimFor(a, "racing-call", 50)
	claim(t, s, d)
	other, err := openTest(accountingContext, filepath.Dir(s.info.Path))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	p := recoveryContract(t, s, a, "racing-proof")
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() {
		<-start
		_, err := s.SealAuthorityProof(accountingContext, p, accountingOptions())
		done <- err
	}()
	go func() {
		<-start
		done <- other.AcceptUsageReceipt(accountingContext, admissionReceipt(d, "racing-usage", "final", counter(20)), accountingOptions())
	}()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	_, sealed, err := loadAuthorityProof(accountingContext, s.db, p.ProofID)
	if err != nil {
		t.Fatal(err)
	}
	var snapshotUsage, snapshotReceipts [][]any
	for _, table := range sealed.Snapshot {
		switch table.Name {
		case "usage":
			snapshotUsage = table.Rows
		case "receipts":
			snapshotReceipts = table.Rows
		}
	}
	if len(snapshotUsage) != 1 {
		t.Fatal("lost call state")
	}
	// A receipt must be present precisely when the current-usage row references it.
	var hasReceipt bool
	for _, cell := range snapshotUsage[0] {
		if cell == "racing-usage" {
			hasReceipt = true
		}
	}
	if hasReceipt != (len(snapshotReceipts) == 1) {
		t.Fatal("mixed receipt/current-usage snapshot")
	}
}

func TestAuthorityDependencySubstitutionDetected(t *testing.T) {
	s, a := recoveryFixture(t)
	p := recoveryContract(t, s, a, "dependency-identity")
	ref := sealProof(t, s, p)
	extra := authorityArtifact(t, s, "unexpected-artifact", "test", []byte("unexpected"))
	if _, err := s.db.Exec(`INSERT INTO authority_proof_dependencies(proof_id,artifact_id) VALUES(?,?)`, p.ProofID, extra.EvidenceID); err == nil {
		t.Fatal("unsealed dependency inserted")
	}
	// Simulate on-disk corruption bypassing the database protection.
	execSQL(t, s.db, `DROP TRIGGER authority_dependency_sealed_inventory`)
	execSQL(t, s.db, `INSERT INTO authority_proof_dependencies(proof_id,artifact_id) VALUES(?,?)`, p.ProofID, extra.EvidenceID)
	if _, err := s.ValidateCurrentAuthorityProof(accountingContext, a.RunID, ref); err == nil {
		t.Fatal("unsigned dependency inventory substituted")
	}
	dir := filepath.Dir(s.info.Path)
	s.Close()
	if reopened, err := openTest(accountingContext, dir); err == nil {
		reopened.Close()
		t.Fatal("corrupt sealed dependency inventory survived restart")
	}
}

// Fixtures are controller contracts, not a Worker wire or state-import format.
// Bind placeholder identities to real durable records; body semantics stay intact.
func TestAuthorityPermanentContractFixtures(t *testing.T) {
	entries, err := os.ReadDir("testdata/authority-proofs")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		t.Run(entry.Name(), func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("testdata/authority-proofs", entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			var p AuthorityProof
			if err = json.Unmarshal(raw, &p); err != nil {
				t.Fatal(err)
			}
			var s *StateStore
			switch p.Kind {
			case "RECOVERY":
				var a InvocationAdmission
				s, a = recoveryFixture(t)
				p.RunID, p.ContentIdentity, p.Boundary = a.RunID, a.Binding.ContentIdentity, authorityBoundary(t, s, a.RunID, a.Binding)
				if len(p.Recovery.ContainmentEvidence) > 0 {
					p.Recovery.ContainmentEvidence = []ProofArtifact{authorityArtifact(t, s, "fixture-containment", "controller_containment", []byte("observed containment"))}
				}
			case "CONTROLLER_VALIDATION":
				w := proofWorkflowFixture(t)
				s = w.s
				template := validationContract(t, w, w.worker, "fixture-validation")
				p.RunID, p.ContentIdentity, p.Boundary = template.RunID, template.ContentIdentity, template.Boundary
				if p.Validation.ResultID == "fixture-result" {
					p.Validation.ResultID = template.Validation.ResultID
				}
				p.Validation.ValidationEvidence = template.Validation.ValidationEvidence
			case "REVIEW_ADMISSION", "TERMINAL":
				w, terminal := terminalFixture(t)
				s = w.s
				p.RunID, p.ContentIdentity = terminal.RunID, terminal.ContentIdentity
				if p.Kind == "TERMINAL" {
					p.Boundary = terminal.Boundary
					p.Terminal = terminal.Terminal
				} else {
					_, original, e := loadAuthorityProof(accountingContext, s.db, "review-boundary")
					if e != nil {
						t.Fatal(e)
					}
					// Rebind the historical review contract at this later boundary.
					p.Boundary = authorityBoundary(t, s, p.RunID, original.Request.Boundary.Binding)
					p.Review.Completion = terminal.Terminal.Completion
					p.Review.Containment = terminal.Terminal.Containment
					p.Review.AdmissionID = original.Request.Review.AdmissionID
				}
			}
			before := resultCount(t, s, "authority_proofs")
			_, err = s.SealAuthorityProof(accountingContext, p, accountingOptions())
			valid := strings.HasPrefix(entry.Name(), "valid-")
			if (err == nil) != valid {
				t.Fatal("fixture validity mismatch", err)
			}
			if !valid && resultCount(t, s, "authority_proofs") != before {
				t.Fatal("invalid fixture persisted")
			}
		})
	}
}

func TestAuthoritySourceIdentityAndAuditReplay(t *testing.T) {
	s, a := recoveryFixture(t)
	p := recoveryContract(t, s, a, "source-first")
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	first, err := s.SealAuthorityProof(accountingContext, p, opts)
	if err != nil {
		t.Fatal(err)
	}
	count := resultCount(t, s, "events")
	opts.At = "2026-01-01T00:00:00Z"
	if replay, err := s.SealAuthorityProof(accountingContext, p, opts); err != nil || replay != first || resultCount(t, s, "events") != count {
		t.Fatal("replay changed seal/audit", err)
	}
	p.ProofID = "source-second"
	second, err := s.SealAuthorityProof(accountingContext, p, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceDigest != second.SourceDigest || first.SealedAt == second.SealedAt {
		t.Fatal("seal time or audit became source identity")
	}
	for _, r := range []ProofRecord{first, second} {
		if _, err := s.ValidateCurrentAuthorityProof(accountingContext, a.RunID, ProofReference{r.ProofID, r.SHA256}); err != nil {
			t.Fatal(err)
		}
	}
}
func TestAuthorityHistoricalParentFileCannotSubstitute(t *testing.T) {
	w, p := terminalFixture(t)
	path := filepath.Join(privateTempDir(t), "validation.json")
	raw := []byte(`{"controller_validation":true}`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	sha := fmt.Sprintf("%x", sha256.Sum256(raw))
	execSQL(t, w.s.db, `INSERT INTO evidence_artifacts VALUES('file-controller-validation','controller_validation','file',?,?,NULL,?,?)`, sha, int64(len(raw)), path, semanticTimestamp)
	fresh := validationContract(t, w, w.worker, "file-terminal-completion")
	fresh.Validation.ValidationEvidence = []ProofArtifact{{"file-controller-validation", "controller_validation", sha}}
	p.Terminal.Completion = sealProof(t, w.s, fresh)
	ref := sealProof(t, w.s, p)
	if err := os.WriteFile(path, []byte(`{"controller_validation":false}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.s.ValidateCurrentAuthorityProof(accountingContext, p.RunID, ref); err == nil {
		t.Fatal("transitive file substituted despite unchanged SQLite snapshot")
	}
	if _, err := w.s.AuthorityProofRetention(accountingContext, p.RunID, ref); err == nil {
		t.Fatal("retention accepted corrupt transitive bytes")
	}
	// Unavailable historical external bytes cannot become state reconstruction input.
	dir := filepath.Dir(w.s.info.Path)
	w.s.Close()
	restarted, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal("startup read historical file as authority", err)
	}
	defer restarted.Close()
	if _, err := restarted.ValidateCurrentAuthorityProof(accountingContext, p.RunID, ref); err == nil {
		t.Fatal("restart restored invalid file authority")
	}
}
