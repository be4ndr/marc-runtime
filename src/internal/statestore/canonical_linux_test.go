package statestore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"marc-runtime/internal/identity"
	"marc-runtime/src/internal/roleresult"
	"reflect"
	"testing"
)

func TestB3SameRunControllerResolution(t *testing.T) {
	w := proofWorkflowFixture(t)
	qa, _ := w.addWorker(t, "qa-failed", "qa-source", 4)
	var raw map[string]any
	if err := json.Unmarshal(resultFixture(t, "qa-failed"), &raw); err != nil {
		t.Fatal(err)
	}
	raw["result_id"], raw["result_ordinal"] = qa.ResultID, qa.ResultOrdinal
	raw["invocation_id"], raw["execution_id"], raw["attempt_id"] = qa.InvocationID, qa.ExecutionID, qa.AttemptID
	actor, err := w.s.GetActiveActor(accountingContext, qa.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.s.AcceptRoleResult(accountingContext, wire(t, raw), []roleresult.Finding{newFinding(qa)}, ResultFence{actor.OwnershipEpoch, actor.Revision}); err != nil {
		t.Fatal(err)
	}
	w.bindLead(t, "resolution-lead", &qa, nil, false)
	validation := validationContract(t, w, w.worker, "same-run-controller-validation")
	sealed := sealProof(t, w.s, validation)
	if _, err := w.s.ValidateCurrentAuthorityProof(accountingContext, qa.RunID, sealed); err != nil {
		t.Fatal(err)
	}
	evidence := validation.Validation.ValidationEvidence[0].EvidenceID
	var links int
	if err := w.s.db.QueryRow(`SELECT count(*) FROM authority_proofs p JOIN authority_proof_dependencies d USING(proof_id) WHERE p.run_id=? AND p.proof_id=? AND d.artifact_id=?`, qa.RunID, sealed.ProofID, evidence).Scan(&links); err != nil || links != 1 {
		t.Fatal("missing accepted same-Run provenance", links, err)
	}
	doc := cloneDoc(t, w.state)
	doc["lead_state_id"] = "resolved-controller-evidence"
	doc["resolved_findings"] = []any{map[string]any{"finding_id": "fnd-000005", "source_role": "qa", "source_result_ref": map[string]any{"result_id": qa.ResultID}, "content_identity": qa.ContentIdentity, "blocking": true, "summary": "Stable semantic issue.", "resolution_ref": map[string]any{"evidence_id": evidence}}}
	b := authorityBoundary(t, w.s, qa.RunID, w.lead)
	err = w.s.AcceptLeadState(accountingContext, wire(t, doc), MutationOptions{Fence: ResultFence{b.Owner.OwnershipEpoch, b.Owner.ActorRevision}, ExpectedSemanticRevision: b.SemanticRevision})
	t.Logf("same-Run accepted controller proof=%s evidence=%s links=%d; AcceptLeadState=%v", sealed.ProofID, evidence, links, err)
	if err != nil {
		t.Fatal("same-Run controller evidence rejected", err)
	}
}

func TestB3SQLGrantProvenance(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	seedInvocation(t, s)
	foreign := ResourceAccount{PoolID: "foreign-work", RunID: "r1", Scope: "RUN", ResourceType: "WORK"}
	createPool(t, s, foreign, "DEVELOPER", "IMPLEMENT")
	execSQL(t, s.db, `INSERT INTO resource_allocations(allocation_id,run_id,resource_type,amount,source,created_at,pool_id) VALUES('foreign-allocation','r1','WORK',1,'fixture',?,?)`, semanticTimestamp, foreign.PoolID)
	if _, err := s.db.Exec(`INSERT INTO resource_pools(pool_id,run_id,scope,resource_type,created_at) VALUES('wrong-pool',?,'RUN','WORK',?)`, a.RunID, semanticTimestamp); err == nil {
		t.Fatal("second normal bank address accepted")
	}
	for _, id := range []string{"foreign-allocation"} {
		if _, err := s.db.Exec(`INSERT INTO admission_grants VALUES(?,'WORK',?)`, a.AdmissionID, id); err == nil {
			t.Fatal("SQL accepted mismatched grant", id)
		}
	}
	g := ResourceGrant{Account: a.Resources[0].Account, AdmissionID: a.AdmissionID, AllocationID: "valid-linked-grant", Source: "controller", Amount: 1}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{`UPDATE admission_grants SET allocation_id='foreign-allocation' WHERE allocation_id='valid-linked-grant'`, `DELETE FROM admission_grants WHERE allocation_id='valid-linked-grant'`} {
		if _, err := s.db.Exec(sql); err == nil {
			t.Fatal("SQL mutated immutable grant link", sql)
		}
	}
	if err := validateDatabase(accountingContext, s.db); err != nil {
		t.Fatal(err)
	}
}

func TestB3ArtifactNamespace(t *testing.T) {
	s := testStore(t)
	for _, kind := range []string{"authority-proof", "human-readiness"} {
		oldStyle := kind + "-" + fmt.Sprintf("%x", sha256.Sum256([]byte("new-source")))
		authorityArtifact(t, s, oldStyle, "other", []byte("external registration"))
		generated := sealedArtifactID(kind, "new-source")
		if generated == oldStyle || !identity.ValidID(generated) {
			t.Fatal("generated namespace collision", generated)
		}
		if _, err := s.db.Exec(`INSERT INTO evidence_artifacts VALUES(?,'other','sqlite',?,1,?,NULL,?)`, generated, fmt.Sprintf("%x", sha256.Sum256([]byte("x"))), []byte("x"), semanticTimestamp); err == nil {
			t.Fatal("ordinary registration entered reserved namespace")
		}
	}
}

func countSchemaObject(t *testing.T, s *StateStore, name string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name=?`, name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestB3ProofSourceIDsCannotAlias(t *testing.T) {
	w := proofWorkflowFixture(t)
	source := "new-proof"
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(source)))
	for _, id := range []string{hash, source} {
		p := validationContract(t, w, w.worker, id)
		ref := sealProof(t, w.s, p)
		if _, err := w.s.ValidateCurrentAuthorityProof(accountingContext, p.RunID, ref); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := w.s.db.QueryRow(`SELECT count(DISTINCT evidence_id) FROM authority_proofs`).Scan(&count); err != nil || count != 2 {
		t.Fatal("proof artifact alias", count, err)
	}
	w.s = restartAgentStore(t, w.s)
	for _, id := range []string{hash, source} {
		if _, _, err := loadAuthorityProof(accountingContext, w.s.db, id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestB3UnsealedControllerEvidenceCannotResolve(t *testing.T) {
	s := testStore(t)
	doc, opts := semanticFixture(t, s)
	ref := authorityArtifact(t, s, "unsealed-controller", "controller_validation", []byte("registered only"))
	f := doc["open_findings"].([]any)[0].(map[string]any)
	f["resolution_ref"] = map[string]any{"evidence_id": ref.EvidenceID}
	doc["open_findings"], doc["resolved_findings"] = []any{}, []any{f}
	before := mutationDatabaseSnapshot(t, s)
	if err := s.AcceptLeadState(accountingContext, wire(t, doc), opts); err == nil {
		t.Fatal("unsealed controller registration supplied Run provenance")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("resolution rejection changed authority")
	}
}

func TestB3SameRunHumanEvidenceResolution(t *testing.T) {
	w := proofWorkflowFixture(t)
	qa, _ := w.addWorker(t, "qa-failed", "qa-source", 4)
	var raw map[string]any
	if err := json.Unmarshal(resultFixture(t, "qa-failed"), &raw); err != nil {
		t.Fatal(err)
	}
	raw["result_id"], raw["result_ordinal"] = qa.ResultID, qa.ResultOrdinal
	raw["invocation_id"], raw["execution_id"], raw["attempt_id"] = qa.InvocationID, qa.ExecutionID, qa.AttemptID
	actor, err := w.s.GetActiveActor(accountingContext, qa.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.s.AcceptRoleResult(accountingContext, wire(t, raw), []roleresult.Finding{newFinding(qa)}, ResultFence{actor.OwnershipEpoch, actor.Revision}); err != nil {
		t.Fatal(err)
	}
	w.bindLead(t, "resolution-lead", &qa, nil, false)
	if err := w.s.numericMutation(accountingContext, "resolution stop fixture", qa.RunID, nil, func(c *sql.Conn) error {
		if err := persistHardStop(accountingContext, c, qa.RunID, "WORK", "DISPATCH_BOUND_EXCEEDED", "resolution-human", counter(21), counter(20), semanticTimestamp); err != nil {
			return err
		}
		return resolveHardStop(accountingContext, c, stopIdentity(qa.RunID, "DISPATCH_BOUND_EXCEEDED", "resolution-human"), "OBSERVED_DISPATCH_BOUND_EXCEEDED", semanticTimestamp)
	}); err != nil {
		t.Fatal(err)
	}
	stops, err := w.s.ListRunHardStops(accountingContext, qa.RunID)
	if err != nil || len(stops) != 1 {
		t.Fatal(stops, err)
	}
	h := humanContract(t, w, stops[0], "resolution-human-disposition", "ALLOW_TERMINAL_EVALUATION")
	if err := w.s.RecordHumanReadinessDisposition(accountingContext, h, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	evidence := h.Evidence[0].EvidenceID
	doc := cloneDoc(t, w.state)
	doc["lead_state_id"] = "resolved-controller-evidence"
	doc["resolved_findings"] = []any{map[string]any{"finding_id": "fnd-000005", "source_role": "qa", "source_result_ref": map[string]any{"result_id": qa.ResultID}, "content_identity": qa.ContentIdentity, "blocking": true, "summary": "Stable semantic issue.", "resolution_ref": map[string]any{"evidence_id": evidence}}}
	b := authorityBoundary(t, w.s, qa.RunID, w.lead)
	err = w.s.AcceptLeadState(accountingContext, wire(t, doc), MutationOptions{Fence: ResultFence{b.Owner.OwnershipEpoch, b.Owner.ActorRevision}, ExpectedSemanticRevision: b.SemanticRevision})
	if err != nil {
		t.Fatal("same-Run controller evidence rejected", err)
	}
}

func approvedBudget(id string) HumanBudgetApproval {
	return HumanBudgetApproval{ApprovalID: id, InteractionID: id + "-interaction", Prompt: "Confirm the exact normal WORK token budget.", Response: "I approve this budget.", Decision: "APPROVE"}
}
