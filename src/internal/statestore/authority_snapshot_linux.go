package statestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

// A snapshot binds facts at a decision boundary. It is evidence only: it is
// never imported/replayed to reconstruct current state. Ordering is canonical
// serialization, not a rule for selecting authority or a latest receipt.
type authoritySource struct{ Name, SQL, Dependency string }

var authoritySources = []authoritySource{
	{"workflow", `SELECT * FROM workflow_runs WHERE run_id=?`, ""},
	{"workflow_steps", `SELECT * FROM workflow_steps WHERE run_id=? ORDER BY step_no`, ""},
	{"workflow_outputs", `SELECT e.* FROM evidence_artifacts e JOIN workflow_steps s ON s.output_id=e.evidence_id WHERE s.run_id=? ORDER BY s.step_no`, "artifact"},
	{"budget_confirmation", `SELECT * FROM run_budget_confirmations WHERE run_id=?`, ""},
	{"budget_increases", `SELECT * FROM run_budget_increases WHERE run_id=? ORDER BY approval_id`, ""},
	{"hard_requests", `SELECT * FROM hard_limit_requests WHERE run_id=? ORDER BY request_id`, ""},
	{"hard_decisions", `SELECT d.* FROM hard_limit_decisions d JOIN hard_limit_requests r USING(request_id) WHERE r.run_id=? ORDER BY d.request_id`, ""},
	{"closures", `SELECT c.proof_id,c.admission_id,c.assertion,c.closed_at,c.contract_digest FROM admission_closures c JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY c.admission_id`, "proof"},
	{"release_authorizations", `SELECT r.* FROM admission_release_authorizations r JOIN admission_dispatch_claims d USING(claim_id) JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY r.claim_id`, ""},
	{"run", `SELECT * FROM runs WHERE run_id=? ORDER BY run_id`, ""},
	{"cycles", `SELECT * FROM cycles WHERE run_id=? ORDER BY cycle_no`, ""},
	{"owner", `SELECT * FROM run_active_actor WHERE run_id=?`, ""},
	{"semantic", `SELECT * FROM run_current_semantic_state WHERE run_id=?`, ""},
	{"executions", `SELECT * FROM executions WHERE run_id=? ORDER BY execution_id`, ""},
	{"attempts", `SELECT * FROM attempts WHERE run_id=? ORDER BY attempt_id`, ""},
	{"invocations", `SELECT * FROM invocations WHERE run_id=? ORDER BY invocation_id`, "invocation"},
	{"results", `SELECT * FROM role_results WHERE run_id=? ORDER BY result_id`, "result"},
	{"result_evidence", `SELECT e.* FROM role_result_evidence e JOIN role_results r USING(result_id) WHERE r.run_id=? ORDER BY e.result_id,e.evidence_id`, ""},
	{"checks", `SELECT c.* FROM qa_checks c JOIN role_results r USING(result_id) WHERE r.run_id=? ORDER BY c.result_id,c.check_id`, ""},
	{"check_evidence", `SELECT c.* FROM qa_check_evidence c JOIN role_results r USING(result_id) WHERE r.run_id=? ORDER BY c.result_id,c.check_id,c.evidence_id`, ""},
	{"remaining", `SELECT c.* FROM developer_remaining_work c JOIN role_results r USING(result_id) WHERE r.run_id=? ORDER BY c.result_id,c.ordinal`, ""},
	{"blockers", `SELECT c.* FROM developer_blockers c JOIN role_results r USING(result_id) WHERE r.run_id=? ORDER BY c.result_id`, ""},
	{"blocker_evidence", `SELECT c.* FROM developer_blocker_evidence c JOIN role_results r USING(result_id) WHERE r.run_id=? ORDER BY c.result_id,c.evidence_id`, ""},
	{"findings", `SELECT * FROM findings WHERE run_id=? ORDER BY finding_id`, ""},
	{"occurrences", `SELECT * FROM finding_occurrences WHERE run_id=? ORDER BY occurrence_id`, ""},
	{"occurrence_evidence", `SELECT e.* FROM finding_occurrence_evidence e JOIN finding_occurrences o USING(occurrence_id) WHERE o.run_id=? ORDER BY e.occurrence_id,e.evidence_id`, ""},
	{"occurrence_locations", `SELECT e.* FROM finding_occurrence_locations e JOIN finding_occurrences o USING(occurrence_id) WHERE o.run_id=? ORDER BY e.occurrence_id,e.ordinal`, ""},
	{"lead_states", `SELECT * FROM lead_states WHERE run_id=? ORDER BY lead_state_id`, "lead_state"},
	{"lead_bindings", `SELECT * FROM lead_state_bindings WHERE run_id=? ORDER BY lead_state_id`, ""},
	{"criteria", `SELECT c.* FROM lead_acceptance_criteria c JOIN lead_states l USING(lead_state_id) WHERE l.run_id=? ORDER BY c.lead_state_id,c.ordinal`, ""},
	{"dispositions", `SELECT * FROM lead_finding_dispositions WHERE run_id=? ORDER BY lead_state_id,finding_id`, ""},
	{"finding_sources", `SELECT * FROM lead_finding_sources WHERE run_id=? ORDER BY lead_state_id,finding_id`, ""},
	{"pools", `SELECT * FROM resource_pools WHERE run_id=? ORDER BY pool_id`, "pool"},
	{"eligibility", `SELECT e.* FROM resource_pool_eligibility e JOIN resource_pools p USING(pool_id) WHERE p.run_id=? ORDER BY e.pool_id,e.role,e.purpose`, ""},
	{"grants", `SELECT * FROM resource_allocations WHERE run_id=? ORDER BY allocation_id`, "allocation"},
	{"calls", `SELECT p.* FROM provider_calls p JOIN invocations i USING(invocation_id) WHERE i.run_id=? ORDER BY p.provider_call_id`, "provider_call"},
	{"usage", `SELECT u.* FROM provider_call_usage u JOIN provider_calls p USING(provider_call_id) JOIN invocations i USING(invocation_id) WHERE i.run_id=? ORDER BY u.provider_call_id`, ""},
	// Receipts here are retained immutable evidence. No current usage is selected
	// from them; the separately bound current-usage table is authoritative.
	{"receipts", `SELECT r.* FROM usage_receipts r JOIN invocations i USING(invocation_id) WHERE i.run_id=? ORDER BY r.receipt_id`, "receipt"},
	{"policy", `SELECT * FROM run_admission_policy WHERE run_id=?`, ""},
	{"budgets", `SELECT * FROM run_resource_budgets WHERE run_id=? ORDER BY resource_type`, ""},
	{"deadlines", `SELECT * FROM execution_admission_deadlines WHERE run_id=? ORDER BY execution_id`, ""},
	{"admissions", `SELECT * FROM invocation_admissions WHERE run_id=? ORDER BY admission_id`, "admission"},
	{"admission_resources", `SELECT c.* FROM admission_resources c JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY c.admission_id,c.resource_type`, ""},
	{"contingency", `SELECT t.* FROM admission_contingency_authorizations t JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY t.admission_id`, ""},
	{"admission_grants", `SELECT c.* FROM admission_grants c JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY c.admission_id,c.resource_type,c.allocation_id`, ""},
	{"claims", `SELECT c.* FROM admission_dispatch_claims c JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY c.claim_id`, ""},
	{"bounds", `SELECT b.* FROM admission_dispatch_bounds b JOIN admission_dispatch_claims c USING(claim_id) JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY b.claim_id,b.dimension`, ""},
	{"non_dispatch", `SELECT p.* FROM admission_non_dispatch_proofs p JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY p.proof_id`, ""},
	{"release_evidence", `SELECT e.* FROM admission_controller_release_evidence e JOIN admission_dispatch_claims c USING(claim_id) JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY e.evidence_id`, ""},
	{"submitted_bounds", `SELECT b.* FROM admission_controller_release_bounds b JOIN admission_controller_release_evidence e USING(evidence_id) JOIN admission_dispatch_claims c USING(claim_id) JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY b.evidence_id,b.dimension`, ""},
	{"stops", `SELECT * FROM run_hard_stops WHERE run_id=? ORDER BY stop_id`, "hard_stop"},
	{"stop_decisions", `SELECT d.* FROM hard_stop_decisions d JOIN run_hard_stops s USING(stop_id) WHERE s.run_id=? ORDER BY d.stop_id,d.ordinal`, ""},
	{"stop_usage", `SELECT e.* FROM hard_stop_usage_evidence e JOIN run_hard_stops s USING(stop_id) WHERE s.run_id=? ORDER BY e.stop_id,e.ordinal,e.provider_call_id,e.resource_type`, ""},
	{"human", `SELECT * FROM human_readiness_dispositions WHERE run_id=? ORDER BY disposition_id`, "human_disposition"},
	{"evidence", `SELECT * FROM evidence_artifacts WHERE evidence_id IN (
 SELECT e.evidence_id FROM role_result_evidence e JOIN role_results r USING(result_id) WHERE r.run_id=?1
 UNION SELECT e.evidence_id FROM qa_check_evidence e JOIN role_results r USING(result_id) WHERE r.run_id=?1
 UNION SELECT e.evidence_id FROM developer_blocker_evidence e JOIN role_results r USING(result_id) WHERE r.run_id=?1
 UNION SELECT e.evidence_id FROM finding_occurrence_evidence e JOIN finding_occurrences o USING(occurrence_id) WHERE o.run_id=?1
 UNION SELECT resolution_ref FROM lead_finding_dispositions WHERE run_id=?1)
 ORDER BY evidence_id`, "artifact"},
}

type authorityTable struct {
	Name    string
	Columns []string
	Rows    [][]any
}
type proofDependency struct{ Kind, ID string }

func captureAuthority(ctx context.Context, q querier, run string) ([]authorityTable, []proofDependency, error) {
	var out []authorityTable
	var deps []proofDependency
	sources := append(append([]authoritySource(nil), authoritySources...),
		authoritySource{"agent_profiles", `SELECT * FROM run_agent_token_profiles WHERE run_id=? ORDER BY role`, ""},
		authoritySource{"agent_limits", `SELECT l.* FROM admission_agent_token_limits l JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY l.admission_id`, ""},
		authoritySource{"topup_denials", `SELECT d.* FROM admission_token_topup_denials d JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? ORDER BY d.decision_id`, ""})
	for _, source := range sources {
		rows, err := q.QueryContext(ctx, source.SQL, run)
		if err != nil {
			return nil, nil, fmt.Errorf("authority snapshot %s: %w", source.Name, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		t := authorityTable{Name: source.Name, Columns: columns, Rows: [][]any{}}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				return nil, nil, err
			}
			// database/sql may reuse byte buffers. Copy before advancing the cursor.
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					values[i] = append([]byte{}, b...)
				}
			}
			t.Rows = append(t.Rows, values)
			if source.Dependency != "" {
				id, ok := values[0].(string)
				if !ok {
					rows.Close()
					return nil, nil, fmt.Errorf("invalid authority key")
				}
				deps = append(deps, proofDependency{source.Dependency, id})
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, nil, err
		}
		out = append(out, t)
	}
	return out, deps, nil
}
func authorityDigest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}

// Artifact identities are controller-registered. Verify actual bytes, not just
// metadata or presence. File paths must already be absolute local artifact paths;
// the registered digest is checked again at every proof use.
func verifyAuthorityArtifact(ctx context.Context, q querier, ref ProofArtifact) error {
	var kind, storage, digest string
	var size int64
	var inline []byte
	var path *string
	if err := q.QueryRowContext(ctx, `SELECT kind,storage_kind,sha256,size_bytes,inline_blob,external_path FROM evidence_artifacts WHERE evidence_id=?`, ref.EvidenceID).Scan(&kind, &storage, &digest, &size, &inline, &path); err != nil {
		return err
	}
	if ref.Kind != kind || ref.SHA256 != digest {
		return fmt.Errorf("proof artifact identity/kind/digest mismatch")
	}
	var reader io.Reader
	var file *os.File
	if storage == "sqlite" {
		if int64(len(inline)) != size {
			return fmt.Errorf("evidence size mismatch")
		}
		actual := fmt.Sprintf("%x", sha256.Sum256(inline))
		if actual != digest {
			return fmt.Errorf("evidence bytes digest mismatch")
		}
		return nil
	}
	if path == nil {
		return fmt.Errorf("file evidence path missing")
	}
	file, err := openAuthorityArtifact(*path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if size < 0 || info.Size() != size {
		return fmt.Errorf("file evidence size mismatch")
	}
	reader = file
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(reader, size))
	if err != nil {
		return err
	}
	if n != size || fmt.Sprintf("%x", h.Sum(nil)) != digest {
		return fmt.Errorf("file evidence bytes/size mismatch")
	}
	var extra [1]byte
	if n, err := file.Read(extra[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("file evidence grew during verification")
	}
	return nil
}
func artifactsForDependencies(ctx context.Context, q querier, deps []proofDependency) ([]ProofArtifact, error) {
	refs := []ProofArtifact{}
	for _, d := range deps {
		if d.Kind != "artifact" {
			continue
		}
		var r ProofArtifact
		r.EvidenceID = d.ID
		if err := q.QueryRowContext(ctx, `SELECT kind,sha256 FROM evidence_artifacts WHERE evidence_id=?`, d.ID).Scan(&r.Kind, &r.SHA256); err != nil {
			return nil, err
		}
		refs = append(refs, r)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].EvidenceID < refs[j].EvidenceID })
	return refs, nil
}

// Return the source inventory from one SQLite read transaction, never metrics.
func readAuthoritySnapshot(ctx context.Context, s *StateStore, run string, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Completeness is reported independently of readiness. Known partial subtotals
// stay partial even when a FINAL receipt omits its primitive counters.
func snapshotAccountingIncomplete(snapshot []authorityTable) bool {
	for _, t := range snapshot {
		if t.Name != "usage" {
			continue
		}
		for _, name := range []string{"work_state", "spend_state", "receipt_state"} {
			index := -1
			for i, c := range t.Columns {
				if c == name {
					index = i
					break
				}
			}
			if index < 0 {
				return true
			}
			for _, r := range t.Rows {
				if r[index] != "final" {
					return true
				}
			}
		}
	}
	return false
}
