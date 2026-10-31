package statestore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type proofResult struct {
	ID, LogicalID, Invocation, Role, Outcome, Content string
	Cycle                                             int
}

func readProofResult(ctx context.Context, q querier, run, id, content string) (proofResult, error) {
	var r proofResult
	r.LogicalID = id
	err := q.QueryRowContext(ctx, `SELECT result_id,invocation_id,role,outcome,content_identity,cycle_no FROM role_results WHERE run_id=? AND result_id=?`, run, ResultKey(run, id)).Scan(&r.ID, &r.Invocation, &r.Role, &r.Outcome, &r.Content, &r.Cycle)
	if err != nil {
		return r, err
	}
	if r.Content != content {
		return r, fmt.Errorf("result content substitution")
	}
	return r, nil
}
func positiveResult(r proofResult) bool {
	return r.Role == "DEVELOPER" && r.Outcome == "completed" || r.Role == "QA" && r.Outcome == "passed" || r.Role == "REVIEWER" && r.Outcome == "approved"
}
func boundParent(ctx context.Context, q querier, p AuthorityProof, ref ProofReference, kind string, current bool) (sealedAuthorityPayload, error) {
	r, body, err := loadAuthorityProof(ctx, q, ref.ProofID)
	if err != nil {
		return body, err
	}
	if r.SHA256 != ref.SHA256 || r.RunID != p.RunID || r.ContentIdentity != p.ContentIdentity || r.Kind != kind {
		return body, fmt.Errorf("parent proof identity/type/content substitution")
	}
	if err = verifyRetainedArtifacts(ctx, q, ref.ProofID); err != nil {
		return body, err
	}
	if current {
		snapshot, _, err := captureAuthority(ctx, q, p.RunID)
		if err != nil {
			return body, err
		}
		digest, err := authorityDigest(snapshot)
		if err != nil {
			return body, err
		}
		if r.SourceDigest != digest {
			return body, fmt.Errorf("stale supporting controller proof")
		}
	}
	return body, nil
}
func confirmedContainment(ctx context.Context, q querier, p AuthorityProof, ref ProofReference) error {
	body, err := boundParent(ctx, q, p, ref, "RECOVERY", true)
	if err != nil {
		return err
	}
	external, err := externalWorkflowStep(ctx, q, p.RunID, body.Request.Boundary.Binding.InvocationID)
	if err != nil {
		return err
	}
	if body.Request.Verdict != "RECOVERABLE" || (body.Request.Recovery.Containment != "CONFIRMED" && !(external && body.Request.Recovery.Containment == "EXTERNAL_NO_RELEASE")) {
		return fmt.Errorf("confirmed controller containment proof required")
	}
	return nil
}
func validateProofBody(ctx context.Context, c *sql.Conn, p AuthorityProof) ([]proofDependency, error) {
	deps := []proofDependency{}
	switch p.Kind {
	case "RECOVERY":
		external, err := externalWorkflowStep(ctx, c, p.RunID, p.Boundary.Binding.InvocationID)
		if err != nil {
			return nil, err
		}
		if external {
			if p.Recovery.Containment != "EXTERNAL_NO_RELEASE" || len(p.Recovery.ContainmentEvidence) == 0 {
				return nil, fmt.Errorf("external proof cannot claim process containment")
			}
			deps = append(deps, proofDependency{"invocation", p.Boundary.Binding.InvocationID})
			break
		}
		if p.Recovery.Containment == "EXTERNAL_NO_RELEASE" {
			return nil, fmt.Errorf("external no-release evidence requires external workflow mode")
		}
		var admission string
		if err := c.QueryRowContext(ctx, `SELECT admission_id FROM invocation_admissions WHERE invocation_id=? AND run_id=?`, p.Boundary.Binding.InvocationID, p.RunID).Scan(&admission); err != nil {
			return nil, fmt.Errorf("recovery needs original admission: %w", err)
		}
		deps = append(deps, proofDependency{"admission", admission})
		if p.Verdict == "RECOVERABLE" && (p.Recovery.Containment != "CONFIRMED" || len(p.Recovery.ContainmentEvidence) == 0) {
			return nil, fmt.Errorf("recoverable boundary requires controller containment evidence")
		}
	case "CONTROLLER_VALIDATION":
		r, err := readProofResult(ctx, c, p.RunID, p.Validation.ResultID, p.ContentIdentity)
		if err != nil {
			return nil, err
		}
		if r.Invocation != p.Boundary.Binding.InvocationID || r.Cycle != p.Boundary.Binding.Cycle {
			return nil, fmt.Errorf("validation result ancestry substitution")
		}
		if p.Verdict == "VALIDATED" && !positiveResult(r) {
			return nil, fmt.Errorf("negative/incomplete worker result cannot supply positive completion")
		}
		if p.Verdict == "VALIDATED" {
			for _, ref := range p.Validation.ValidationEvidence {
				var reused int
				if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM role_result_evidence WHERE result_id=? AND evidence_id=?) OR EXISTS(SELECT 1 FROM qa_check_evidence WHERE result_id=? AND evidence_id=?) OR EXISTS(SELECT 1 FROM finding_occurrence_evidence e JOIN finding_occurrences o USING(occurrence_id) WHERE o.result_id=? AND e.evidence_id=?)`, r.ID, ref.EvidenceID, r.ID, ref.EvidenceID, r.ID, ref.EvidenceID).Scan(&reused); err != nil {
					return nil, err
				}
				if reused != 0 {
					return nil, fmt.Errorf("worker's own evidence is not separate controller validation")
				}
			}
		}
		external, err := externalWorkflowStep(ctx, c, p.RunID, r.Invocation)
		if err != nil {
			return nil, err
		}
		deps = append(deps, proofDependency{"result", r.ID})
		if external {
			deps = append(deps, proofDependency{"invocation", r.Invocation})
		} else {
			var admission string
			if err = c.QueryRowContext(ctx, `SELECT admission_id FROM invocation_admissions WHERE invocation_id=? AND run_id=?`, r.Invocation, p.RunID).Scan(&admission); err != nil {
				return nil, fmt.Errorf("validation lacks admission provenance: %w", err)
			}
			deps = append(deps, proofDependency{"admission", admission})
		}
	case "REVIEW_ADMISSION":
		completion, err := boundParent(ctx, c, p, p.Review.Completion, "CONTROLLER_VALIDATION", true)
		if err != nil {
			return nil, err
		}
		r, err := readProofResult(ctx, c, p.RunID, completion.Request.Validation.ResultID, p.ContentIdentity)
		if err != nil {
			return nil, err
		}
		if completion.Request.Verdict != "VALIDATED" || r.Role != "DEVELOPER" || r.Outcome != "completed" {
			return nil, fmt.Errorf("review needs accepted completion and controller validation")
		}
		external, err := externalWorkflowStep(ctx, c, p.RunID, p.Boundary.Binding.InvocationID)
		if err != nil {
			return nil, err
		}
		if external {
			if p.Review.AdmissionID != p.Boundary.Binding.InvocationID || p.Boundary.Binding.Purpose != "REVIEW" || p.Boundary.Owner.Role != "REVIEWER" {
				return nil, fmt.Errorf("external review needs exact independent role admission")
			}
			if _, err := validateOwner(ctx, c, p.RunID, p.Boundary.Owner); err != nil {
				return nil, err
			}
			if p.Verdict != "ELIGIBLE" {
				return nil, fmt.Errorf("unsupported external review verdict")
			}
			if err := confirmedContainment(ctx, c, p, p.Review.Containment); err != nil {
				return nil, err
			}
			if err := proofResourcesSettled(ctx, c, p.RunID, p.Boundary.Binding.InvocationID); err != nil {
				return nil, err
			}
			deps = append(deps, proofDependency{"proof", p.Review.Completion.ProofID}, proofDependency{"proof", p.Review.Containment.ProofID}, proofDependency{"invocation", p.Boundary.Binding.InvocationID})
			break
		}
		a, err := readAdmission(ctx, c, p.RunID, p.Review.AdmissionID)
		if err != nil {
			return nil, err
		}
		if a.Binding != p.Boundary.Binding || a.Binding.InvocationID == r.Invocation {
			return nil, fmt.Errorf("review admission ancestry/content mismatch")
		}
		var role string
		if err = c.QueryRowContext(ctx, `SELECT role FROM invocations WHERE invocation_id=?`, a.Binding.InvocationID).Scan(&role); err != nil {
			return nil, err
		}
		if role != "REVIEWER" || (a.Binding.Purpose != "REVIEW" && a.Binding.Purpose != "FINALIZE_REVIEW") || a.Kind != "NORMAL" {
			return nil, fmt.Errorf("independent reviewer eligibility required")
		}
		if p.Verdict == "ELIGIBLE" {
			if err = validateAdmissionFence(ctx, c, p.RunID, a.Binding, a.Fence); err != nil {
				return nil, err
			}
			if err = confirmedContainment(ctx, c, p, p.Review.Containment); err != nil {
				return nil, err
			}
			if err = proofResourcesSettled(ctx, c, p.RunID, a.Binding.InvocationID); err != nil {
				return nil, err
			}
			var stopped int
			if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM v_run_token_hard_stops WHERE (resource_type IS NULL OR resource_type IN ('WORK','SPEND','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) AND run_id=?)`, p.RunID).Scan(&stopped); err != nil {
				return nil, err
			}
			if stopped != 0 {
				return nil, fmt.Errorf("hard stop cannot authorize normal consuming review")
			}
		} else if p.Review.Containment.ProofID != "" {
			if _, err = boundParent(ctx, c, p, p.Review.Containment, "RECOVERY", true); err != nil {
				return nil, err
			}
		}
		deps = append(deps, proofDependency{"proof", p.Review.Completion.ProofID}, proofDependency{"admission", p.Review.AdmissionID})
		if p.Review.Containment.ProofID != "" {
			deps = append(deps, proofDependency{"proof", p.Review.Containment.ProofID})
		}
	case "TERMINAL":
		t := p.Terminal
		refs := []ProofReference{t.Completion, t.QAValidation, t.ReviewValidation}
		roles := []string{"DEVELOPER", "QA", "REVIEWER"}
		results := make([]proofResult, 3)
		for i, ref := range refs {
			body, err := boundParent(ctx, c, p, ref, "CONTROLLER_VALIDATION", true)
			if err != nil {
				return nil, err
			}
			r, err := readProofResult(ctx, c, p.RunID, body.Request.Validation.ResultID, p.ContentIdentity)
			if err != nil {
				return nil, err
			}
			if body.Request.Verdict != "VALIDATED" || r.Role != roles[i] || !positiveResult(r) || r.Cycle != p.Boundary.Binding.Cycle {
				return nil, fmt.Errorf("terminal lacks independent positive authority chain")
			}
			results[i] = r
			deps = append(deps, proofDependency{"proof", ref.ProofID})
		}
		if results[0].Invocation == results[1].Invocation || results[0].Invocation == results[2].Invocation || results[1].Invocation == results[2].Invocation {
			return nil, fmt.Errorf("terminal evidence is not independent")
		}
		admission, err := boundParent(ctx, c, p, t.ReviewAdmission, "REVIEW_ADMISSION", false)
		if err != nil {
			return nil, err
		}
		if admission.Request.Boundary.Binding.InvocationID != results[2].Invocation {
			return nil, fmt.Errorf("review result does not match original review proof")
		}
		originalCompletion, err := boundParent(ctx, c, p, admission.Request.Review.Completion, "CONTROLLER_VALIDATION", false)
		if err != nil {
			return nil, err
		}
		if originalCompletion.Request.Validation.ResultID != results[0].LogicalID {
			return nil, fmt.Errorf("review completion substitution")
		}
		var selected, content, status, producer string
		var qa, review *string
		if err = c.QueryRowContext(ctx, `SELECT s.lead_state_id,b.content_identity,l.semantic_status,l.producer_invocation_id,b.qa_result_id,b.review_result_id FROM run_current_semantic_state s JOIN lead_states l USING(lead_state_id) JOIN lead_state_bindings b USING(lead_state_id) WHERE s.run_id=?`, p.RunID).Scan(&selected, &content, &status, &producer, &qa, &review); err != nil {
			return nil, err
		}
		if selected != t.LeadStateID || content != p.ContentIdentity || status != "approved" || producer != p.Boundary.Binding.InvocationID || qa == nil || *qa != results[1].ID || review == nil || *review != results[2].ID {
			return nil, fmt.Errorf("terminal requires current Lead approval and exact QA/review bindings")
		}
		if _, err := acceptedRoleState(ctx, c, p.RunID, p.ContentIdentity, "QA", roleState{"passed", &resultReference{results[1].LogicalID}}); err != nil {
			return nil, fmt.Errorf("terminal QA freshness: %w", err)
		}
		if _, err := acceptedRoleState(ctx, c, p.RunID, p.ContentIdentity, "REVIEWER", roleState{"approved", &resultReference{results[2].LogicalID}}); err != nil {
			return nil, fmt.Errorf("terminal review freshness: %w", err)
		}
		var blocking int
		if err = c.QueryRowContext(ctx, `SELECT count(*) FROM lead_finding_dispositions d JOIN findings f USING(run_id,finding_id) WHERE d.lead_state_id=? AND d.disposition='open' AND f.blocking=1`, selected).Scan(&blocking); err != nil {
			return nil, err
		}
		if blocking != 0 {
			return nil, fmt.Errorf("terminal Lead approval has blocking findings")
		}
		deps = append(deps, proofDependency{"lead_state", selected}, proofDependency{"proof", t.ReviewAdmission.ProofID})
		if t.Containment.ProofID != "" {
			if err = confirmedContainment(ctx, c, p, t.Containment); err != nil {
				return nil, err
			}
			deps = append(deps, proofDependency{"proof", t.Containment.ProofID})
		}
		for _, id := range t.HumanDispositions {
			if err = validateHumanForProof(ctx, c, p, id); err != nil {
				return nil, err
			}
			deps = append(deps, proofDependency{"human_disposition", id})
		}
		if p.Verdict == "SUPPORTS_READINESS" {
			if p.Boundary.Owner.Role != "LEAD" || admission.Request.Verdict != "ELIGIBLE" || t.Containment.ProofID == "" {
				return nil, fmt.Errorf("terminal lacks completed authority/containment")
			}
			if err = proofResourcesSettled(ctx, c, p.RunID, ""); err != nil {
				return nil, err
			}
			if err = terminalStopDispositions(ctx, c, p); err != nil {
				return nil, err
			}
		}
	}
	return deps, nil
}
func proofResourcesSettled(ctx context.Context, q querier, run, excludeInvocation string) error {
	var unresolved bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_runs w JOIN invocations i USING(run_id) WHERE w.run_id=? AND json_extract(w.spec_json,'$.Mode')='EXTERNAL' AND i.invocation_id<>? AND i.completed_at IS NULL)`, run, excludeInvocation).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved {
		return fmt.Errorf("external role completion remains open")
	}
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM invocation_admissions a WHERE a.run_id=?1 AND a.invocation_id<>?2
 AND NOT EXISTS(SELECT 1 FROM admission_closures c WHERE c.admission_id=a.admission_id)
 AND NOT EXISTS(SELECT 1 FROM admission_non_dispatch_proofs n WHERE n.admission_id=a.admission_id))
 OR EXISTS(SELECT 1 FROM provider_calls p JOIN invocations i USING(invocation_id) LEFT JOIN provider_call_usage u USING(provider_call_id)
 WHERE i.run_id=?1 AND i.invocation_id<>?2 AND (p.duration_ns IS NULL OR u.receipt_state IS NOT 'final'))`, run, excludeInvocation).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved {
		return fmt.Errorf("no-further-call, physical completion or FINAL obligation remains open")
	}
	return nil
}

func validateHumanForProof(ctx context.Context, q querier, p AuthorityProof, id string) error {
	h, err := loadHumanDisposition(ctx, q, id)
	if err != nil {
		return err
	}
	if h.RunID != p.RunID || h.ContentIdentity != p.ContentIdentity {
		return fmt.Errorf("human disposition content/run substitution")
	}
	return nil
}
func terminalStopDispositions(ctx context.Context, q querier, p AuthorityProof) error {
	rows, err := q.QueryContext(ctx, `SELECT s.stop_id,s.source_id,COALESCE(d.classification,'PENDING') FROM v_run_token_hard_stops s LEFT JOIN hard_stop_decisions d ON d.stop_id=s.stop_id AND d.ordinal=1 WHERE s.run_id=? AND (s.resource_type IS NULL OR s.resource_type IN ('WORK','SPEND','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) ORDER BY s.stop_id`, p.RunID)
	if err != nil {
		return err
	}
	type stop struct{ id, source, class string }
	var stops []stop
	for rows.Next() {
		var s stop
		if err = rows.Scan(&s.id, &s.source, &s.class); err != nil {
			rows.Close()
			return err
		}
		stops = append(stops, s)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	for _, id := range p.Terminal.HumanDispositions {
		h, err := loadHumanDisposition(ctx, q, id)
		if err != nil {
			return err
		}
		if h.RunID != p.RunID || h.ContentIdentity != p.ContentIdentity {
			return fmt.Errorf("human disposition content/run substitution")
		}
		if h.Decision == "ALLOW_TERMINAL_EVALUATION" {
			allowed[h.StopID] = true
		}
	}
	for _, s := range stops {
		if s.class == "PENDING" {
			return fmt.Errorf("pending hard stop cannot support readiness")
		}
		if s.class == "BOUNDED_TERMINAL_OVERRUN" {
			if !strings.HasPrefix(s.source, "agent:") {
				continue
			}
			// An old misclassification cannot waive human disposition. The
			// appended correction preserves its original stop and decision.
			correction := stopIdentity(p.RunID, "BUDGET_EXCEEDED", "agent-hard:"+strings.TrimPrefix(s.source, "agent:"))
			if !allowed[correction] {
				return fmt.Errorf("agent Hard violation requires explicit allowing human disposition")
			}
			continue
		}
		if !allowed[s.id] {
			return fmt.Errorf("hard stop requires explicit allowing human disposition")
		}
	}
	return nil
}
