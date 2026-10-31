package statestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"marc-runtime/internal/identity"
)

// All acceptance APIs are controller-only, just like admission/ownership APIs.
// Evidence bytes supplied by Workers cannot call these APIs or assert human input.
type ProofArtifact struct{ EvidenceID, Kind, SHA256 string }
type ProofReference struct{ ProofID, SHA256 string }
type ProofBoundary struct {
	Binding          InvocationBinding
	Owner            SemanticFence
	SemanticRevision int64
}
type RecoveryProof struct {
	Containment         string
	ContainmentEvidence []ProofArtifact
}
type ControllerValidationProof struct {
	ResultID           string
	ValidationEvidence []ProofArtifact
}
type ReviewAdmissionProof struct {
	Completion, Containment ProofReference
	AdmissionID             string
}
type TerminalProof struct {
	Completion, QAValidation, ReviewValidation, ReviewAdmission, Containment ProofReference
	LeadStateID                                                              string
	HumanDispositions                                                        []string
}
type AuthorityProof struct {
	ProofID, RunID, Kind, ContentIdentity, Verdict, Reason string
	Boundary                                               ProofBoundary
	Evidence                                               []ProofArtifact
	Recovery                                               *RecoveryProof
	Validation                                             *ControllerValidationProof
	Review                                                 *ReviewAdmissionProof
	Terminal                                               *TerminalProof
}
type HumanReadinessDisposition struct {
	DispositionID, RunID, StopID, Classification, ContentIdentity string
	InteractionID, Prompt, Response, Decision                     string
	Boundary                                                      ProofBoundary
	Evidence                                                      []ProofArtifact
}
type sealedAuthorityPayload struct {
	Version              int
	Request              AuthorityProof
	Snapshot             []authorityTable
	Dependencies         []proofDependency
	AccountingIncomplete bool
}
type ProofRecord struct {
	ProofID, RunID, Kind, ContentIdentity, Verdict, SHA256, SourceDigest, SealedAt string
	AccountingIncomplete                                                           bool
}

var errProofReplay = errors.New("identical sealed proof replay")

func openAuthorityArtifact(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("proof requires registered absolute canonical artifact path")
	}
	return openLocalRegularFile(path)
}

// Hash every source ID, rather than mixing literal and hashed names that can
// alias each other. The prefix separates proof and human-response artifacts.
func sealedArtifactID(kind, id string) string {
	return fmt.Sprintf("sealed-v1-%s-%x", kind, sha256.Sum256([]byte(id)))
}

func putSealedArtifact(ctx context.Context, c *sql.Conn, id, kind, at string, payload any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	_, err = c.ExecContext(ctx, `INSERT INTO evidence_artifacts VALUES(?,?,'sqlite',?,?,?,NULL,?)`, id, kind, digest, int64(len(raw)), raw, at)
	return digest, err
}
func normalizeArtifacts(refs []ProofArtifact) ([]ProofArtifact, error) {
	out := append([]ProofArtifact{}, refs...)
	sort.Slice(out, func(i, j int) bool { return out[i].EvidenceID < out[j].EvidenceID })
	for i, r := range out {
		if err := identity.CheckID("evidence_id", r.EvidenceID); err != nil {
			return nil, err
		}
		if strings.TrimSpace(r.EvidenceID) == "" || r.Kind == "" || len(r.SHA256) != 64 || i > 0 && out[i-1].EvidenceID == r.EvidenceID {
			return nil, fmt.Errorf("invalid or duplicate proof evidence")
		}
		if err := admissionUTF8(r.EvidenceID, r.Kind, r.SHA256); err != nil {
			return nil, err
		}
	}
	return out, nil
}
func normalizeProof(p AuthorityProof) (AuthorityProof, error) {
	var err error
	if err = requireIDs(
		struct{ name, value string }{"proof_id", p.ProofID}, struct{ name, value string }{"run_id", p.RunID},
		struct{ name, value string }{"content_identity", p.ContentIdentity}, struct{ name, value string }{"invocation_id", p.Boundary.Binding.InvocationID},
		struct{ name, value string }{"execution_id", p.Boundary.Binding.ExecutionID}, struct{ name, value string }{"attempt_id", p.Boundary.Binding.AttemptID},
	); err != nil {
		return p, err
	}
	if err = admissionUTF8(p.ProofID, p.RunID, p.Kind, p.ContentIdentity, p.Verdict, p.Boundary.Binding.InvocationID, p.Boundary.Binding.ExecutionID, p.Boundary.Binding.AttemptID, p.Boundary.Binding.ContentIdentity, p.Boundary.Binding.Purpose); err != nil {
		return p, err
	}
	if err := descriptionUTF8(p.Reason); err != nil {
		return p, err
	}
	if err = optionalAdmissionUTF8(p.Boundary.Owner.InvocationID, p.Boundary.Owner.ContentIdentity); err != nil {
		return p, err
	}
	ownerFields := []struct{ name, value string }{}
	if p.Boundary.Owner.InvocationID != nil {
		ownerFields = append(ownerFields, struct{ name, value string }{"owner_invocation_id", *p.Boundary.Owner.InvocationID})
	}
	if p.Boundary.Owner.ContentIdentity != nil {
		ownerFields = append(ownerFields, struct{ name, value string }{"owner_content_identity", *p.Boundary.Owner.ContentIdentity})
	}
	if err = requireIDs(ownerFields...); err != nil {
		return p, err
	}
	if p.ProofID == "" || p.RunID == "" || p.ContentIdentity == "" || strings.TrimSpace(p.Reason) == "" {
		return p, fmt.Errorf("proof requires identity/content/controller reason")
	}
	p.Evidence, err = normalizeArtifacts(p.Evidence)
	if err != nil {
		return p, err
	}
	if p.Recovery != nil {
		v := *p.Recovery
		p.Recovery = &v
		v.ContainmentEvidence, err = normalizeArtifacts(v.ContainmentEvidence)
		if err != nil {
			return p, err
		}
	}
	if p.Validation != nil {
		if err = identity.CheckID("result_id", p.Validation.ResultID); err != nil {
			return p, err
		}
		if err = admissionUTF8(p.Validation.ResultID); err != nil {
			return p, err
		}
		v := *p.Validation
		p.Validation = &v
		v.ValidationEvidence, err = normalizeArtifacts(v.ValidationEvidence)
		if err != nil {
			return p, err
		}
	}
	if p.Review != nil {
		if err = requireIDs(struct{ name, value string }{"admission_id", p.Review.AdmissionID}, struct{ name, value string }{"proof_id", p.Review.Completion.ProofID}); err != nil {
			return p, err
		}
		if err = admissionUTF8(p.Review.AdmissionID, p.Review.Completion.ProofID, p.Review.Completion.SHA256); err != nil {
			return p, err
		}
		if err = optionalAdmissionUTF8(&p.Review.Containment.ProofID, &p.Review.Containment.SHA256); err != nil {
			return p, err
		}
		if p.Review.Containment.ProofID != "" {
			if err = identity.CheckID("proof_id", p.Review.Containment.ProofID); err != nil {
				return p, err
			}
		}
	}
	if p.Terminal != nil {
		for _, ref := range []ProofReference{p.Terminal.Completion, p.Terminal.QAValidation, p.Terminal.ReviewValidation, p.Terminal.ReviewAdmission} {
			if err = identity.CheckID("proof_id", ref.ProofID); err != nil {
				return p, err
			}
			if err = admissionUTF8(ref.ProofID, ref.SHA256); err != nil {
				return p, err
			}
		}
		if err = admissionUTF8(p.Terminal.LeadStateID); err != nil {
			return p, err
		}
		if p.Terminal.LeadStateID != "" {
			if err = identity.CheckID("lead_state_id", p.Terminal.LeadStateID); err != nil {
				return p, err
			}
		}
		if err = optionalAdmissionUTF8(&p.Terminal.Containment.ProofID, &p.Terminal.Containment.SHA256); err != nil {
			return p, err
		}
		if p.Terminal.Containment.ProofID != "" {
			if err = identity.CheckID("proof_id", p.Terminal.Containment.ProofID); err != nil {
				return p, err
			}
		}
		v := *p.Terminal
		p.Terminal = &v
		v.HumanDispositions = append([]string{}, v.HumanDispositions...)
		sort.Strings(v.HumanDispositions)
		for i, id := range v.HumanDispositions {
			if err = identity.CheckID("disposition_id", id); err != nil {
				return p, err
			}
			if err = admissionUTF8(id); err != nil {
				return p, err
			}
			if id == "" || i > 0 && id == v.HumanDispositions[i-1] {
				return p, fmt.Errorf("duplicate/empty human disposition")
			}
		}
	}
	bodies := 0
	for _, present := range []bool{p.Recovery != nil, p.Validation != nil, p.Review != nil, p.Terminal != nil} {
		if present {
			bodies++
		}
	}
	if bodies != 1 {
		return p, fmt.Errorf("proof requires exactly its typed body")
	}
	switch p.Kind {
	case "RECOVERY":
		if p.Recovery == nil || (p.Verdict != "RECOVERABLE" && p.Verdict != "UNCONFIRMED") || (p.Recovery.Containment != "CONFIRMED" && p.Recovery.Containment != "UNKNOWN" && p.Recovery.Containment != "EXTERNAL_NO_RELEASE") {
			return p, fmt.Errorf("invalid recovery contract")
		}
	case "CONTROLLER_VALIDATION":
		if p.Validation == nil || (p.Verdict != "VALIDATED" && p.Verdict != "REJECTED") || p.Validation.ResultID == "" || len(p.Validation.ValidationEvidence) == 0 {
			return p, fmt.Errorf("controller validation requires separate evidence")
		}
	case "REVIEW_ADMISSION":
		if p.Review == nil || (p.Verdict != "ELIGIBLE" && p.Verdict != "BLOCKED") {
			return p, fmt.Errorf("invalid review contract")
		}
	case "TERMINAL":
		if p.Terminal == nil || (p.Verdict != "SUPPORTS_READINESS" && p.Verdict != "BLOCKED") {
			return p, fmt.Errorf("invalid terminal contract")
		}
	default:
		return p, fmt.Errorf("unknown proof kind")
	}
	return p, nil
}
func validateProofBoundary(ctx context.Context, c *sql.Conn, run, content string, b ProofBoundary, recovery bool) error {
	if b.Owner.InvocationID != nil && b.Owner.ContentIdentity == nil {
		return fmt.Errorf("bound proof owner requires content fence")
	}
	if _, err := validateOwner(ctx, c, run, b.Owner); err != nil {
		return err
	}
	var execution, attempt, purpose string
	var cycle, currentCycle int
	var storedContent *string
	if err := c.QueryRowContext(ctx, `SELECT i.execution_id,i.attempt_id,i.cycle_no,i.purpose,i.content_identity,r.current_cycle FROM invocations i JOIN runs r USING(run_id) WHERE i.invocation_id=? AND i.run_id=?`, b.Binding.InvocationID, run).Scan(&execution, &attempt, &cycle, &purpose, &storedContent, &currentCycle); err != nil {
		return err
	}
	if execution != b.Binding.ExecutionID || attempt != b.Binding.AttemptID || cycle != b.Binding.Cycle || purpose != b.Binding.Purpose || storedContent == nil || *storedContent != content || b.Binding.ContentIdentity != content {
		return fmt.Errorf("proof ancestry/content substitution")
	}
	var revision int64
	var current *string
	if err := c.QueryRowContext(ctx, `SELECT revision,lead_state_id FROM run_current_semantic_state WHERE run_id=?`, run).Scan(&revision, &current); err != nil {
		return err
	}
	if revision != b.SemanticRevision {
		return fmt.Errorf("stale proof semantic fence")
	}
	if !recovery {
		if cycle != currentCycle {
			return fmt.Errorf("proof cycle is not current")
		}
		if current == nil {
			return fmt.Errorf("authority requires a selected Lead content snapshot")
		}
		var selectedContent string
		if err := c.QueryRowContext(ctx, `SELECT content_identity FROM lead_state_bindings WHERE lead_state_id=? AND run_id=?`, *current, run).Scan(&selectedContent); err != nil {
			return err
		}
		if selectedContent != content {
			return fmt.Errorf("proof is not for current content")
		}
	}
	return nil
}

// SealAuthorityProof validates/binds controller facts. It changes no lifecycle,
// ownership, result, budget or accounting closure and grants no execution.
func (s *StateStore) SealAuthorityProof(ctx context.Context, p AuthorityProof, opts AccountingOptions) (ProofRecord, error) {
	var out ProofRecord
	p, err := normalizeProof(p)
	if err != nil {
		return out, err
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return out, numericFailure("SealAuthorityProof", p.RunID, p.Boundary.Binding.InvocationID, "", err)
	}
	digest, err := authorityDigest(p)
	if err != nil {
		return out, err
	}
	err = s.numericMutation(ctx, "SealAuthorityProof", p.RunID, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT request_digest FROM authority_proofs WHERE proof_id=?`, p.ProofID).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting sealed proof replay")
			}
			record, _, err := loadAuthorityProof(ctx, c, p.ProofID)
			if err != nil {
				return err
			}
			out = record
			return errProofReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err = refreshRunAdmission(ctx, c, p.RunID); err != nil {
			return err
		}
		if err = validateProofBoundary(ctx, c, p.RunID, p.ContentIdentity, p.Boundary, p.Kind == "RECOVERY"); err != nil {
			return err
		}
		configured, err := policyExists(ctx, c, p.RunID)
		if err != nil {
			return err
		}
		if !configured {
			external, err := externalWorkflowStep(ctx, c, p.RunID, p.Boundary.Binding.InvocationID)
			if err != nil {
				return err
			}
			if !external {
				return fmt.Errorf("proof requires authoritative Run admission policy")
			}
		}
		deps, err := validateProofBody(ctx, c, p)
		if err != nil {
			return err
		}
		snapshot, sourceDeps, err := captureAuthority(ctx, c, p.RunID)
		if err != nil {
			return err
		}
		deps = append(deps, sourceDeps...)
		refs, err := artifactsForDependencies(ctx, c, sourceDeps)
		if err != nil {
			return err
		}
		refs = append(refs, p.Evidence...)
		if p.Recovery != nil {
			refs = append(refs, p.Recovery.ContainmentEvidence...)
		}
		if p.Validation != nil {
			refs = append(refs, p.Validation.ValidationEvidence...)
		}
		for _, r := range refs {
			if err = verifyAuthorityArtifact(ctx, c, r); err != nil {
				return err
			}
			deps = append(deps, proofDependency{"artifact", r.EvidenceID})
		}
		sourceDigest, err := authorityDigest(snapshot)
		if err != nil {
			return err
		}
		eid := sealedArtifactID("authority-proof", p.ProofID)
		deps = canonicalProofDependencies(deps)
		sha, err := putSealedArtifact(ctx, c, eid, "authority_proof", at, sealedAuthorityPayload{1, p, snapshot, deps, snapshotAccountingIncomplete(snapshot)})
		if err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO authority_proofs VALUES(?,?,?,?,?,?,?,?,?,?)`, p.ProofID, p.RunID, p.Kind, p.Boundary.Binding.InvocationID, p.ContentIdentity, p.Verdict, eid, digest, sourceDigest, at); err != nil {
			return err
		}
		if err = insertProofDependencies(ctx, c, p.ProofID, deps); err != nil {
			return err
		}
		out = ProofRecord{p.ProofID, p.RunID, p.Kind, p.ContentIdentity, p.Verdict, sha, sourceDigest, at, snapshotAccountingIncomplete(snapshot)}
		return nil
	})
	if errors.Is(err, errProofReplay) {
		return out, nil
	}
	return out, err
}
func insertProofDependencies(ctx context.Context, c *sql.Conn, id string, deps []proofDependency) error {
	seen := map[proofDependency]bool{}
	allowed := map[string]bool{"invocation": true, "result": true, "lead_state": true, "provider_call": true, "receipt": true, "admission": true, "pool": true, "allocation": true, "hard_stop": true, "artifact": true, "proof": true, "human_disposition": true}
	for _, d := range deps {
		if seen[d] {
			continue
		}
		seen[d] = true
		if !allowed[d.Kind] {
			return fmt.Errorf("invalid proof dependency kind")
		}
		column := d.Kind + "_id"
		if d.Kind == "proof" {
			column = "parent_proof_id"
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO authority_proof_dependencies(proof_id,`+column+`) VALUES(?,?)`, id, d.ID); err != nil {
			return err
		}
	}
	return nil
}
func loadAuthorityProof(ctx context.Context, q querier, id string) (ProofRecord, sealedAuthorityPayload, error) {
	var r ProofRecord
	var p sealedAuthorityPayload
	var eid, requestDigest, invocation string
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT p.proof_id,p.run_id,p.kind,p.content_identity,p.verdict,a.sha256,p.source_digest,p.sealed_at,p.evidence_id,p.request_digest,a.inline_blob,p.invocation_id FROM authority_proofs p JOIN evidence_artifacts a USING(evidence_id) WHERE p.proof_id=?`, id).Scan(&r.ProofID, &r.RunID, &r.Kind, &r.ContentIdentity, &r.Verdict, &r.SHA256, &r.SourceDigest, &r.SealedAt, &eid, &requestDigest, &raw, &invocation)
	if err != nil {
		return r, p, err
	}
	if err = verifyAuthorityArtifact(ctx, q, ProofArtifact{eid, "authority_proof", r.SHA256}); err != nil {
		return r, p, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err = dec.Decode(&p); err != nil {
		return r, p, err
	}
	// Snapshot numbers retain exact integer spelling through UseNumber.
	sd, err := authorityDigest(p.Snapshot)
	if err != nil || sd != r.SourceDigest {
		return r, p, fmt.Errorf("sealed source digest mismatch")
	}
	rd, err := authorityDigest(p.Request)
	if err != nil || rd != requestDigest || p.Version != 1 || p.Request.ProofID != id || p.Request.RunID != r.RunID || p.Request.Kind != r.Kind || p.Request.ContentIdentity != r.ContentIdentity || p.Request.Verdict != r.Verdict || p.Request.Boundary.Binding.InvocationID != invocation {
		return r, p, fmt.Errorf("sealed contract/provenance mismatch")
	}
	if _, err = normalizeProof(p.Request); err != nil {
		return r, p, err
	}
	storedDeps, err := readProofDependencies(ctx, q, id)
	if err != nil {
		return r, p, err
	}
	if len(storedDeps) != len(p.Dependencies) {
		return r, p, fmt.Errorf("sealed dependency count mismatch")
	}
	expected, err := authorityDigest(p.Dependencies)
	if err != nil {
		return r, p, err
	}
	actual, err := authorityDigest(canonicalProofDependencies(storedDeps))
	if err != nil || actual != expected {
		return r, p, fmt.Errorf("sealed dependency inventory mismatch")
	}
	if p.AccountingIncomplete != snapshotAccountingIncomplete(p.Snapshot) {
		return r, p, fmt.Errorf("sealed accounting completeness mismatch")
	}
	r.AccountingIncomplete = p.AccountingIncomplete
	return r, p, nil
}

// ValidateCurrentAuthorityProof checks currentness and all bound evidence in one
// SQLite snapshot. It never applies a proof or changes readiness/execution state.
func (s *StateStore) ValidateCurrentAuthorityProof(ctx context.Context, run string, ref ProofReference) (ProofRecord, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"proof_id", ref.ProofID}); err != nil {
		return ProofRecord{}, err
	}
	var out ProofRecord
	err := readAuthoritySnapshot(ctx, s, run, func(tx *sql.Tx) error {
		r, _, err := loadAuthorityProof(ctx, tx, ref.ProofID)
		if err != nil {
			return err
		}
		if r.RunID != run || r.SHA256 != ref.SHA256 {
			return fmt.Errorf("proof reference substitution")
		}
		snapshot, _, err := captureAuthority(ctx, tx, run)
		if err != nil {
			return err
		}
		digest, err := authorityDigest(snapshot)
		if err != nil {
			return err
		}
		if digest != r.SourceDigest {
			return fmt.Errorf("stale authority proof")
		}
		if err = verifyRetainedArtifacts(ctx, tx, ref.ProofID); err != nil {
			return err
		}
		// A valid BLOCKED/UNCONFIRMED/REJECTED record remains evidence, never permission.
		out = r
		return nil
	})
	return out, err
}
func verifyRetainedArtifacts(ctx context.Context, q querier, id string) error {
	return walkProofDependencies(ctx, q, id, map[string]bool{}, map[string]bool{}, map[ProofDependency]bool{}, map[string]RetainedProofArtifact{})
}
