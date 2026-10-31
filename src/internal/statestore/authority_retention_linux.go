package statestore

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

type ProofDependency struct{ Kind, ID string }
type RetainedProofArtifact struct {
	ProofArtifact
	StorageKind  string
	ExternalPath *string
	SizeBytes    int64
}
type AuthorityRetention struct {
	Dependencies []ProofDependency
	Artifacts    []RetainedProofArtifact
}

// This is a read-only dependency inventory, not cleanup policy execution. Every
// returned database object and artifact must remain reachable while the proof is
// retained. Historical proofs are roots too, even after currentness has changed.
func (s *StateStore) AuthorityProofRetention(ctx context.Context, run string, ref ProofReference) (AuthorityRetention, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"proof_id", ref.ProofID}); err != nil {
		return AuthorityRetention{}, err
	}
	var out AuthorityRetention
	err := readAuthoritySnapshot(ctx, s, run, func(tx *sql.Tx) error {
		r, _, err := loadAuthorityProof(ctx, tx, ref.ProofID)
		if err != nil {
			return err
		}
		if r.RunID != run || r.SHA256 != ref.SHA256 {
			return fmt.Errorf("retention proof reference substitution")
		}
		deps := map[ProofDependency]bool{}
		artifacts := map[string]RetainedProofArtifact{}
		if err = walkProofDependencies(ctx, tx, ref.ProofID, map[string]bool{}, map[string]bool{}, deps, artifacts); err != nil {
			return err
		}
		for d := range deps {
			out.Dependencies = append(out.Dependencies, d)
		}
		for _, a := range artifacts {
			out.Artifacts = append(out.Artifacts, a)
		}
		sort.Slice(out.Dependencies, func(i, j int) bool {
			a, b := out.Dependencies[i], out.Dependencies[j]
			if a.Kind != b.Kind {
				return a.Kind < b.Kind
			}
			return a.ID < b.ID
		})
		sort.Slice(out.Artifacts, func(i, j int) bool { return out.Artifacts[i].EvidenceID < out.Artifacts[j].EvidenceID })
		return nil
	})
	return out, err
}
func retainArtifact(ctx context.Context, q querier, id string, artifacts map[string]RetainedProofArtifact) error {
	if _, ok := artifacts[id]; ok {
		return nil
	}
	var a RetainedProofArtifact
	a.EvidenceID = id
	if err := q.QueryRowContext(ctx, `SELECT kind,sha256,storage_kind,external_path,size_bytes FROM evidence_artifacts WHERE evidence_id=?`, id).Scan(&a.Kind, &a.SHA256, &a.StorageKind, &a.ExternalPath, &a.SizeBytes); err != nil {
		return err
	}
	if err := verifyAuthorityArtifact(ctx, q, a.ProofArtifact); err != nil {
		return err
	}
	artifacts[id] = a
	return nil
}

var proofDependencyColumns = []string{"invocation", "result", "lead_state", "provider_call", "receipt", "admission", "pool", "allocation", "hard_stop", "artifact", "proof", "human_disposition"}

func walkProofDependencies(ctx context.Context, q querier, id string, active, seen map[string]bool, deps map[ProofDependency]bool, artifacts map[string]RetainedProofArtifact) error {
	if active[id] {
		return fmt.Errorf("cyclic sealed proof dependency")
	}
	if seen[id] {
		return nil
	}
	active[id] = true
	defer delete(active, id)
	_, _, err := loadAuthorityProof(ctx, q, id)
	if err != nil {
		return err
	}
	deps[ProofDependency{"proof", id}] = true
	var eid string
	if err = q.QueryRowContext(ctx, `SELECT evidence_id FROM authority_proofs WHERE proof_id=?`, id).Scan(&eid); err != nil {
		return err
	}
	if err = retainArtifact(ctx, q, eid, artifacts); err != nil {
		return err
	}
	children, err := readProofDependencies(ctx, q, id)
	if err != nil {
		return err
	}
	for _, d := range children {
		deps[ProofDependency(d)] = true
		switch d.Kind {
		case "proof":
			if err = walkProofDependencies(ctx, q, d.ID, active, seen, deps, artifacts); err != nil {
				return err
			}
		case "artifact":
			if err = retainArtifact(ctx, q, d.ID, artifacts); err != nil {
				return err
			}
		case "human_disposition":
			h, err := loadHumanDisposition(ctx, q, d.ID)
			if err != nil {
				return err
			}
			var artifact string
			if err = q.QueryRowContext(ctx, `SELECT evidence_id FROM human_readiness_dispositions WHERE disposition_id=?`, d.ID).Scan(&artifact); err != nil {
				return err
			}
			if err = retainArtifact(ctx, q, artifact, artifacts); err != nil {
				return err
			}
			for _, ref := range h.Evidence {
				if err = retainArtifact(ctx, q, ref.EvidenceID, artifacts); err != nil {
					return err
				}
			}
		}
	}
	seen[id] = true
	return nil
}
func validateSealedAuthorityDatabase(ctx context.Context, q querier) error {
	rows, err := q.QueryContext(ctx, `SELECT proof_id FROM authority_proofs ORDER BY proof_id`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, _, err = loadAuthorityProof(ctx, q, id); err != nil {
			return err
		}
	}
	rows, err = q.QueryContext(ctx, `SELECT disposition_id FROM human_readiness_dispositions ORDER BY disposition_id`)
	if err != nil {
		return err
	}
	ids = nil
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, err = loadHumanDispositionRecord(ctx, q, id, false); err != nil {
			return err
		}
	}
	// External evidence is reverified when a proof is used, not used to rebuild
	// state or force unrelated historical filesystem reads during startup.
	return nil
}

func readProofDependencies(ctx context.Context, q querier, id string) ([]proofDependency, error) {
	rows, err := q.QueryContext(ctx, `SELECT invocation_id,result_id,lead_state_id,provider_call_id,receipt_id,admission_id,pool_id,allocation_id,hard_stop_id,artifact_id,parent_proof_id,human_disposition_id FROM authority_proof_dependencies WHERE proof_id=?`, id)
	if err != nil {
		return nil, err
	}
	var children []proofDependency
	for rows.Next() {
		values := make([]*string, len(proofDependencyColumns))
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			rows.Close()
			return nil, err
		}
		for i, v := range values {
			if v != nil {
				children = append(children, proofDependency{proofDependencyColumns[i], *v})
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return children, nil
}
func canonicalProofDependencies(deps []proofDependency) []proofDependency {
	seen := map[proofDependency]bool{}
	out := []proofDependency{}
	for _, d := range deps {
		if !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].ID < out[j].ID
	})
	return out
}
