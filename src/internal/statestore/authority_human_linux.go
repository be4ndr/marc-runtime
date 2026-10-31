package statestore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// RecordHumanReadinessDisposition accepts only a response actually obtained by
// the owning Orchestrator's normal interactive flow. It is not an agent wire,
// autonomous waiver, operator API or permission to continue consuming work.
func (s *StateStore) RecordHumanReadinessDisposition(ctx context.Context, h HumanReadinessDisposition, opts AccountingOptions) error {
	if err := admissionUTF8(h.DispositionID, h.RunID, h.StopID, h.Classification, h.ContentIdentity, h.InteractionID, h.Decision); err != nil {
		return err
	}
	if err := descriptionUTF8(h.Prompt, h.Response); err != nil {
		return err
	}
	for _, v := range []string{h.DispositionID, h.RunID, h.StopID, h.ContentIdentity, h.InteractionID, h.Prompt, h.Response} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("human disposition requires exact interaction and response")
		}
	}
	if h.Decision != "ALLOW_TERMINAL_EVALUATION" && h.Decision != "DENY_TERMINAL_EVALUATION" {
		return fmt.Errorf("invalid human readiness decision")
	}
	if err := admissionUTF8(h.Boundary.Binding.InvocationID, h.Boundary.Binding.ExecutionID, h.Boundary.Binding.AttemptID, h.Boundary.Binding.ContentIdentity, h.Boundary.Binding.Purpose); err != nil {
		return err
	}
	if err := requireIDs(
		struct{ name, value string }{"disposition_id", h.DispositionID}, struct{ name, value string }{"run_id", h.RunID},
		struct{ name, value string }{"stop_id", h.StopID}, struct{ name, value string }{"content_identity", h.ContentIdentity},
		struct{ name, value string }{"interaction_id", h.InteractionID}, struct{ name, value string }{"invocation_id", h.Boundary.Binding.InvocationID},
		struct{ name, value string }{"execution_id", h.Boundary.Binding.ExecutionID}, struct{ name, value string }{"attempt_id", h.Boundary.Binding.AttemptID},
	); err != nil {
		return err
	}
	if err := optionalAdmissionUTF8(h.Boundary.Owner.InvocationID, h.Boundary.Owner.ContentIdentity); err != nil {
		return err
	}
	var err error
	h.Evidence, err = normalizeArtifacts(h.Evidence)
	if err != nil {
		return err
	}
	if len(h.Evidence) == 0 {
		return fmt.Errorf("human disposition needs supporting evidence")
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return numericFailure("RecordHumanReadinessDisposition", h.RunID, h.Boundary.Binding.InvocationID, "", err)
	}
	digest, err := authorityDigest(h)
	if err != nil {
		return err
	}
	err = s.numericMutation(ctx, "RecordHumanReadinessDisposition", h.RunID, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT request_digest FROM human_readiness_dispositions WHERE disposition_id=?`, h.DispositionID).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting human response replay")
			}
			if _, err = loadHumanDisposition(ctx, c, h.DispositionID); err != nil {
				return err
			}
			return errProofReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err = validateProofBoundary(ctx, c, h.RunID, h.ContentIdentity, h.Boundary, false); err != nil {
			return err
		}
		var run, class string
		err = c.QueryRowContext(ctx, `SELECT s.run_id,d.classification FROM run_hard_stops s JOIN hard_stop_decisions d ON d.stop_id=s.stop_id AND d.ordinal=1 WHERE s.stop_id=?`, h.StopID).Scan(&run, &class)
		if err != nil {
			return fmt.Errorf("human response requires resolved hard stop: %w", err)
		}
		if run != h.RunID || class != h.Classification || (class != "CONTROLLER_ADMISSION_BUG" && class != "OBSERVED_DISPATCH_BOUND_EXCEEDED") {
			return fmt.Errorf("human disposition stop/classification substitution")
		}
		for _, ref := range h.Evidence {
			if err = verifyAuthorityArtifact(ctx, c, ref); err != nil {
				return err
			}
		}
		eid := sealedArtifactID("human-readiness", h.DispositionID)
		if _, err = putSealedArtifact(ctx, c, eid, "human_readiness_disposition", at, h); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO human_readiness_dispositions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, h.DispositionID, h.RunID, h.StopID, h.Classification, h.ContentIdentity, h.InteractionID, h.Decision, eid, digest, at, h.Boundary.Binding.InvocationID, h.Boundary.Owner.InvocationID); err != nil {
			return err
		}
		for _, ref := range h.Evidence {
			if _, err = c.ExecContext(ctx, `INSERT INTO human_disposition_dependencies VALUES(?,?)`, h.DispositionID, ref.EvidenceID); err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, errProofReplay) {
		return nil
	}
	return err
}
func loadHumanDisposition(ctx context.Context, q querier, id string) (HumanReadinessDisposition, error) {
	return loadHumanDispositionRecord(ctx, q, id, true)
}
func loadHumanDispositionRecord(ctx context.Context, q querier, id string, verifySupporting bool) (HumanReadinessDisposition, error) {
	var h HumanReadinessDisposition
	var run, stop, class, content, interaction, decision, eid, digest, sha string
	var raw []byte
	var invocation string
	var owner *string
	err := q.QueryRowContext(ctx, `SELECT h.run_id,h.stop_id,h.classification,h.content_identity,h.interaction_id,h.decision,h.evidence_id,h.request_digest,a.sha256,a.inline_blob,h.invocation_id,h.owner_invocation_id FROM human_readiness_dispositions h JOIN evidence_artifacts a USING(evidence_id) WHERE disposition_id=?`, id).Scan(&run, &stop, &class, &content, &interaction, &decision, &eid, &digest, &sha, &raw, &invocation, &owner)
	if err != nil {
		return h, err
	}
	if err = verifyAuthorityArtifact(ctx, q, ProofArtifact{eid, "human_readiness_disposition", sha}); err != nil {
		return h, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&h); err != nil {
		return h, err
	}
	actual, err := authorityDigest(h)
	if err != nil {
		return h, err
	}
	if actual != digest || h.DispositionID != id || h.RunID != run || h.StopID != stop || h.Classification != class || h.ContentIdentity != content || h.InteractionID != interaction || h.Decision != decision || h.Boundary.Binding.InvocationID != invocation || (owner == nil) != (h.Boundary.Owner.InvocationID == nil) || (owner != nil && *owner != *h.Boundary.Owner.InvocationID) {
		return h, fmt.Errorf("human disposition integrity/provenance mismatch")
	}
	var storedRun, storedClass string
	if err = q.QueryRowContext(ctx, `SELECT s.run_id,d.classification FROM run_hard_stops s JOIN hard_stop_decisions d ON d.stop_id=s.stop_id AND d.ordinal=1 WHERE s.stop_id=?`, stop).Scan(&storedRun, &storedClass); err != nil {
		return h, err
	}
	if storedRun != run || storedClass != class {
		return h, fmt.Errorf("human disposition hard stop changed")
	}
	rows, err := q.QueryContext(ctx, `SELECT evidence_id FROM human_disposition_dependencies WHERE disposition_id=? ORDER BY evidence_id`, id)
	if err != nil {
		return h, err
	}
	var ids []string
	for rows.Next() {
		var eid string
		if err = rows.Scan(&eid); err != nil {
			rows.Close()
			return h, err
		}
		ids = append(ids, eid)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return h, err
	}
	if len(ids) != len(h.Evidence) {
		return h, fmt.Errorf("human evidence dependencies mismatch")
	}
	for i, ref := range h.Evidence {
		if ids[i] != ref.EvidenceID {
			return h, fmt.Errorf("human evidence substitution")
		}
		if !verifySupporting {
			continue
		}
		if err = verifyAuthorityArtifact(ctx, q, ref); err != nil {
			return h, err
		}
	}
	return h, nil
}
