package statestore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"marc-runtime/src/internal/roleresult"
)

// ResultFence binds acceptance to the active semantic invocation and the actor
// revision observed by the Orchestrator. It does not transfer actor ownership.
type ResultFence struct {
	OwnershipEpoch int64
	ActorRevision  int64
}

// ResultKey explicitly qualifies a run-scoped logical ID for the installation
// PK. The components come from structured fields; this key is never parsed.
func ResultKey(runID, resultID string) string { return runID + "/" + resultID }

// ResultReturnOptions binds semantic acceptance to its Worker fence and supplies
// the controller's return time and optional opaque audit rows.
type ResultReturnOptions struct {
	Fence      ResultFence
	ReturnedAt string
	Audit      []AuditRow
}

// AcceptRoleResult preserves the P1.6 semantic checks and atomically returns
// ownership to unbound Lead. Call AcceptRoleResultAndReturnToLead to supply an
// explicit controller timestamp and audit. Neither path changes lifecycle,
// dispositions or resources. Normal completion/output policy is checked by the
// Orchestrator before calling; evidence must already be registered.
func (s *StateStore) AcceptRoleResult(ctx context.Context, raw []byte, definitions []roleresult.Finding, fence ResultFence) error {
	return s.AcceptRoleResultAndReturnToLead(ctx, raw, definitions, ResultReturnOptions{
		Fence: fence, ReturnedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// AcceptRoleResultAndReturnToLead accepts every valid Worker semantic outcome,
// including incomplete/blocked/failed/inconclusive, then returns supervision to
// Lead in the same transaction. Execution failure is never a semantic outcome.
func (s *StateStore) AcceptRoleResultAndReturnToLead(ctx context.Context, raw []byte, definitions []roleresult.Finding, options ResultReturnOptions) error {
	r, err := roleresult.Decode(raw)
	if err != nil {
		return numericFailure("AcceptRoleResultAndReturnToLead", r.RunID, r.InvocationID, "", err)
	}
	for _, f := range definitions {
		if err := roleresult.ValidateFinding(f); err != nil {
			return err
		}
	}
	if err := validateOwnershipTime(options.ReturnedAt); err != nil {
		return numericFailure("AcceptRoleResultAndReturnToLead", r.RunID, r.InvocationID, "", err)
	}
	return s.numericMutation(ctx, "AcceptRoleResultAndReturnToLead", r.RunID, options.Audit, func(c *sql.Conn) error {
		if err := acceptRoleResultFacts(ctx, c, r, definitions, options.Fence); err != nil {
			return err
		}
		actor, err := validateOwner(ctx, c, r.RunID, SemanticFence{
			Role: r.Role, InvocationID: &r.InvocationID, OwnershipEpoch: options.Fence.OwnershipEpoch,
			ActorRevision: options.Fence.ActorRevision, ContentIdentity: &r.ContentIdentity,
		})
		if err != nil {
			return err
		}
		return updateOwner(ctx, c, actor, "LEAD", nil, options.ReturnedAt)
	})
}

func validateRoleResultFacts(ctx context.Context, tx *sql.Conn, r roleresult.Result, definitions []roleresult.Finding, fence ResultFence) ([]roleresult.Finding, error) {
	if err := validateWorkflowSemanticLifecycle(ctx, tx, r.RunID, r.InvocationID); err != nil {
		return nil, err
	}
	var role, invocation string
	var epoch, revision int64
	if err := tx.QueryRowContext(ctx, "SELECT active_role,active_invocation_id,ownership_epoch,revision FROM run_active_actor WHERE run_id=?", r.RunID).Scan(&role, &invocation, &epoch, &revision); err != nil {
		return nil, fmt.Errorf("active actor: %w", err)
	}
	if role != r.Role || invocation != r.InvocationID || epoch != fence.OwnershipEpoch || revision != fence.ActorRevision {
		return nil, fmt.Errorf("stale or conflicting result fence")
	}
	var run, execution, attempt, invocationRole, content string
	var cycle int
	if err := tx.QueryRowContext(ctx, "SELECT run_id,cycle_no,execution_id,attempt_id,role,content_identity FROM invocations WHERE invocation_id=?", r.InvocationID).Scan(&run, &cycle, &execution, &attempt, &invocationRole, &content); err != nil {
		return nil, err
	}
	if run != r.RunID || cycle != r.Cycle || execution != r.ExecutionID || attempt != r.AttemptID || invocationRole != r.Role || content != r.ContentIdentity {
		return nil, fmt.Errorf("result does not match persisted invocation binding")
	}
	// Structured cycle/ordinal order must agree with acceptance order for a
	// role's relevant content. Otherwise a later verdict could appear older.
	var backwards bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM role_results WHERE run_id=? AND role=? AND content_identity=? AND (cycle_no>? OR (cycle_no=? AND result_ordinal>=?)))`, r.RunID, r.Role, r.ContentIdentity, r.Cycle, r.Cycle, r.ResultOrdinal).Scan(&backwards); err != nil {
		return nil, err
	}
	if backwards {
		return nil, fmt.Errorf("result cycle/ordinal does not advance the relevant role history")
	}
	findings := map[string]roleresult.Finding{}
	rows, err := tx.QueryContext(ctx, `SELECT f.finding_id,f.finding_ordinal,f.origin_cycle,r.cycle_no,r.result_ordinal,f.blocking,f.summary
 FROM findings f JOIN role_results r ON r.result_id=f.origin_result_id AND r.run_id=f.run_id WHERE f.run_id=?`, r.RunID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var f roleresult.Finding
		var originCycle, originOrdinal int
		if err := rows.Scan(&f.FindingID, &f.FindingOrdinal, &f.OriginCycle, &originCycle, &originOrdinal, &f.Blocking, &f.Summary); err != nil {
			rows.Close()
			return nil, err
		}
		f.OriginResultID = roleresult.ResultID(originCycle, originOrdinal)
		if err := roleresult.ValidateFinding(f); err != nil {
			rows.Close()
			return nil, err
		}
		findings[f.FindingID] = f
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	newFindings := []roleresult.Finding{}
	seen := map[string]bool{}
	observed := map[string]bool{}
	for _, o := range r.FindingOccurrences {
		observed[o.FindingID] = true
	}
	for _, f := range definitions {
		if seen[f.FindingID] {
			return nil, fmt.Errorf("duplicate stable finding definition")
		}
		seen[f.FindingID] = true
		if !observed[f.FindingID] {
			return nil, fmt.Errorf("finding definition has no occurrence in this result")
		}
		if existing, ok := findings[f.FindingID]; ok {
			if !roleresult.SameFinding(existing, f) {
				return nil, fmt.Errorf("immutable finding changed: %s", f.FindingID)
			}
		} else {
			if f.OriginCycle != r.Cycle || f.OriginResultID != r.ResultID {
				return nil, fmt.Errorf("new finding origin does not match this result")
			}
			findings[f.FindingID] = f
			newFindings = append(newFindings, f)
		}
	}
	evidence := map[string]string{}
	refs := append([]roleresult.EvidenceRef{}, r.EvidenceRefs...)
	if r.Blocker != nil {
		refs = append(refs, r.Blocker.EvidenceRefs...)
	}
	for _, c := range r.Checks {
		refs = append(refs, c.EvidenceRefs...)
	}
	for _, o := range r.FindingOccurrences {
		refs = append(refs, o.EvidenceRefs...)
	}
	for _, e := range refs {
		if _, ok := evidence[e.EvidenceID]; ok {
			continue
		}
		var kind string
		if err := tx.QueryRowContext(ctx, "SELECT kind FROM evidence_artifacts WHERE evidence_id=?", e.EvidenceID).Scan(&kind); err != nil {
			return nil, fmt.Errorf("evidence %s: %w", e.EvidenceID, err)
		}
		evidence[e.EvidenceID] = kind
	}
	if err := roleresult.ValidateReferences(r, findings, evidence); err != nil {
		return nil, err
	}
	return newFindings, nil
}

// acceptRoleResultFacts is the unchanged P1.6 validation/mapping within the
// caller's physical connection and transaction; it cannot commit independently.
func acceptRoleResultFacts(ctx context.Context, tx *sql.Conn, r roleresult.Result, definitions []roleresult.Finding, fence ResultFence) error {
	newFindings, err := validateRoleResultFacts(ctx, tx, r, definitions, fence)
	if err != nil {
		return err
	}
	key := ResultKey(r.RunID, r.ResultID)
	if _, err := tx.ExecContext(ctx, `INSERT INTO role_results(result_id,run_id,cycle_no,result_ordinal,execution_id,attempt_id,invocation_id,role,outcome,summary,content_identity,produced_at)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, key, r.RunID, r.Cycle, r.ResultOrdinal, r.ExecutionID, r.AttemptID, r.InvocationID, r.Role, r.Outcome, r.Summary, r.ContentIdentity, r.ProducedAt); err != nil {
		return err
	}
	for _, f := range newFindings {
		if _, err := tx.ExecContext(ctx, `INSERT INTO findings(run_id,finding_id,finding_ordinal,origin_cycle,origin_result_id,blocking,summary,created_at) VALUES (?,?,?,?,?,?,?,?)`, r.RunID, f.FindingID, f.FindingOrdinal, f.OriginCycle, key, f.Blocking, f.Summary, r.ProducedAt); err != nil {
			return err
		}
	}
	for i, work := range r.RemainingWork {
		if _, err := tx.ExecContext(ctx, "INSERT INTO developer_remaining_work VALUES (?,?,?)", key, i+1, work); err != nil {
			return err
		}
	}
	if r.Blocker != nil {
		if _, err := tx.ExecContext(ctx, "INSERT INTO developer_blockers VALUES (?,?)", key, r.Blocker.Summary); err != nil {
			return err
		}
		if err := insertEvidence(ctx, tx, "developer_blocker_evidence", []any{key}, r.Blocker.EvidenceRefs); err != nil {
			return err
		}
	}
	if err := insertEvidence(ctx, tx, "role_result_evidence", []any{key}, r.EvidenceRefs); err != nil {
		return err
	}
	for _, c := range r.Checks {
		if _, err := tx.ExecContext(ctx, "INSERT INTO qa_checks VALUES (?,?,?,?,?)", key, c.CheckID, c.Required, c.Outcome, c.Summary); err != nil {
			return err
		}
		if err := insertEvidence(ctx, tx, "qa_check_evidence", []any{key, c.CheckID}, c.EvidenceRefs); err != nil {
			return err
		}
	}
	for _, o := range r.FindingOccurrences {
		var id int64
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(occurrence_id),0) FROM finding_occurrences").Scan(&id); err != nil {
			return err
		}
		id, err := checkedNext(id, "finding_occurrence_id")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO finding_occurrences(occurrence_id,run_id,finding_id,result_id,observed_at) VALUES (?,?,?,?,?)", id, r.RunID, o.FindingID, key, r.ProducedAt); err != nil {
			return err
		}
		if err := insertEvidence(ctx, tx, "finding_occurrence_evidence", []any{id}, o.EvidenceRefs); err != nil {
			return err
		}
		for i, l := range o.Locations {
			if _, err := tx.ExecContext(ctx, "INSERT INTO finding_occurrence_locations VALUES (?,?,?,?,?)", id, i+1, l.Path, l.StartLine, l.EndLine); err != nil {
				return err
			}
		}
	}
	return nil
}

func insertEvidence(ctx context.Context, tx *sql.Conn, table string, owner []any, refs []roleresult.EvidenceRef) error {
	// Table/columns are selected exclusively from this fixed internal list.
	statements := map[string]string{
		"role_result_evidence":        "INSERT INTO role_result_evidence VALUES (?,?,?,?,?)",
		"developer_blocker_evidence":  "INSERT INTO developer_blocker_evidence VALUES (?,?,?,?,?)",
		"qa_check_evidence":           "INSERT INTO qa_check_evidence VALUES (?,?,?,?,?,?)",
		"finding_occurrence_evidence": "INSERT INTO finding_occurrence_evidence VALUES (?,?,?,?,?)",
	}
	statement, ok := statements[table]
	if !ok {
		return fmt.Errorf("unknown evidence link table")
	}
	for _, e := range refs {
		var path, start, end any
		if e.Location != nil {
			path = e.Location.Path
			start = e.Location.StartLine
			end = e.Location.EndLine
		}
		args := append(append([]any{}, owner...), e.EvidenceID, path, start, end)
		if _, err := tx.ExecContext(ctx, statement, args...); err != nil {
			return err
		}
	}
	return nil
}
