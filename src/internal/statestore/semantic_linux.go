package statestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"marc-runtime/internal/identity"
	"marc-runtime/internal/numeric"
	"marc-runtime/schemas"
)

type producer struct {
	ExecutionID  string `json:"execution_id"`
	Attempt      int    `json:"attempt"`
	InvocationID string `json:"invocation_id"`
	Invocation   int    `json:"invocation"`
}
type resultReference struct {
	ResultID string `json:"result_id"`
}
type stateReference struct {
	LeadStateID string `json:"lead_state_id"`
}
type evidenceReference struct {
	EvidenceID string `json:"evidence_id"`
}
type roleState struct {
	Status    string           `json:"status"`
	ResultRef *resultReference `json:"result_ref"`
}
type findingReference struct {
	FindingID       string             `json:"finding_id"`
	SourceRole      string             `json:"source_role"`
	SourceResultRef resultReference    `json:"source_result_ref"`
	ContentIdentity string             `json:"content_identity"`
	Blocking        bool               `json:"blocking"`
	Summary         string             `json:"summary"`
	ResolutionRef   *evidenceReference `json:"resolution_ref"`
}
type leadState struct {
	LeadStateID        string             `json:"lead_state_id"`
	RunID              string             `json:"run_id"`
	Cycle              int                `json:"cycle"`
	Producer           producer           `json:"producer"`
	ContentIdentity    string             `json:"content_identity"`
	Goal               string             `json:"goal"`
	AcceptanceCriteria []string           `json:"acceptance_criteria"`
	OpenFindings       []findingReference `json:"open_findings"`
	ResolvedFindings   []findingReference `json:"resolved_findings"`
	QA                 roleState          `json:"qa"`
	Review             roleState          `json:"review"`
	SemanticStatus     string             `json:"semantic_status"`
	CreatedAt          string             `json:"created_at"`
}
type nextAction struct {
	ActionID         string         `json:"action_id"`
	RunID            string         `json:"run_id"`
	Cycle            int            `json:"cycle"`
	Producer         producer       `json:"producer"`
	BasedOnLeadState stateReference `json:"based_on_lead_state"`
	ContentIdentity  string         `json:"content_identity"`
	TargetRole       string         `json:"target_role"`
	Purpose          string         `json:"purpose"`
	FindingIDs       []string       `json:"finding_ids"`
	Intent           string         `json:"intent"`
	CreatedAt        string         `json:"created_at"`
}
type resourceRequest struct {
	RunID          string `json:"run_id"`
	Cycle          int    `json:"cycle"`
	RequestOrdinal int    `json:"request_ordinal"`
	RequestID      string `json:"request_id"`
	Requester      struct {
		InvocationID string `json:"invocation_id"`
	} `json:"requester"`
	Target struct {
		ExecutionID     string `json:"execution_id"`
		AttemptID       string `json:"attempt_id"`
		Role            string `json:"role"`
		Purpose         string `json:"purpose"`
		ContentIdentity string `json:"content_identity"`
	} `json:"target"`
	SourceInvocationID string `json:"source_invocation_id"`
	Requested          struct {
		WorkTokens *json.Number `json:"work_tokens"`
	} `json:"requested"`
	Justification string `json:"justification"`
	BasedOn       struct {
		LeadStateRef    stateReference  `json:"lead_state_ref"`
		SourceResultRef resultReference `json:"source_result_ref"`
	} `json:"based_on"`
	CreatedAt string `json:"created_at"`
}

func decodeSemantic(schema string, raw []byte, out any) error {
	if err := schemas.Validate(schema+".schema.json", raw); err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// validateLeadProducer uses explicit persisted ancestry and the active actor;
// producer ordinals are compared with columns, never extracted from IDs.
func validateLeadProducer(ctx context.Context, c *sql.Conn, run string, cycle int, invocation, content string, p *producer, fence ResultFence) error {
	if err := validateWorkflowSemanticLifecycle(ctx, c, run, invocation); err != nil {
		return err
	}
	var role, active string
	var epoch, revision int64
	if err := c.QueryRowContext(ctx, "SELECT active_role,active_invocation_id,ownership_epoch,revision FROM run_active_actor WHERE run_id=?", run).Scan(&role, &active, &epoch, &revision); err != nil {
		return err
	}
	if role != "LEAD" || active != invocation || epoch != fence.OwnershipEpoch || revision != fence.ActorRevision {
		return fmt.Errorf("stale or invalid Lead fence")
	}
	var storedRole, execution string
	var storedContent sql.NullString
	var storedCycle, attemptOrdinal, invocationOrdinal, currentCycle int
	if err := c.QueryRowContext(ctx, `SELECT i.role,i.cycle_no,i.execution_id,i.content_identity,t.attempt_ordinal,i.invocation_ordinal,r.current_cycle
 FROM invocations i JOIN attempts t ON t.attempt_id=i.attempt_id AND t.execution_id=i.execution_id AND t.run_id=i.run_id
 JOIN runs r ON r.run_id=i.run_id WHERE i.invocation_id=? AND i.run_id=?`, invocation, run).Scan(&storedRole, &storedCycle, &execution, &storedContent, &attemptOrdinal, &invocationOrdinal, &currentCycle); err != nil {
		return err
	}
	if storedRole != "LEAD" || storedCycle != cycle || currentCycle != cycle || !storedContent.Valid || storedContent.String != content {
		return fmt.Errorf("Lead invocation/run/cycle/content binding mismatch")
	}
	if p != nil && (p.ExecutionID != execution || p.Attempt != attemptOrdinal || p.Invocation != invocationOrdinal) {
		return fmt.Errorf("Lead producer ancestry mismatch")
	}
	return nil
}

func currentSemanticBinding(ctx context.Context, c *sql.Conn, run, id string, cycle int, content string, expected int64) error {
	var state sql.NullString
	var revision int64
	if err := c.QueryRowContext(ctx, "SELECT lead_state_id,revision FROM run_current_semantic_state WHERE run_id=?", run).Scan(&state, &revision); err != nil {
		return err
	}
	if !state.Valid || state.String != id || revision != expected {
		return fmt.Errorf("stale current semantic binding")
	}
	var storedCycle int
	var storedContent string
	if err := c.QueryRowContext(ctx, `SELECT s.cycle_no,b.content_identity FROM lead_states s JOIN lead_state_bindings b ON b.lead_state_id=s.lead_state_id AND b.run_id=s.run_id WHERE s.lead_state_id=? AND s.run_id=?`, id, run).Scan(&storedCycle, &storedContent); err != nil {
		return err
	}
	if storedCycle != cycle || storedContent != content {
		return fmt.Errorf("Lead snapshot run/cycle/content mismatch")
	}
	return nil
}

func acceptedRoleState(ctx context.Context, c *sql.Conn, run, content, role string, state roleState) (any, error) {
	if state.ResultRef == nil {
		if state.Status != "not_run" {
			return nil, fmt.Errorf("QA/review status requires an accepted result reference")
		}
		var exists bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM role_results WHERE run_id=? AND content_identity=? AND role=?)`, run, content, role).Scan(&exists); err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("not_run cannot hide an accepted QA/review result")
		}
		return nil, nil
	}
	key := ResultKey(run, state.ResultRef.ResultID)
	var gotRole, gotContent, outcome string
	var latest string
	if err := c.QueryRowContext(ctx, `SELECT result_id,role,content_identity,outcome FROM role_results WHERE run_id=? AND role=? AND content_identity=? ORDER BY cycle_no DESC,result_ordinal DESC LIMIT 1`, run, role, content).Scan(&latest, &gotRole, &gotContent, &outcome); err != nil {
		return nil, err
	}
	if latest != key || gotRole != role || gotContent != content || outcome != state.Status {
		return nil, fmt.Errorf("QA/review result reference disagrees with snapshot")
	}
	return key, nil
}

func validateFindingReference(ctx context.Context, c *sql.Conn, run string, f findingReference) error {
	var blocking bool
	var summary string
	if err := c.QueryRowContext(ctx, "SELECT blocking,summary FROM findings WHERE run_id=? AND finding_id=?", run, f.FindingID).Scan(&blocking, &summary); err != nil {
		return err
	}
	var sourceRole, sourceContent string
	if err := c.QueryRowContext(ctx, `SELECT r.role,r.content_identity FROM finding_occurrences o JOIN role_results r ON r.result_id=o.result_id AND r.run_id=o.run_id
 WHERE o.run_id=? AND o.finding_id=? AND o.result_id=?`, run, f.FindingID, ResultKey(run, f.SourceResultRef.ResultID)).Scan(&sourceRole, &sourceContent); err != nil {
		return err
	}
	if blocking != f.Blocking || summary != f.Summary || sourceRole != strings.ToUpper(f.SourceRole) || sourceContent != f.ContentIdentity {
		return fmt.Errorf("immutable finding/source reference mismatch")
	}
	if f.ResolutionRef != nil {
		// Registration alone grants no Run provenance. Accept only evidence
		// linked to an accepted result or sealed controller proof in this Run.
		var accepted bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM role_results r JOIN role_result_evidence e USING(result_id) WHERE r.run_id=? AND e.evidence_id=?
 UNION ALL SELECT 1 FROM role_results r JOIN qa_check_evidence e USING(result_id) WHERE r.run_id=? AND e.evidence_id=?
 UNION ALL SELECT 1 FROM role_results r JOIN developer_blocker_evidence e USING(result_id) WHERE r.run_id=? AND e.evidence_id=?
 UNION ALL SELECT 1 FROM finding_occurrences o JOIN finding_occurrence_evidence e USING(occurrence_id) WHERE o.run_id=? AND e.evidence_id=?)`, run, f.ResolutionRef.EvidenceID, run, f.ResolutionRef.EvidenceID, run, f.ResolutionRef.EvidenceID, run, f.ResolutionRef.EvidenceID).Scan(&accepted); err != nil {
			return err
		}
		if !accepted {
			rows, err := c.QueryContext(ctx, `SELECT DISTINCT 'proof',p.proof_id FROM authority_proofs p LEFT JOIN authority_proof_dependencies d USING(proof_id) WHERE p.run_id=?1 AND (p.evidence_id=?2 OR d.artifact_id=?2)
 UNION SELECT 'human',h.disposition_id FROM human_readiness_dispositions h LEFT JOIN human_disposition_dependencies d USING(disposition_id) WHERE h.run_id=?1 AND (h.evidence_id=?2 OR d.evidence_id=?2) ORDER BY 1,2`, run, f.ResolutionRef.EvidenceID)
			if err != nil {
				return err
			}
			var proofs []struct{ kind, id string }
			for rows.Next() {
				var id struct{ kind, id string }
				if err := rows.Scan(&id.kind, &id.id); err != nil {
					rows.Close()
					return err
				}
				proofs = append(proofs, id)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, record := range proofs {
				if record.kind == "human" {
					if _, err := loadHumanDisposition(ctx, c, record.id); err != nil {
						return err
					}
					accepted = true
					continue
				}
				if _, _, err := loadAuthorityProof(ctx, c, record.id); err != nil {
					return err
				}
				if err := verifyRetainedArtifacts(ctx, c, record.id); err != nil {
					return err
				}
				accepted = true
			}
		}
		if !accepted {
			return fmt.Errorf("resolution evidence is not accepted for this Run")
		}
	}
	return nil
}

func validateLeadStateFacts(ctx context.Context, c *sql.Conn, state leadState, options MutationOptions) (any, any, error) {
	if err := validateLeadProducer(ctx, c, state.RunID, state.Cycle, state.Producer.InvocationID, state.ContentIdentity, &state.Producer, options.Fence); err != nil {
		return nil, nil, err
	}
	var revision int64
	if err := c.QueryRowContext(ctx, "SELECT revision FROM run_current_semantic_state WHERE run_id=?", state.RunID).Scan(&revision); err != nil {
		return nil, nil, err
	}
	if revision != options.ExpectedSemanticRevision {
		return nil, nil, fmt.Errorf("stale semantic revision")
	}
	qa, err := acceptedRoleState(ctx, c, state.RunID, state.ContentIdentity, "QA", state.QA)
	if err != nil {
		return nil, nil, err
	}
	review, err := acceptedRoleState(ctx, c, state.RunID, state.ContentIdentity, "REVIEWER", state.Review)
	if err != nil {
		return nil, nil, err
	}
	seen := map[string]bool{}
	for _, f := range append(append([]findingReference{}, state.OpenFindings...), state.ResolvedFindings...) {
		if seen[f.FindingID] {
			return nil, nil, fmt.Errorf("duplicate finding disposition")
		}
		seen[f.FindingID] = true
		if err := validateFindingReference(ctx, c, state.RunID, f); err != nil {
			return nil, nil, err
		}
	}
	return qa, review, nil
}

// ValidateWorkflowLeadState checks the same facts as acceptance without selecting
// a semantic snapshot, appending events or refreshing observational output.
func (s *StateStore) ValidateWorkflowLeadState(ctx context.Context, raw []byte, options MutationOptions) error {
	var state leadState
	if err := decodeSemantic("lead-state", raw, &state); err != nil {
		return err
	}
	err := s.numericMutation(ctx, "ValidateWorkflowLeadState", state.RunID, nil, func(c *sql.Conn) error {
		if _, _, err := validateLeadStateFacts(ctx, c, state, options); err != nil {
			return err
		}
		return errAdmissionReplay
	})
	if errors.Is(err, errAdmissionReplay) {
		return nil
	}
	return err
}

// AcceptLeadState persists an immutable snapshot and advances only its explicit
// current pointer. Legal lifecycle transitions and semantic evidence sufficiency
// remain Orchestrator/Lead responsibilities outside these persistence mechanics.
func (s *StateStore) AcceptLeadState(ctx context.Context, raw []byte, options MutationOptions) error {
	var state leadState
	if err := decodeSemantic("lead-state", raw, &state); err != nil {
		return numericFailure("AcceptLeadState", state.RunID, state.Producer.InvocationID, "", err)
	}
	return s.numericMutation(ctx, "AcceptLeadState", state.RunID, options.Audit, func(c *sql.Conn) error {
		qa, review, err := validateLeadStateFacts(ctx, c, state, options)
		if err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO lead_states(lead_state_id,run_id,cycle_no,producer_invocation_id,semantic_status,qa_status,review_status,goal,revision,created_at) VALUES (?,?,?,?,?,?,?,?,0,?)`, state.LeadStateID, state.RunID, state.Cycle, state.Producer.InvocationID, state.SemanticStatus, state.QA.Status, state.Review.Status, state.Goal, state.CreatedAt); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, "INSERT INTO lead_state_bindings VALUES (?,?,?,?,?)", state.LeadStateID, state.RunID, state.ContentIdentity, qa, review); err != nil {
			return err
		}
		for i, criterion := range state.AcceptanceCriteria {
			if _, err := c.ExecContext(ctx, "INSERT INTO lead_acceptance_criteria VALUES (?,?,?)", state.LeadStateID, i+1, criterion); err != nil {
				return err
			}
		}
		for _, collection := range []struct {
			disposition string
			findings    []findingReference
		}{{"open", state.OpenFindings}, {"resolved", state.ResolvedFindings}} {
			for _, f := range collection.findings {
				var resolution any
				if f.ResolutionRef != nil {
					resolution = f.ResolutionRef.EvidenceID
				}
				if _, err := c.ExecContext(ctx, "INSERT INTO lead_finding_dispositions VALUES (?,?,?,?,?)", state.LeadStateID, state.RunID, f.FindingID, collection.disposition, resolution); err != nil {
					return err
				}
				if _, err := c.ExecContext(ctx, "INSERT INTO lead_finding_sources VALUES (?,?,?,?)", state.LeadStateID, f.FindingID, state.RunID, ResultKey(state.RunID, f.SourceResultRef.ResultID)); err != nil {
					return err
				}
			}
		}
		nextRevision, err := checkedNext(options.ExpectedSemanticRevision, "semantic_revision")
		if err != nil {
			return err
		}
		changed, err := c.ExecContext(ctx, "UPDATE run_current_semantic_state SET lead_state_id=?,revision=?,updated_at=? WHERE run_id=? AND revision=?", state.LeadStateID, nextRevision, state.CreatedAt, state.RunID, options.ExpectedSemanticRevision)
		if err != nil {
			return err
		}
		count, err := changed.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("stale semantic pointer update")
		}
		return nil
	})
}

// AcceptNextAction accepts intent only; it does not create work or dispatch.
func (s *StateStore) AcceptNextAction(ctx context.Context, raw []byte, options MutationOptions) error {
	var action nextAction
	if err := decodeSemantic("next-action", raw, &action); err != nil {
		return numericFailure("AcceptNextAction", action.RunID, action.Producer.InvocationID, "", err)
	}
	return s.numericMutation(ctx, "AcceptNextAction", action.RunID, options.Audit, func(c *sql.Conn) error {
		if err := validateLeadProducer(ctx, c, action.RunID, action.Cycle, action.Producer.InvocationID, action.ContentIdentity, &action.Producer, options.Fence); err != nil {
			return err
		}
		if err := currentSemanticBinding(ctx, c, action.RunID, action.BasedOnLeadState.LeadStateID, action.Cycle, action.ContentIdentity, options.ExpectedSemanticRevision); err != nil {
			return err
		}
		for _, id := range action.FindingIDs {
			var found string
			if err := c.QueryRowContext(ctx, "SELECT finding_id FROM findings WHERE run_id=? AND finding_id=?", action.RunID, id).Scan(&found); err != nil {
				return err
			}
		}
		if _, err := c.ExecContext(ctx, "INSERT INTO next_actions VALUES (?,?,?,?,?,?,?,?)", action.ActionID, action.RunID, action.Cycle, action.BasedOnLeadState.LeadStateID, action.TargetRole, action.Purpose, action.Intent, action.CreatedAt); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, "INSERT INTO next_action_producers VALUES (?,?,?)", action.ActionID, action.RunID, action.Producer.InvocationID); err != nil {
			return err
		}
		for _, id := range action.FindingIDs {
			if _, err := c.ExecContext(ctx, "INSERT INTO next_action_findings VALUES (?,?,?)", action.ActionID, action.RunID, id); err != nil {
				return err
			}
		}
		return nil
	})
}

// AcceptResourceRequest stores requested amounts and provenance only. It never
// allocates, reserves, creates an attempt/invocation or selects a future ID.
func requestStorageKey(runID, requestID string) string { return runID + "/" + requestID }

func (s *StateStore) AcceptResourceRequest(ctx context.Context, raw []byte, options MutationOptions) error {
	if err := identity.CheckJSONEnvelope(raw); err != nil {
		return err
	}
	var request resourceRequest
	// Parse exact resource units before the schema validator can expand exponent
	// syntax into rational numbers. Schema validation still checks duplicate keys
	// and the entire contract before accepting any authoritative state.
	if err := json.Unmarshal(raw, &request); err != nil {
		return err
	}
	amounts, err := requestedAmounts(request)
	if err != nil {
		return numericFailure("AcceptResourceRequest", request.RunID, request.Requester.InvocationID, "", err)
	}
	if err := decodeSemantic("resource-request", raw, &request); err != nil {
		return numericFailure("AcceptResourceRequest", request.RunID, request.Requester.InvocationID, "", err)
	}
	if request.RequestID != fmt.Sprintf("rr-c%03d-r%03d", request.Cycle, request.RequestOrdinal) {
		return fmt.Errorf("request_id disagrees with structured fields")
	}
	return s.numericMutation(ctx, "AcceptResourceRequest", request.RunID, options.Audit, func(c *sql.Conn) error {
		if err := validateLeadProducer(ctx, c, request.RunID, request.Cycle, request.Requester.InvocationID, request.Target.ContentIdentity, nil, options.Fence); err != nil {
			return err
		}
		if err := currentSemanticBinding(ctx, c, request.RunID, request.BasedOn.LeadStateRef.LeadStateID, request.Cycle, request.Target.ContentIdentity, options.ExpectedSemanticRevision); err != nil {
			return err
		}
		var execution, attempt, role, purpose, content, executionRole string
		var cycle int
		if err := c.QueryRowContext(ctx, `SELECT i.execution_id,i.attempt_id,i.role,i.purpose,i.content_identity,i.cycle_no,e.role
 FROM invocations i JOIN executions e ON e.execution_id=i.execution_id AND e.run_id=i.run_id WHERE i.invocation_id=? AND i.run_id=?`, request.SourceInvocationID, request.RunID).Scan(&execution, &attempt, &role, &purpose, &content, &cycle, &executionRole); err != nil {
			return err
		}
		// Work/finalization invocations can share an execution and attempt.
		if execution != request.Target.ExecutionID || attempt != request.Target.AttemptID || role != request.Target.Role || executionRole != role || purpose != request.Target.Purpose || content != request.Target.ContentIdentity || cycle != request.Cycle {
			return fmt.Errorf("resource target/source binding mismatch")
		}
		sourceKey := ResultKey(request.RunID, request.BasedOn.SourceResultRef.ResultID)
		var invocation string
		if err := c.QueryRowContext(ctx, "SELECT invocation_id FROM role_results WHERE result_id=? AND run_id=? AND content_identity=?", sourceKey, request.RunID, request.Target.ContentIdentity).Scan(&invocation); err != nil {
			return err
		}
		if invocation != request.SourceInvocationID {
			return fmt.Errorf("request source result belongs to another invocation")
		}
		key := requestStorageKey(request.RunID, request.RequestID)
		if _, err := c.ExecContext(ctx, `INSERT INTO resource_requests(request_id,run_id,cycle_no,request_ordinal,requester_invocation_id,source_invocation_id,target_execution_id,target_attempt_id,target_role,target_purpose,semantic_justification,status,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,'PENDING',?)`, key, request.RunID, request.Cycle, request.RequestOrdinal, request.Requester.InvocationID, request.SourceInvocationID, request.Target.ExecutionID, request.Target.AttemptID, request.Target.Role, request.Target.Purpose, request.Justification, request.CreatedAt); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, "INSERT INTO resource_request_sources VALUES (?,?,?,?)", key, request.RunID, request.BasedOn.LeadStateRef.LeadStateID, sourceKey); err != nil {
			return err
		}
		for dimension, amount := range amounts {
			if _, err := c.ExecContext(ctx, "INSERT INTO resource_request_amounts VALUES (?,?,?)", key, dimension, amount); err != nil {
				return err
			}
		}
		return nil
	})
}

func requestedAmounts(request resourceRequest) (map[string]int64, error) {
	amounts := map[string]int64{}
	for dimension, value := range map[string]*json.Number{"WORK": request.Requested.WorkTokens} {
		if value != nil {
			n, err := scaledDecimal(value.String(), 0)
			if err != nil || n <= 0 {
				if err != nil {
					return nil, numeric.WithContext(err, numeric.Context{Domain: dimension})
				}
				return nil, fmt.Errorf("requested tokens must be positive exact int64")
			}
			amounts[dimension] = n
		}
	}

	return amounts, nil
}
