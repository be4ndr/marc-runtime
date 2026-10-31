package statestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"marc-runtime/internal/identity"
	"marc-runtime/src/internal/roleresult"
)

// WorkflowSpec is immutable input for the deterministic fake or external MCP
// workflow. Goal allows 1024 Unicode code points; structural limits are unchanged.
// Managed live providers cannot enter this runtime.
type WorkflowSpec struct {
	RunID, Goal, ContentIdentity, StartedAt, HardDeadline string
	Mode, WorktreePath                                    string
	NormalTokens                                          int64
	Confirmation                                          HumanBudgetApproval
	MCPClientName                                         string `json:",omitempty"`
}
type WorkflowStep struct {
	CurrentInvocation
	StepNo                                   int
	Status, StartedAt, CompletedAt, ActionID string
	Intent                                   string
	FindingIDs                               []string
	Output                                   []byte
	ResultID                                 string
	Admission                                *InvocationAdmission
	ClaimedAt, AuthorizedAt                  string
	DurationNS                               *int64
	ReceiptState                             string
	Closed                                   bool
}
type WorkflowSnapshot struct {
	ContentIdentity      string
	Spec                 WorkflowSpec
	Status               string
	Revision             int64
	Owner                SemanticFence
	SemanticRevision     int64
	LeadStateID          *string
	Current              *WorkflowStep
	Steps                []WorkflowStep
	Terminal             ProofReference
	AccountingIncomplete bool
	AccountingStatus     string
}

func workflowID(run, kind string, ordinal int) string {
	return fmt.Sprintf("wf-%x-%s-%d", sha256.Sum256([]byte(run)), kind, ordinal)
}

func (s *StateStore) CreateWorkflow(ctx context.Context, spec WorkflowSpec) error {
	if err := identity.CheckID("run_id", spec.RunID); err != nil {
		return err
	}
	if err := identity.CheckID("content_identity", spec.ContentIdentity); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Goal) == "" || !identity.ValidDescription(spec.Goal) {
		return fmt.Errorf("invalid workflow goal")
	}
	if err := validateBudgetApproval(spec.Confirmation, true); err != nil {
		return err
	}
	at, err := canonicalTime(spec.StartedAt)
	if err != nil {
		return err
	}
	deadline, err := canonicalTime(spec.HardDeadline)
	if err != nil || !beforeDeadline(at, deadline) {
		return fmt.Errorf("workflow needs future immutable deadline")
	}
	if spec.Mode != "" && spec.Mode != "EXTERNAL" {
		return fmt.Errorf("unsupported workflow mode")
	}
	if spec.MCPClientName != "" {
		if spec.Mode != "EXTERNAL" {
			return fmt.Errorf("mcp_client_name requires an external workflow")
		}
		if err := identity.CheckText("mcp_client_name", spec.MCPClientName); err != nil {
			return err
		}
		for _, c := range spec.MCPClientName {
			if unicode.IsSpace(c) || unicode.IsControl(c) {
				return fmt.Errorf("mcp_client_name must contain no whitespace or control characters")
			}
		}
	}
	if spec.Mode == "EXTERNAL" && (spec.NormalTokens != 0 || spec.WorktreePath == "") {
		return fmt.Errorf("external work requires a worktree and no managed token budget")
	}
	if spec.Mode != "EXTERNAL" && spec.NormalTokens <= 0 {
		return fmt.Errorf("positive normal budget required")
	}
	if _, err := checkedAdd(spec.NormalTokens, spec.NormalTokens/10); err != nil {
		return err
	}
	spec.StartedAt, spec.HardDeadline = at, deadline
	raw, _ := json.Marshal(spec)
	err = s.numericMutation(ctx, "CreateWorkflow", spec.RunID, nil, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT spec_json FROM workflow_runs WHERE run_id=?`, spec.RunID).Scan(&old)
		if err == nil {
			if old != string(raw) {
				return fmt.Errorf("conflicting workflow creation replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES(?,'RUNNING',1,?,?)`, spec.RunID, at, at); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES(?,1,'RUNNING',?)`, spec.RunID, at); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO run_active_actor VALUES(?,'LEAD',NULL,1,?,0)`, spec.RunID, at); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO workflow_runs(run_id,spec_json) VALUES(?,?)`, spec.RunID, string(raw)); err != nil {
			return err
		}
		return appendAudit(ctx, c, spec.RunID, []AuditRow{{EventType: "workflow_created", ActorRole: "ORCHESTRATOR", OccurredAt: at}})
	})
	if errors.Is(err, errAdmissionReplay) {
		return nil
	}
	return err
}

// BeginWorkflowStep creates ancestry, consumes an action once, and binds ownership
// in one transaction. Worker -> Worker transitions have no runtime entry point.
func (s *StateStore) BeginWorkflowStep(ctx context.Context, run, action string, expected SemanticFence, at string) (WorkflowStep, error) {
	var out WorkflowStep
	at, err := canonicalTime(at)
	if err != nil {
		return out, err
	}
	err = s.numericMutation(ctx, "BeginWorkflowStep", run, nil, func(c *sql.Conn) error {
		owner, err := validateOwner(ctx, c, run, expected)
		if err != nil {
			return err
		}
		var status, content, deadline string
		var current *string
		var n int64
		var currentCycle int
		if err = c.QueryRowContext(ctx, `SELECT r.status,w.current_invocation_id,w.next_step,COALESCE(i.content_identity,json_extract(w.spec_json,'$.ContentIdentity')),CASE WHEN json_extract(w.spec_json,'$.Mode')='EXTERNAL' THEN json_extract(w.spec_json,'$.HardDeadline') ELSE p.hard_deadline END,r.current_cycle FROM workflow_runs w JOIN runs r USING(run_id) LEFT JOIN run_admission_policy p USING(run_id) LEFT JOIN invocations i ON i.invocation_id=w.current_invocation_id WHERE w.run_id=?`, run).Scan(&status, &current, &n, &content, &deadline, &currentCycle); err != nil {
			return err
		}
		if status != "RUNNING" {
			return fmt.Errorf("workflow step requires a running workflow")
		}
		if owner.Role != "LEAD" {
			return fmt.Errorf("workflow step requires Lead ownership")
		}
		if !beforeDeadline(at, deadline) {
			return fmt.Errorf("workflow original deadline reached: at %s, deadline %s; no new role may be issued", at, deadline)
		}
		if current != nil {
			var finished bool
			if err = c.QueryRowContext(ctx, `SELECT completed_at IS NOT NULL FROM invocations WHERE invocation_id=?`, *current).Scan(&finished); err != nil {
				return err
			}
			if !finished {
				return fmt.Errorf("previous workflow step unfinished")
			}
		}
		role, purpose := "LEAD", "LEAD"
		epoch := owner.OwnershipEpoch
		if action != "" {
			if err := identity.CheckID("action_id", action); err != nil {
				return err
			}
			var lead, producer, actionContent string
			var cycle int
			if err = c.QueryRowContext(ctx, `SELECT a.target_role,a.purpose,a.lead_state_id,p.producer_invocation_id,b.content_identity,a.cycle_no FROM next_actions a JOIN next_action_producers p USING(action_id,run_id) JOIN lead_state_bindings b USING(lead_state_id,run_id) WHERE a.run_id=? AND a.action_id=?`, run, action).Scan(&role, &purpose, &lead, &producer, &actionContent, &cycle); err != nil {
				return err
			}
			var selected *string
			if err = c.QueryRowContext(ctx, `SELECT lead_state_id FROM run_current_semantic_state WHERE run_id=?`, run).Scan(&selected); err != nil {
				return err
			}
			if cycle != currentCycle || selected == nil || *selected != lead || owner.InvocationID == nil || *owner.InvocationID != producer || actionContent != content {
				return fmt.Errorf("stale action/producer/content")
			}
			var stale bool
			if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM next_action_findings f WHERE f.action_id=? AND NOT EXISTS(SELECT 1 FROM lead_finding_dispositions d WHERE d.lead_state_id=? AND d.finding_id=f.finding_id AND d.disposition='open'))`, action, lead).Scan(&stale); err != nil {
				return err
			}
			if stale {
				return fmt.Errorf("action targets findings outside current open set")
			}
			var external bool
			if err = c.QueryRowContext(ctx, "SELECT json_extract(spec_json,'$.Mode')='EXTERNAL' FROM workflow_runs WHERE run_id=?", run).Scan(&external); err != nil {
				return err
			}
			if external && purpose == "REMEDIATE" {
				if currentCycle >= 3 {
					return fmt.Errorf("two remediation cycles exhausted; Lead must stop")
				}
				if _, err = c.ExecContext(ctx, "UPDATE cycles SET status='FINISHED',completed_at=?,revision=revision+1 WHERE run_id=? AND cycle_no=?", at, run, currentCycle); err != nil {
					return err
				}
				currentCycle++
				content = workflowID(run, "content", currentCycle)
				if _, err = c.ExecContext(ctx, "INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES(?,?,'RUNNING',?)", run, currentCycle, at); err != nil {
					return err
				}
				if _, err = c.ExecContext(ctx, "UPDATE runs SET current_cycle=?,revision=revision+1,updated_at=? WHERE run_id=?", currentCycle, at, run); err != nil {
					return err
				}
				if err = appendAudit(ctx, c, run, []AuditRow{{EventType: "workflow_remediation_cycle_started", ActorRole: "ORCHESTRATOR", OccurredAt: at}}); err != nil {
					return err
				}
			}
			epoch, err = checkedNext(epoch, "ownership_epoch")
			if err != nil {
				return err
			}
		} else if owner.InvocationID != nil {
			return fmt.Errorf("Lead step requires unbound Lead")
		}
		next, err := checkedNext(n, "workflow_step")
		if err != nil {
			return err
		}
		id := workflowID(run, "inv", int(n))
		ex := workflowID(run, "ex", int(n))
		attempt := workflowID(run, "at", int(n))
		if _, err = c.ExecContext(ctx, `INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES(?,?,?,?,?,'RUNNING',?)`, ex, run, currentCycle, role, purpose, at); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES(?,?,?,?,1,'RUNNING',?)`, attempt, ex, run, currentCycle, at); err != nil {
			return err
		}
		// Role invocation ordinals are cycle-global even though each step has its own attempt.
		var ordinal int
		if err = c.QueryRowContext(ctx, `SELECT count(*)+1 FROM invocations WHERE run_id=? AND cycle_no=? AND role=?`, run, currentCycle, role).Scan(&ordinal); err != nil {
			return err
		}
		provider, model, effort := "fake", "deterministic", "none"
		var mode string
		if err = c.QueryRowContext(ctx, `SELECT COALESCE(json_extract(spec_json,'$.Mode'),'') FROM workflow_runs WHERE run_id=?`, run).Scan(&mode); err != nil {
			return err
		}
		if mode == "EXTERNAL" {
			provider = "external"
			level := currentCycle - 1
			if role != "LEAD" {
				if err = c.QueryRowContext(ctx, "SELECT count(*) FROM workflow_steps s JOIN invocations i USING(invocation_id) WHERE s.run_id=? AND i.role=?", run, role).Scan(&level); err != nil {
					return err
				}
			}
			if err = c.QueryRowContext(ctx, "SELECT model,effort FROM v_workflow_role_profiles WHERE role=? AND level=?", role, level).Scan(&model, &effort); err != nil {
				return fmt.Errorf("role escalation exhausted: %w", err)
			}
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch) VALUES(?,?,?,?,?,?,?,?,?,?,?,'RUNNING',?,?,?)`, id, attempt, ex, run, currentCycle, ordinal, role, purpose, provider, model, effort, at, content, epoch); err != nil {
			return err
		}
		var aid any
		if action != "" {
			aid = action
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO workflow_steps(invocation_id,run_id,step_no,action_id) VALUES(?,?,?,?)`, id, run, n, aid); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `UPDATE workflow_runs SET current_invocation_id=?,next_step=? WHERE run_id=?`, id, next, run); err != nil {
			return err
		}
		if err = updateOwner(ctx, c, owner, role, &id, at); err != nil {
			return err
		}
		out = WorkflowStep{CurrentInvocation: CurrentInvocation{run, currentCycle, ex, attempt, id, ordinal, role, purpose, &content, provider, model, effort}, StepNo: int(n), Status: "RUNNING", StartedAt: at, ActionID: action}
		return appendAudit(ctx, c, run, []AuditRow{{EventType: "workflow_invocation_started", ActorRole: "ORCHESTRATOR", OccurredAt: at, Cycle: &currentCycle, ExecutionID: &ex, AttemptID: &attempt, InvocationID: &id}})
	})
	return out, err
}

func (s *StateStore) RegisterWorkflowArtifact(ctx context.Context, run, id, kind string, raw []byte, at string) (ProofArtifact, error) {
	out := ProofArtifact{id, kind, fmt.Sprintf("%x", sha256.Sum256(raw))}
	if err := identity.CheckID("evidence_id", id); err != nil {
		return out, err
	}
	if len(raw) > identity.MaxJSONBytes || len(raw) == 0 || kind == "authority_proof" || kind == "human_readiness_disposition" {
		return out, fmt.Errorf("invalid workflow artifact")
	}
	at, err := canonicalTime(at)
	if err != nil {
		return out, err
	}
	err = s.numericMutation(ctx, "RegisterWorkflowArtifact", run, nil, func(c *sql.Conn) error {
		return registerWorkflowArtifact(ctx, c, id, kind, raw, at)
	})
	if errors.Is(err, errAdmissionReplay) {
		err = nil
	}
	return out, err
}

func registerWorkflowArtifact(ctx context.Context, c *sql.Conn, id, kind string, raw []byte, at string) error {
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	var old, oldKind string
	var blob []byte
	err := c.QueryRowContext(ctx, `SELECT sha256,kind,inline_blob FROM evidence_artifacts WHERE evidence_id=?`, id).Scan(&old, &oldKind, &blob)
	if err == nil {
		if old != digest || oldKind != kind || string(blob) != string(raw) {
			return fmt.Errorf("conflicting artifact replay")
		}
		return errAdmissionReplay
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = c.ExecContext(ctx, `INSERT INTO evidence_artifacts VALUES(?,?,'sqlite',?,?,?,NULL,?)`, id, kind, digest, len(raw), raw, at)
	return err
}

func (s *StateStore) CaptureWorkflowOutput(ctx context.Context, run, id string, raw []byte, at string) error {
	return s.captureWorkflowOutput(ctx, run, id, raw, at, nil, 0, nil)
}

// External output is a durable client submission, never a provider observation.
func (s *StateStore) CaptureExternalWorkflowOutput(ctx context.Context, run, id string, raw []byte, at string, owner SemanticFence, semanticRevision int64) error {
	return s.captureWorkflowOutput(ctx, run, id, raw, at, &owner, semanticRevision, nil)
}

type externalWorkflowResult struct {
	result      roleresult.Result
	definitions []roleresult.Finding
	evidence    []byte
}

// Validate the Worker result and stage its evidence in the same transaction as
// capture. Invalid references, findings or verdicts leave no captured intent.
func (s *StateStore) CaptureExternalWorkflowResult(ctx context.Context, run, id string, raw, resultJSON []byte, definitions []roleresult.Finding, evidence []byte, at string, owner SemanticFence, semanticRevision int64) error {
	r, err := roleresult.Decode(resultJSON)
	if err != nil {
		return err
	}
	if r.RunID != run || r.InvocationID != id || len(evidence) == 0 || len(evidence) > identity.MaxJSONBytes {
		return fmt.Errorf("invalid external result binding/evidence")
	}
	for _, f := range definitions {
		if err := roleresult.ValidateFinding(f); err != nil {
			return err
		}
	}
	return s.captureWorkflowOutput(ctx, run, id, raw, at, &owner, semanticRevision, &externalWorkflowResult{r, definitions, evidence})
}

func (s *StateStore) captureWorkflowOutput(ctx context.Context, run, id string, raw []byte, at string, owner *SemanticFence, semanticRevision int64, result *externalWorkflowResult) error {
	if err := identity.CheckJSONEnvelope(raw); err != nil {
		return err
	}
	var err error
	at, err = canonicalTime(at)
	if err != nil {
		return err
	}
	artifact := workflowID(run, "output", 0) + "-" + fmt.Sprintf("%x", sha256.Sum256([]byte(id)))
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	err = s.numericMutation(ctx, "CaptureWorkflowOutput", run, nil, func(c *sql.Conn) error {
		external, err := externalWorkflowStep(ctx, c, run, id)
		if err != nil {
			return err
		}
		if external != (owner != nil) {
			return fmt.Errorf("workflow capture mode mismatch")
		}
		var old *string
		var current string
		if err := c.QueryRowContext(ctx, `SELECT s.output_id,w.current_invocation_id FROM workflow_steps s JOIN workflow_runs w USING(run_id) WHERE s.run_id=? AND s.invocation_id=?`, run, id).Scan(&old, &current); err != nil {
			return err
		}
		if old != nil {
			var stored []byte
			if err := c.QueryRowContext(ctx, `SELECT inline_blob FROM evidence_artifacts WHERE evidence_id=?`, *old).Scan(&stored); err != nil {
				return err
			}
			if *old != artifact || string(stored) != string(raw) {
				return fmt.Errorf("conflicting captured output replay")
			}
			return errAdmissionReplay
		}
		if current != id {
			return fmt.Errorf("stale workflow output")
		}
		var authorized bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admission_dispatch_claims d JOIN admission_release_authorizations z USING(claim_id) JOIN provider_calls p USING(provider_call_id) WHERE p.invocation_id=?)`, id).Scan(&authorized); err != nil {
			return err
		}
		if external {
			if owner.Role != "LEAD" && result == nil {
				return fmt.Errorf("external Worker capture requires validated role result")
			}
			if _, err := validateOwner(ctx, c, run, *owner); err != nil {
				return err
			}
			var valid bool
			if err := c.QueryRowContext(ctx, `SELECT r.status='RUNNING' AND s.revision=? AND i.status='RUNNING' AND i.completed_at IS NULL FROM runs r JOIN run_current_semantic_state s USING(run_id) JOIN invocations i USING(run_id) WHERE r.run_id=? AND i.invocation_id=?`, semanticRevision, run, id).Scan(&valid); err != nil {
				return err
			}
			if !valid || owner.InvocationID == nil || *owner.InvocationID != id {
				return fmt.Errorf("stale external submission")
			}
		} else if !authorized {
			return fmt.Errorf("captured workflow output requires prior release authorization")
		}
		if result != nil {
			if err := registerWorkflowArtifact(ctx, c, id+"-evidence", "test_result", result.evidence, at); err != nil && !errors.Is(err, errAdmissionReplay) {
				return err
			}
			if _, err := validateRoleResultFacts(ctx, c, result.result, result.definitions, ResultFence{owner.OwnershipEpoch, owner.ActorRevision}); err != nil {
				return err
			}
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO evidence_artifacts VALUES(?,'workflow_output','sqlite',?,?,?,NULL,?)`, artifact, digest, len(raw), raw, at); err != nil {
			return err
		}
		_, err = c.ExecContext(ctx, `UPDATE workflow_steps SET output_id=? WHERE invocation_id=?`, artifact, id)
		return err
	})
	if errors.Is(err, errAdmissionReplay) {
		return nil
	}
	return err
}

// External role admission is the immutable workflow-step binding. It never
// admits a model request or substitutes for normal provider admission.
func externalWorkflowStep(ctx context.Context, q querier, run, invocation string) (bool, error) {
	var external bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_runs w JOIN workflow_steps s USING(run_id) WHERE w.run_id=? AND s.invocation_id=? AND json_extract(w.spec_json,'$.Mode')='EXTERNAL')`, run, invocation).Scan(&external)
	return external, err
}

func (s *StateStore) FinishWorkflowStep(ctx context.Context, run, id string, at string) error {
	var canonicalErr error
	at, canonicalErr = canonicalTime(at)
	if canonicalErr != nil {
		return canonicalErr
	}
	err := s.numericMutation(ctx, "FinishWorkflowStep", run, nil, func(c *sql.Conn) error {
		var status, role, ex, attempt string
		var cycle int
		var completed *string
		if err := c.QueryRowContext(ctx, `SELECT status,role,cycle_no,execution_id,attempt_id,completed_at FROM invocations WHERE run_id=? AND invocation_id=?`, run, id).Scan(&status, &role, &cycle, &ex, &attempt, &completed); err != nil {
			return err
		}
		if completed != nil {
			return errAdmissionReplay
		}
		var okay bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_steps s JOIN workflow_runs w USING(run_id) WHERE s.invocation_id=? AND w.current_invocation_id=s.invocation_id AND s.output_id IS NOT NULL)
 AND (EXISTS(SELECT 1 FROM role_results WHERE invocation_id=?1) OR EXISTS(SELECT 1 FROM lead_states WHERE producer_invocation_id=?1))
 AND NOT EXISTS(SELECT 1 FROM invocation_admissions a WHERE a.invocation_id=?1 AND NOT EXISTS(SELECT 1 FROM admission_closures WHERE admission_id=a.admission_id))
 AND NOT EXISTS(SELECT 1 FROM provider_calls p JOIN provider_call_usage u USING(provider_call_id) WHERE p.invocation_id=?1 AND (p.duration_ns IS NULL OR u.receipt_state IS NOT 'final'))`, id).Scan(&okay); err != nil {
			return err
		}
		if !okay {
			return fmt.Errorf("workflow semantic/closure/FINAL obligations unresolved")
		}
		for _, x := range []struct{ table, key, value string }{{"invocations", "invocation_id", id}, {"attempts", "attempt_id", attempt}, {"executions", "execution_id", ex}} {
			if _, err := c.ExecContext(ctx, `UPDATE `+x.table+` SET status='FINISHED',completed_at=?,revision=revision+1 WHERE `+x.key+`=? AND completed_at IS NULL`, at, x.value); err != nil {
				return err
			}
		}
		return appendAudit(ctx, c, run, []AuditRow{{EventType: "workflow_invocation_finished", ActorRole: "ORCHESTRATOR", OccurredAt: at, Cycle: &cycle, ExecutionID: &ex, AttemptID: &attempt, InvocationID: &id}})
	})
	if errors.Is(err, errAdmissionReplay) {
		return nil
	}
	return err
}

func (s *StateStore) BlockWorkflow(ctx context.Context, run, reason, at string) error {
	var canonicalErr error
	at, canonicalErr = canonicalTime(at)
	if canonicalErr != nil {
		return canonicalErr
	}
	return s.numericMutation(ctx, "BlockWorkflow", run, nil, func(c *sql.Conn) error {
		var status string
		if err := c.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=?`, run).Scan(&status); err != nil {
			return err
		}
		if status == "BLOCKED" {
			return nil
		}
		if status != "RUNNING" {
			return fmt.Errorf("cannot block terminal workflow")
		}
		var role string
		var id *string
		var epoch, rev int64
		if err := c.QueryRowContext(ctx, `SELECT active_role,active_invocation_id,ownership_epoch,revision FROM run_active_actor WHERE run_id=?`, run).Scan(&role, &id, &epoch, &rev); err != nil {
			return err
		}
		if workerRole(role) || id != nil {
			if err := updateOwner(ctx, c, ActiveActor{RunID: run, Role: role, InvocationID: id, OwnershipEpoch: epoch, Revision: rev}, "LEAD", nil, at); err != nil {
				return err
			}
		}
		invocationStatus := "FAILED"
		if strings.HasPrefix(reason, "UNKNOWN_OUTCOME:") {
			invocationStatus = "UNKNOWN_OUTCOME"
		}
		if _, err := c.ExecContext(ctx, `UPDATE invocations SET status=?,revision=revision+1 WHERE invocation_id=(SELECT current_invocation_id FROM workflow_runs WHERE run_id=?) AND completed_at IS NULL`, invocationStatus, run); err != nil {
			return err
		}
		_, err := c.ExecContext(ctx, `UPDATE runs SET status='BLOCKED',revision=revision+1,updated_at=? WHERE run_id=?`, at, run)
		if err != nil {
			return err
		}
		details, _ := json.Marshal(map[string]string{"reason": reason})
		return appendAudit(ctx, c, run, []AuditRow{{EventType: "workflow_blocked", ActorRole: "ORCHESTRATOR", OccurredAt: at, Details: details}})
	})
}

func (s *StateStore) EnterWorkflowReadiness(ctx context.Context, run string, ref ProofReference, at string) error {
	var canonicalErr error
	at, canonicalErr = canonicalTime(at)
	if canonicalErr != nil {
		return canonicalErr
	}
	return s.numericMutation(ctx, "EnterWorkflowReadiness", run, nil, func(c *sql.Conn) error {
		var status string
		var old, sha *string
		if err := c.QueryRowContext(ctx, `SELECT r.status,w.terminal_proof_id,w.terminal_sha256 FROM runs r JOIN workflow_runs w USING(run_id) WHERE run_id=?`, run).Scan(&status, &old, &sha); err != nil {
			return err
		}
		if status == "READY_FOR_HUMAN_MERGE" {
			if old != nil && sha != nil && *old == ref.ProofID && *sha == ref.SHA256 {
				return nil
			}
			return fmt.Errorf("conflicting terminal replay")
		}
		if status != "RUNNING" {
			return fmt.Errorf("workflow is not running")
		}
		p, body, err := loadAuthorityProof(ctx, c, ref.ProofID)
		if err != nil {
			return err
		}
		if p.RunID != run || p.Kind != "TERMINAL" || p.Verdict != "SUPPORTS_READINESS" || p.SHA256 != ref.SHA256 {
			return fmt.Errorf("positive exact terminal proof required")
		}
		if err = verifyRetainedArtifacts(ctx, c, p.ProofID); err != nil {
			return err
		}
		snap, _, err := captureAuthority(ctx, c, run)
		if err != nil {
			return err
		}
		digest, err := authorityDigest(snap)
		if err != nil {
			return err
		}
		if digest != p.SourceDigest {
			return fmt.Errorf("stale terminal proof")
		}
		if _, err = validateProofBody(ctx, c, body.Request); err != nil {
			return err
		}
		var active bool
		if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM invocations WHERE run_id=?1 AND completed_at IS NULL) OR EXISTS(SELECT 1 FROM executions WHERE run_id=?1 AND completed_at IS NULL)`, run).Scan(&active); err != nil {
			return err
		}
		if active {
			return fmt.Errorf("active workflow execution")
		}
		if _, err = c.ExecContext(ctx, `UPDATE workflow_runs SET terminal_proof_id=?,terminal_sha256=? WHERE run_id=?`, ref.ProofID, ref.SHA256, run); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `DELETE FROM run_active_actor WHERE run_id=?`, run); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `UPDATE cycles SET status='FINISHED',completed_at=?,revision=revision+1 WHERE run_id=? AND cycle_no=(SELECT current_cycle FROM runs WHERE run_id=?)`, at, run, run); err != nil {
			return err
		}
		if _, err = c.ExecContext(ctx, `UPDATE runs SET status='READY_FOR_HUMAN_MERGE',terminal_at=?,updated_at=?,revision=revision+1 WHERE run_id=?`, at, at, run); err != nil {
			return err
		}
		return appendAudit(ctx, c, run, []AuditRow{{EventType: "workflow_ready_for_human_merge", ActorRole: "ORCHESTRATOR", OccurredAt: at}})
	})
}

func (s *StateStore) GetWorkflow(ctx context.Context, run string) (WorkflowSnapshot, error) {
	var out WorkflowSnapshot
	if err := identity.CheckID("run_id", run); err != nil {
		return out, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	var raw string
	var current *string
	if err = tx.QueryRowContext(ctx, `SELECT w.spec_json,r.status,r.revision,w.current_invocation_id,COALESCE(w.terminal_proof_id,''),COALESCE(w.terminal_sha256,''),COALESCE(i.content_identity,json_extract(w.spec_json,'$.ContentIdentity')) FROM workflow_runs w JOIN runs r USING(run_id) LEFT JOIN invocations i ON i.invocation_id=w.current_invocation_id WHERE w.run_id=?`, run).Scan(&raw, &out.Status, &out.Revision, &current, &out.Terminal.ProofID, &out.Terminal.SHA256, &out.ContentIdentity); err != nil {
		return out, err
	}
	if err = json.Unmarshal([]byte(raw), &out.Spec); err != nil {
		return out, err
	}
	err = tx.QueryRowContext(ctx, `SELECT active_role,active_invocation_id,ownership_epoch,revision FROM run_active_actor WHERE run_id=?`, run).Scan(&out.Owner.Role, &out.Owner.InvocationID, &out.Owner.OwnershipEpoch, &out.Owner.ActorRevision)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	if out.Owner.InvocationID != nil {
		if err = tx.QueryRowContext(ctx, `SELECT content_identity FROM invocations WHERE invocation_id=?`, *out.Owner.InvocationID).Scan(&out.Owner.ContentIdentity); err != nil {
			return out, err
		}
	}
	if err = tx.QueryRowContext(ctx, `SELECT lead_state_id,revision FROM run_current_semantic_state WHERE run_id=?`, run).Scan(&out.LeadStateID, &out.SemanticRevision); err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT i.run_id,i.cycle_no,i.execution_id,i.attempt_id,i.invocation_id,i.invocation_ordinal,i.role,i.purpose,i.content_identity,i.provider,i.model,i.reasoning_effort,s.step_no,i.status,i.started_at,COALESCE(i.completed_at,''),COALESCE(s.action_id,''),e.inline_blob,COALESCE(r.result_id,'') FROM workflow_steps s JOIN invocations i USING(invocation_id) LEFT JOIN evidence_artifacts e ON e.evidence_id=s.output_id LEFT JOIN role_results r USING(invocation_id) WHERE s.run_id=? ORDER BY s.step_no`, run)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var x WorkflowStep
		if err = rows.Scan(&x.RunID, &x.Cycle, &x.ExecutionID, &x.AttemptID, &x.InvocationID, &x.Ordinal, &x.Role, &x.Purpose, &x.ContentIdentity, &x.Provider, &x.Model, &x.ReasoningEffort, &x.StepNo, &x.Status, &x.StartedAt, &x.CompletedAt, &x.ActionID, &x.Output, &x.ResultID); err != nil {
			rows.Close()
			return out, err
		}
		out.Steps = append(out.Steps, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	for i := range out.Steps {
		x := &out.Steps[i]
		var aid string
		err = tx.QueryRowContext(ctx, `SELECT admission_id FROM invocation_admissions WHERE run_id=? AND invocation_id=?`, run, x.InvocationID).Scan(&aid)
		if err == nil {
			a, e := readAdmission(ctx, tx, run, aid)
			if e != nil {
				return out, e
			}
			x.Admission = &a
		} else if !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
		err = tx.QueryRowContext(ctx, `SELECT d.claimed_at,COALESCE(z.authorized_at,''),p.duration_ns,u.receipt_state FROM admission_dispatch_claims d JOIN provider_calls p USING(provider_call_id) JOIN provider_call_usage u USING(provider_call_id) LEFT JOIN admission_release_authorizations z USING(claim_id) WHERE p.invocation_id=?`, x.InvocationID).Scan(&x.ClaimedAt, &x.AuthorizedAt, &x.DurationNS, &x.ReceiptState)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admission_closures c JOIN invocation_admissions a USING(admission_id) WHERE a.invocation_id=?)`, x.InvocationID).Scan(&x.Closed); err != nil {
			return out, err
		}
		if current != nil && x.InvocationID == *current {
			out.Current = x
		}
	}
	if out.Current != nil && out.Current.ActionID != "" {
		if err = tx.QueryRowContext(ctx, "SELECT intent FROM next_actions WHERE action_id=?", out.Current.ActionID).Scan(&out.Current.Intent); err != nil {
			return out, err
		}
		rows, e := tx.QueryContext(ctx, "SELECT finding_id FROM next_action_findings WHERE action_id=? ORDER BY finding_id", out.Current.ActionID)
		if e != nil {
			return out, e
		}
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				rows.Close()
				return out, e
			}
			out.Current.FindingIDs = append(out.Current.FindingIDs, id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return out, e
		}
	}
	if out.Terminal.ProofID != "" {
		record, _, err := loadAuthorityProof(ctx, tx, out.Terminal.ProofID)
		if err != nil {
			return out, err
		}
		if record.RunID != run || record.SHA256 != out.Terminal.SHA256 || record.Kind != "TERMINAL" || record.Verdict != "SUPPORTS_READINESS" {
			return out, fmt.Errorf("terminal reference mismatch")
		}
	}
	var total, incomplete int
	if err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(u.provider_call_id IS NULL OR u.receipt_state IS NOT 'final' OR u.work_state IS NOT 'final' OR u.spend_state IS NOT 'final' OR u.work_amount IS NULL OR u.spend_amount IS NULL),0)
 FROM provider_calls p JOIN invocations i USING(invocation_id) LEFT JOIN provider_call_usage u USING(provider_call_id) WHERE i.run_id=?`, run).Scan(&total, &incomplete); err != nil {
		return out, err
	}
	out.AccountingStatus = accountingStatus(out.Spec.Mode == "EXTERNAL", total, incomplete)
	out.AccountingIncomplete = out.AccountingStatus == "unknown" || out.AccountingStatus == "incomplete"
	return out, tx.Commit()
}

// Reporting describes registered Orchestrator-owned calls, not workflow success
// or future requests. Excluded external Codex usage is never reported as zero.
func accountingStatus(external bool, calls, incomplete int) string {
	if external {
		return "excluded"
	}
	if calls == 0 {
		return "unknown"
	}
	if incomplete > 0 {
		return "incomplete"
	}
	return "complete"
}

// Find accepted structured output after a lost acknowledgement without relaunch.
func (s *StateStore) WorkflowResult(ctx context.Context, run, id string) (roleresult.Result, error) {
	var r roleresult.Result
	err := s.db.QueryRowContext(ctx, `SELECT run_id,invocation_id,role,outcome,content_identity,cycle_no,result_ordinal FROM role_results WHERE run_id=? AND invocation_id=?`, run, id).Scan(&r.RunID, &r.InvocationID, &r.Role, &r.Outcome, &r.ContentIdentity, &r.Cycle, &r.ResultOrdinal)
	r.ResultID = roleresult.ResultID(r.Cycle, r.ResultOrdinal)
	return r, err
}

func (s *StateStore) WorkflowActionAccepted(ctx context.Context, run, producer string) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM next_action_producers WHERE run_id=? AND producer_invocation_id=?)`, run, producer).Scan(&found)
	return found, err
}
func (s *StateStore) WorkflowProofReference(ctx context.Context, run, id string) (ProofReference, error) {
	var ref ProofReference
	err := s.db.QueryRowContext(ctx, `SELECT p.proof_id,e.sha256 FROM authority_proofs p JOIN evidence_artifacts e USING(evidence_id) WHERE p.run_id=? AND p.proof_id=?`, run, id).Scan(&ref.ProofID, &ref.SHA256)
	return ref, err
}

func (s *StateStore) WorkflowHardRequest(ctx context.Context, run, id string) (HardLimitRequest, string, error) {
	var p HardLimitRequest
	var raw, deadline string
	err := s.db.QueryRowContext(ctx, `SELECT proposal_json,deadline FROM hard_limit_requests WHERE run_id=? AND request_id=?`, run, id).Scan(&raw, &deadline)
	if err == nil {
		err = json.Unmarshal([]byte(raw), &p)
	}
	return p, deadline, err
}
func (s *StateStore) WorkflowHardDecisionTime(ctx context.Context, run, id string) (string, error) {
	var at string
	err := s.db.QueryRowContext(ctx, `SELECT d.decided_at FROM hard_limit_decisions d JOIN hard_limit_requests r USING(request_id) WHERE r.run_id=? AND r.request_id=?`, run, id).Scan(&at)
	return at, err
}

// Workflow lifecycle forbids new semantic submissions or rebinding retired work.
// Historical usage/proof observations remain independent and are not gated here.
func validateWorkflowSemanticLifecycle(ctx context.Context, q querier, run, invocation string) error {
	var invalid bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_runs w JOIN runs r USING(run_id)
 WHERE w.run_id=? AND (r.status<>'RUNNING' OR EXISTS(SELECT 1 FROM invocations i WHERE i.run_id=w.run_id AND i.invocation_id=? AND (i.completed_at IS NOT NULL OR i.status<>'RUNNING'))))`, run, invocation).Scan(&invalid)
	if err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("workflow semantic submission targets retired or nonrunning work")
	}
	return nil
}
