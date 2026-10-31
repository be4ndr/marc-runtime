package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"marc-runtime/src/internal/statestore"
)

type Runtime struct {
	store    *statestore.StateStore
	command  []string
	now      func() time.Time
	hook     func(string) error // deterministic interruption cut points in package tests
	behavior string
}

func New(s *statestore.StateStore) (*Runtime, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return &Runtime{store: s, command: []string{exe, "--fake-worker"}, now: time.Now}, nil
}
func (r *Runtime) at() string { return r.now().UTC().Format(time.RFC3339Nano) }
func (r *Runtime) cut(name string) error {
	if r.hook != nil {
		return r.hook(name)
	}
	return nil
}
func account(run string) statestore.ResourceAccount {
	return statestore.ResourceAccount{PoolID: fmt.Sprintf("fake-work-%x", sha256.Sum256([]byte(run))), RunID: run, Scope: "RUN", ResourceType: "WORK"}
}
func binding(s statestore.WorkflowStep) statestore.InvocationBinding {
	return statestore.InvocationBinding{InvocationID: s.InvocationID, ExecutionID: s.ExecutionID, AttemptID: s.AttemptID, Cycle: s.Cycle, Purpose: s.Purpose, ContentIdentity: *s.ContentIdentity}
}
func boundary(w statestore.WorkflowSnapshot, s statestore.WorkflowStep) statestore.ProofBoundary {
	return statestore.ProofBoundary{Binding: binding(s), Owner: w.Owner, SemanticRevision: w.SemanticRevision}
}
func (r *Runtime) Initialize(ctx context.Context, spec statestore.WorkflowSpec) error {
	if spec.Mode == "EXTERNAL" {
		return fmt.Errorf("external workflows use MCP orchestration")
	}
	if err := r.store.CreateWorkflow(ctx, spec); err != nil {
		return err
	}
	policy := statestore.RunAdmissionPolicy{RunID: spec.RunID, PolicyID: "fake-policy", HardDeadline: spec.HardDeadline, Confirmation: spec.Confirmation, Budgets: []statestore.RunResourceBudget{{ResourceType: "WORK", HardLimit: spec.NormalTokens}}, AgentTokenProfiles: []statestore.AgentTokenProfile{{Role: "LEAD", SoftPercent: ptr(12), HardPercent: ptr(15)}, {Role: "DEVELOPER", SoftPercent: ptr(28), HardPercent: ptr(35)}, {Role: "QA", SoftPercent: ptr(12), HardPercent: ptr(15)}, {Role: "REVIEWER", SoftPercent: ptr(16), HardPercent: ptr(20)}}}
	if err := r.store.ConfigureRunAdmission(ctx, policy, statestore.AccountingOptions{At: spec.StartedAt, Audit: []statestore.AuditRow{{EventType: "workflow_budget_confirmed", ActorRole: "ORCHESTRATOR", OccurredAt: spec.StartedAt}}}); err != nil {
		return err
	}
	if _, err := r.store.GetResourceBalance(ctx, account(spec.RunID)); err == nil {
		return nil
	}
	return r.store.CreateResourcePool(ctx, statestore.ResourcePool{Account: account(spec.RunID), Eligibility: []statestore.ResourceEligibility{{Role: "LEAD", Purpose: "LEAD"}, {Role: "DEVELOPER", Purpose: "IMPLEMENT"}, {Role: "DEVELOPER", Purpose: "REMEDIATE"}, {Role: "DEVELOPER", Purpose: "FINALIZE_IMPLEMENTATION"}, {Role: "QA", Purpose: "QA"}, {Role: "REVIEWER", Purpose: "REVIEW"}, {Role: "REVIEWER", Purpose: "FINALIZE_REVIEW"}, {Role: "ORCHESTRATOR", Purpose: "RECOVER"}}}, statestore.AccountingOptions{At: spec.StartedAt})
}

// Run returns at an explicit blocked boundary or human merge readiness. It never
// turns a physical finish or a recovered claim into an automatic retry.
func (r *Runtime) Run(ctx context.Context, run string) (statestore.WorkflowSnapshot, error) {
	for {
		if err := ctx.Err(); err != nil {
			return statestore.WorkflowSnapshot{}, err
		}
		w, err := r.store.GetWorkflow(ctx, run)
		if err != nil {
			return w, err
		}
		if w.Status != "RUNNING" {
			return w, nil
		}
		if err = r.Advance(ctx, w); err != nil {
			var denied *statestore.AdmissionDeniedError
			if errors.As(err, &denied) {
				err = errors.Join(err, r.store.BlockWorkflow(context.WithoutCancel(ctx), run, "admission denied: "+denied.Reason, r.at()))
			}
			return w, err
		}
	}
}
func (r *Runtime) Advance(ctx context.Context, w statestore.WorkflowSnapshot) error {
	if w.Spec.Mode == "EXTERNAL" {
		return fmt.Errorf("external workflows use MCP orchestration")
	}
	run := w.Spec.RunID
	if w.Status != "RUNNING" {
		return fmt.Errorf("workflow is not running")
	}
	if w.Current == nil || w.Current.CompletedAt != "" {
		action := ""
		if w.Current != nil && w.Current.Role == "LEAD" {
			var o captured
			if err := json.Unmarshal(w.Current.Output, &o); err != nil {
				return err
			}
			var lead struct {
				State  map[string]any
				Action map[string]any
			}
			if err := json.Unmarshal(o.Response.Semantic, &lead); err != nil {
				return err
			}
			switch lead.State["semantic_status"] {
			case "approved":
				return r.ready(ctx, w)
			case "blocked":
				return r.store.BlockWorkflow(ctx, run, "Lead blocked dependent work", r.at())
			default:
				if lead.Action == nil {
					return r.store.BlockWorkflow(ctx, run, "Lead provided no next action", r.at())
				}
				action, _ = lead.Action["action_id"].(string)
			}
		}
		if _, err := r.store.BeginWorkflowStep(ctx, run, action, w.Owner, r.at()); err != nil {
			return err
		}
		if err := r.cut("after_step"); err != nil {
			return err
		}
		var err error
		w, err = r.store.GetWorkflow(ctx, run)
		if err != nil {
			return err
		}
	}
	return r.invoke(ctx, w)
}
func (r *Runtime) invoke(ctx context.Context, w statestore.WorkflowSnapshot) error {
	s := *w.Current
	run := w.Spec.RunID
	if len(s.Output) == 0 && s.AuthorizedAt != "" {
		return r.store.BlockWorkflow(ctx, run, "UNKNOWN_OUTCOME: release authorized without captured outcome; do not redispatch", r.at())
	}
	if s.Admission == nil {
		a := statestore.InvocationAdmission{AdmissionID: s.InvocationID + "-admission", RunID: run, Binding: binding(s), Fence: statestore.AdmissionFence{Owner: w.Owner, SemanticRevision: w.SemanticRevision, RunRevision: w.Revision}, HardDeadline: w.Spec.HardDeadline, Resources: []statestore.AdmissionResource{{Account: account(run), Amount: 0}}}
		if created, err := r.store.AdmitInvocation(ctx, a, statestore.AccountingOptions{At: r.at()}); err != nil || !created {
			if err == nil {
				err = fmt.Errorf("admission replay requires reconciliation")
			}
			return fmt.Errorf("initial admission: %w", err)
		}
		if err := r.cut("after_admission"); err != nil {
			return err
		}
		var err error
		w, err = r.store.GetWorkflow(ctx, run)
		if err != nil {
			return err
		}
		s = *w.Current
	}
	call := statestore.ProviderCall{ProviderCallID: s.InvocationID + "-call", RunID: run, InvocationID: s.InvocationID, Provider: "fake", Model: "deterministic", ReasoningEffort: "none"}
	d := statestore.AdmissionDispatchClaim{ClaimID: s.InvocationID + "-claim", AdmissionID: s.Admission.AdmissionID, RunID: run, ProviderCall: &call, Bounds: []statestore.DispatchBound{{Dimension: "WORK", Amount: 4}, {Dimension: "INPUT_TOKENS", Amount: 0}, {Dimension: "OUTPUT_TOKENS", Amount: 4}}, Capabilities: &statestore.ProviderModelCapabilities{Provider: "fake", Model: "deterministic", ContextWindowTokens: 16, MaxOutputTokens: 4}}
	if s.ClaimedAt == "" {
		if created, err := r.store.ClaimAdmissionDispatch(ctx, d, statestore.AccountingOptions{At: r.at()}); err != nil || !created {
			if err == nil {
				err = fmt.Errorf("claim replay requires reconciliation")
			}
			return fmt.Errorf("dispatch claim: %w", err)
		}
		if err := r.cut("after_claim"); err != nil {
			return err
		}
	}
	var observation captured
	if len(s.Output) == 0 {
		q := Request{RunID: run, InvocationID: s.InvocationID, ExecutionID: s.ExecutionID, AttemptID: s.AttemptID, Role: s.Role, Purpose: s.Purpose, ContentIdentity: w.ContentIdentity, Goal: w.Spec.Goal, At: s.StartedAt, Deadline: w.Spec.HardDeadline, Step: s.StepNo, InvocationOrdinal: s.Ordinal, Behavior: r.behavior}
		// Fresh values from accepted results, never transcripts or another role's context.
		for _, x := range w.Steps {
			if x.ResultID != "" {
				v, err := r.store.WorkflowResult(ctx, run, x.InvocationID)
				if err != nil {
					return err
				}
				if x.Role == "DEVELOPER" {
					var observed captured
					if err := json.Unmarshal(x.Output, &observed); err != nil {
						return err
					}
					q.TaskArtifact = observed.Response.Artifact
				}
				if s.Role == "LEAD" || s.Role == "REVIEWER" {
					switch x.Role {
					case "DEVELOPER":
						q.Developer = &v
					case "QA":
						q.QA = &v
					case "REVIEWER":
						q.Reviewer = &v
					}
				}
			}
		}
		if s.Role == "REVIEWER" {
			if err := r.reviewAdmission(ctx, w); err != nil {
				return err
			}
		}
		// The gate is adjacent to this fixed fake subprocess release. A replay or any
		// error, including committed projection failure, never enters executeFake.
		gateAt := r.at()
		if created, err := r.store.AuthorizeAdmissionRelease(ctx, run, d.ClaimID, statestore.AccountingOptions{At: gateAt}); err != nil || !created {
			if err == nil {
				err = fmt.Errorf("release replay grants no permission")
			}
			return fmt.Errorf("pre-release authorization: %w", err)
		}
		if err := r.cut("after_authorize"); err != nil {
			return err
		}
		var err error
		observation, err = executeFake(ctx, q, r.command)
		if err != nil {
			cleanup := context.WithoutCancel(ctx)
			if observation.ReleasedAt != "" {
				finishErr := r.store.FinishProviderCall(cleanup, run, call.ProviderCallID, observation.DurationNS, statestore.AccountingOptions{At: observation.At})
				err = errors.Join(err, finishErr)
			}
			if e := r.store.BlockWorkflow(cleanup, run, err.Error(), r.at()); e != nil {
				return errors.Join(err, e)
			}
			return err
		}
		if err = r.cut("after_process"); err != nil {
			return err
		}
		if err = r.store.CaptureWorkflowOutput(ctx, run, s.InvocationID, marshal(observation), observation.At); err != nil {
			return err
		}
		if err = r.cut("after_capture"); err != nil {
			return err
		}
	} else {
		if err := json.Unmarshal(s.Output, &observation); err != nil {
			return err
		}
	}
	if observation.ReleasedAt != "" {
		if _, err := r.store.RecordControllerReleaseEvidence(ctx, statestore.ControllerReleaseEvidence{EvidenceID: s.InvocationID + "-release", RunID: run, ClaimID: d.ClaimID, ReleasedAt: observation.ReleasedAt, SubmittedBounds: d.Bounds}, statestore.AccountingOptions{At: observation.At}); err != nil {
			return err
		}
	}
	if s.DurationNS == nil {
		if err := r.store.FinishProviderCall(ctx, run, call.ProviderCallID, observation.DurationNS, statestore.AccountingOptions{At: observation.At}); err != nil {
			return err
		}
	}
	if s.ReceiptState != "final" {
		if err := r.store.AcceptUsageReceipt(ctx, statestore.UsageReceipt{ReceiptID: s.InvocationID + "-final", RunID: run, InvocationID: s.InvocationID, ProviderCallID: call.ProviderCallID, Completeness: "final", InputTokens: observation.Response.Input, OutputTokens: observation.Response.Output, CachedInputTokens: ptr(0), ReasoningTokens: ptr(0), ReportedAt: observation.At}, statestore.AccountingOptions{At: observation.At}); err != nil {
			return err
		}
	}
	if err := r.cut("after_usage"); err != nil {
		return err
	}
	if s.Role == "LEAD" {
		if err := r.acceptLead(ctx, w, observation); err != nil {
			return err
		}
	} else if s.ResultID == "" {
		if string(observation.Response.Artifact) != string(taskArtifact(w.Spec.Goal)) {
			return r.store.BlockWorkflow(ctx, run, "controller task artifact validation failed", r.at())
		}
		if _, err := r.store.RegisterWorkflowArtifact(ctx, run, s.InvocationID+"-evidence", "test_result", observation.Response.Artifact, s.StartedAt); err != nil {
			return err
		}
		if err := r.store.AcceptRoleResultAndReturnToLead(ctx, observation.Response.Semantic, nil, statestore.ResultReturnOptions{Fence: statestore.ResultFence{OwnershipEpoch: w.Owner.OwnershipEpoch, ActorRevision: w.Owner.ActorRevision}, ReturnedAt: observation.At}); err != nil {
			var projection *statestore.MetricsProjectionError
			if errors.As(err, &projection) {
				// Acceptance and ownership return committed. Resume reconciles
				// the persisted result without reaccepting or redispatching it.
				return err
			}
			_ = r.store.BlockWorkflow(ctx, run, "invalid structured result: "+err.Error(), r.at())
			return err
		}
	}
	if err := r.cut("after_result"); err != nil {
		return err
	}
	fresh, err := r.store.GetWorkflow(ctx, run)
	if err != nil {
		return err
	}
	if !fresh.Current.Closed {
		ref, err := r.containment(ctx, fresh, s, "close")
		if err != nil {
			return err
		}
		if _, err = r.store.CloseAdmission(ctx, run, s.Admission.AdmissionID, ref, statestore.AccountingOptions{At: r.at()}); err != nil {
			return err
		}
	}
	if err = r.cut("after_close"); err != nil {
		return err
	}
	if err = r.store.FinishWorkflowStep(ctx, run, s.InvocationID, r.at()); err != nil {
		return err
	}
	return r.cut("after_finish")
}
func (r *Runtime) acceptLead(ctx context.Context, w statestore.WorkflowSnapshot, o captured) error {
	var lead struct {
		State  map[string]any
		Action map[string]any
	}
	if err := json.Unmarshal(o.Response.Semantic, &lead); err != nil {
		return errors.Join(err, r.store.BlockWorkflow(ctx, w.Spec.RunID, "invalid Lead response", r.at()))
	}
	s := *w.Current
	stateID, _ := lead.State["lead_state_id"].(string)
	if w.LeadStateID == nil || *w.LeadStateID != stateID {
		if err := r.store.AcceptLeadState(ctx, marshal(lead.State), statestore.MutationOptions{Fence: statestore.ResultFence{OwnershipEpoch: w.Owner.OwnershipEpoch, ActorRevision: w.Owner.ActorRevision}, ExpectedSemanticRevision: w.SemanticRevision}); err != nil {
			var projection *statestore.MetricsProjectionError
			if errors.As(err, &projection) {
				return err
			}
			return errors.Join(err, r.store.BlockWorkflow(ctx, w.Spec.RunID, "invalid Lead state", r.at()))
		}
	}
	if err := r.cut("after_lead_state"); err != nil {
		return err
	}
	if lead.Action != nil {
		// Reconcile an already accepted action after lost acknowledgement.
		found, err := r.store.WorkflowActionAccepted(ctx, w.Spec.RunID, s.InvocationID)
		if err != nil {
			return err
		}
		if !found {
			fresh, err := r.store.GetWorkflow(ctx, w.Spec.RunID)
			if err != nil {
				return err
			}
			if err = r.store.AcceptNextAction(ctx, marshal(lead.Action), statestore.MutationOptions{Fence: statestore.ResultFence{OwnershipEpoch: fresh.Owner.OwnershipEpoch, ActorRevision: fresh.Owner.ActorRevision}, ExpectedSemanticRevision: fresh.SemanticRevision}); err != nil {
				return err
			}
		}
	}
	return r.cut("after_action")
}
func (r *Runtime) proof(ctx context.Context, p statestore.AuthorityProof) (statestore.ProofReference, error) {
	v, err := r.store.SealAuthorityProof(ctx, p, statestore.AccountingOptions{At: r.at()})
	return statestore.ProofReference{ProofID: v.ProofID, SHA256: v.SHA256}, err
}
func (r *Runtime) containment(ctx context.Context, w statestore.WorkflowSnapshot, s statestore.WorkflowStep, tag string) (statestore.ProofReference, error) {
	id := fmt.Sprintf("%s-%s-%d-containment", s.InvocationID, tag, w.Current.StepNo)
	containment, observation := "CONFIRMED", "fixed fake request exited and owned group was killed"
	reason := "fake request contained; preserve all accounting"
	if w.Spec.Mode == "EXTERNAL" {
		containment, observation = "EXTERNAL_NO_RELEASE", "no controller-owned provider request or process was released; external coding is outside containment"
		reason = "external role workflow released no controller-owned request/process"
	}
	evidence, err := r.store.RegisterWorkflowArtifact(ctx, w.Spec.RunID, id+"-fact", "controller_containment", marshal(map[string]string{"observation": observation, "invocation": s.InvocationID}), s.StartedAt)
	if err != nil {
		return statestore.ProofReference{}, err
	}
	return r.proof(ctx, statestore.AuthorityProof{ProofID: id, RunID: w.Spec.RunID, Kind: "RECOVERY", ContentIdentity: w.ContentIdentity, Verdict: "RECOVERABLE", Reason: reason, Boundary: boundary(w, s), Recovery: &statestore.RecoveryProof{Containment: containment, ContainmentEvidence: []statestore.ProofArtifact{evidence}}})
}
func (r *Runtime) validation(ctx context.Context, w statestore.WorkflowSnapshot, s statestore.WorkflowStep, tag string) (statestore.ProofReference, error) {
	result, err := r.store.WorkflowResult(ctx, w.Spec.RunID, s.InvocationID)
	if err != nil {
		return statestore.ProofReference{}, err
	}
	id := fmt.Sprintf("%s-%s-%d-validation", s.InvocationID, tag, w.Current.StepNo)
	var evidence statestore.ProofArtifact
	reason := "separate controller checks exact deterministic task artifact"
	if w.Spec.Mode == "EXTERNAL" {
		reason = "controller captured exact current Git worktree separately from Worker claims"
		evidence, err = r.externalValidationEvidence(ctx, w, s, id+"-fact")
	} else {
		var out captured
		if err = json.Unmarshal(s.Output, &out); err != nil {
			return statestore.ProofReference{}, err
		}
		if string(out.Response.Artifact) != string(taskArtifact(w.Spec.Goal)) {
			return statestore.ProofReference{}, fmt.Errorf("independent controller validation failed")
		}
		evidence, err = r.store.RegisterWorkflowArtifact(ctx, w.Spec.RunID, id+"-fact", "controller_validation", marshal(map[string]string{"expected": string(taskArtifact(w.Spec.Goal)), "verified": "exact bytes and accepted binding"}), s.StartedAt)
	}
	if err != nil {
		return statestore.ProofReference{}, err
	}
	return r.proof(ctx, statestore.AuthorityProof{ProofID: id, RunID: w.Spec.RunID, Kind: "CONTROLLER_VALIDATION", ContentIdentity: w.ContentIdentity, Verdict: "VALIDATED", Reason: reason, Boundary: boundary(w, s), Validation: &statestore.ControllerValidationProof{ResultID: result.ResultID, ValidationEvidence: []statestore.ProofArtifact{evidence}}})
}
func selected(w statestore.WorkflowSnapshot, role string) (statestore.WorkflowStep, error) {
	for i := len(w.Steps) - 1; i >= 0; i-- {
		if w.Steps[i].Role == role && w.Steps[i].ResultID != "" && w.Steps[i].ContentIdentity != nil && *w.Steps[i].ContentIdentity == w.ContentIdentity {
			return w.Steps[i], nil
		}
	}
	return statestore.WorkflowStep{}, fmt.Errorf("missing accepted %s result", role)
}
func (r *Runtime) reviewAdmission(ctx context.Context, w statestore.WorkflowSnapshot) error {
	developer, err := selected(w, "DEVELOPER")
	if err != nil {
		return err
	}
	completion, err := r.validation(ctx, w, developer, "review")
	if err != nil {
		return err
	}
	containment, err := r.containment(ctx, w, developer, "review")
	if err != nil {
		return err
	}
	admissionID := w.Current.InvocationID
	if w.Spec.Mode != "EXTERNAL" {
		admissionID = w.Current.Admission.AdmissionID
	}
	_, err = r.proof(ctx, statestore.AuthorityProof{ProofID: w.Current.InvocationID + "-review-admission", RunID: w.Spec.RunID, Kind: "REVIEW_ADMISSION", ContentIdentity: w.ContentIdentity, Verdict: "ELIGIBLE", Reason: "closed prior work and separate current completion validation", Boundary: boundary(w, *w.Current), Review: &statestore.ReviewAdmissionProof{Completion: completion, Containment: containment, AdmissionID: admissionID}})
	return err
}
func (r *Runtime) ready(ctx context.Context, w statestore.WorkflowSnapshot) error {
	refs := make([]statestore.ProofReference, 3)
	steps := make([]statestore.WorkflowStep, 3)
	for i, role := range []string{"DEVELOPER", "QA", "REVIEWER"} {
		s, err := selected(w, role)
		if err != nil {
			return err
		}
		steps[i] = s
		refs[i], err = r.validation(ctx, w, s, "terminal")
		if err != nil {
			return err
		}
	}
	contain, err := r.containment(ctx, w, steps[0], "terminal")
	if err != nil {
		return err
	}
	review, err := r.store.WorkflowProofReference(ctx, w.Spec.RunID, steps[2].InvocationID+"-review-admission")
	if err != nil {
		return err
	}
	reason := "complete deterministic fake authority chain"
	if w.Spec.Mode == "EXTERNAL" {
		reason = "complete current external role and content authority chain"
	}
	ref, err := r.proof(ctx, statestore.AuthorityProof{ProofID: w.Current.InvocationID + "-terminal", RunID: w.Spec.RunID, Kind: "TERMINAL", ContentIdentity: w.ContentIdentity, Verdict: "SUPPORTS_READINESS", Reason: reason, Boundary: boundary(w, *w.Current), Terminal: &statestore.TerminalProof{Completion: refs[0], QAValidation: refs[1], ReviewValidation: refs[2], ReviewAdmission: review, Containment: contain, LeadStateID: *w.LeadStateID}})
	if err != nil {
		return err
	}
	if err = r.cut("before_terminal"); err != nil {
		return err
	}
	if err = r.store.EnterWorkflowReadiness(ctx, w.Spec.RunID, ref, r.at()); err != nil {
		return err
	}
	return r.cut("after_ready")
}
