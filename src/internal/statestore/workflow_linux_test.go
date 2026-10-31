package statestore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func workflowSpec() WorkflowSpec {
	return WorkflowSpec{RunID: "workflow-run", Goal: "Verify a deterministic task.", ContentIdentity: "content-one", StartedAt: semanticTimestamp, HardDeadline: "2100-01-01T00:00:00Z", NormalTokens: 1000, Confirmation: approvedBudget("workflow-confirmation")}
}
func TestB4WorkflowConfirmationAndCreationReplay(t *testing.T) {
	s := testStore(t)
	spec := workflowSpec()
	bad := spec
	bad.Confirmation.Decision = "DENY"
	before := mutationDatabaseSnapshot(t, s)
	if err := s.CreateWorkflow(accountingContext, bad); err == nil {
		t.Fatal("denied workflow created")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("denied confirmation changed state")
	}
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	before = mutationDatabaseSnapshot(t, s)
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal("creation replay", err)
	}
	spec.NormalTokens++
	if err := s.CreateWorkflow(accountingContext, spec); err == nil {
		t.Fatal("changed confirmed input accepted")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("creation replay changed authority")
	}
	if _, err := s.db.Exec(`UPDATE workflow_runs SET spec_json='{}'`); err == nil {
		t.Fatal("SQL changed immutable input")
	}
}
func TestB4WorkflowMutationsRequireOrchestrator(t *testing.T) {
	s := testStore(t)
	spec := workflowSpec()
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	mutations := []func(context.Context) error{
		func(ctx context.Context) error { return s.CreateWorkflow(ctx, spec) },
		func(ctx context.Context) error {
			_, err := s.BeginWorkflowStep(ctx, spec.RunID, "", SemanticFence{}, semanticTimestamp)
			return err
		},
		func(ctx context.Context) error {
			_, err := s.RegisterWorkflowArtifact(ctx, spec.RunID, "worker-artifact", "test_result", []byte(`{}`), semanticTimestamp)
			return err
		},
		func(ctx context.Context) error {
			return s.CaptureWorkflowOutput(ctx, spec.RunID, "invocation", []byte(`{}`), semanticTimestamp)
		},
		func(ctx context.Context) error {
			return s.FinishWorkflowStep(ctx, spec.RunID, "invocation", semanticTimestamp)
		},
		func(ctx context.Context) error {
			return s.BlockWorkflow(ctx, spec.RunID, "untrusted Worker blocks Run", semanticTimestamp)
		},
		func(ctx context.Context) error {
			return s.EnterWorkflowReadiness(ctx, spec.RunID, ProofReference{"proof", ""}, semanticTimestamp)
		},
	}
	before := mutationDatabaseSnapshot(t, s)
	for _, role := range []string{"", "LEAD", "DEVELOPER", "QA", "REVIEWER"} {
		for i, m := range mutations {
			if err := m(WithMutationActor(context.Background(), role)); !errors.Is(err, ErrMutationAuthority) {
				t.Fatal("unauthorized mutation", role, i, err)
			}
		}
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("unauthorized Worker changed authority")
	}
}
func TestB4WorkflowActionFreshnessAndSingleConsumption(t *testing.T) {
	for _, mode := range []string{"stale", "closed-finding", "valid"} {
		t.Run(mode, func(t *testing.T) {
			s := testStore(t)
			state, opts := semanticFixture(t, s)
			if err := s.AcceptLeadState(accountingContext, wire(t, state), opts); err != nil {
				t.Fatal(err)
			}
			opts.ExpectedSemanticRevision = 1
			action := actionFixture(state)
			if err := s.AcceptNextAction(accountingContext, wire(t, action), opts); err != nil {
				t.Fatal(err)
			}
			p := admissionPolicy(1000)
			p.RunID = "run1"
			p.HardDeadline = "2100-01-01T00:00:00Z"
			if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			spec := workflowSpec()
			spec.RunID = "run1"
			spec.ContentIdentity = "content2"
			raw, _ := json.Marshal(spec)
			execSQL(t, s.db, `UPDATE invocations SET status='RUNNING' WHERE invocation_id=?`, state["producer"].(map[string]any)["invocation_id"])
			execSQL(t, s.db, `INSERT INTO workflow_runs(run_id,spec_json) VALUES('run1',?)`, string(raw))
			if mode != "valid" {
				newer := cloneDoc(t, state)
				newer["lead_state_id"] = "lead-new"
				if mode == "closed-finding" {
					f := newer["open_findings"].([]any)[0].(map[string]any)
					f["resolution_ref"] = map[string]any{"evidence_id": "proof"}
					newer["open_findings"] = []any{}
					newer["resolved_findings"] = []any{f}
				}
				if err := s.AcceptLeadState(accountingContext, wire(t, newer), opts); err != nil {
					t.Fatal(err)
				}
				if mode == "closed-finding" {
					action["action_id"] = "closed-action"
					action["based_on_lead_state"] = map[string]any{"lead_state_id": "lead-new"}
					opts.ExpectedSemanticRevision = 2
					if err := s.AcceptNextAction(accountingContext, wire(t, action), opts); err != nil {
						t.Fatal(err)
					}
				}
			}
			w, err := s.GetWorkflow(accountingContext, "run1")
			if err != nil {
				t.Fatal(err)
			}
			before := mutationDatabaseSnapshot(t, s)
			step, err := s.BeginWorkflowStep(accountingContext, "run1", action["action_id"].(string), w.Owner, semanticTimestamp)
			if mode != "valid" {
				if err == nil {
					t.Fatal("invalid action executed")
				}
				if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
					t.Fatal("rejected action changed state")
				}
				return
			}
			if err != nil || step.Role != "DEVELOPER" {
				t.Fatal(step, err)
			}
			w, err = s.GetWorkflow(accountingContext, "run1")
			if err != nil {
				t.Fatal(err)
			}
			before = mutationDatabaseSnapshot(t, s)
			if _, err = s.BeginWorkflowStep(accountingContext, "run1", action["action_id"].(string), w.Owner, semanticTimestamp); err == nil {
				t.Fatal("Worker bypassed Lead/consumed action twice")
			}
			producer := state["producer"].(map[string]any)["invocation_id"].(string)
			if _, err = s.db.Exec(`INSERT INTO workflow_steps VALUES(?,'run1',2,?,NULL)`, producer, action["action_id"]); err == nil {
				t.Fatal("SQL consumed action twice")
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("repeat changed binding")
			}
		})
	}
}
func TestB4WorkflowCannotCompleteOrReadyWithoutAuthority(t *testing.T) {
	s := testStore(t)
	spec := workflowSpec()
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	before := mutationDatabaseSnapshot(t, s)
	if err := s.EnterWorkflowReadiness(accountingContext, spec.RunID, ProofReference{"missing-proof", ""}, semanticTimestamp); err == nil {
		t.Fatal("unproved terminal readiness")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("failed terminal transition changed state")
	}
	if err := s.BlockWorkflow(accountingContext, spec.RunID, "controller blocks", semanticTimestamp); err != nil {
		t.Fatal(err)
	}
	before = mutationDatabaseSnapshot(t, s)
	w, err := s.GetWorkflow(accountingContext, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginWorkflowStep(accountingContext, spec.RunID, "", w.Owner, semanticTimestamp); err == nil {
		t.Fatal("blocked workflow started")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("blocked rejection changed state")
	}
}

func TestB4BlockedWorkflowCannotRebindRetiredLead(t *testing.T) {
	s := testStore(t)
	state, opts := semanticFixture(t, s)
	if err := s.AcceptLeadState(accountingContext, wire(t, state), opts); err != nil {
		t.Fatal(err)
	}
	spec := workflowSpec()
	spec.RunID = "run1"
	raw, _ := json.Marshal(spec)
	execSQL(t, s.db, `INSERT INTO workflow_runs(run_id,spec_json) VALUES('run1',?)`, string(raw))
	w, err := s.GetWorkflow(accountingContext, "run1")
	if err != nil {
		t.Fatal(err)
	}
	old := *w.Owner.InvocationID
	var ex, attempt, purpose, content string
	if err = s.db.QueryRow(`SELECT execution_id,attempt_id,purpose,content_identity FROM invocations WHERE invocation_id=?`, old).Scan(&ex, &attempt, &purpose, &content); err != nil {
		t.Fatal(err)
	}
	if err = s.BlockWorkflow(accountingContext, "run1", "interrupted Lead", semanticTimestamp); err != nil {
		t.Fatal(err)
	}
	w, err = s.GetWorkflow(accountingContext, "run1")
	if err != nil {
		t.Fatal(err)
	}
	before := mutationDatabaseSnapshot(t, s)
	if err = s.BindActiveInvocation(accountingContext, "run1", InvocationBinding{old, ex, attempt, 2, purpose, content}, OwnershipOptions{Expected: w.Owner, ChangedAt: semanticTimestamp}); err == nil {
		t.Fatal("blocked Lead reacquired semantic authority")
	}
	state["lead_state_id"] = "blocked-submission"
	if err = s.AcceptLeadState(accountingContext, wire(t, state), MutationOptions{Fence: ResultFence{OwnershipEpoch: w.Owner.OwnershipEpoch, ActorRevision: w.Owner.ActorRevision}, ExpectedSemanticRevision: w.SemanticRevision}); err == nil {
		t.Fatal("blocked Lead changed semantics")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("blocked semantic rejection changed state")
	}
}

func TestB4WorkflowOutputRequiresAuthorizedRelease(t *testing.T) {
	s := testStore(t)
	spec := workflowSpec()
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	p := admissionPolicy(1000)
	p.RunID = spec.RunID
	p.HardDeadline = spec.HardDeadline
	if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	w, err := s.GetWorkflow(accountingContext, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	step, err := s.BeginWorkflowStep(accountingContext, spec.RunID, "", w.Owner, semanticTimestamp)
	if err != nil {
		t.Fatal(err)
	}
	before := mutationDatabaseSnapshot(t, s)
	if err = s.CaptureWorkflowOutput(accountingContext, spec.RunID, step.InvocationID, []byte(`{}`), semanticTimestamp); err == nil {
		t.Fatal("output accepted without authorized release")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("rejection created orphan evidence")
	}
	ref, err := s.RegisterWorkflowArtifact(accountingContext, spec.RunID, "unreleased-output", "workflow_output", []byte(`{}`), semanticTimestamp)
	if err != nil {
		t.Fatal(err)
	}
	before = mutationDatabaseSnapshot(t, s)
	if _, err = s.db.Exec(`UPDATE workflow_steps SET output_id=? WHERE invocation_id=?`, ref.EvidenceID, step.InvocationID); err == nil {
		t.Fatal("SQL linked unauthorized output")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("SQL rejection changed authority")
	}
}

func TestB5ExternalScopeFencingAndSQLAccountingGuards(t *testing.T) {
	s := testStore(t)
	spec := workflowSpec()
	spec.Mode = "EXTERNAL"
	spec.WorktreePath = "/tmp/external-fixture"
	spec.NormalTokens = 0
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	owner := SemanticFence{Role: "LEAD", OwnershipEpoch: 1, ActorRevision: 0}
	step, err := s.BeginWorkflowStep(accountingContext, spec.RunID, "", owner, semanticTimestamp)
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.GetWorkflow(accountingContext, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	before := mutationDatabaseSnapshot(t, s)
	for _, ctx := range []context.Context{context.Background(), WithMutationActor(context.Background(), "DEVELOPER")} {
		if err := s.CaptureExternalWorkflowOutput(ctx, spec.RunID, step.InvocationID, []byte(`{}`), semanticTimestamp, w.Owner, w.SemanticRevision); !errors.Is(err, ErrMutationAuthority) {
			t.Fatal(err)
		}
	}
	if err := s.CaptureWorkflowOutput(accountingContext, spec.RunID, step.InvocationID, []byte(`{}`), semanticTimestamp); err == nil {
		t.Fatal("fake capture adopted external work")
	}
	stale := w.Owner
	stale.ActorRevision++
	if err := s.CaptureExternalWorkflowOutput(accountingContext, spec.RunID, step.InvocationID, []byte(`{}`), semanticTimestamp, stale, w.SemanticRevision); err == nil {
		t.Fatal("stale owner accepted")
	}
	_, err = s.db.Exec(`INSERT INTO provider_calls(provider_call_id,invocation_id,call_ordinal,provider,model,reasoning_effort,started_at) VALUES('forbidden-call',?,1,'codex','external','none',?)`, step.InvocationID, semanticTimestamp)
	if err == nil || !strings.Contains(err.Error(), "external Codex requests are outside Orchestrator accounting") {
		t.Fatal("direct SQL synthesized external ProviderCall")
	}
	_, err = s.db.Exec(`INSERT INTO invocation_admissions(admission_id,run_id,invocation_id,execution_id,attempt_id,cycle_no,role,purpose,kind,content_identity,ownership_epoch,actor_revision,actor_role,semantic_revision,run_revision,execution_revision,attempt_revision,invocation_revision,admitted_at,contract_digest)
 VALUES('forbidden-admission',?,?,?,?,1,'LEAD','LEAD','NORMAL',?,1,0,'LEAD',0,0,0,0,0,?,?)`, spec.RunID, step.InvocationID, step.ExecutionID, step.AttemptID, spec.ContentIdentity, semanticTimestamp, strings.Repeat("0", 64))
	if err == nil || !strings.Contains(err.Error(), "external workflow steps do not admit model requests") {
		t.Fatal("direct SQL synthesized managed admission", err)
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("rejection changed authoritative state")
	}
	if err := s.CaptureExternalWorkflowOutput(accountingContext, spec.RunID, step.InvocationID, []byte(`{}`), semanticTimestamp, w.Owner, w.SemanticRevision); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishWorkflowStep(accountingContext, spec.RunID, step.InvocationID, semanticTimestamp); err == nil {
		t.Fatal("captured intent substituted for accepted semantics")
	}
}

func TestB7RequestedProfilesAndSQLRejection(t *testing.T) {
	s := testStore(t)
	spec := workflowSpec()
	spec.Mode = "EXTERNAL"
	spec.NormalTokens = 0
	spec.WorktreePath = "/fixture"
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	w, err := s.GetWorkflow(accountingContext, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	step, err := s.BeginWorkflowStep(accountingContext, spec.RunID, "", w.Owner, semanticTimestamp)
	if err != nil {
		t.Fatal(err)
	}
	if step.Model != "GPT-6.1 Sol" || step.ReasoningEffort != "Medium" {
		t.Fatal(step)
	}
	for ordinal, tc := range []struct{ id, model, effort string }{{"wrong-model", "unapproved-model", "Medium"}, {"skipped-level", "GPT-6 Astra", "High"}, {"wrong-effort", "GPT-6.1 Sol", "XHigh"}} {
		t.Run(tc.id, func(t *testing.T) {
			execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch)
 SELECT ?,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal+?,'LEAD',purpose,provider,?,?,'RUNNING',started_at,content_identity,ownership_epoch FROM invocations WHERE invocation_id=?`, tc.id, ordinal+1, tc.model, tc.effort, step.InvocationID)
			before := mutationDatabaseSnapshot(t, s)
			if _, err := s.db.Exec("INSERT INTO workflow_steps(invocation_id,run_id,step_no) VALUES(?,?,2)", tc.id, spec.RunID); err == nil {
				t.Fatal("SQL accepted unauthorized profile")
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("SQL profile rejection changed state")
			}
			// These are deliberately invalid unbound fixture rows, not admitted work.
		})
	}
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM v_workflow_role_profiles").Scan(&n); err != nil || n != 12 {
		t.Fatal(n, err)
	}
}
