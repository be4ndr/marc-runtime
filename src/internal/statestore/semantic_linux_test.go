package statestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"ai-control-plane/src/internal/roleresult"
)

const semanticTimestamp = "2026-10-05T12:00:00Z"

func semanticFixture(t *testing.T, s *StateStore) (map[string]any, MutationOptions) {
	t.Helper()
	raw := resultFixture(t, "qa-failed")
	r, err := roleresult.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	seedResultBinding(t, s, r, 1)
	if err := s.AcceptRoleResult(orchestratorContext(), raw, []roleresult.Finding{newFinding(r)}, ResultFence{1, 0}); err != nil {
		t.Fatal(err)
	}
	leadID := seedLeadProducer(t, s, r.RunID, r.Cycle)
	execSQL(t, s.db, "UPDATE invocations SET content_identity=?,ownership_epoch=2 WHERE invocation_id=?", r.ContentIdentity, leadID)
	if err := s.BindActiveInvocation(orchestratorContext(), r.RunID, InvocationBinding{leadID, leadID + "-ex", leadID + "-at", r.Cycle, "LEAD", r.ContentIdentity}, OwnershipOptions{
		Expected: SemanticFence{Role: "LEAD", OwnershipEpoch: 2, ActorRevision: 1}, ChangedAt: semanticTimestamp,
	}); err != nil {
		t.Fatal(err)
	}
	state := map[string]any{
		"schema_version": 1, "lead_state_id": "lead-1", "run_id": r.RunID, "cycle": r.Cycle,
		"producer":         map[string]any{"execution_id": leadID + "-ex", "attempt": 1, "invocation_id": leadID, "invocation": 1},
		"content_identity": r.ContentIdentity, "goal": "Resolve the accepted issue.", "acceptance_criteria": []string{"Required checks pass."},
		"open_findings":     []any{map[string]any{"finding_id": "fnd-000005", "source_role": "qa", "source_result_ref": map[string]any{"result_id": r.ResultID}, "content_identity": r.ContentIdentity, "blocking": true, "summary": "Stable semantic issue."}},
		"resolved_findings": []any{}, "qa": map[string]any{"status": "failed", "result_ref": map[string]any{"result_id": r.ResultID}}, "review": map[string]any{"status": "not_run"}, "semantic_status": "active", "created_at": semanticTimestamp,
	}
	return state, MutationOptions{Fence: ResultFence{2, 2}, ExpectedSemanticRevision: 0}
}
func wire(t *testing.T, doc map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func cloneDoc(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	var clone map[string]any
	if err := json.Unmarshal(wire(t, doc), &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}
func actionFixture(state map[string]any) map[string]any {
	return map[string]any{"schema_version": 1, "action_id": "action-1", "run_id": state["run_id"], "cycle": state["cycle"], "producer": state["producer"], "based_on_lead_state": map[string]any{"lead_state_id": state["lead_state_id"]}, "content_identity": state["content_identity"], "target_role": "DEVELOPER", "purpose": "REMEDIATE", "finding_ids": []string{"fnd-000005"}, "intent": "Resolve this bounded issue.", "created_at": semanticTimestamp}
}
func requestFixture(state map[string]any) map[string]any {
	return map[string]any{"schema_version": 1, "run_id": state["run_id"], "cycle": state["cycle"], "request_ordinal": 1, "request_id": "rr-c002-r001", "requester": map[string]any{"role": "lead", "invocation_id": state["producer"].(map[string]any)["invocation_id"]}, "target": map[string]any{"execution_id": "execution2", "attempt_id": "attempt2", "role": "QA", "purpose": "QA", "content_identity": state["content_identity"]}, "source_invocation_id": "invocation2", "requested": map[string]any{"work_tokens": 100}, "justification": "Examine different evidence for the bounded check.", "based_on": map[string]any{"lead_state_ref": map[string]any{"lead_state_id": state["lead_state_id"]}, "source_result_ref": map[string]any{"result_id": "res-c002-r003"}}, "created_at": semanticTimestamp}
}
func fixtureAudit() []AuditRow {
	cycle := 2
	return []AuditRow{{EventType: "opaque-fixture", Cycle: &cycle, ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp, Details: json.RawMessage(`{"fixture":true}`)}}
}

func TestAcceptLeadStateAndPointerRevision(t *testing.T) {
	s := testStore(t)
	doc, options := semanticFixture(t, s)
	ctx := orchestratorContext()
	initial, err := s.GetCurrentSemanticState(ctx, "run1")
	if err != nil || initial.LeadStateID != nil || initial.Revision != 0 {
		t.Fatal(initial, err)
	}
	options.Audit = fixtureAudit()
	if err := s.AcceptLeadState(ctx, wire(t, doc), options); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetCurrentSemanticState(ctx, "run1")
	if err != nil || current.LeadStateID == nil || *current.LeadStateID != "lead-1" || current.Revision != 1 {
		t.Fatal(current, err)
	}
	for _, table := range []string{"lead_states", "lead_state_bindings", "lead_acceptance_criteria", "lead_finding_dispositions", "lead_finding_sources", "events"} {
		if resultCount(t, s, table) != 1 {
			t.Fatal("missing mapped row", table)
		}
	}
	doc["lead_state_id"] = "lead-2"
	if err := s.AcceptLeadState(ctx, wire(t, doc), options); err == nil {
		t.Fatal("stale revision accepted")
	}
	if resultCount(t, s, "events") != 1 || resultCount(t, s, "lead_states") != 1 {
		t.Fatal("stale mutation persisted")
	}
	options.ExpectedSemanticRevision = 1
	finding := doc["open_findings"].([]any)[0].(map[string]any)
	finding["resolution_ref"] = map[string]any{"evidence_id": "proof"}
	doc["open_findings"] = []any{}
	doc["resolved_findings"] = []any{finding}
	if err := s.AcceptLeadState(ctx, wire(t, doc), options); err != nil {
		t.Fatal(err)
	}
	current, err = s.GetCurrentSemanticState(ctx, "run1")
	if err != nil || *current.LeadStateID != "lead-2" || current.Revision != 2 {
		t.Fatal(current, err)
	}
	var disposition, resolution string
	if err := s.db.QueryRow("SELECT disposition,resolution_ref FROM lead_finding_dispositions WHERE lead_state_id='lead-2'").Scan(&disposition, &resolution); err != nil || disposition != "resolved" || resolution != "proof" {
		t.Fatal(disposition, resolution, err)
	}
	// Snapshot one remains immutable after snapshot two is selected.
	var first string
	if err := s.db.QueryRow("SELECT disposition FROM lead_finding_dispositions WHERE lead_state_id='lead-1'").Scan(&first); err != nil || first != "open" {
		t.Fatal(first, err)
	}
}
func TestLeadAcceptanceFailuresProduceNoAudit(t *testing.T) {
	for _, failure := range []string{"wrong-producer", "producer-attempt", "producer-invocation", "producer-execution", "epoch", "actor-revision", "semantic-revision", "cycle", "content", "qa-reference", "qa-status", "finding-summary", "finding-source", "duplicate-disposition", "resolution", "unknown-property"} {
		t.Run(failure, func(t *testing.T) {
			s := testStore(t)
			doc, options := semanticFixture(t, s)
			options.Audit = fixtureAudit()
			p := doc["producer"].(map[string]any)
			f := doc["open_findings"].([]any)[0].(map[string]any)
			switch failure {
			case "wrong-producer":
				p["invocation_id"] = "invocation2"
			case "producer-attempt":
				p["attempt"] = 2
			case "producer-invocation":
				p["invocation"] = 2
			case "producer-execution":
				p["execution_id"] = "execution2"
			case "epoch":
				options.Fence.OwnershipEpoch = 1
			case "actor-revision":
				options.Fence.ActorRevision = 0
			case "semantic-revision":
				options.ExpectedSemanticRevision = 1
			case "cycle":
				doc["cycle"] = 3
			case "content":
				doc["content_identity"] = "stale-content"
			case "qa-reference":
				doc["qa"].(map[string]any)["result_ref"] = map[string]any{"result_id": "res-c002-r004"}
			case "qa-status":
				doc["qa"].(map[string]any)["status"] = "inconclusive"
			case "finding-summary":
				f["summary"] = "changed"
			case "finding-source":
				f["source_role"] = "reviewer"
			case "duplicate-disposition":
				doc["resolved_findings"] = []any{f}
				f["resolution_ref"] = map[string]any{"evidence_id": "proof"}
			case "resolution":
				doc["open_findings"] = []any{}
				doc["resolved_findings"] = []any{f}
				f["resolution_ref"] = map[string]any{"evidence_id": "missing"}
			case "unknown-property":
				doc["history"] = []any{}
			}
			if err := s.AcceptLeadState(orchestratorContext(), wire(t, doc), options); err == nil {
				t.Fatal("invalid LeadState accepted")
			}
			if resultCount(t, s, "lead_states") != 0 || resultCount(t, s, "events") != 0 {
				t.Fatal("invalid state produced state/audit")
			}
			pointer, _ := s.GetCurrentSemanticState(context.Background(), "run1")
			if pointer.LeadStateID != nil || pointer.Revision != 0 {
				t.Fatal("invalid state advanced pointer")
			}
		})
	}
}
func TestActionAndRequestAcceptance(t *testing.T) {
	s := testStore(t)
	state, options := semanticFixture(t, s)
	if err := s.AcceptLeadState(orchestratorContext(), wire(t, state), options); err != nil {
		t.Fatal(err)
	}
	options.ExpectedSemanticRevision = 1
	options.Audit = fixtureAudit()
	if err := s.AcceptNextAction(orchestratorContext(), wire(t, actionFixture(state)), options); err != nil {
		t.Fatal(err)
	}
	if err := s.AcceptResourceRequest(orchestratorContext(), wire(t, requestFixture(state)), options); err != nil {
		t.Fatal(err)
	}
	for table, want := range map[string]int{"next_actions": 1, "next_action_findings": 1, "next_action_producers": 1, "resource_requests": 1, "resource_request_sources": 1, "resource_request_amounts": 1, "events": 2, "invocations": 2, "resource_allocations": 0} {
		if got := resultCount(t, s, table); got != want {
			t.Fatal(table, got, want)
		}
	}
	var status, key string
	if err := s.db.QueryRow("SELECT request_id,status FROM resource_requests").Scan(&key, &status); err != nil || key != "run1/rr-c002-r001" || status != "PENDING" {
		t.Fatal(key, status, err)
	}
	pointer, _ := s.GetCurrentSemanticState(context.Background(), "run1")
	if pointer.Revision != 1 || *pointer.LeadStateID != "lead-1" {
		t.Fatal("intent mutated current snapshot")
	}
	if err := s.AcceptNextAction(orchestratorContext(), wire(t, actionFixture(state)), options); err == nil {
		t.Fatal("duplicate action replaced immutable row")
	}
	if err := s.AcceptResourceRequest(orchestratorContext(), wire(t, requestFixture(state)), options); err == nil {
		t.Fatal("duplicate request replaced immutable row")
	}
	if resultCount(t, s, "events") != 2 {
		t.Fatal("duplicate produced audit")
	}
}
func TestIntentFailuresProduceNoAudit(t *testing.T) {
	for _, kind := range []string{"action", "request"} {
		for _, failure := range []string{"stale-pointer", "stale-revision", "cross-run", "cycle", "role-purpose", "finding-or-source", "wrong-producer", "invalid-amount-or-findings", "derived-id-or-content"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				s := testStore(t)
				state, options := semanticFixture(t, s)
				if err := s.AcceptLeadState(orchestratorContext(), wire(t, state), options); err != nil {
					t.Fatal(err)
				}
				options.ExpectedSemanticRevision = 1
				options.Audit = fixtureAudit()
				doc := actionFixture(state)
				if kind == "request" {
					doc = requestFixture(state)
				}
				switch failure {
				case "stale-pointer":
					if kind == "action" {
						doc["based_on_lead_state"] = map[string]any{"lead_state_id": "old"}
					} else {
						doc["based_on"].(map[string]any)["lead_state_ref"] = map[string]any{"lead_state_id": "old"}
					}
				case "stale-revision":
					options.ExpectedSemanticRevision = 0
				case "cross-run":
					doc["run_id"] = "other-run"
				case "cycle":
					doc["cycle"] = 3
				case "role-purpose":
					if kind == "action" {
						doc["target_role"] = "QA"
					} else {
						doc["target"].(map[string]any)["purpose"] = "REVIEW"
					}
				case "finding-or-source":
					if kind == "action" {
						doc["finding_ids"] = []string{"fnd-000006"}
					} else {
						doc["source_invocation_id"] = "missing-invocation"
					}
				case "wrong-producer":
					if kind == "action" {
						doc["producer"].(map[string]any)["invocation_id"] = "invocation2"
					} else {
						doc["requester"].(map[string]any)["invocation_id"] = "invocation2"
					}
				case "invalid-amount-or-findings":
					if kind == "action" {
						doc["finding_ids"] = []string{}
					} else {
						doc["requested"] = map[string]any{"work_tokens": 0}
					}
				case "derived-id-or-content":
					if kind == "action" {
						doc["content_identity"] = "wrong"
					} else {
						doc["request_id"] = "rr-c002-r002"
					}
				}
				var err error
				if kind == "action" {
					err = s.AcceptNextAction(orchestratorContext(), wire(t, doc), options)
				} else {
					err = s.AcceptResourceRequest(orchestratorContext(), wire(t, doc), options)
				}
				if err == nil {
					t.Fatal("invalid intent accepted")
				}
				for _, table := range []string{"next_actions", "resource_requests", "events"} {
					if resultCount(t, s, table) != 0 {
						t.Fatal("invalid intent created row", table)
					}
				}
			})
		}
	}
}
func TestAuditAtomicityForAllSemanticMutations(t *testing.T) {
	for _, kind := range []string{"lead", "action", "request"} {
		for _, bad := range []string{"json", "foreign-key", "timestamp", "role", "empty-type", "second-row"} {
			t.Run(kind+"/"+bad, func(t *testing.T) {
				s := testStore(t)
				state, options := semanticFixture(t, s)
				if kind != "lead" {
					if err := s.AcceptLeadState(orchestratorContext(), wire(t, state), options); err != nil {
						t.Fatal(err)
					}
					options.ExpectedSemanticRevision = 1
				}
				options.Audit = fixtureAudit()
				switch bad {
				case "json":
					options.Audit[0].Details = json.RawMessage(`{"bad":`)
				case "foreign-key":
					id := "absent"
					options.Audit[0].InvocationID = &id
				case "timestamp":
					options.Audit[0].OccurredAt = "invalid"
				case "role":
					options.Audit[0].ActorRole = "INVALID"
				case "empty-type":
					options.Audit[0].EventType = ""
				case "second-row":
					options.Audit = append(options.Audit, AuditRow{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp})
				}
				var err error
				switch kind {
				case "lead":
					err = s.AcceptLeadState(orchestratorContext(), wire(t, state), options)
				case "action":
					err = s.AcceptNextAction(orchestratorContext(), wire(t, actionFixture(state)), options)
				case "request":
					err = s.AcceptResourceRequest(orchestratorContext(), wire(t, requestFixture(state)), options)
				}
				if err == nil {
					t.Fatal("invalid audit accepted")
				}
				if resultCount(t, s, "events") != 0 || resultCount(t, s, "next_actions") != 0 || resultCount(t, s, "resource_requests") != 0 {
					t.Fatal("partial state/audit survived")
				}
				pointer, _ := s.GetCurrentSemanticState(context.Background(), "run1")
				if kind == "lead" {
					if resultCount(t, s, "lead_states") != 0 || pointer.Revision != 0 || pointer.LeadStateID != nil {
						t.Fatal("Lead state survived failed audit")
					}
				} else if pointer.Revision != 1 {
					t.Fatal("intent changed pointer")
				}
			})
		}
	}
}
func TestCurrentAncestryUsesOnlyActiveInvocation(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	ctx := orchestratorContext()
	execSQL(t, s.db, "INSERT INTO run_active_actor VALUES ('r1','DEVELOPER','i1',1,'now',0)")
	// Create newer unrelated ancestry, without transferring semantic ownership.
	execSQL(t, s.db, `INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES ('new-ex','r1',1,'DEVELOPER','IMPLEMENT','fixture','9999');
 INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES ('new-at','new-ex','r1',1,99,'fixture','9999');
 INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch) VALUES ('new-iv','new-at','new-ex','r1',1,99,'DEVELOPER','IMPLEMENT','fake','new-model','high','fixture','9999','content1',1)`)
	actor, err := s.GetActiveActor(ctx, "r1")
	if err != nil || *actor.InvocationID != "i1" {
		t.Fatal(actor, err)
	}
	i, err := s.GetCurrentInvocation(ctx, "r1")
	if err != nil || i.InvocationID != "i1" || i.Model != "fake-model" {
		t.Fatal(i, err)
	}
	a, err := s.GetCurrentAttempt(ctx, "r1")
	if err != nil || a.AttemptID != "a1" {
		t.Fatal(a, err)
	}
	e, err := s.GetCurrentExecution(ctx, "r1")
	if err != nil || e.ExecutionID != "e1" {
		t.Fatal(e, err)
	}
	if err := s.BindActiveInvocation(ctx, "r1", InvocationBinding{"new-iv", "new-ex", "new-at", 1, "IMPLEMENT", "content1"}, OwnershipOptions{
		Expected: SemanticFence{Role: "DEVELOPER", InvocationID: stringPointer("i1"), OwnershipEpoch: 1, ActorRevision: 0}, ChangedAt: semanticTimestamp,
	}); err != nil {
		t.Fatal(err)
	}
	i, err = s.GetCurrentInvocation(ctx, "r1")
	if err != nil || i.InvocationID != "new-iv" {
		t.Fatal(i, err)
	}
	a, err = s.GetCurrentAttempt(ctx, "r1")
	if err != nil || a.AttemptID != "new-at" {
		t.Fatal(a, err)
	}
	e, err = s.GetCurrentExecution(ctx, "r1")
	if err != nil || e.ExecutionID != "new-ex" {
		t.Fatal(e, err)
	}
	if err := s.ClearActiveInvocation(ctx, "r1", OwnershipOptions{
		Expected: SemanticFence{Role: "DEVELOPER", InvocationID: stringPointer("new-iv"), OwnershipEpoch: 1, ActorRevision: 1}, ChangedAt: semanticTimestamp,
	}); err != nil {
		t.Fatal(err)
	}
	actor, err = s.GetActiveActor(ctx, "r1")
	if err != nil || actor.InvocationID != nil {
		t.Fatal(actor, err)
	}
	if _, err := s.GetCurrentInvocation(ctx, "r1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("NULL actor selected a latest invocation", err)
	}
	if _, err := s.GetCurrentAttempt(ctx, "r1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if _, err := s.GetCurrentExecution(ctx, "r1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
}
func TestCurrentLeadSnapshotRequiresExplicitPointer(t *testing.T) {
	s := testStore(t)
	state, options := semanticFixture(t, s)
	if err := s.AcceptLeadState(orchestratorContext(), wire(t, state), options); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s.db, "INSERT INTO lead_states SELECT 'unselected',run_id,cycle_no,producer_invocation_id,semantic_status,qa_status,review_status,goal,revision,'9999' FROM lead_states WHERE lead_state_id='lead-1'")
	current, err := s.GetCurrentSemanticState(context.Background(), "run1")
	if err != nil || *current.LeadStateID != "lead-1" {
		t.Fatal(current, err)
	}
	execSQL(t, s.db, "UPDATE run_active_actor SET revision=revision+1 WHERE run_id='run1'")
	current, err = s.GetCurrentSemanticState(context.Background(), "run1")
	if err != nil || *current.LeadStateID != "lead-1" || current.Revision != 1 {
		t.Fatal(current, err)
	}
}
func TestRequestedAmountsAndSourceValidation(t *testing.T) {
	for _, test := range []string{"work-only", "spend-only", "time-only", "no-amount", "negative", "large-integer", "source-result", "target-attempt", "target-role", "future-invocation", "grant"} {
		t.Run(test, func(t *testing.T) {
			s := testStore(t)
			state, options := semanticFixture(t, s)
			if err := s.AcceptLeadState(orchestratorContext(), wire(t, state), options); err != nil {
				t.Fatal(err)
			}
			options.ExpectedSemanticRevision = 1
			doc := requestFixture(state)
			valid := false
			switch test {
			case "work-only":
				doc["requested"] = map[string]any{"work_tokens": 100}
				valid = true
			case "spend-only":
				doc["requested"] = map[string]any{"spend_tokens": 100}
			case "time-only":
				doc["requested"] = map[string]any{"work_tokens": 1}
				valid = true
			case "no-amount":
				doc["requested"] = map[string]any{}
			case "negative":
				doc["requested"] = map[string]any{"work_tokens": -1}
			case "large-integer":
				doc["requested"] = map[string]any{"work_tokens": int64(9007199254740993)}
				valid = true
			case "source-result":
				doc["based_on"].(map[string]any)["source_result_ref"] = map[string]any{"result_id": "res-c002-r004"}
			case "target-attempt":
				doc["target"].(map[string]any)["attempt_id"] = "other"
			case "target-role":
				doc["target"].(map[string]any)["role"] = "REVIEWER"
				doc["target"].(map[string]any)["purpose"] = "REVIEW"
			case "future-invocation":
				doc["target"].(map[string]any)["invocation_id"] = "future"
			case "grant":
				doc["grant_id"] = "not-a-request"
			}
			err := s.AcceptResourceRequest(orchestratorContext(), wire(t, doc), options)
			if (err == nil) != valid {
				t.Fatal(test, err)
			}
			if !valid && resultCount(t, s, "resource_requests") != 0 {
				t.Fatal("invalid amounts/source persisted")
			}
		})
	}
}

func TestAuthoritativeMutationRollbackAfterCancellation(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	ctx, cancel := context.WithCancel(orchestratorContext())
	err := s.authoritativeMutation(ctx, "r1", nil, func(c *sql.Conn) error {
		if _, err := c.ExecContext(ctx, "UPDATE runs SET revision=1 WHERE run_id='r1'"); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var revision int
	if err := s.db.QueryRow("SELECT revision FROM runs WHERE run_id='r1'").Scan(&revision); err != nil || revision != 0 {
		t.Fatal("cancelled transaction leaked", revision, err)
	}
	if err := s.authoritativeMutation(orchestratorContext(), "r1", nil, func(c *sql.Conn) error {
		_, err := c.ExecContext(orchestratorContext(), "UPDATE runs SET revision=2 WHERE run_id='r1'")
		return err
	}); err != nil {
		t.Fatal(fmt.Errorf("connection not reusable: %w", err))
	}
}

func TestAuthoritativeMutationLocksBeforeValidation(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	ctx := orchestratorContext()
	// A deferred driver cannot provide the lock implicitly for the helper.
	db, err := sql.Open("sqlite", s.Info().Path+"?_txlock=deferred&_pragma=busy_timeout(0)&_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	contender := &StateStore{db: db}
	writer, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	called := false
	err = contender.authoritativeMutation(ctx, "r1", nil, func(c *sql.Conn) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "begin immediate authoritative mutation") || called {
		t.Fatal("validation ran without acquiring the writer lock", called, err)
	}
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := contender.authoritativeMutation(ctx, "r1", nil, func(c *sql.Conn) error {
		_, err := c.ExecContext(ctx, "UPDATE runs SET revision=revision+1 WHERE run_id='r1'")
		return err
	}); err != nil {
		t.Fatal("failed to recover after lock release", err)
	}
}

func TestConcurrentLeadAcceptanceHasOneRevisionWinner(t *testing.T) {
	s := testStore(t)
	state, options := semanticFixture(t, s)
	options.Audit = fixtureAudit()
	ctx := orchestratorContext()
	if err := s.AcceptLeadState(ctx, wire(t, state), options); err != nil {
		t.Fatal(err)
	}
	options.ExpectedSemanticRevision = 1
	results := make(chan error, 2)
	start := make(chan struct{})
	for _, id := range []string{"lead-2", "lead-3"} {
		doc := cloneDoc(t, state)
		doc["lead_state_id"] = id
		raw := wire(t, doc)
		go func() {
			<-start
			results <- s.AcceptLeadState(ctx, raw, options)
		}()
	}
	close(start)
	winners := 0
	for range 2 {
		if err := <-results; err == nil {
			winners++
		}
	}
	current, err := s.GetCurrentSemanticState(ctx, "run1")
	if err != nil || current.Revision != 2 || winners != 1 || resultCount(t, s, "lead_states") != 2 || resultCount(t, s, "events") != 2 {
		t.Fatal("concurrent revision protection failed", winners, current, err)
	}
}

func TestIntentRejectsSupersededAndForeignSnapshots(t *testing.T) {
	for _, kind := range []string{"action", "request"} {
		for _, binding := range []string{"superseded", "foreign"} {
			t.Run(kind+"/"+binding, func(t *testing.T) {
				s := testStore(t)
				state, options := semanticFixture(t, s)
				ctx := orchestratorContext()
				if err := s.AcceptLeadState(ctx, wire(t, state), options); err != nil {
					t.Fatal(err)
				}
				doc := actionFixture(state)
				if kind == "request" {
					doc = requestFixture(state)
				}
				options.ExpectedSemanticRevision = 1
				if binding == "superseded" {
					newState := cloneDoc(t, state)
					newState["lead_state_id"] = "lead-2"
					if err := s.AcceptLeadState(ctx, wire(t, newState), options); err != nil {
						t.Fatal(err)
					}
					// Even a fresh revision cannot authorize intent against old state.
					options.ExpectedSemanticRevision = 2
				} else {
					execSQL(t, s.db, "INSERT INTO runs VALUES ('foreign-run','CREATED',2,0,'now','now',NULL)")
					execSQL(t, s.db, "INSERT INTO cycles VALUES ('foreign-run',2,'active','now',NULL,0)")
					invocation := seedLeadProducer(t, s, "foreign-run", 2)
					execSQL(t, s.db, "INSERT INTO lead_states VALUES ('foreign-state','foreign-run',2,?,'active','not_run','not_run','foreign goal',0,'now')", invocation)
					if kind == "action" {
						doc["based_on_lead_state"] = map[string]any{"lead_state_id": "foreign-state"}
					} else {
						doc["based_on"].(map[string]any)["lead_state_ref"] = map[string]any{"lead_state_id": "foreign-state"}
					}
				}
				options.Audit = fixtureAudit()
				var err error
				if kind == "action" {
					err = s.AcceptNextAction(ctx, wire(t, doc), options)
				} else {
					err = s.AcceptResourceRequest(ctx, wire(t, doc), options)
				}
				if err == nil || resultCount(t, s, "events") != 0 || resultCount(t, s, "next_actions") != 0 || resultCount(t, s, "resource_requests") != 0 {
					t.Fatal("invalid snapshot binding accepted", err)
				}
			})
		}
	}
}

func TestRequestCanTargetFinalizationWithinExistingAttempt(t *testing.T) {
	s := testStore(t)
	state, options := semanticFixture(t, s)
	var resultDoc map[string]any
	if err := json.Unmarshal(resultFixture(t, "reviewer-inconclusive"), &resultDoc); err != nil {
		t.Fatal(err)
	}
	resultDoc["result_id"], resultDoc["result_ordinal"] = "res-c002-r004", 4
	resultDoc["execution_id"], resultDoc["attempt_id"], resultDoc["invocation_id"] = "review-ex", "review-at", "review-iv"
	raw := wire(t, resultDoc)
	r, err := roleresult.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	seedResultBinding(t, s, r, 3)
	// The execution's original purpose is REVIEW, while this bounded invocation
	// is FINALIZE_REVIEW. Both retain the same explicit execution/attempt binding.
	execSQL(t, s.db, "UPDATE invocations SET purpose='FINALIZE_REVIEW' WHERE invocation_id='review-iv'")
	ctx := orchestratorContext()
	if err := s.AcceptRoleResult(ctx, raw, nil, ResultFence{3, 3}); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch)
 SELECT 'return-lead',attempt_id,execution_id,run_id,cycle_no,2,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,4 FROM invocations WHERE invocation_id='run1-lead'`)
	if err := s.BindActiveInvocation(ctx, "run1", InvocationBinding{"return-lead", "run1-lead-ex", "run1-lead-at", 2, "LEAD", r.ContentIdentity}, OwnershipOptions{
		Expected: SemanticFence{Role: "LEAD", OwnershipEpoch: 4, ActorRevision: 4}, ChangedAt: semanticTimestamp,
	}); err != nil {
		t.Fatal(err)
	}
	state["producer"].(map[string]any)["invocation_id"] = "return-lead"
	state["producer"].(map[string]any)["invocation"] = 2
	options.Fence = ResultFence{4, 5}
	state["review"] = map[string]any{"status": "inconclusive", "result_ref": map[string]any{"result_id": r.ResultID}}
	if err := s.AcceptLeadState(ctx, wire(t, state), options); err != nil {
		t.Fatal(err)
	}
	options.ExpectedSemanticRevision = 1
	request := requestFixture(state)
	request["target"] = map[string]any{"execution_id": r.ExecutionID, "attempt_id": r.AttemptID, "role": "REVIEWER", "purpose": "FINALIZE_REVIEW", "content_identity": r.ContentIdentity}
	request["source_invocation_id"] = r.InvocationID
	request["based_on"].(map[string]any)["source_result_ref"] = map[string]any{"result_id": r.ResultID}
	if err := s.AcceptResourceRequest(ctx, wire(t, request), options); err != nil {
		t.Fatal(err)
	}
	if resultCount(t, s, "attempts") != 3 || resultCount(t, s, "invocations") != 4 {
		t.Fatal("request created new work")
	}
}

func TestSemanticSelectionDoesNotInferHistoricalSnapshot(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	dir := privateTempDir(t)
	ctx := orchestratorContext()
	s, err := openTestMigrations(ctx, dir, migrations)
	if err != nil {
		t.Fatal(err)
	}
	seedInvocation(t, s)
	seedLeadProducer(t, s, "r1", 1)
	execSQL(t, s.db, "INSERT INTO lead_states VALUES ('historical','r1',1,'r1-lead','active','not_run','not_run','historical goal',0,'now')")
	var oldHistory string
	if err := s.db.QueryRow("SELECT group_concat(checksum,'|') FROM (SELECT checksum FROM schema_migrations ORDER BY version)").Scan(&oldHistory); err != nil {
		t.Fatal(err)
	}
	s.Close()
	for restart := 0; restart < 2; restart++ {
		s, err = openTest(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		if s.Info().SchemaVersion != 1 {
			t.Fatal(s.Info())
		}
		current, err := s.GetCurrentSemanticState(ctx, "r1")
		if err != nil || current.LeadStateID != nil || current.Revision != 0 {
			t.Fatal("restart inferred historical current state", current, err)
		}
		var history string
		if err := s.db.QueryRow("SELECT group_concat(checksum,'|') FROM (SELECT checksum FROM schema_migrations ORDER BY version)").Scan(&history); err != nil || history != oldHistory {
			t.Fatal("old history changed", err)
		}
		if resultCount(t, s, "lead_states") != 1 {
			t.Fatal("history lost")
		}
		execSQL(t, s.db, "INSERT OR IGNORE INTO runs VALUES ('r2','CREATED',1,0,'now','now',NULL)")
		if _, err := s.db.Exec("UPDATE run_current_semantic_state SET lead_state_id='historical' WHERE run_id='r2'"); err == nil {
			t.Fatal("cross-run Lead pointer accepted")
		}
		if err := validateDatabase(ctx, s.db); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
}
