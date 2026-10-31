package statestore

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"ai-control-plane/src/internal/roleresult"
)

const ownershipTimestamp = "2026-10-05T12:10:00Z"

func stringPointer(value string) *string { return &value }

func ownershipFixture(t *testing.T, s *StateStore, name string) ([]byte, roleresult.Result, InvocationBinding, OwnershipOptions) {
	t.Helper()
	raw := resultFixture(t, name)
	r, err := roleresult.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	seedResultInvocation(t, s, r, 11)
	lead := seedLeadProducer(t, s, r.RunID, r.Cycle)
	execSQL(t, s.db, "UPDATE invocations SET ownership_epoch=10,content_identity=? WHERE invocation_id=?", r.ContentIdentity, lead)
	execSQL(t, s.db, "INSERT INTO run_active_actor VALUES (?,'LEAD',?,10,?,5)", r.RunID, lead, semanticTimestamp)
	purpose := map[string]string{"DEVELOPER": "IMPLEMENT", "QA": "QA", "REVIEWER": "REVIEW"}[r.Role]
	target := InvocationBinding{r.InvocationID, r.ExecutionID, r.AttemptID, r.Cycle, purpose, r.ContentIdentity}
	options := OwnershipOptions{Expected: SemanticFence{Role: "LEAD", InvocationID: &lead, OwnershipEpoch: 10, ActorRevision: 5, ContentIdentity: &r.ContentIdentity}, ChangedAt: ownershipTimestamp, Audit: fixtureAudit()}
	return raw, r, target, options
}

func ownerOptions(actor ActiveActor) OwnershipOptions {
	audit := fixtureAudit()
	audit[0].Cycle = nil
	return OwnershipOptions{Expected: SemanticFence{Role: actor.Role, InvocationID: actor.InvocationID, OwnershipEpoch: actor.OwnershipEpoch, ActorRevision: actor.Revision}, ChangedAt: ownershipTimestamp, Audit: audit}
}

func requireOwner(t *testing.T, s *StateStore, run, role string, invocation *string, epoch, revision int64) ActiveActor {
	t.Helper()
	actor, err := s.GetActiveActor(context.Background(), run)
	if err != nil || actor.Role != role || !sameOptionalString(actor.InvocationID, invocation) || actor.OwnershipEpoch != epoch || actor.Revision != revision {
		t.Fatalf("actor=%+v, invocation=%v, err=%v; want %s epoch=%d revision=%d", actor, actor.InvocationID, err, role, epoch, revision)
	}
	return actor
}

func TestOwnershipTransferAndExplicitReturn(t *testing.T) {
	for _, name := range []string{"developer-completed", "qa-passed", "reviewer-approved"} {
		t.Run(name, func(t *testing.T) {
			s := testStore(t)
			_, r, target, options := ownershipFixture(t, s, name)
			ctx := orchestratorContext()
			if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options); err != nil {
				t.Fatal(err)
			}
			worker := requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6)
			if worker.ActivatedAt != ownershipTimestamp || resultCount(t, s, "events") != 1 {
				t.Fatal("transfer did not commit activation and audit")
			}
			if err := s.TransferSemanticOwnership(ctx, r.RunID, "LEAD", nil, ownerOptions(worker)); err != nil {
				t.Fatal(err)
			}
			requireOwner(t, s, r.RunID, "LEAD", nil, 12, 7)
			if resultCount(t, s, "events") != 2 || resultCount(t, s, "role_results") != 0 {
				t.Fatal("transfer invented semantic evidence")
			}
		})
	}
}

func TestSameOwnerBindAndClearPreserveEpoch(t *testing.T) {
	s := testStore(t)
	_, r, target, options := ownershipFixture(t, s, "developer-completed")
	ctx := orchestratorContext()
	if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch)
 SELECT 'continuation',attempt_id,execution_id,run_id,cycle_no,2,role,'FINALIZE_IMPLEMENTATION',provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch FROM invocations WHERE invocation_id=?`, r.InvocationID)
	next := target
	next.InvocationID, next.Purpose = "continuation", "FINALIZE_IMPLEMENTATION"
	worker := requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6)
	bind := ownerOptions(worker)
	bind.ChangedAt = "2026-10-05T12:20:00Z"
	if err := s.BindActiveInvocation(ctx, r.RunID, next, bind); err != nil {
		t.Fatal(err)
	}
	bound := requireOwner(t, s, r.RunID, r.Role, &next.InvocationID, 11, 7)
	if bound.ActivatedAt != worker.ActivatedAt {
		t.Fatal("binding reset owner activation time")
	}
	if err := s.BindActiveInvocation(ctx, r.RunID, target, bind); err == nil {
		t.Fatal("stale binding revision accepted")
	}
	if err := s.ClearActiveInvocation(ctx, r.RunID, ownerOptions(bound)); err != nil {
		t.Fatal(err)
	}
	clear := requireOwner(t, s, r.RunID, r.Role, nil, 11, 8)
	if err := s.BindActiveInvocation(ctx, r.RunID, target, ownerOptions(clear)); err != nil {
		t.Fatal(err)
	}
	requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 9)
	if resultCount(t, s, "events") != 4 || resultCount(t, s, "invocations") != 3 || countSchemaObject(t, s, "resource_reservations") != 0 {
		t.Fatal("same-owner operations changed work/resources or stale operation appended audit")
	}
}

func TestOwnershipTransferRejectsInvalidBindings(t *testing.T) {
	for _, failure := range []string{"run", "epoch", "revision", "role", "expected-invocation", "expected-null", "expected-content", "target-missing", "target-cross-run", "target-role", "target-execution", "target-attempt", "target-cycle", "target-purpose", "target-content", "target-epoch", "worker-null", "same-owner", "orchestrator", "timestamp"} {
		t.Run(failure, func(t *testing.T) {
			s := testStore(t)
			_, r, target, options := ownershipFixture(t, s, "developer-completed")
			before, err := s.GetActiveActor(context.Background(), r.RunID)
			if err != nil {
				t.Fatal(err)
			}
			run, role := r.RunID, r.Role
			var binding *InvocationBinding = &target
			switch failure {
			case "run":
				run = "missing-run"
			case "epoch":
				options.Expected.OwnershipEpoch = 9
			case "revision":
				options.Expected.ActorRevision = 4
			case "role":
				options.Expected.Role = "QA"
			case "expected-invocation":
				options.Expected.InvocationID = &r.InvocationID
			case "expected-null":
				options.Expected.InvocationID = nil
			case "expected-content":
				options.Expected.ContentIdentity = stringPointer("old-content")
			case "target-missing":
				target.InvocationID = "missing-invocation"
			case "target-cross-run":
				foreign := r
				foreign.RunID, foreign.ExecutionID, foreign.AttemptID, foreign.InvocationID = "foreign", "foreign-ex", "foreign-at", "foreign-iv"
				seedResultInvocation(t, s, foreign, 11)
				target.InvocationID, target.ExecutionID, target.AttemptID = foreign.InvocationID, foreign.ExecutionID, foreign.AttemptID
			case "target-role":
				execSQL(t, s.db, "UPDATE invocations SET role='QA' WHERE invocation_id=?", r.InvocationID)
			case "target-execution":
				target.ExecutionID = "wrong-execution"
			case "target-attempt":
				target.AttemptID = "wrong-attempt"
			case "target-cycle":
				target.Cycle = 3
			case "target-purpose":
				target.Purpose = "RECOVER"
			case "target-content":
				target.ContentIdentity = "different-content"
			case "target-epoch":
				execSQL(t, s.db, "UPDATE invocations SET ownership_epoch=10 WHERE invocation_id=?", r.InvocationID)
			case "worker-null":
				binding = nil
			case "same-owner":
				role = "LEAD"
			case "orchestrator":
				role = "ORCHESTRATOR"
			case "timestamp":
				options.ChangedAt = "invalid"
			}
			if err := s.TransferSemanticOwnership(orchestratorContext(), run, role, binding, options); err == nil {
				t.Fatal("invalid transfer accepted")
			}
			after, err := s.GetActiveActor(context.Background(), r.RunID)
			if err != nil || !reflect.DeepEqual(before, after) || resultCount(t, s, "events") != 0 {
				t.Fatal("invalid transfer changed state/audit", after, err)
			}
		})
	}
}

func TestSameOwnerBindingRejectsInvalidTarget(t *testing.T) {
	for _, failure := range []string{"cross-run", "role", "epoch", "content", "execution", "attempt", "cycle", "purpose"} {
		t.Run(failure, func(t *testing.T) {
			s := testStore(t)
			_, r, target, options := ownershipFixture(t, s, "developer-completed")
			ctx := orchestratorContext()
			if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options); err != nil {
				t.Fatal(err)
			}
			execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch)
 SELECT 'new-binding',attempt_id,execution_id,run_id,cycle_no,2,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch FROM invocations WHERE invocation_id=?`, r.InvocationID)
			target.InvocationID = "new-binding"
			actor := requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6)
			switch failure {
			case "cross-run":
				foreign := r
				foreign.RunID, foreign.ExecutionID, foreign.AttemptID, foreign.InvocationID = "foreign", "foreign-ex", "foreign-at", "foreign-iv"
				seedResultInvocation(t, s, foreign, 11)
				target.InvocationID, target.ExecutionID, target.AttemptID = foreign.InvocationID, foreign.ExecutionID, foreign.AttemptID
			case "role":
				execSQL(t, s.db, "UPDATE invocations SET role='QA' WHERE invocation_id='new-binding'")
			case "epoch":
				execSQL(t, s.db, "UPDATE invocations SET ownership_epoch=10 WHERE invocation_id='new-binding'")
			case "content":
				target.ContentIdentity = "wrong-content"
			case "execution":
				target.ExecutionID = "wrong-execution"
			case "attempt":
				target.AttemptID = "wrong-attempt"
			case "cycle":
				target.Cycle = 3
			case "purpose":
				target.Purpose = "REVIEW"
			}
			if err := s.BindActiveInvocation(ctx, r.RunID, target, ownerOptions(actor)); err == nil {
				t.Fatal("invalid same-owner invocation accepted")
			}
			requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6)
			if resultCount(t, s, "events") != 1 {
				t.Fatal("invalid binding appended audit")
			}
		})
	}
}

func TestResultAcceptanceReturnsEverySemanticOutcomeAtomically(t *testing.T) {
	for _, name := range []string{"developer-completed", "developer-incomplete", "developer-blocked", "qa-passed", "qa-failed", "qa-inconclusive", "qa-not-run", "reviewer-approved", "reviewer-changes-required", "reviewer-inconclusive"} {
		t.Run(name, func(t *testing.T) {
			s := testStore(t)
			raw, r, target, options := ownershipFixture(t, s, name)
			ctx := orchestratorContext()
			if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options); err != nil {
				t.Fatal(err)
			}
			var definitions []roleresult.Finding
			if len(r.FindingOccurrences) != 0 {
				definitions = []roleresult.Finding{newFinding(r)}
			}
			resultOptions := ResultReturnOptions{Fence: ResultFence{11, 6}, ReturnedAt: ownershipTimestamp, Audit: fixtureAudit()}
			if err := s.AcceptRoleResultAndReturnToLead(ctx, raw, definitions, resultOptions); err != nil {
				t.Fatal(err)
			}
			requireOwner(t, s, r.RunID, "LEAD", nil, 12, 7)
			if resultCount(t, s, "role_results") != 1 || resultCount(t, s, "events") != 2 {
				t.Fatal("result/ownership/audit did not commit together")
			}
			for table, want := range map[string]int{"qa_checks": len(r.Checks), "developer_remaining_work": len(r.RemainingWork), "finding_occurrences": len(r.FindingOccurrences), "findings": len(definitions), "role_result_evidence": len(r.EvidenceRefs)} {
				if resultCount(t, s, table) != want {
					t.Fatal("P1.6 mapping weakened", table)
				}
			}
			if err := s.AcceptRoleResultAndReturnToLead(ctx, raw, definitions, resultOptions); err == nil {
				t.Fatal("late Worker result accepted")
			}
			if err := s.AcceptRoleResult(ctx, raw, definitions, resultOptions.Fence); err == nil {
				t.Fatal("legacy entry point accepted late Worker")
			}
			if resultCount(t, s, "events") != 2 || resultCount(t, s, "role_results") != 1 {
				t.Fatal("late Worker wrote state or audit")
			}
			// Reopening cannot restore Worker authority alongside its accepted result.
			dir := filepath.Dir(s.Info().Path)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := openTest(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			requireOwner(t, restarted, r.RunID, "LEAD", nil, 12, 7)
			if resultCount(t, restarted, "role_results") != 1 {
				t.Fatal("accepted result missing on restart")
			}
		})
	}
}

func TestResultAndOwnershipRollbackTogether(t *testing.T) {
	for _, failure := range []string{"owner-update", "zero-row-update", "audit", "late-result-write", "evidence", "aggregate", "epoch", "revision", "timestamp"} {
		t.Run(failure, func(t *testing.T) {
			s := testStore(t)
			raw, r, target, options := ownershipFixture(t, s, "qa-failed")
			ctx := orchestratorContext()
			if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options); err != nil {
				t.Fatal(err)
			}
			resultOptions := ResultReturnOptions{Fence: ResultFence{11, 6}, ReturnedAt: ownershipTimestamp, Audit: fixtureAudit()}
			switch failure {
			case "owner-update":
				execSQL(t, s.db, "CREATE TEMP TRIGGER reject_owner_return BEFORE UPDATE ON run_active_actor BEGIN SELECT RAISE(ABORT,'injected return failure'); END")
			case "zero-row-update":
				execSQL(t, s.db, "CREATE TEMP TRIGGER ignore_owner_return BEFORE UPDATE ON run_active_actor BEGIN SELECT RAISE(IGNORE); END")
			case "audit":
				resultOptions.Audit = append(resultOptions.Audit, AuditRow{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: ownershipTimestamp})
			case "late-result-write":
				execSQL(t, s.db, "CREATE TEMP TRIGGER reject_result_link BEFORE INSERT ON qa_check_evidence BEGIN SELECT RAISE(ABORT,'injected link failure'); END")
			case "evidence":
				execSQL(t, s.db, "DELETE FROM evidence_artifacts")
			case "aggregate":
				var doc map[string]any
				if err := json.Unmarshal(raw, &doc); err != nil {
					t.Fatal(err)
				}
				doc["outcome"] = "passed"
				raw = wire(t, doc)
			case "epoch":
				resultOptions.Fence.OwnershipEpoch = 10
			case "revision":
				resultOptions.Fence.ActorRevision = 5
			case "timestamp":
				resultOptions.ReturnedAt = "invalid"
			}
			if err := s.AcceptRoleResultAndReturnToLead(ctx, raw, []roleresult.Finding{newFinding(r)}, resultOptions); err == nil {
				t.Fatal("invalid atomic result acceptance succeeded")
			}
			requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6)
			for _, table := range []string{"role_results", "findings", "qa_checks", "qa_check_evidence", "finding_occurrences", "finding_occurrence_evidence", "role_result_evidence", "finding_occurrence_locations"} {
				if resultCount(t, s, table) != 0 {
					t.Fatal("partial result survived", table)
				}
			}
			if resultCount(t, s, "events") != 1 {
				t.Fatal("failed result appended audit")
			}
		})
	}
}

func TestExplicitRecoveryFencesLateWorkerAndCreatesNoResult(t *testing.T) {
	for _, unbound := range []bool{false, true} {
		t.Run(map[bool]string{false: "bound", true: "unbound"}[unbound], func(t *testing.T) {
			s := testStore(t)
			raw, r, target, options := ownershipFixture(t, s, "developer-completed")
			ctx := orchestratorContext()
			if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options); err != nil {
				t.Fatal(err)
			}
			worker := requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6)
			if unbound {
				if err := s.ClearActiveInvocation(ctx, r.RunID, ownerOptions(worker)); err != nil {
					t.Fatal(err)
				}
				worker = requireOwner(t, s, r.RunID, r.Role, nil, 11, 7)
			}
			if err := s.RecoverSemanticOwnershipToLead(ctx, r.RunID, ownerOptions(worker)); err != nil {
				t.Fatal(err)
			}
			lead := requireOwner(t, s, r.RunID, "LEAD", nil, 12, worker.Revision+1)
			if err := s.AcceptRoleResult(ctx, raw, nil, ResultFence{11, 6}); err == nil {
				t.Fatal("old Worker submission accepted after recovery")
			}
			if err := s.RecoverSemanticOwnershipToLead(ctx, r.RunID, ownerOptions(lead)); err == nil {
				t.Fatal("Lead recovered without a Worker owner")
			}
			if resultCount(t, s, "role_results") != 0 || resultCount(t, s, "invocations") != 2 || resultCount(t, s, "attempts") != 2 || countSchemaObject(t, s, "resource_reservations") != 0 {
				t.Fatal("recovery invented a result, retry or reservation")
			}
		})
	}
}

func TestRecoveryThenNewWorkerRetainsOldInvocationFence(t *testing.T) {
	s := testStore(t)
	raw, r, target, options := ownershipFixture(t, s, "developer-completed")
	ctx := orchestratorContext()
	if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options); err != nil {
		t.Fatal(err)
	}
	worker := requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6)
	if err := s.RecoverSemanticOwnershipToLead(ctx, r.RunID, ownerOptions(worker)); err != nil {
		t.Fatal(err)
	}
	lead := requireOwner(t, s, r.RunID, "LEAD", nil, 12, 7)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch)
 SELECT 'new-worker',attempt_id,execution_id,run_id,cycle_no,2,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,13 FROM invocations WHERE invocation_id=?`, r.InvocationID)
	target.InvocationID = "new-worker"
	if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, ownerOptions(lead)); err != nil {
		t.Fatal(err)
	}
	requireOwner(t, s, r.RunID, r.Role, &target.InvocationID, 13, 8)
	if err := s.AcceptRoleResult(ctx, raw, nil, ResultFence{11, 6}); err == nil {
		t.Fatal("recovered Worker regained authority through a later same-role owner")
	}
	var oldEpoch int
	if err := s.db.QueryRow("SELECT ownership_epoch FROM invocations WHERE invocation_id=?", r.InvocationID).Scan(&oldEpoch); err != nil || oldEpoch != 11 {
		t.Fatal("ownership transfer rewrote invocation provenance", oldEpoch, err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	doc["invocation_id"], doc["result_id"], doc["result_ordinal"] = target.InvocationID, "res-c002-r004", 4
	if err := s.AcceptRoleResultAndReturnToLead(ctx, wire(t, doc), nil, ResultReturnOptions{Fence: ResultFence{13, 8}, ReturnedAt: ownershipTimestamp, Audit: fixtureAudit()}); err != nil {
		t.Fatal(err)
	}
	requireOwner(t, s, r.RunID, "LEAD", nil, 14, 9)
	if resultCount(t, s, "role_results") != 1 || resultCount(t, s, "events") != 4 {
		t.Fatal("owner changes were not counted exactly once")
	}
}

func TestOwnershipMutationAuditAndStaleFenceRollback(t *testing.T) {
	for _, operation := range []string{"transfer", "bind", "clear", "recover"} {
		for _, failure := range []string{"audit", "epoch", "revision", "role", "invocation", "content"} {
			t.Run(operation+"/"+failure, func(t *testing.T) {
				s := testStore(t)
				_, r, target, options := ownershipFixture(t, s, "developer-completed")
				ctx := orchestratorContext()
				if operation != "transfer" {
					options.Audit = nil
					if err := s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options); err != nil {
						t.Fatal(err)
					}
					options = ownerOptions(requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6))
				}
				before, err := s.GetActiveActor(ctx, r.RunID)
				if err != nil {
					t.Fatal(err)
				}
				switch failure {
				case "audit":
					options.Audit = append(options.Audit, AuditRow{EventType: "invalid", ActorRole: "ORCHESTRATOR", OccurredAt: ownershipTimestamp, Details: json.RawMessage(`{`)})
				case "epoch":
					options.Expected.OwnershipEpoch--
				case "revision":
					options.Expected.ActorRevision--
				case "role":
					options.Expected.Role = "QA"
				case "invocation":
					options.Expected.InvocationID = nil
				case "content":
					options.Expected.ContentIdentity = stringPointer("stale-content")
				}
				switch operation {
				case "transfer":
					err = s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options)
				case "bind":
					err = s.BindActiveInvocation(ctx, r.RunID, target, options)
				case "clear":
					err = s.ClearActiveInvocation(ctx, r.RunID, options)
				case "recover":
					err = s.RecoverSemanticOwnershipToLead(ctx, r.RunID, options)
				}
				after, readErr := s.GetActiveActor(ctx, r.RunID)
				if err == nil || readErr != nil || !reflect.DeepEqual(before, after) || resultCount(t, s, "events") != 0 {
					t.Fatal("failed ownership mutation committed state/audit", err, after, readErr)
				}
			})
		}
	}
}

func TestConcurrentOwnershipTransferHasOneWinner(t *testing.T) {
	s := testStore(t)
	_, r, target, options := ownershipFixture(t, s, "developer-completed")
	ctx := orchestratorContext()
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			results <- s.TransferSemanticOwnership(ctx, r.RunID, r.Role, &target, options)
		}()
	}
	close(start)
	winners := 0
	for range 2 {
		if err := <-results; err == nil {
			winners++
		}
	}
	if winners != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("transfer had multiple winners", winners)
	}
	requireOwner(t, s, r.RunID, r.Role, &r.InvocationID, 11, 6)
}

func TestOwnershipCountersFailClosedInSQLite(t *testing.T) {
	s := testStore(t)
	_, r, _, _ := ownershipFixture(t, s, "developer-completed")
	for _, sql := range []string{
		"UPDATE run_active_actor SET active_invocation_id=NULL",
		"UPDATE run_active_actor SET active_invocation_id=NULL,ownership_epoch=11,revision=revision+1",
		"UPDATE run_active_actor SET active_role='QA',active_invocation_id=NULL,revision=revision+1",
		"UPDATE run_active_actor SET active_role='QA',active_invocation_id=NULL,ownership_epoch=12,revision=revision+1",
		"UPDATE run_active_actor SET active_invocation_id=NULL,revision=revision+2",
		"UPDATE run_active_actor SET active_invocation_id=NULL,revision=revision-1",
	} {
		if _, err := s.db.Exec(sql); err == nil {
			t.Fatal("invalid ownership counters accepted", sql)
		}
	}
	requireOwner(t, s, r.RunID, "LEAD", stringPointer("run1-lead"), 10, 5)
	// Every explicit mutation advances revision, including a repeated NULL clear.
	for revision := int64(5); revision < 7; revision++ {
		actor, err := s.GetActiveActor(context.Background(), r.RunID)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.ClearActiveInvocation(orchestratorContext(), r.RunID, ownerOptions(actor)); err != nil {
			t.Fatal(err)
		}
		requireOwner(t, s, r.RunID, "LEAD", nil, 10, revision+1)
	}
}
