package statestore

import (
	"encoding/json"
	"os"
	"testing"

	"marc-runtime/src/internal/roleresult"
)

func resultFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../../schemas/testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func seedResultBinding(t *testing.T, s *StateStore, r roleresult.Result, epoch int) {
	t.Helper()
	seedResultInvocation(t, s, r, epoch)
	execSQL(t, s.db, `INSERT INTO run_active_actor VALUES (?,?,?,?,'now',0) ON CONFLICT(run_id) DO UPDATE SET active_role=excluded.active_role,active_invocation_id=excluded.active_invocation_id,ownership_epoch=excluded.ownership_epoch,revision=revision+1`, r.RunID, r.Role, r.InvocationID, epoch)
}
func seedResultInvocation(t *testing.T, s *StateStore, r roleresult.Result, epoch int) {
	t.Helper()
	purpose := map[string]string{"DEVELOPER": "IMPLEMENT", "QA": "QA", "REVIEWER": "REVIEW"}[r.Role]
	execSQL(t, s.db, "INSERT OR IGNORE INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES (?,'RUNNING',?,'now','now')", r.RunID, r.Cycle)
	execSQL(t, s.db, "INSERT OR IGNORE INTO cycles VALUES (?,?,'RUNNING','now',NULL,0)", r.RunID, r.Cycle)
	execSQL(t, s.db, "INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES (?,?,?,?,?,'fixture','now')", r.ExecutionID, r.RunID, r.Cycle, r.Role, purpose)
	execSQL(t, s.db, "INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES (?,?,?,?,1,'fixture','now')", r.AttemptID, r.ExecutionID, r.RunID, r.Cycle)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch) VALUES (?,?,?,?,?,1,?,?,'fake','fake','high','fixture','now',?,?)`, r.InvocationID, r.AttemptID, r.ExecutionID, r.RunID, r.Cycle, r.Role, purpose, r.ContentIdentity, epoch)
	execSQL(t, s.db, `INSERT OR IGNORE INTO evidence_artifacts(evidence_id,kind,storage_kind,sha256,size_bytes,inline_blob,created_at) VALUES ('proof','test_result','sqlite','e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',0,X'','now')`)
}
func newFinding(r roleresult.Result) roleresult.Finding {
	return roleresult.Finding{FindingID: "fnd-000005", FindingOrdinal: 5, OriginCycle: r.Cycle, OriginResultID: r.ResultID, Blocking: true, Summary: "Stable semantic issue."}
}
func resultCount(t *testing.T, s *StateStore, table string) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func TestRoleResultAcceptanceMapping(t *testing.T) {
	for _, name := range []string{"developer-completed", "developer-incomplete", "developer-blocked", "qa-passed", "qa-failed", "qa-inconclusive", "qa-not-run", "reviewer-approved", "reviewer-changes-required", "reviewer-inconclusive"} {
		t.Run(name, func(t *testing.T) {
			s := testStore(t)
			raw := resultFixture(t, name)
			r, err := roleresult.Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			seedResultBinding(t, s, r, 1)
			var definitions []roleresult.Finding
			if len(r.FindingOccurrences) > 0 {
				definitions = []roleresult.Finding{newFinding(r)}
			}
			if err := s.AcceptRoleResult(orchestratorContext(), raw, definitions, ResultFence{1, 0}); err != nil {
				t.Fatal(err)
			}
			if resultCount(t, s, "role_results") != 1 {
				t.Fatal("missing result")
			}
			requireOwner(t, s, r.RunID, "LEAD", nil, 2, 1)
			var key, outcome, invocation string
			if err := s.db.QueryRow("SELECT result_id,outcome,invocation_id FROM role_results").Scan(&key, &outcome, &invocation); err != nil {
				t.Fatal(err)
			}
			if key != ResultKey(r.RunID, r.ResultID) || outcome != r.Outcome || invocation != r.InvocationID {
				t.Fatal("incorrect envelope mapping")
			}
			counts := map[string]int{"developer_remaining_work": len(r.RemainingWork), "qa_checks": len(r.Checks), "role_result_evidence": len(r.EvidenceRefs), "finding_occurrences": len(r.FindingOccurrences), "findings": len(definitions)}
			for table, want := range counts {
				if got := resultCount(t, s, table); got != want {
					t.Fatalf("%s=%d want %d", table, got, want)
				}
			}
			if r.Blocker != nil {
				if resultCount(t, s, "developer_blockers") != 1 || resultCount(t, s, "developer_blocker_evidence") != len(r.Blocker.EvidenceRefs) {
					t.Fatal("missing blocker mapping")
				}
			}
			if len(r.Checks) > 0 {
				if resultCount(t, s, "qa_check_evidence") != len(r.Checks[0].EvidenceRefs) {
					t.Fatal("missing check evidence")
				}
			}
			if len(r.FindingOccurrences) > 0 {
				o := r.FindingOccurrences[0]
				if resultCount(t, s, "finding_occurrence_evidence") != len(o.EvidenceRefs) || resultCount(t, s, "finding_occurrence_locations") != len(o.Locations) {
					t.Fatal("missing occurrence links/locations")
				}
				var path string
				var start, end int
				if err := s.db.QueryRow("SELECT location_path,start_line,end_line FROM finding_occurrence_evidence").Scan(&path, &start, &end); err != nil {
					t.Fatal(err)
				}
				if path != "tests/example_test.go" || start != 2 || end != 4 {
					t.Fatal("lost evidence location")
				}
			}
			if err := s.AcceptRoleResult(orchestratorContext(), raw, definitions, ResultFence{1, 0}); err == nil {
				t.Fatal("duplicate invocation accepted")
			}
			if resultCount(t, s, "role_results") != 1 {
				t.Fatal("duplicate acceptance changed state")
			}
			if resultCount(t, s, "events") != 0 {
				t.Fatal("P1.6 invented event semantics")
			}
		})
	}
}
func TestSameStableFindingInLaterCycle(t *testing.T) {
	s := testStore(t)
	raw := resultFixture(t, "qa-failed")
	r, _ := roleresult.Decode(raw)
	seedResultBinding(t, s, r, 1)
	f := newFinding(r)
	if err := s.AcceptRoleResult(orchestratorContext(), raw, []roleresult.Finding{f}, ResultFence{1, 0}); err != nil {
		t.Fatal(err)
	}
	// Re-evaluation receives the known ID explicitly; origin is not rewritten.
	var doc map[string]any
	json.Unmarshal(resultFixture(t, "reviewer-changes-required"), &doc)
	doc["cycle"] = 3
	doc["result_ordinal"] = 4
	doc["result_id"] = roleresult.ResultID(3, 4)
	doc["execution_id"] = "later-execution"
	doc["attempt_id"] = "later-attempt"
	doc["invocation_id"] = "later-invocation"
	doc["content_identity"] = "later-content"
	raw, _ = json.Marshal(doc)
	later, _ := roleresult.Decode(raw)
	seedResultBinding(t, s, later, 3)
	// Reusing an ID with changed immutable fields must fail, before any result insert.
	changed := f
	changed.Summary = "changed identity evidence"
	if err := s.AcceptRoleResult(orchestratorContext(), raw, []roleresult.Finding{changed}, ResultFence{3, 2}); err == nil {
		t.Fatal("finding mutated")
	}
	if err := s.AcceptRoleResult(orchestratorContext(), raw, nil, ResultFence{3, 2}); err != nil {
		t.Fatal(err)
	}
	if resultCount(t, s, "findings") != 1 || resultCount(t, s, "finding_occurrences") != 2 {
		t.Fatal("stable finding was duplicated")
	}
	var originCycle int
	var originResult string
	if err := s.db.QueryRow("SELECT origin_cycle,origin_result_id FROM findings").Scan(&originCycle, &originResult); err != nil {
		t.Fatal(err)
	}
	if originCycle != 2 || originResult != ResultKey(r.RunID, r.ResultID) {
		t.Fatal("finding origin changed")
	}
}
func TestAcceptanceFailsClosedAndRollsBack(t *testing.T) {
	for _, failure := range []string{"epoch", "revision", "binding", "content", "missing-evidence", "evidence-kind", "aggregate", "late-sql"} {
		t.Run(failure, func(t *testing.T) {
			s := testStore(t)
			raw := resultFixture(t, "qa-failed")
			r, _ := roleresult.Decode(raw)
			seedResultBinding(t, s, r, 1)
			f := newFinding(r)
			fence := ResultFence{1, 0}
			var doc map[string]any
			json.Unmarshal(raw, &doc)
			switch failure {
			case "epoch":
				fence.OwnershipEpoch = 2
			case "revision":
				fence.ActorRevision = 1
			case "binding":
				doc["attempt_id"] = "other-attempt"
			case "content":
				doc["content_identity"] = "stale-content"
			case "missing-evidence":
				execSQL(t, s.db, "DELETE FROM evidence_artifacts")
			case "evidence-kind":
				execSQL(t, s.db, "UPDATE evidence_artifacts SET kind='log'")
			case "aggregate":
				doc["outcome"] = "passed"
			case "late-sql":
				execSQL(t, s.db, "CREATE TEMP TRIGGER reject_check_link BEFORE INSERT ON qa_check_evidence BEGIN SELECT RAISE(ABORT,'injected'); END")
			}
			raw, _ = json.Marshal(doc)
			if err := s.AcceptRoleResult(orchestratorContext(), raw, []roleresult.Finding{f}, fence); err == nil {
				t.Fatal("invalid acceptance succeeded")
			}
			for _, table := range []string{"role_results", "findings", "qa_checks", "finding_occurrences", "role_result_evidence", "finding_occurrence_locations"} {
				if resultCount(t, s, table) != 0 {
					t.Fatal("partial acceptance survived", table)
				}
			}
		})
	}
}
func TestInconclusiveResultMapsToLeadStatus(t *testing.T) {
	for _, role := range []string{"qa", "reviewer"} {
		t.Run(role, func(t *testing.T) {
			s := testStore(t)
			raw := resultFixture(t, role+"-inconclusive")
			r, _ := roleresult.Decode(raw)
			seedResultBinding(t, s, r, 1)
			if err := s.AcceptRoleResult(orchestratorContext(), raw, nil, ResultFence{1, 0}); err != nil {
				t.Fatal(err)
			}
			producer := seedLeadProducer(t, s, r.RunID, r.Cycle)
			execSQL(t, s.db, "INSERT INTO lead_states VALUES ('ls1',?,?,?,'active','passed','approved','goal',0,'now')", r.RunID, r.Cycle, producer)
			var acceptedOutcome string
			if err := s.db.QueryRow("SELECT outcome FROM role_results WHERE invocation_id=?", r.InvocationID).Scan(&acceptedOutcome); err != nil {
				t.Fatal(err)
			}
			qa, review := "passed", "approved"
			if role == "qa" {
				qa = acceptedOutcome
			} else {
				review = acceptedOutcome
			}
			execSQL(t, s.db, "UPDATE lead_states SET qa_status=?,review_status=? WHERE lead_state_id='ls1'", qa, review)
			if _, err := s.db.Exec("UPDATE lead_states SET semantic_status='approved'"); err == nil {
				t.Fatal("inconclusive authorized approval")
			}
			var gotQA, gotReview, semantic string
			if err := s.db.QueryRow("SELECT qa_status,review_status,semantic_status FROM lead_states").Scan(&gotQA, &gotReview, &semantic); err != nil {
				t.Fatal(err)
			}
			if gotQA != qa || gotReview != review || semantic != "active" {
				t.Fatal("inconclusive status lost")
			}
		})
	}
}

func TestRunScopedSemanticIdentities(t *testing.T) {
	s := testStore(t)
	for _, run := range []string{"run1", "run2"} {
		var doc map[string]any
		json.Unmarshal(resultFixture(t, "qa-failed"), &doc)
		doc["run_id"] = run
		doc["execution_id"] = run + "-execution"
		doc["attempt_id"] = run + "-attempt"
		doc["invocation_id"] = run + "-invocation"
		raw, _ := json.Marshal(doc)
		r, err := roleresult.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		seedResultBinding(t, s, r, 1)
		if err := s.AcceptRoleResult(orchestratorContext(), raw, []roleresult.Finding{newFinding(r)}, ResultFence{1, 0}); err != nil {
			t.Fatal(err)
		}
	}
	if resultCount(t, s, "role_results") != 2 || resultCount(t, s, "findings") != 2 {
		t.Fatal("run scopes collided")
	}
}

func TestOccurrenceLocationConstraints(t *testing.T) {
	s := testStore(t)
	raw := resultFixture(t, "qa-failed")
	r, _ := roleresult.Decode(raw)
	seedResultBinding(t, s, r, 1)
	if err := s.AcceptRoleResult(orchestratorContext(), raw, []roleresult.Finding{newFinding(r)}, ResultFence{1, 0}); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"INSERT INTO finding_occurrence_locations VALUES (1,3,'source',0,NULL)",
		"INSERT INTO finding_occurrence_locations VALUES (1,3,'source',NULL,1)",
		"INSERT INTO finding_occurrence_locations VALUES (1,3,'source',2,1)",
		"INSERT INTO finding_occurrence_locations VALUES (1,3,'',NULL,NULL)",
		"INSERT INTO finding_occurrence_locations VALUES (999,3,'source',NULL,NULL)",
	} {
		if _, err := s.db.Exec(statement); err == nil {
			t.Fatal("invalid independent occurrence location", statement)
		}
	}
}

func seedLeadProducer(t *testing.T, s *StateStore, run string, cycle int) string {
	t.Helper()
	id := run + "-lead"
	execSQL(t, s.db, "INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES (?,?,?,'LEAD','LEAD','fixture','now')", id+"-ex", run, cycle)
	execSQL(t, s.db, "INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES (?,?,?,?,1,'fixture','now')", id+"-at", id+"-ex", run, cycle)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch) VALUES (?,?,?,?,?,1,'LEAD','LEAD','fake','fake','high','fixture','now',1)`, id, id+"-at", id+"-ex", run, cycle)
	return id
}
