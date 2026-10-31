package orchestrator

import (
	"marc-runtime/internal/identity"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"marc-runtime/src/internal/roleresult"
	"marc-runtime/src/internal/statestore"
)

func externalFixture(t *testing.T) (context.Context, *statestore.StateStore, *Runtime, string, ExternalView) {
	t.Helper()
	return externalFixtureWithContent(t, nil)
}

func externalFixtureWithContent(t *testing.T, setup func(string)) (context.Context, *statestore.StateStore, *Runtime, string, ExternalView) {
	t.Helper()
	ctx, s, r, _ := fixture(t)
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		if raw, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v %s", err, raw)
		}
	}
	git("init", "-q")
	os.WriteFile(filepath.Join(repo, "task.txt"), []byte("before\n"), 0600)
	if err := os.WriteFile(filepath.Join(repo, "source.txt"), []byte(strings.Repeat("source line\n", 100)), 0600); err != nil {
		t.Fatal(err)
	}
	if setup != nil {
		setup(repo)
	}
	git("add", ".")
	git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
	tree := filepath.Join(t.TempDir(), "worktree")
	git("worktree", "add", "--detach", tree, "HEAD")
	v, err := r.ExternalStart(ctx, ExternalStart{MCPClientName: "fixture-control", RunID: "external-run", Goal: "Change task.txt to after and independently verify it.", WorktreePath: tree, ApprovalResponse: "I approve this workflow."})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, s, r, tree, v
}
func externalInput(v ExternalView) ExternalSubmission {
	return ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Summary: "Role result verified."}
}
func submitExternal(t *testing.T, ctx context.Context, r *Runtime, v ExternalView, decision, outcome string) ExternalView {
	t.Helper()
	in := externalInput(v)
	in.Decision = decision
	in.Outcome = outcome
	if v.Step.Role != "LEAD" {
		in.Evidence = "Observed task.txt contains after; validation completed."
	}
	if v.Step.Role == "QA" {
		in.Checks = []ExternalCheck{{"task-check", true, "passed", "Read task.txt and checked exact content."}}
	}
	next, err := r.ExternalAdvance(ctx, v.RunID, &in)
	if err != nil {
		t.Fatal(err)
	}
	return next
}
func advanceExternal(t *testing.T, ctx context.Context, r *Runtime, v ExternalView) ExternalView {
	t.Helper()
	next, err := r.ExternalAdvance(ctx, v.RunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	return next
}
func finishExternal(t *testing.T, ctx context.Context, r *Runtime, tree string, v ExternalView) ExternalView {
	t.Helper()
	for _, role := range []string{"DEVELOPER", "QA", "REVIEWER"} {
		if v.Step.Role == "LEAD" {
			v = submitExternal(t, ctx, r, v, role, "")
			v = advanceExternal(t, ctx, r, v)
		}
		if v.Step.Role == "DEVELOPER" {
			if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if v.Step.Role == "QA" && len(v.AcceptedResults) > 0 {
			t.Fatal("QA received semantic claims")
		}
		outcome := map[string]string{"DEVELOPER": "completed", "QA": "passed", "REVIEWER": "approved"}[role]
		v = submitExternal(t, ctx, r, v, "", outcome)
		v = advanceExternal(t, ctx, r, v)
	}
	v = submitExternal(t, ctx, r, v, "APPROVE", "")
	v, err := r.ExternalComplete(ctx, v.RunID)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestExternalWorkflowWorktreeAndReplay(t *testing.T) {
	ctx, s, r, tree, v := externalFixture(t)
	if v.AccountingStatus != "excluded" || v.AccountingIncomplete {
		t.Fatal("external usage misreported", v)
	}
	metricsDir := t.TempDir()
	if err := os.Chmod(metricsDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.EnableMetricsProjection(ctx, v.RunID, metricsDir); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ExternalComplete(ctx, v.RunID); err == nil {
		t.Fatal("premature readiness")
	}
	in := externalInput(v)
	in.Decision = "APPROVE"
	if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
		t.Fatal("Lead bypassed QA/Reviewer")
	}
	v = finishExternal(t, ctx, r, tree, v)
	w, err := s.GetWorkflow(ctx, v.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if w.Status != "READY_FOR_HUMAN_MERGE" || len(w.Steps) != 7 || v.ContentDigest == "" {
		t.Fatal(v)
	}
	if w.AccountingStatus != "excluded" || w.AccountingIncomplete || v.AccountingStatus != "excluded" || v.AccountingIncomplete {
		t.Fatal("external readiness invented accounting", v, w)
	}
	if _, err := s.AuthorityProofRetention(ctx, v.RunID, v.Terminal); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", s.Info().Path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"provider_calls", "usage_receipts", "invocation_admissions", "run_budget_confirmations"} {
		var n int
		query := "SELECT count(*) FROM " + table + " WHERE run_id=?"
		if table == "provider_calls" || table == "usage_receipts" {
			query = "SELECT count(*) FROM " + table + " p JOIN invocations i USING(invocation_id) WHERE i.run_id=?"
		}
		if err := db.QueryRow(query, v.RunID).Scan(&n); err != nil || n != 0 {
			t.Fatal("synthetic accounting", table, n, err)
		}
	}
	events, err := s.ListEvents(ctx, v.RunID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	s, r = restart(t, ctx, s)
	if err := s.RefreshMetrics(ctx, v.RunID); err == nil {
		t.Fatal("restart silently retained configuration")
	}
	next, err := r.ExternalComplete(ctx, v.RunID)
	if err != nil || next.Terminal != v.Terminal || next.AccountingStatus != "excluded" || next.AccountingIncomplete {
		t.Fatal(next, err)
	}
	if err := os.WriteFile(filepath.Join(metricsDir, "metrics.json"), []byte("stale observational output"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.EnableMetricsProjection(ctx, v.RunID, metricsDir); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(metricsDir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	var metrics struct {
		AccountingStatus     string `json:"accounting_status"`
		AccountingIncomplete bool   `json:"accounting_incomplete"`
		Totals               struct {
			Usage struct {
				Work struct {
					Value *int64 `json:"value"`
				} `json:"work_tokens"`
			} `json:"usage"`
		} `json:"totals"`
	}
	if err := json.Unmarshal(raw, &metrics); err != nil || metrics.AccountingStatus != "excluded" || metrics.AccountingIncomplete || metrics.Totals.Usage.Work.Value != nil {
		t.Fatal("rebind invented usage or imported metrics", metrics, err)
	}
	after, err := s.ListEvents(ctx, v.RunID, 0, 1000)
	if err != nil || !reflect.DeepEqual(events, after) {
		t.Fatal("terminal replay mutated history", err)
	}
	if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("changed later\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ExternalComplete(ctx, v.RunID); err == nil {
		t.Fatal("stale worktree still authorized")
	}
}

func TestB6WorktreeSnapshotExactByteBound(t *testing.T) {
	ctx, _, r, tree, _ := externalFixture(t)
	name := "encoded-\u2028\".txt"
	for _, file := range []string{name, "empty.txt"} {
		if err := os.WriteFile(filepath.Join(tree, file), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	base, err := r.captureWorktree(ctx, tree, 0)
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(base.Files, func(f worktreeFile) bool { return f.Path == name })
	if index < 0 {
		t.Fatal("fixture file absent")
	}
	expected := func(n int) worktreeSnapshot {
		s := worktreeSnapshot{HEAD: base.HEAD, Files: slices.Clone(base.Files)}
		data := bytes.Repeat([]byte{255}, n)
		var chunks [][]byte
		for len(data) > 0 {
			size := min(len(data), 192)
			chunks = append(chunks, data[:size])
			data = data[size:]
		}
		s.Files[index].Chunks = chunks
		return s
	}
	low, high := 0, identity.MaxJSONBytes/2
	for low < high {
		mid := (low + high + 1) / 2
		if len(marshal(expected(mid))) <= identity.MaxJSONBytes/2 {
			low = mid
		} else {
			high = mid - 1
		}
	}
	if err := os.WriteFile(filepath.Join(tree, name), bytes.Repeat([]byte{255}, low), 0600); err != nil {
		t.Fatal(err)
	}
	actual, err := r.captureWorktree(ctx, tree, 0)
	if err != nil || !bytes.Equal(marshal(actual), marshal(expected(low))) {
		t.Fatal("valid boundary snapshot changed", err)
	}
	if err := os.WriteFile(filepath.Join(tree, name), bytes.Repeat([]byte{255}, low+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.captureWorktree(ctx, tree, 0); err == nil {
		t.Fatal("encoded byte bound weakened")
	}
}
func TestExternalWorkflowCaptureAndResultRestart(t *testing.T) {
	for _, cut := range []string{"external_after_capture", "external_after_result"} {
		t.Run(cut, func(t *testing.T) {
			ctx, s, r, tree, v := externalFixture(t)
			v = submitExternal(t, ctx, r, v, "DEVELOPER", "")
			v = advanceExternal(t, ctx, r, v)
			if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600); err != nil {
				t.Fatal(err)
			}
			in := externalInput(v)
			in.Outcome = "completed"
			in.Evidence = strings.Repeat("😀", 1024)
			in.Summary = strings.Repeat("界", 1024)
			stop := errors.New("lost external ACK")
			r.hook = func(stage string) error {
				if stage == cut {
					return stop
				}
				return nil
			}
			if _, err := r.ExternalAdvance(ctx, v.RunID, &in); !errors.Is(err, stop) {
				t.Fatal(err)
			}
			s, r = restart(t, ctx, s)
			v, err := r.ExternalAdvance(ctx, v.RunID, nil)
			if err != nil || !v.StepCompleted || v.Step.Role != "DEVELOPER" {
				t.Fatal(v, err)
			}
			if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
				t.Fatal("identical replay rejected", err)
			}
			in.Evidence = "Conflicting replay"
			if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
				t.Fatal("conflicting replay accepted")
			}
			v = advanceExternal(t, ctx, r, v)
			for _, role := range []string{"QA", "REVIEWER"} {
				v = submitExternal(t, ctx, r, v, role, "")
				v = advanceExternal(t, ctx, r, v)
				v = submitExternal(t, ctx, r, v, "", map[string]string{"QA": "passed", "REVIEWER": "approved"}[role])
				v = advanceExternal(t, ctx, r, v)
			}
			v = submitExternal(t, ctx, r, v, "APPROVE", "")
			if _, err := r.ExternalComplete(ctx, v.RunID); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestExternalWorkflowRejectsStaleFenceAndChangedContent(t *testing.T) {
	ctx, s, r, tree, v := externalFixture(t)
	w, _ := s.GetWorkflow(ctx, v.RunID)
	in := externalInput(v)
	in.ActorRevision++
	in.Decision = "DEVELOPER"
	if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
		t.Fatal("stale fence accepted")
	}
	after, _ := s.GetWorkflow(ctx, v.RunID)
	if !reflect.DeepEqual(w, after) {
		t.Fatal("stale rejection changed state")
	}
	v = submitExternal(t, ctx, r, v, "DEVELOPER", "")
	v = advanceExternal(t, ctx, r, v)
	os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600)
	v = submitExternal(t, ctx, r, v, "", "completed")
	v = advanceExternal(t, ctx, r, v)
	v = submitExternal(t, ctx, r, v, "QA", "")
	v = advanceExternal(t, ctx, r, v)
	in = externalInput(v)
	in.Outcome = "passed"
	in.Evidence = "QA assertion" // Missing checks cannot authorize passed.
	if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
		t.Fatal("QA passed without required checks")
	}
	os.WriteFile(filepath.Join(tree, "task.txt"), []byte("different\n"), 0600)
	in.Checks = []ExternalCheck{{"check", true, "passed", "Actual check"}}
	if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
		t.Fatal("QA approved stale content")
	}
	os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600)
	if _, err := r.ExternalAdvance(context.Background(), v.RunID, &in); err == nil {
		t.Fatal("MCP caller acquired mutation authority")
	}
}

func TestExternalWorkflowNegativeResultAndOriginalDeadline(t *testing.T) {
	ctx, s, r, tree, v := externalFixture(t)
	start := ExternalStart{MCPClientName: "fixture-control", RunID: v.RunID, Goal: v.Goal, WorktreePath: tree, ApprovalResponse: "I approve this workflow."}
	v = submitExternal(t, ctx, r, v, "DEVELOPER", "")
	// Starting again must inspect, not consume the finished Lead action.
	same, err := r.ExternalStart(ctx, start)
	if err != nil || same.Step.InvocationID != v.Step.InvocationID || !same.StepCompleted {
		t.Fatal("start replay advanced", same, err)
	}
	v = advanceExternal(t, ctx, r, v)
	in := externalInput(v)
	in.Outcome = "incomplete"
	in.Evidence = "Task remains unfinished."
	in.RemainingWork = []string{"Finish task.txt change."}
	v, err = r.ExternalAdvance(ctx, v.RunID, &in)
	if err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	bad := externalInput(v)
	bad.Decision = "QA"
	if _, err := r.ExternalAdvance(ctx, v.RunID, &bad); err == nil {
		t.Fatal("incomplete Developer authorized QA")
	}
	v = submitExternal(t, ctx, r, v, "BLOCK", "")
	v = advanceExternal(t, ctx, r, v)
	if v.Status != "BLOCKED" {
		t.Fatal(v)
	}
	if _, err := r.ExternalComplete(ctx, v.RunID); err == nil {
		t.Fatal("blocked result became ready")
	}
	w, err := s.GetWorkflow(ctx, v.RunID)
	if err != nil || w.Owner.InvocationID != nil {
		t.Fatal("blocked ownership retained", err)
	}
	ctx, _, r, _, v = externalFixture(t)
	w, err = r.store.GetWorkflow(ctx, v.RunID)
	if err != nil {
		t.Fatal(err)
	}
	expiry, err := time.Parse(time.RFC3339Nano, w.Spec.HardDeadline)
	if err != nil {
		t.Fatal(err)
	}
	v = submitExternal(t, ctx, r, v, "DEVELOPER", "")
	r.now = func() time.Time { return expiry }
	if _, err := r.ExternalAdvance(ctx, v.RunID, nil); err == nil {
		t.Fatal("new external role after original deadline")
	}
}

func TestExternalWorkflowRejectsUnsafeOrOversizedSnapshot(t *testing.T) {
	for _, kind := range []string{"symlink", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			ctx, s, r, tree, v := externalFixture(t)
			v = submitExternal(t, ctx, r, v, "DEVELOPER", "")
			v = advanceExternal(t, ctx, r, v)
			before, err := s.GetWorkflow(ctx, v.RunID)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(tree, "untrusted-file")
			if kind == "symlink" {
				err = os.Symlink(filepath.Join(tree, "task.txt"), path)
			} else {
				err = os.WriteFile(path, nil, 0600)
				if err == nil {
					err = os.Truncate(path, maxWorktreeFileBytes+1)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			in := externalInput(v)
			in.Outcome = "completed"
			in.Evidence = "Developer submission."
			if _, err = r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
				t.Fatal("unsafe snapshot accepted")
			}
			after, err := s.GetWorkflow(ctx, v.RunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("snapshot rejection changed workflow", err)
			}
		})
	}
}

func externalReviewFixture(t *testing.T, role string) (context.Context, *statestore.StateStore, *Runtime, string, ExternalView) {
	t.Helper()
	ctx, s, r, tree, v := externalFixture(t)
	v = submitExternal(t, ctx, r, v, "DEVELOPER", "")
	v = advanceExternal(t, ctx, r, v)
	if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600); err != nil {
		t.Fatal(err)
	}
	v = submitExternal(t, ctx, r, v, "", "completed")
	v = advanceExternal(t, ctx, r, v)
	v = submitExternal(t, ctx, r, v, "QA", "")
	v = advanceExternal(t, ctx, r, v)
	if role == "REVIEWER" {
		v = submitExternal(t, ctx, r, v, "", "passed")
		v = advanceExternal(t, ctx, r, v)
		v = submitExternal(t, ctx, r, v, "REVIEWER", "")
		v = advanceExternal(t, ctx, r, v)
	}
	return ctx, s, r, tree, v
}

func externalNegative(v ExternalView) ExternalSubmission {
	in := externalInput(v)
	in.Outcome = map[string]string{"QA": "failed", "REVIEWER": "changes_required"}[v.Step.Role]
	in.Evidence = "Confirmed a blocking issue in the frozen candidate."
	in.FindingDefinitions = []roleresult.Finding{{FindingID: "fnd-000001", FindingOrdinal: 1, OriginCycle: v.Step.Cycle, OriginResultID: v.ResultID, Blocking: true, Summary: "Confirmed blocking issue."}}
	in.FindingOccurrences = []roleresult.Occurrence{{FindingID: "fnd-000001", EvidenceRefs: []roleresult.EvidenceRef{{EvidenceID: v.EvidenceID, Kind: "test_result"}}, Locations: []roleresult.Location{{Path: "task.txt"}}}}
	if v.Step.Role == "QA" {
		in.Checks = []ExternalCheck{{"required-check", true, "failed", "Negative condition established."}}
	}
	return in
}

// Compare every table, including immutable artifacts and findings, on rejection.
func externalDatabase(t *testing.T, s *statestore.StateStore) map[string][]string {
	t.Helper()
	db, err := sql.Open("sqlite", s.Info().Path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	out := map[string][]string{}
	for _, name := range names {
		rows, err := db.Query(`SELECT * FROM "` + name + `"`)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		out[name] = []string{}
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			out[name] = append(out[name], string(marshal(values)))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		slices.Sort(out[name])
	}
	return out
}

func TestExternalNegativeFindingsRestartReplayAndBlock(t *testing.T) {
	for _, role := range []string{"QA", "REVIEWER"} {
		for _, cut := range []string{"", "external_after_capture", "external_after_result"} {
			t.Run(role+"/"+cut, func(t *testing.T) {
				ctx, s, r, _, v := externalReviewFixture(t, role)
				in := externalNegative(v)
				lost := errors.New("lost MCP acknowledgement")
				if cut != "" {
					r.hook = func(stage string) error {
						if stage == cut {
							return lost
						}
						return nil
					}
				}
				next, err := r.ExternalAdvance(ctx, v.RunID, &in)
				if cut == "" && err != nil {
					t.Fatal(err)
				}
				if cut != "" && !errors.Is(err, lost) {
					t.Fatal(err)
				}
				s, r = restart(t, ctx, s)
				if cut != "" {
					next, err = r.ExternalAdvance(ctx, v.RunID, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				if !next.StepCompleted {
					t.Fatal("negative verdict unfinished")
				}
				before := externalDatabase(t, s)
				if _, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
					t.Fatal(err)
				}
				conflict := in
				conflict.Summary = "Conflicting replay."
				if _, err = r.ExternalAdvance(ctx, v.RunID, &conflict); err == nil {
					t.Fatal("conflict accepted")
				}
				if !reflect.DeepEqual(before, externalDatabase(t, s)) {
					t.Fatal("replay changed authority")
				}
				v = advanceExternal(t, ctx, r, next)
				facts := v.AcceptedResults[len(v.AcceptedResults)-1]
				if !reflect.DeepEqual(facts.FindingDefinitions, in.FindingDefinitions) || !reflect.DeepEqual(facts.FindingOccurrences, in.FindingOccurrences) {
					t.Fatal("Lead projection lost findings", facts)
				}
				if _, err = r.ExternalComplete(ctx, v.RunID); err == nil {
					t.Fatal("negative verdict reached readiness")
				}
				bad := externalInput(v)
				bad.Decision = "APPROVE"
				if _, err = r.ExternalAdvance(ctx, v.RunID, &bad); err == nil {
					t.Fatal("Lead overrode negative result")
				}
				v = submitExternal(t, ctx, r, v, "BLOCK", "")
				v = advanceExternal(t, ctx, r, v)
				if v.Status != "BLOCKED" {
					t.Fatal(v)
				}
				w, err := s.GetWorkflow(ctx, v.RunID)
				if err != nil {
					t.Fatal(err)
				}
				var lead externalOutput
				if err = json.Unmarshal(w.Current.Output, &lead); err != nil {
					t.Fatal(err)
				}
				semantic, err := r.externalSemantic(ctx, w, lead.Submission, lead.At)
				if err != nil {
					t.Fatal(err)
				}
				var mapped struct {
					State struct {
						OpenFindings []any `json:"open_findings"`
					}
				}
				if err = json.Unmarshal(semantic, &mapped); err != nil || len(mapped.State.OpenFindings) != 1 {
					t.Fatal("blocking finding omitted", err)
				}
			})
		}
	}
}

func TestExternalInvalidFindingsRejectBeforeCapture(t *testing.T) {
	for _, role := range []string{"QA", "REVIEWER"} {
		for _, bad := range []string{"missing-occurrence", "missing-definition", "orphan-definition", "duplicate-definition", "unknown-evidence", "wrong-origin", "nonblocking", "malformed-id", "bad-location", "typed-utf8", "positive-with-blocker"} {
			t.Run(role+"/"+bad, func(t *testing.T) {
				ctx, s, r, _, v := externalReviewFixture(t, role)
				in := externalNegative(v)
				switch bad {
				case "missing-occurrence":
					in.FindingDefinitions = nil
					in.FindingOccurrences = nil
				case "missing-definition":
					in.FindingDefinitions = nil
				case "orphan-definition":
					in.FindingOccurrences = nil
				case "duplicate-definition":
					in.FindingDefinitions = append(in.FindingDefinitions, in.FindingDefinitions[0])
				case "unknown-evidence":
					in.FindingOccurrences[0].EvidenceRefs[0].EvidenceID = "missing-evidence"
				case "wrong-origin":
					in.FindingDefinitions[0].OriginResultID = "res-c001-r999"
				case "nonblocking":
					in.FindingDefinitions[0].Blocking = false
				case "malformed-id":
					in.FindingDefinitions[0].FindingID = "fnd-000002"
				case "bad-location":
					end := 2
					in.FindingOccurrences[0].Locations[0].EndLine = &end
				case "typed-utf8":
					in.FindingOccurrences[0].Locations[0].Path = string([]byte{255})
				case "positive-with-blocker":
					if role == "QA" {
						in.Outcome = "passed"
						in.Checks[0].Outcome = "passed"
					} else {
						in.Outcome = "approved"
					}
				}
				before := externalDatabase(t, s)
				if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
					t.Fatal("invalid result accepted")
				}
				if !reflect.DeepEqual(before, externalDatabase(t, s)) {
					t.Fatal("rejection changed authoritative tables")
				}
				s, r = restart(t, ctx, s)
				if _, err := r.ExternalAdvance(ctx, v.RunID, nil); err != nil {
					t.Fatal("rejection poisoned restart", err)
				}
				valid := externalNegative(v)
				if _, err := r.ExternalAdvance(ctx, v.RunID, &valid); err != nil {
					t.Fatal("corrected result rejected", err)
				}
			})
		}
	}
}

func TestExternalStagingOnlyPreservesAuthorityAndProofReplay(t *testing.T) {
	ctx, s, r, tree, v := externalFixture(t)
	v = submitExternal(t, ctx, r, v, "DEVELOPER", "")
	v = advanceExternal(t, ctx, r, v)
	if err := os.WriteFile(filepath.Join(tree, "zz-added.txt"), []byte("new file\n"), 0600); err != nil {
		t.Fatal(err)
	}
	v = submitExternal(t, ctx, r, v, "", "completed")
	digest := v.ContentDigest
	v = advanceExternal(t, ctx, r, v)
	v = submitExternal(t, ctx, r, v, "QA", "")
	v = advanceExternal(t, ctx, r, v)
	v = submitExternal(t, ctx, r, v, "", "passed")
	v = advanceExternal(t, ctx, r, v)
	v = submitExternal(t, ctx, r, v, "REVIEWER", "")
	v = advanceExternal(t, ctx, r, v)
	// Review admission already sealed with the original Git listing order.
	before := externalDatabase(t, s)
	cmd := exec.Command("git", "-C", tree, "add", "zz-added.txt")
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(raw))
	}
	s, r = restart(t, ctx, s)
	next, err := r.ExternalInspect(ctx, v.RunID)
	if err != nil || next.ContentDigest != digest {
		t.Fatal("staging invalidated unchanged content", err)
	}
	if !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("inspection/restart changed authority")
	}
	v = submitExternal(t, ctx, r, next, "", "approved")
	v = advanceExternal(t, ctx, r, v)
	v = submitExternal(t, ctx, r, v, "APPROVE", "")
	v, err = r.ExternalComplete(ctx, v.RunID)
	if err != nil {
		t.Fatal("sealed proof replay changed after staging", err)
	}
	s, r = restart(t, ctx, s)
	if _, err = r.ExternalComplete(ctx, v.RunID); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, "zz-added.txt"), []byte("changed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = r.ExternalInspect(ctx, v.RunID); err == nil {
		t.Fatal("real content change accepted")
	}
}

func TestExternalFailedCheckRemainsInconclusiveWithoutFinding(t *testing.T) {
	ctx, s, r, _, v := externalReviewFixture(t, "QA")
	in := externalInput(v)
	in.Outcome = "inconclusive"
	in.Evidence = "A required check failed; no confirmed blocker is established."
	in.Checks = []ExternalCheck{{"required-check", true, "failed", "Required condition failed."}}
	next, err := r.ExternalAdvance(ctx, v.RunID, &in)
	if err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, next)
	facts := v.AcceptedResults[len(v.AcceptedResults)-1]
	if facts.Outcome != "inconclusive" || len(facts.FindingDefinitions) != 0 || len(facts.FindingOccurrences) != 0 {
		t.Fatal("check converted into a finding/verdict", facts)
	}
	rows := externalDatabase(t, s)
	if len(rows["findings"]) != 0 || len(rows["finding_occurrences"]) != 0 {
		t.Fatal("findings invented")
	}
	v = submitExternal(t, ctx, r, v, "BLOCK", "")
	v = advanceExternal(t, ctx, r, v)
	if v.Status != "BLOCKED" {
		t.Fatal(v)
	}
}

func TestExternalExistingFindingIdentityAndNonblockingReadiness(t *testing.T) {
	ctx, s, r, _, v := externalReviewFixture(t, "QA")
	in := externalNegative(v)
	in.Outcome = "passed"
	in.Checks[0].Outcome = "passed"
	in.FindingDefinitions[0].Blocking = false
	in.FindingDefinitions[0].Summary = "Confirmed nonblocking issue."
	in.Summary = "QA passed with a nonblocking observation."
	var err error
	v, err = r.ExternalAdvance(ctx, v.RunID, &in)
	if err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	v = submitExternal(t, ctx, r, v, "REVIEWER", "")
	v = advanceExternal(t, ctx, r, v)
	// Reviewer cannot change an existing stable Finding's meaning or origin.
	bad := externalNegative(v)
	before := externalDatabase(t, s)
	if _, err = r.ExternalAdvance(ctx, v.RunID, &bad); err == nil {
		t.Fatal("immutable finding replaced")
	}
	if !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("conflicting definition changed authority")
	}
	valid := externalInput(v)
	valid.Outcome = "approved"
	valid.Evidence = "Reviewed and re-observed the same nonblocking finding."
	valid.FindingOccurrences = []roleresult.Occurrence{{FindingID: "fnd-000001", EvidenceRefs: []roleresult.EvidenceRef{{EvidenceID: v.EvidenceID, Kind: "test_result"}}}}
	v, err = r.ExternalAdvance(ctx, v.RunID, &valid)
	if err != nil {
		t.Fatal("existing finding could not be referenced", err)
	}
	v = advanceExternal(t, ctx, r, v)
	v = submitExternal(t, ctx, r, v, "APPROVE", "")
	v, err = r.ExternalComplete(ctx, v.RunID)
	if err != nil || v.Status != "READY_FOR_HUMAN_MERGE" {
		t.Fatal(v, err)
	}
	if len(externalDatabase(t, s)["findings"]) != 1 {
		t.Fatal("re-observation duplicated finding")
	}
	s, r = restart(t, ctx, s)
	if _, err = r.ExternalComplete(ctx, v.RunID); err != nil {
		t.Fatal("nonblocking proof replay failed", err)
	}
}

func TestB7RemediationResolutionRestartAndReplay(t *testing.T) {
	for _, role := range []string{"QA", "REVIEWER"} {
		t.Run(role, func(t *testing.T) {
			ctx, s, r, tree, v := externalReviewFixture(t, role)
			originalContent := v.ContentIdentity
			negative := externalNegative(v)
			var err error
			v, err = r.ExternalAdvance(ctx, v.RunID, &negative)
			if err != nil {
				t.Fatal(err)
			}
			v = advanceExternal(t, ctx, r, v)
			if v.NextDecision != "REMEDIATE" {
				t.Fatal(v.NextDecision)
			}
			in := externalInput(v)
			in.Decision = "REMEDIATE"
			in.Summary = "Repair the independently confirmed blocker."
			in.EscalationReason = "Prior implementation missed the reported boundary condition."
			before := externalDatabase(t, s)
			bad := in
			bad.FindingIDs = []string{"fnd-999999"}
			if _, err = r.ExternalAdvance(ctx, v.RunID, &bad); err == nil {
				t.Fatal("unknown finding accepted")
			}
			if !reflect.DeepEqual(before, externalDatabase(t, s)) {
				t.Fatal("invalid remediation captured state")
			}
			v, err = r.ExternalAdvance(ctx, v.RunID, &in)
			if err != nil {
				t.Fatal(err)
			}
			s, r = restart(t, ctx, s)
			stop := errors.New("lost cycle-transition acknowledgement")
			r.hook = func(stage string) error {
				if stage == "external_after_step" {
					return stop
				}
				return nil
			}
			if _, err = r.ExternalAdvance(ctx, v.RunID, nil); !errors.Is(err, stop) {
				t.Fatal(err)
			}
			s, r = restart(t, ctx, s)
			v = advanceExternal(t, ctx, r, v)
			if v.Step.Role != "DEVELOPER" || v.Step.Cycle != 2 || v.Step.Purpose != "REMEDIATE" || v.Step.Model != "GPT-6 Luna" || v.Step.ReasoningEffort != "High" || v.ContentIdentity == originalContent {
				t.Fatal(v)
			}
			candidate := v.ContentIdentity
			if err = os.WriteFile(filepath.Join(tree, "task.txt"), []byte("repaired\n"), 0600); err != nil {
				t.Fatal(err)
			}
			v = submitExternal(t, ctx, r, v, "", "completed")
			v = advanceExternal(t, ctx, r, v)
			in = externalInput(v)
			in.Decision = "QA"
			in.Summary = "Verify the repaired candidate."
			in.EscalationReason = "Revalidate the previously failing boundary on changed content."
			v, err = r.ExternalAdvance(ctx, v.RunID, &in)
			if err != nil {
				t.Fatal(err)
			}
			v = advanceExternal(t, ctx, r, v)
			if v.Step.Role != "QA" || v.Step.Model != "GPT-6.1 Sol" || v.Step.ReasoningEffort != "Medium" {
				t.Fatal(v)
			}
			qaEvidence := v.EvidenceID
			v = submitExternal(t, ctx, r, v, "", "passed")
			v = advanceExternal(t, ctx, r, v)
			in = externalInput(v)
			in.Decision = "REVIEWER"
			in.Summary = "Review the exact repaired candidate."
			if role == "REVIEWER" {
				in.EscalationReason = "Recheck the earlier review blocker on the repaired candidate."
			}
			v, err = r.ExternalAdvance(ctx, v.RunID, &in)
			if err != nil {
				t.Fatal(err)
			}
			v = advanceExternal(t, ctx, r, v)
			v = submitExternal(t, ctx, r, v, "", "approved")
			v = advanceExternal(t, ctx, r, v)
			in = externalInput(v)
			in.Decision = "APPROVE"
			in.Summary = "QA and review confirm the repaired candidate."
			in.ResolvedFindings = []ExternalResolution{{"fnd-000001", qaEvidence}}
			before = externalDatabase(t, s)
			bad = in
			bad.ResolvedFindings = []ExternalResolution{{"fnd-000001", "unaccepted-evidence"}}
			if _, err = r.ExternalAdvance(ctx, v.RunID, &bad); err == nil {
				t.Fatal("unaccepted resolution captured")
			}
			if !reflect.DeepEqual(before, externalDatabase(t, s)) {
				t.Fatal("invalid resolution mutated state")
			}
			v, err = r.ExternalAdvance(ctx, v.RunID, &in)
			if err != nil {
				t.Fatal(err)
			}
			v, err = r.ExternalComplete(ctx, v.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if v.Status != "READY_FOR_HUMAN_MERGE" || v.ContentIdentity != candidate || v.AccountingStatus != "excluded" {
				t.Fatal(v)
			}
			history := externalDatabase(t, s)
			s, r = restart(t, ctx, s)
			next, err := r.ExternalComplete(ctx, v.RunID)
			if err != nil || next.Terminal != v.Terminal {
				t.Fatal(next, err)
			}
			if !reflect.DeepEqual(history, externalDatabase(t, s)) {
				t.Fatal("restart changed retained history")
			}
			if _, err = r.ExternalAdvance(ctx, v.RunID, &negative); err != nil {
				t.Fatal("old result replay", err)
			}
		})
	}
}

func TestB7TwoEscalationsThenControlledStop(t *testing.T) {
	ctx, s, r, _, v := externalReviewFixture(t, "QA")
	var err error
	for cycle := 1; cycle <= 3; cycle++ {
		in := externalNegative(v)
		if cycle > 1 {
			in.FindingDefinitions = nil
		}
		v, err = r.ExternalAdvance(ctx, v.RunID, &in)
		if err != nil {
			t.Fatal(err)
		}
		v = advanceExternal(t, ctx, r, v)
		if cycle == 3 {
			break
		}
		in = externalInput(v)
		in.Decision = "REMEDIATE"
		in.Summary = "Repair the unresolved finding."
		in.EscalationReason = "The last candidate still fails the boundary test."
		v, err = r.ExternalAdvance(ctx, v.RunID, &in)
		if err != nil {
			t.Fatal(err)
		}
		s, r = restart(t, ctx, s)
		v = advanceExternal(t, ctx, r, v)
		if v.EscalationLevel != cycle {
			t.Fatal(v.EscalationLevel)
		}
		v = submitExternal(t, ctx, r, v, "", "completed")
		v = advanceExternal(t, ctx, r, v)
		in = externalInput(v)
		in.Decision = "QA"
		in.Summary = "Rerun the failing check."
		in.EscalationReason = "Validate the next candidate against the unresolved defect."
		v, err = r.ExternalAdvance(ctx, v.RunID, &in)
		if err != nil {
			t.Fatal(err)
		}
		v = advanceExternal(t, ctx, r, v)
	}
	if v.NextDecision != "BLOCK" {
		t.Fatal("exhaustion not reported", v)
	}
	before := externalDatabase(t, s)
	in := externalInput(v)
	in.Decision = "REMEDIATE"
	in.Summary = "Try again."
	in.EscalationReason = "Still failing."
	if _, err = r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
		t.Fatal("fourth candidate admitted")
	}
	if !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("exhausted retry changed state")
	}
	v = submitExternal(t, ctx, r, v, "BLOCK", "")
	v = advanceExternal(t, ctx, r, v)
	if v.Status != "BLOCKED" {
		t.Fatal(v)
	}
	if _, err = r.ExternalComplete(ctx, v.RunID); err == nil {
		t.Fatal("negative workflow became ready")
	}
	s, r = restart(t, ctx, s)
	v = advanceExternal(t, ctx, r, v)
	if v.Status != "BLOCKED" {
		t.Fatal("restart unblocked workflow")
	}
}

func TestB7InconclusiveEscalationLimit(t *testing.T) {
	for _, role := range []string{"QA", "REVIEWER"} {
		t.Run(role, func(t *testing.T) {
			ctx, s, r, _, v := externalReviewFixture(t, role)
			content := v.ContentIdentity
			profiles := map[string][][2]string{
				"QA":       {{"GPT-6 Luna", "High"}, {"GPT-6.1 Sol", "Medium"}, {"GPT-6.1 Sol", "High"}},
				"REVIEWER": {{"GPT-6.1 Sol", "High"}, {"GPT-6.1 Sol", "XHigh"}, {"GPT-6 Astra", "High"}},
			}
			for level := 0; level < 3; level++ {
				if v.Step.Role != role || v.Step.Model != profiles[role][level][0] || v.Step.ReasoningEffort != profiles[role][level][1] || v.ContentIdentity != content {
					t.Fatal("incorrect escalation", v)
				}
				in := externalInput(v)
				in.Outcome = "inconclusive"
				in.Evidence = "The required boundary could not be established."
				if role == "QA" {
					in.Checks = []ExternalCheck{{"required", true, "inconclusive", "Insufficient evidence."}}
				}
				var err error
				if v, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
					t.Fatal(err)
				}
				v = advanceExternal(t, ctx, r, v)
				in = externalInput(v)
				in.Decision = role
				before := externalDatabase(t, s)
				if _, err = r.ExternalAdvance(ctx, v.RunID, &in); err == nil {
					t.Fatal("unjustified or exhausted escalation accepted")
				}
				if !reflect.DeepEqual(before, externalDatabase(t, s)) {
					t.Fatal("rejection changed state")
				}
				if level == 2 {
					if v.NextDecision != "BLOCK" {
						t.Fatal("exhaustion did not require stop")
					}
					v = submitExternal(t, ctx, r, v, "BLOCK", "")
					v = advanceExternal(t, ctx, r, v)
					if v.Status != "BLOCKED" {
						t.Fatal(v.Status)
					}
					break
				}
				in.EscalationReason = "A distinct required boundary needs a focused independent check."
				if v, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
					t.Fatal(err)
				}
				s, r = restart(t, ctx, s)
				v = advanceExternal(t, ctx, r, v)
			}
		})
	}
}

func TestB7LaterOccurrenceReopensResolvedFinding(t *testing.T) {
	ctx, _, r, tree, v := externalReviewFixture(t, "QA")
	negative := externalNegative(v)
	var err error
	if v, err = r.ExternalAdvance(ctx, v.RunID, &negative); err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	in := externalInput(v)
	in.Decision, in.EscalationReason = "REMEDIATE", "Repair the confirmed task-byte defect."
	if v, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("repaired\n"), 0600); err != nil {
		t.Fatal(err)
	}
	v = submitExternal(t, ctx, r, v, "", "completed")
	v = advanceExternal(t, ctx, r, v)
	in = externalInput(v)
	in.Decision, in.EscalationReason = "QA", "Recheck the repaired candidate."
	if v, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	qaEvidence := v.EvidenceID
	v = submitExternal(t, ctx, r, v, "", "passed")
	v = advanceExternal(t, ctx, r, v)
	in = externalInput(v)
	in.Decision = "REVIEWER"
	in.ResolvedFindings = []ExternalResolution{{"fnd-000001", qaEvidence}}
	if v, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	if len(v.Findings) != 1 || v.Findings[0].ResolutionEvidenceID != qaEvidence {
		t.Fatal("explicit resolution lost", v.Findings)
	}
	in = externalNegative(v)
	in.FindingDefinitions = nil // Reobserve the QA origin; never redefine it.
	if v, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	if len(v.Findings) != 1 || v.Findings[0].ResolutionEvidenceID != "" || v.Findings[0].Definition != negative.FindingDefinitions[0] || v.Findings[0].SourceRole != "REVIEWER" || v.NextDecision != "REMEDIATE" {
		t.Fatal("later occurrence failed to reopen its stable origin", v.Findings)
	}
	in = externalInput(v)
	in.Decision, in.EscalationReason = "REMEDIATE", "The independent review reestablished the same blocker."
	if v, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
		t.Fatal(err)
	}
	v = advanceExternal(t, ctx, r, v)
	if v.Step.Cycle != 3 || len(v.FindingIDs) != 1 || v.FindingIDs[0] != "fnd-000001" {
		t.Fatal("reopened finding did not authorize focused remediation", v)
	}
}

func TestExternalLargeRepositoryManifest(t *testing.T) {
	for _, checkout := range []string{"original", "clean-worktree"} {
		t.Run(checkout, func(t *testing.T) {
			data := bytes.Repeat([]byte{0, 255, 17, 42}, 512*1024)
			var original string
			ctx, s, r, tree, v := externalFixtureWithContent(t, func(repo string) {
				original = repo
				for name, raw := range map[string][]byte{"large.bin": data, ".gitignore": []byte(".local/\n")} {
					if err := os.WriteFile(filepath.Join(repo, name), raw, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Mkdir(filepath.Join(repo, ".local"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(repo, ".local", "books.archive"), []byte("untouched archive"), 0600); err != nil {
					t.Fatal(err)
				}
			})
			if checkout == "original" {
				tree = original
				var err error
				v, err = r.ExternalStart(ctx, ExternalStart{MCPClientName: "fixture-control", RunID: "original-run", Goal: "Verify large repository.", WorktreePath: tree, ApprovalResponse: "Approved."})
				if err != nil {
					t.Fatal(err)
				}
			}
			snap, err := r.snapshotWorktree(ctx, tree)
			if err != nil {
				t.Fatal(err)
			}
			if snap.Version != 1 || len(marshal(snap)) >= maxWorktreeManifestBytes {
				t.Fatal("manifest not bounded")
			}
			i := slices.IndexFunc(snap.Files, func(f worktreeFile) bool { return f.Path == "large.bin" })
			if i < 0 || snap.Files[i].SHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) || snap.Files[i].Size != int64(len(data)) || len(snap.Files[i].Chunks) != 0 {
				t.Fatal("incomplete file identity")
			}
			for _, f := range snap.Files {
				if strings.HasPrefix(f.Path, ".local/") {
					t.Fatal("ignored archive captured")
				}
			}
			if _, err := r.captureWorktree(ctx, tree, 0); err == nil {
				t.Fatal("fixture did not exceed old bound")
			}
			v = finishExternal(t, ctx, r, tree, v)
			events, err := s.ListEvents(ctx, v.RunID, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("git", "-C", tree, "add", ".")
			if raw, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("stage unchanged content: %v %s", err, raw)
			}
			s, r = restart(t, ctx, s)
			if _, err := r.ExternalComplete(ctx, v.RunID); err != nil {
				t.Fatal("restart replay", err)
			}
			after, err := s.ListEvents(ctx, v.RunID, 0, 1000)
			if err != nil || !reflect.DeepEqual(events, after) {
				t.Fatal("replay changed history", err)
			}
			data[len(data)-1] ^= 1
			if err := os.WriteFile(filepath.Join(tree, "large.bin"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := r.ExternalComplete(ctx, v.RunID); err == nil {
				t.Fatal("changed tail beyond old read limit authorized")
			}
			raw, err := os.ReadFile(filepath.Join(original, ".local", "books.archive"))
			if err != nil || string(raw) != "untouched archive" {
				t.Fatal("archive changed", err)
			}
		})
	}
}

func TestExternalSnapshotExcludesLocalTestData(t *testing.T) {
	var original string
	ctx, _, r, tree, _ := externalFixtureWithContent(t, func(repo string) {
		original = repo
		if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte("/.local/\n"), 0600); err != nil {
			t.Fatal(err)
		}
	})
	for i, repo := range []string{original, tree} {
		if err := os.Mkdir(filepath.Join(repo, ".local"), 0700); err != nil {
			t.Fatal(err)
		}
		archive := filepath.Join(repo, ".local", "books.zip")
		if err := os.WriteFile(archive, nil, 0600); err != nil {
			t.Fatal(err)
		}
		// An ignored sparse archive must never reach the per-file read bound.
		if err := os.Truncate(archive, maxWorktreeFileBytes+1); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(archive, 0400); err != nil {
			t.Fatal(err)
		}
		before, err := os.Stat(archive)
		if err != nil {
			t.Fatal(err)
		}
		snap, err := r.snapshotWorktree(ctx, repo)
		if err != nil {
			t.Fatal("ignored local data blocked capture", err)
		}
		for _, file := range snap.Files {
			if strings.HasPrefix(file.Path, ".local/") {
				t.Fatal("local test data included in snapshot")
			}
		}
		if _, err := r.ExternalStart(ctx, ExternalStart{MCPClientName: "fixture-control", RunID: fmt.Sprintf("ignored-local-%d", i), Goal: "Check local start without reading book data.", WorktreePath: repo, ApprovalResponse: "Approved fixture."}); err != nil {
			t.Fatal(err)
		}
		after, err := os.Stat(archive)
		if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || before.Mode() != after.Mode() || !before.ModTime().Equal(after.ModTime()) {
			t.Fatal("local test data modified", err)
		}
	}
}

func TestExternalLegacySnapshotValidation(t *testing.T) {
	ctx, s, r, tree, v := externalFixture(t)
	old, err := r.captureWorktree(ctx, tree, 0)
	if err != nil {
		t.Fatal(err)
	}
	raw := marshal(old)
	if bytes.Contains(raw, []byte(`"Version"`)) || bytes.Contains(raw, []byte(`"SHA256"`)) || bytes.Contains(raw, []byte(`"Size"`)) {
		t.Fatal("legacy encoding changed")
	}
	w, err := s.GetWorkflow(ctx, v.RunID)
	if err != nil {
		t.Fatal(err)
	}
	w.Steps = []statestore.WorkflowStep{{CurrentInvocation: statestore.CurrentInvocation{Role: "DEVELOPER", ContentIdentity: &w.ContentIdentity}, Output: marshal(externalOutput{Worktree: old})}}
	if digest, err := r.frozen(ctx, w); err != nil || digest != snapshotDigest(old) {
		t.Fatal("legacy snapshot rejected", err)
	}
	if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("changed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.frozen(ctx, w); err == nil {
		t.Fatal("legacy stale bytes authorized")
	}
	if _, err := r.captureWorktree(ctx, tree, 2); err == nil {
		t.Fatal("unknown version accepted")
	}
}

func TestExternalManifestSizeBounds(t *testing.T) {
	ctx, _, r, tree, _ := externalFixture(t)
	// Sparse files exercise the aggregate read bound without large allocations.
	for i := 0; i < 5; i++ {
		path := filepath.Join(tree, fmt.Sprintf("large-%d", i))
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(path, maxWorktreeFileBytes); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.snapshotWorktree(ctx, tree); err == nil || !strings.Contains(err.Error(), "byte bound") {
		t.Fatal("aggregate bound not enforced", err)
	}
	for i := 0; i < 5; i++ {
		if err := os.Truncate(filepath.Join(tree, fmt.Sprintf("large-%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 1600; i++ {
		if i == 1400 {
			if snap, err := r.snapshotWorktree(ctx, tree); err != nil || len(marshal(snap)) > maxWorktreeManifestBytes {
				t.Fatal("valid large manifest rejected", err)
			}
		}
		name := fmt.Sprintf("%04d-%s", i, strings.Repeat("x", 235))
		if err := os.WriteFile(filepath.Join(tree, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.snapshotWorktree(ctx, tree); err == nil || !strings.Contains(err.Error(), "snapshot JSON exceeds") {
		t.Fatal("manifest bound not enforced", err)
	}
}
