package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"ai-control-plane/src/internal/statestore"
)

func assertOBSAncestry(t *testing.T, ctx context.Context, s *statestore.StateStore, run string) {
	t.Helper()
	w, err := s.GetWorkflow(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.ListEvents(ctx, run, 0, statestore.MaxJournalPageSize)
	if err != nil {
		t.Fatal(err)
	}
	steps := map[string]statestore.WorkflowStep{}
	counts := map[string]int{}
	for _, step := range w.Steps {
		steps[step.InvocationID] = step
	}
	for _, e := range events {
		if e.EventType != "workflow_invocation_started" && e.EventType != "workflow_invocation_finished" {
			continue
		}
		if e.InvocationID == nil || e.Cycle == nil || e.ExecutionID == nil || e.AttemptID == nil {
			t.Fatalf("incomplete invocation event ancestry: %+v", e)
		}
		step, ok := steps[*e.InvocationID]
		if !ok || e.RunID != run || e.ActorRole != "ORCHESTRATOR" || *e.Cycle != step.Cycle || *e.ExecutionID != step.ExecutionID || *e.AttemptID != step.AttemptID {
			t.Fatalf("incorrect invocation event ancestry: %+v; step=%+v", e, step)
		}
		want := step.StartedAt
		if e.EventType == "workflow_invocation_finished" {
			want = step.CompletedAt
		}
		if e.OccurredAt != want {
			t.Fatalf("event time=%s, want %s", e.OccurredAt, want)
		}
		counts[e.EventType+*e.InvocationID]++
	}
	for _, step := range w.Steps {
		if counts["workflow_invocation_started"+step.InvocationID] != 1 {
			t.Fatal("missing or duplicated start event", step.InvocationID)
		}
		want := 0
		if step.CompletedAt != "" {
			want = 1
		}
		if counts["workflow_invocation_finished"+step.InvocationID] != want {
			t.Fatal("missing or duplicated finish event", step.InvocationID)
		}
	}
}

// Query only a disposable fixture through SQLite's own descriptor management.
func assertOBSSemanticTime(t *testing.T, s *statestore.StateStore, step statestore.WorkflowStep, at string) {
	t.Helper()
	db, err := sql.Open("sqlite", s.Info().Path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queries := []string{"SELECT produced_at FROM role_results WHERE invocation_id=?"}
	if step.Role == "LEAD" {
		queries = []string{
			"SELECT created_at FROM lead_states WHERE producer_invocation_id=?",
			"SELECT a.created_at FROM next_actions a JOIN next_action_producers p USING(action_id,run_id) WHERE p.producer_invocation_id=?",
		}
	}
	for _, query := range queries {
		var got string
		if err := db.QueryRow(query, step.InvocationID).Scan(&got); err != nil || got != at {
			t.Fatalf("persisted semantic timestamp=%q, want %q: %v", got, at, err)
		}
	}
}

func TestOBSExternalSubmissionTimeRestartReplayAndRejection(t *testing.T) {
	for _, tc := range []struct{ role, cut string }{
		{"LEAD", "external_after_capture"},
		{"LEAD", "after_lead_state"},
		{"DEVELOPER", "external_after_capture"},
		{"DEVELOPER", "external_after_result"},
		{"QA", "external_after_capture"},
		{"REVIEWER", "external_after_capture"},
	} {
		t.Run(tc.role+"/"+tc.cut, func(t *testing.T) {
			role := tc.role
			var ctx context.Context
			var s *statestore.StateStore
			var r *Runtime
			var v ExternalView
			if role == "QA" || role == "REVIEWER" {
				ctx, s, r, _, v = externalReviewFixture(t, role)
			} else {
				ctx, s, r, _, v = externalFixture(t)
			}
			if role == "DEVELOPER" {
				v = submitExternal(t, ctx, r, v, "DEVELOPER", "")
				v = advanceExternal(t, ctx, r, v)
			}
			current, err := s.GetWorkflow(ctx, v.RunID)
			if err != nil {
				t.Fatal(err)
			}
			step := *current.Current
			started, err := time.Parse(time.RFC3339Nano, step.StartedAt)
			if err != nil {
				t.Fatal(err)
			}
			observed := started.Add(time.Minute)
			r.now = func() time.Time { return observed }
			in := externalInput(v)
			if role == "LEAD" {
				in.Decision = "DEVELOPER"
			} else {
				in.Outcome = map[string]string{"DEVELOPER": "completed", "QA": "passed", "REVIEWER": "approved"}[role]
				in.Evidence = "Observed the isolated fixture."
				if role == "QA" {
					in.Checks = []ExternalCheck{{"fixture", true, "passed", "Verified fixture."}}
				}
			}
			before := externalDatabase(t, s)
			for _, kind := range []string{"stale", "invalid"} {
				bad := in
				if kind == "stale" {
					bad.ActorRevision--
				} else {
					bad.Outcome = "unsupported"
				}
				if _, err := r.ExternalAdvance(ctx, v.RunID, &bad); err == nil {
					t.Fatal("rejected submission accepted", kind)
				}
				if !reflect.DeepEqual(before, externalDatabase(t, s)) {
					t.Fatal("rejected submission persisted timestamps or events", kind)
				}
			}
			interrupted := errors.New("after durable submission")
			r.hook = func(cut string) error {
				if cut == tc.cut {
					return interrupted
				}
				return nil
			}
			if _, err := r.ExternalAdvance(ctx, v.RunID, &in); !errors.Is(err, interrupted) {
				t.Fatal("capture cut not reached", err)
			}
			w, err := s.GetWorkflow(ctx, v.RunID)
			if err != nil {
				t.Fatal(err)
			}
			var output externalOutput
			if err := json.Unmarshal(w.Current.Output, &output); err != nil || output.At != observed.Format(time.RFC3339Nano) {
				t.Fatal("submission time not captured", output.At, err)
			}
			s, r = restart(t, ctx, s)
			r.now = func() time.Time { return observed.Add(time.Minute) }
			if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
				t.Fatal("resume captured submission", err)
			}
			assertOBSSemanticTime(t, s, step, output.At)
			assertOBSAncestry(t, ctx, s, v.RunID)
			before = externalDatabase(t, s)
			s, r = restart(t, ctx, s)
			r.now = func() time.Time { return observed.Add(2 * time.Minute) }
			if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
				t.Fatal("accepted replay after restart", err)
			}
			conflict := in
			conflict.Summary = "Changed replay."
			if _, err := r.ExternalAdvance(ctx, v.RunID, &conflict); err == nil {
				t.Fatal("conflicting replay accepted")
			}
			if !reflect.DeepEqual(before, externalDatabase(t, s)) {
				t.Fatal("replay changed immutable timestamps, artifacts or events")
			}
		})
	}
}

func TestOBSManagedProductionTimeAndAuditReplay(t *testing.T) {
	ctx, s, r, spec := fixture(t)
	w, err := r.Run(ctx, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range w.Steps {
		var out captured
		if err := json.Unmarshal(step.Output, &out); err != nil {
			t.Fatal(err)
		}
		var semantic struct {
			ProducedAt string `json:"produced_at"`
			State      struct {
				CreatedAt string `json:"created_at"`
			}
		}
		if err := json.Unmarshal(out.Response.Semantic, &semantic); err != nil {
			t.Fatal(err)
		}
		at := semantic.ProducedAt
		if step.Role == "LEAD" {
			at = semantic.State.CreatedAt
		}
		produced, err := time.Parse(time.RFC3339Nano, at)
		started, startErr := time.Parse(time.RFC3339Nano, step.StartedAt)
		observed, observedErr := time.Parse(time.RFC3339Nano, out.At)
		if err != nil || startErr != nil || observedErr != nil || !produced.After(started) || produced.After(observed) {
			t.Fatalf("semantic production time outside observed process interval: start=%s produced=%s release=%s observed=%s", step.StartedAt, at, out.ReleasedAt, out.At)
		}
		if step.Role == "LEAD" && step.InvocationID == w.Current.InvocationID {
			// The final approved Lead creates no NextAction.
			db, err := sql.Open("sqlite", s.Info().Path)
			if err != nil {
				t.Fatal(err)
			}
			var stored string
			err = db.QueryRow("SELECT created_at FROM lead_states WHERE producer_invocation_id=?", step.InvocationID).Scan(&stored)
			db.Close()
			if err != nil || stored != at {
				t.Fatal("final Lead production time", stored, at, err)
			}
		} else {
			assertOBSSemanticTime(t, s, step, at)
		}
	}
	assertOBSAncestry(t, ctx, s, spec.RunID)
	before := externalDatabase(t, s)
	s, r = restart(t, ctx, s)
	if _, err := r.Run(ctx, spec.RunID); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, externalDatabase(t, s)) {
		t.Fatal("managed replay changed immutable timestamps or audit")
	}
}

func TestOBSExternalLegacyLeadTimestampIsNotRewritten(t *testing.T) {
	ctx, s, r, _, v := externalFixture(t)
	w, err := s.GetWorkflow(ctx, v.RunID)
	if err != nil {
		t.Fatal(err)
	}
	in := externalInput(v)
	in.Decision = "DEVELOPER"
	// Recreate the pre-correction cut: captured submission and accepted Lead
	// state, but no NextAction yet. The historical state used invocation start.
	semantic, err := r.externalSemantic(ctx, w, in, w.Current.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := r.snapshotWorktree(ctx, w.Spec.WorktreePath)
	if err != nil {
		t.Fatal(err)
	}
	started, err := time.Parse(time.RFC3339Nano, w.Current.StartedAt)
	if err != nil {
		t.Fatal(err)
	}
	out := externalOutput{in, snap, started.Add(time.Minute).Format(time.RFC3339Nano)}
	if err := s.CaptureExternalWorkflowOutput(ctx, v.RunID, in.InvocationID, marshal(out), out.At, w.Owner, w.SemanticRevision); err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("legacy Lead state accepted")
	r.hook = func(cut string) error {
		if cut == "after_lead_state" {
			return interrupted
		}
		return nil
	}
	if err := r.acceptLead(ctx, w, captured{Response: Response{Semantic: semantic}}); !errors.Is(err, interrupted) {
		t.Fatal(err)
	}
	before := externalDatabase(t, s)
	s, r = restart(t, ctx, s)
	if _, err := r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
		t.Fatal("resume legacy partial acceptance", err)
	}
	after := externalDatabase(t, s)
	if len(before["lead_states"]) != 1 || !reflect.DeepEqual(before["lead_states"], after["lead_states"]) || !reflect.DeepEqual(before["lead_state_bindings"], after["lead_state_bindings"]) {
		t.Fatal("legacy accepted Lead history was rewritten")
	}
	assertOBSAncestry(t, ctx, s, v.RunID)
}
