package orchestrator

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExternalReviewerLeadDeadlineBoundary(t *testing.T) {
	for _, offset := range []time.Duration{-time.Nanosecond, 0, 74 * time.Minute} {
		t.Run(offset.String(), func(t *testing.T) {
			ctx, s, r, tree, v := externalReviewFixture(t, "REVIEWER")
			w, err := s.GetWorkflow(ctx, v.RunID)
			if err != nil {
				t.Fatal(err)
			}
			deadline, err := time.Parse(time.RFC3339Nano, w.Spec.HardDeadline)
			if err != nil {
				t.Fatal(err)
			}
			started, err := time.Parse(time.RFC3339Nano, w.Spec.StartedAt)
			if err != nil || deadline.Sub(started) != time.Hour {
				t.Fatal("omitted Hard window did not use the original one-hour default", err)
			}
			observed := deadline.Add(offset)
			r.now = func() time.Time { return observed }
			// Already issued work may finish after expiry; it does not issue Lead.
			in := externalInput(v)
			in.Outcome, in.Evidence = "approved", "Reviewed the unchanged candidate."
			v, err = r.ExternalAdvance(ctx, v.RunID, &in)
			if err != nil || !v.StepCompleted || v.Step.Role != "REVIEWER" {
				t.Fatal("Reviewer result was not retained", v, err)
			}
			w, err = s.GetWorkflow(ctx, v.RunID)
			if err != nil || w.Owner.Role != "LEAD" || w.Owner.InvocationID != nil || len(w.Steps) != 6 {
				t.Fatal("Worker acceptance did not return ownership to unbound Lead", w, err)
			}
			before := externalDatabase(t, s)
			s, r = restart(t, ctx, s)
			r.now = func() time.Time { return observed }
			if !reflect.DeepEqual(before, externalDatabase(t, s)) {
				t.Fatal("restart changed accepted facts")
			}
			v, err = r.ExternalInspect(ctx, v.RunID)
			if err != nil || len(v.AcceptedResults) != 3 || v.Status != "RUNNING" || v.Terminal.ProofID != "" {
				t.Fatal("inspection lost retained results", v, err)
			}
			var fields map[string]any
			if err := json.Unmarshal(marshal(v), &fields); err != nil || fields["hard_deadline"] != w.Spec.HardDeadline {
				t.Fatal("inspection omitted original deadline", fields, err)
			}
			if offset < 0 {
				v = advanceExternal(t, ctx, r, v)
				if v.Step.Role != "LEAD" {
					t.Fatal("final Lead was not issued before expiry")
				}
				v = submitExternal(t, ctx, r, v, "APPROVE", "")
				v, err = r.ExternalComplete(ctx, v.RunID)
				if err != nil || v.Status != "READY_FOR_HUMAN_MERGE" {
					t.Fatal("valid final proof chain failed", v, err)
				}
				return
			}
			for attempt := 0; attempt < 2; attempt++ {
				_, err = r.ExternalAdvance(ctx, v.RunID, nil)
				if err == nil || !strings.Contains(err.Error(), "original deadline reached") || !strings.Contains(err.Error(), w.Spec.HardDeadline) {
					t.Fatal("expiry cause is ambiguous", err)
				}
				if !reflect.DeepEqual(before, externalDatabase(t, s)) {
					t.Fatal("deadline rejection mutated accepted state")
				}
			}
			if _, err = r.ExternalAdvance(ctx, v.RunID, &in); err != nil {
				t.Fatal("identical accepted Reviewer replay failed after expiry", err)
			}
			start := ExternalStart{RunID: v.RunID, Goal: v.Goal, WorktreePath: tree, ApprovalResponse: w.Spec.Confirmation.Response, HardWindowSeconds: 7200}
			if _, err = r.ExternalStart(ctx, start); err == nil {
				t.Fatal("start replay extended original deadline")
			}
			if !reflect.DeepEqual(before, externalDatabase(t, s)) {
				t.Fatal("replay changed retained state")
			}
		})
	}
}
