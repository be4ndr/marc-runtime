package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"marc-runtime/src/internal/roleresult"
	"marc-runtime/src/internal/statestore"
)

func TestFakeSubprocess(t *testing.T) {
	if os.Getenv("ACP_FAKE_CHILD") != "1" {
		return
	}
	if err := FakeWorker(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
func fixture(t *testing.T, tokens ...int64) (context.Context, *statestore.StateStore, *Runtime, statestore.WorkflowSpec) {
	t.Helper()
	ctx := statestore.WithMutationActor(context.Background(), "ORCHESTRATOR")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := statestore.Open(ctx, dir, statestore.OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	runtime, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	runtime.command = []string{os.Args[0], "-test.run=^TestFakeSubprocess$"}
	at := time.Now().UTC()
	goal := "Produce a deterministic digest of this task."
	spec := statestore.WorkflowSpec{RunID: "fake-run", Goal: goal, ContentIdentity: fmt.Sprintf("content-%x", sha256.Sum256([]byte(goal))), StartedAt: at.Format(time.RFC3339Nano), HardDeadline: at.Add(time.Hour).Format(time.RFC3339Nano), NormalTokens: 1000, Confirmation: statestore.HumanBudgetApproval{ApprovalID: "initial-human-approval", InteractionID: "initial-human-interaction", Prompt: "Confirm 1000 normal WORK tokens.", Response: "I approve 1000 normal WORK tokens.", Decision: "APPROVE"}}
	if len(tokens) > 0 {
		spec.NormalTokens = tokens[0]
	}
	spec.Confirmation.Prompt = fmt.Sprintf("Confirm %d normal WORK tokens.", spec.NormalTokens)
	spec.Confirmation.Response = fmt.Sprintf("I approve %d normal WORK tokens.", spec.NormalTokens)
	if err = runtime.Initialize(ctx, spec); err != nil {
		t.Fatal(err)
	}
	return ctx, s, runtime, spec
}
func restart(t *testing.T, ctx context.Context, s *statestore.StateStore) (*statestore.StateStore, *Runtime) {
	t.Helper()
	path := s.Info().Path
	dir := path[:len(path)-len(statestore.DatabaseName)-1]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := statestore.Open(ctx, dir, statestore.OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { next.Close() })
	runtime, err := New(next)
	if err != nil {
		t.Fatal(err)
	}
	runtime.command = []string{os.Args[0], "-test.run=^TestFakeSubprocess$"}
	return next, runtime
}
func assertReady(t *testing.T, ctx context.Context, s *statestore.StateStore, w statestore.WorkflowSnapshot) {
	t.Helper()
	if w.Status != "READY_FOR_HUMAN_MERGE" || w.Terminal.ProofID == "" {
		t.Fatalf("not ready: %+v", w)
	}
	roles := []string{}
	for _, x := range w.Steps {
		roles = append(roles, x.Role)
		if x.CompletedAt == "" || x.DurationNS == nil || !x.Closed || x.ReceiptState != "final" {
			t.Fatalf("unfinished step: %+v", x)
		}
	}
	if !reflect.DeepEqual(roles, []string{"LEAD", "DEVELOPER", "LEAD", "QA", "LEAD", "REVIEWER", "LEAD"}) {
		t.Fatal(roles)
	}
	if _, err := s.GetActiveActor(ctx, w.Spec.RunID); err == nil {
		t.Fatal("terminal actor retained")
	}
	if _, err := s.AuthorityProofRetention(ctx, w.Spec.RunID, w.Terminal); err != nil {
		t.Fatal(err)
	}
}
func TestWorkflowHappyPathAndReplay(t *testing.T) {
	ctx, s, r, spec := fixture(t)
	w, err := r.Run(ctx, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	assertReady(t, ctx, s, w)
	if w.AccountingIncomplete || w.AccountingStatus != "complete" {
		t.Fatal("FINAL known usage misreported", w)
	}
	balance, err := s.GetRunTokenBalance(ctx, spec.RunID)
	if err != nil || balance.NormalUsed != 28 {
		t.Fatal(balance, err)
	}
	before, err := s.ListEvents(ctx, spec.RunID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		w, err = r.Run(ctx, spec.RunID)
		if err != nil {
			t.Fatal(err)
		}
		assertReady(t, ctx, s, w)
	}
	after, err := s.ListEvents(ctx, spec.RunID, 0, 1000)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("replay changed events", err)
	}
	s, r = restart(t, ctx, s)
	w, err = r.Run(ctx, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	assertReady(t, ctx, s, w)
}
func TestWorkflowRestartCutPoints(t *testing.T) {
	for _, cut := range []string{"after_step", "after_admission", "after_claim", "after_authorize", "after_process", "after_capture", "after_usage", "after_result", "after_lead_state", "after_action", "after_close", "after_finish", "before_terminal", "after_ready"} {
		t.Run(cut, func(t *testing.T) {
			ctx, s, r, spec := fixture(t)
			interrupted := errors.New("simulated interruption")
			fired := false
			r.hook = func(stage string) error {
				if stage == cut && !fired {
					fired = true
					return interrupted
				}
				return nil
			}
			_, err := r.Run(ctx, spec.RunID)
			if !errors.Is(err, interrupted) || !fired {
				t.Fatal("cut not reached", err)
			}
			s, r = restart(t, ctx, s)
			w, err := r.Run(ctx, spec.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if cut == "after_authorize" || cut == "after_process" {
				if w.Status != "BLOCKED" || w.Current.Status != "UNKNOWN_OUTCOME" || w.Owner.InvocationID != nil || len(w.Steps) != 1 || len(w.Current.Output) != 0 {
					t.Fatal("ambiguous release redispatched", w.Status, len(w.Steps))
				}
				return
			}
			assertReady(t, ctx, s, w)
		})
	}
}
func TestWorkflowWorkerLostAcknowledgement(t *testing.T) {
	ctx, s, r, spec := fixture(t)
	cut := errors.New("lost worker ACK")
	r.hook = func(stage string) error {
		if stage == "after_result" {
			w, err := s.GetWorkflow(ctx, spec.RunID)
			if err != nil {
				return err
			}
			if w.Current.Role == "DEVELOPER" {
				return cut
			}
		}
		return nil
	}
	if _, err := r.Run(ctx, spec.RunID); !errors.Is(err, cut) {
		t.Fatal(err)
	}
	w, err := s.GetWorkflow(ctx, spec.RunID)
	if err != nil || w.Current.ResultID == "" || w.Owner.Role != "LEAD" || w.Owner.InvocationID != nil {
		t.Fatal("result/return not atomic", w.Owner, err)
	}
	s, r = restart(t, ctx, s)
	w, err = r.Run(ctx, spec.RunID)
	if err != nil {
		t.Fatal(err)
	}
	assertReady(t, ctx, s, w)
	balance, err := s.GetRunTokenBalance(ctx, spec.RunID)
	if err != nil || balance.NormalUsed != 28 {
		t.Fatal("worker called twice", balance, err)
	}
}
func TestWorkflowWorkerCommittedProjectionFailure(t *testing.T) {
	for _, role := range []string{"DEVELOPER", "QA", "REVIEWER"} {
		for _, reopen := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restart=%t", role, reopen), func(t *testing.T) {
				ctx, s, r, spec := fixture(t)
				metricsDir := t.TempDir()
				if err := os.Chmod(metricsDir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := s.EnableMetricsProjection(ctx, spec.RunID, metricsDir); err != nil {
					t.Fatal(err)
				}
				var lock *os.File
				defer func() {
					if lock != nil {
						lock.Close()
					}
				}()
				fired := false
				r.hook = func(stage string) error {
					if stage != "after_usage" || fired {
						return nil
					}
					w, err := s.GetWorkflow(ctx, spec.RunID)
					if err != nil || w.Current.Role != role {
						return err
					}
					var output captured
					if err := json.Unmarshal(w.Current.Output, &output); err != nil {
						return err
					}
					// Prime the exact artifact so its runtime registration is a no-op
					// replay. The real publisher lock then fails only after result
					// acceptance commits, as in the independent ACP-017 reproducer.
					if _, err := s.RegisterWorkflowArtifact(ctx, spec.RunID, w.Current.InvocationID+"-evidence", "test_result", output.Response.Artifact, w.Current.StartedAt); err != nil {
						return err
					}
					lock, err = os.OpenFile(filepath.Join(metricsDir, ".metrics.lock"), os.O_RDWR, 0600)
					if err != nil {
						return err
					}
					if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
						return err
					}
					fired = true
					return nil
				}
				_, err := r.Run(ctx, spec.RunID)
				var projection *statestore.MetricsProjectionError
				if !fired || !errors.As(err, &projection) || !errors.Is(err, syscall.EWOULDBLOCK) {
					t.Fatal("missing committed publication failure", fired, err)
				}
				w, err := s.GetWorkflow(ctx, spec.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if w.Status != "RUNNING" || w.Current.Status != "RUNNING" || w.Current.ResultID == "" || w.Owner.Role != "LEAD" || w.Owner.InvocationID != nil || w.Current.Closed || w.Current.CompletedAt != "" || w.Current.DurationNS == nil || w.Current.ReceiptState != "final" {
					t.Fatalf("publication failure changed committed result/lifecycle: run=%s invocation=%s result=%q owner=%+v closed=%t completed=%q FINAL=%s", w.Status, w.Current.Status, w.Current.ResultID, w.Owner, w.Current.Closed, w.Current.CompletedAt, w.Current.ReceiptState)
				}
				events, err := s.ListEvents(ctx, spec.RunID, 0, 1000)
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range events {
					if event.EventType == "workflow_blocked" {
						t.Fatal("publication failure blocked valid result")
					}
				}
				if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
					t.Fatal(err)
				}
				if err := s.RefreshMetrics(ctx, spec.RunID); err != nil {
					t.Fatal(err)
				}
				refreshed, err := s.GetWorkflow(ctx, spec.RunID)
				if err != nil || !reflect.DeepEqual(w, refreshed) {
					t.Fatal("metrics refresh changed authority", err)
				}
				after, err := s.ListEvents(ctx, spec.RunID, 0, 1000)
				if err != nil || !reflect.DeepEqual(events, after) {
					t.Fatal("metrics refresh changed events", err)
				}
				if reopen {
					s, r = restart(t, ctx, s)
					if err := s.EnableMetricsProjection(ctx, spec.RunID, metricsDir); err != nil {
						t.Fatal(err)
					}
				}
				w, err = r.Run(ctx, spec.RunID)
				if err != nil {
					t.Fatal(err)
				}
				assertReady(t, ctx, s, w)
				bank, err := s.GetRunTokenBalance(ctx, spec.RunID)
				if err != nil || bank.NormalUsed != 28 {
					t.Fatal("reconciliation repeated a Worker call", bank, err)
				}
			})
		}
	}
}

func TestWorkflowFailureAndUnknownAccounting(t *testing.T) {
	for _, mode := range []string{"invalid", "oversize", "wrong-content", "qa-inconclusive", "unknown", "exit", "overrun"} {
		t.Run(mode, func(t *testing.T) {
			ctx, s, r, spec := fixture(t)
			initial, err := s.GetWorkflow(ctx, spec.RunID)
			if err != nil || initial.AccountingStatus != "unknown" || !initial.AccountingIncomplete {
				t.Fatal("unstarted accounting misreported", initial, err)
			}
			// Fail the relevant Worker; Lead and previous roles retain normal behavior.
			r.hook = func(stage string) error {
				if stage == "after_step" {
					w, err := s.GetWorkflow(ctx, spec.RunID)
					if err != nil {
						return err
					}
					if mode == "qa-inconclusive" && w.Current.Role == "QA" || mode != "qa-inconclusive" && w.Current.Role == "DEVELOPER" {
						r.behavior = mode
					} else {
						r.behavior = ""
					}
				}
				return nil
			}
			w, err := r.Run(ctx, spec.RunID)
			if mode == "unknown" {
				if err != nil {
					t.Fatal(err)
				}
				assertReady(t, ctx, s, w)
				if !w.AccountingIncomplete || w.AccountingStatus != "incomplete" {
					t.Fatal("UNKNOWN hidden by readiness report")
				}
				s, r = restart(t, ctx, s)
				w, err = r.Run(ctx, spec.RunID)
				if err != nil || !w.AccountingIncomplete || w.AccountingStatus != "incomplete" {
					t.Fatal("restart hid incomplete accounting", err)
				}
				return
			}
			if err == nil && w.Status != "BLOCKED" {
				t.Fatal("failure advanced to readiness", w.Status)
			}
			if mode == "overrun" {
				bank, err := s.GetRunTokenBalance(ctx, spec.RunID)
				if err != nil || bank.NormalUsed != 9 {
					t.Fatal("overrun evidence rejected or hidden", bank, err)
				}
				stops, err := s.ListRunHardStops(ctx, spec.RunID)
				if err != nil || len(stops) == 0 {
					t.Fatal("missing durable overrun stop", err)
				}
			}
			w, e := s.GetWorkflow(ctx, spec.RunID)
			if e != nil || w.Status != "BLOCKED" {
				t.Fatal("failure not durably blocked", w.Status, e)
			}
			s, r = restart(t, ctx, s)
			next, e := r.Run(ctx, spec.RunID)
			if e != nil || next.Status != "BLOCKED" || len(next.Steps) != len(w.Steps) {
				t.Fatal("failed run resumed", e)
			}
		})
	}
}
func TestWorkflowCancellation(t *testing.T) {
	ctx, s, r, spec := fixture(t)
	r.behavior = "hang"
	cancelled, cancel := context.WithCancel(ctx)
	// Cancellation interrupts an executing fake, not initial setup.
	go func() {
		for {
			if cancelled.Err() != nil {
				return
			}
			w, err := s.GetWorkflow(ctx, spec.RunID)
			if err == nil && w.Current != nil && w.Current.AuthorizedAt != "" {
				time.Sleep(30 * time.Millisecond)
				cancel()
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	_, err := r.Run(cancelled, spec.RunID)
	if err == nil {
		t.Fatal("hang ignored cancellation")
	}
	w, e := s.GetWorkflow(ctx, spec.RunID)
	if e != nil || w.Status != "BLOCKED" || w.Current.DurationNS == nil || w.Current.ResultID != "" {
		t.Fatal("cancellation synthesized completion", e, w.Status)
	}
}

func TestWorkflowHumanHardDecisionsAndBudgetIncrease(t *testing.T) {
	for _, mode := range []string{"approve", "deny", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			ctx, s, r, spec := fixture(t, 32)
			cut := errors.New("checkpoint at reached Hard")
			r.hook = func(stage string) error {
				if stage == "after_usage" {
					return cut
				}
				return nil
			}
			if _, err := r.Run(ctx, spec.RunID); !errors.Is(err, cut) {
				t.Fatal(err)
			}
			w, err := s.GetWorkflow(ctx, spec.RunID)
			if err != nil {
				t.Fatal(err)
			}
			p := statestore.HardLimitRequest{RequestID: "lead-hard-proposal", RunID: spec.RunID, Justification: "Lead requests ten tokens for the bounded continuation.", Grant: statestore.ResourceGrant{Account: account(spec.RunID), AdmissionID: w.Current.Admission.AdmissionID, AllocationID: "human-hard-extension", Amount: 10, Source: "lead-proposal"}}
			before, err := s.GetWorkflow(ctx, spec.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if err = r.PresentHardProposal(ctx, p, rejectQuestion{}); err == nil {
				t.Fatal("failed delivery created presentation")
			}
			after, err := s.GetWorkflow(ctx, spec.RunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("failed presentation changed authority", err)
			}
			if _, _, err = s.WorkflowHardRequest(ctx, spec.RunID, p.RequestID); err == nil {
				t.Fatal("failed delivery persisted proposal")
			}
			var question bytes.Buffer
			if err = r.PresentHardProposal(ctx, p, &question); err != nil || question.Len() == 0 {
				t.Fatal("question not presented", err)
			}
			question.Reset()
			if err = r.PresentHardProposal(ctx, p, &question); err != nil || question.Len() != 0 {
				t.Fatal("presentation replay", err)
			}
			s, r = restart(t, ctx, s)
			stored, deadline, err := s.WorkflowHardRequest(ctx, spec.RunID, p.RequestID)
			if err != nil {
				t.Fatal(err)
			}
			at, _ := time.Parse(time.RFC3339Nano, stored.PresentedAt)
			var h *statestore.HumanBudgetApproval
			if mode == "timeout" {
				at, _ = time.Parse(time.RFC3339Nano, deadline)
			} else {
				at = at.Add(time.Minute)
				v := statestore.HumanBudgetApproval{ApprovalID: "hard-human", InteractionID: "hard-interaction", Prompt: HardQuestion(10), Response: "I approve ten tokens.", Decision: "APPROVE"}
				if mode == "deny" {
					v.Decision = "DENY"
					v.Response = "I reject the extension."
				}
				h = &v
			}
			r.now = func() time.Time { return at }
			out, err := r.DecideHardProposal(ctx, spec.RunID, p.RequestID, h)
			if err != nil || !out.Created || out.Applied != (mode != "deny") {
				t.Fatal(out, err)
			}
			at = at.Add(time.Minute)
			if out, err = r.DecideHardProposal(ctx, spec.RunID, p.RequestID, h); err != nil || out.Created {
				t.Fatal("human decision replay", out, err)
			}
			if mode == "deny" {
				if out, err = r.DecideHardProposal(ctx, spec.RunID, p.RequestID, nil); err == nil || out.Applied {
					t.Fatal("denial became timeout approval", err)
				}
			}
			approval := statestore.HumanBudgetApproval{ApprovalID: "bank-increase", InteractionID: "bank-interaction", Prompt: "Approve ten more normal WORK tokens?", Response: "I approve ten normal WORK tokens.", Decision: "APPROVE"}
			if created, err := r.IncreaseBudget(ctx, spec.RunID, 10, approval); err != nil || !created {
				t.Fatal(created, err)
			}
			if created, err := r.IncreaseBudget(ctx, spec.RunID, 10, approval); err != nil || created {
				t.Fatal("bank increase replay", created, err)
			}
			bank, err := s.GetRunTokenBalance(ctx, spec.RunID)
			if err != nil || bank.NormalBudget != 42 || bank.NormalUsed != 4 {
				t.Fatal(bank, err)
			}
		})
	}
}

func TestFakeIndependentDeadline(t *testing.T) {
	at := time.Now()
	q := Request{Role: "LEAD", Behavior: "hang", Deadline: at.Add(200 * time.Millisecond).UTC().Format(time.RFC3339Nano)}
	// No controller context deadline: the fake itself must enforce original expiry.
	cmd := exec.Command(os.Args[0], "-test.run=^TestFakeSubprocess$")
	cmd.Env = []string{"ACP_FAKE_CHILD=1"}
	cmd.Stdin = bytes.NewReader(marshal(q))
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 124 {
			t.Fatal("independent deadline", err)
		}
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatal("fake survived original deadline")
	}
}
func TestFakeQARejectsUnverifiedContentAndClaims(t *testing.T) {
	q := Request{Role: "QA", Goal: "Exact task", Deadline: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), TaskArtifact: json.RawMessage(`{"sha256":"unverified"}`)}
	if err := FakeWorker(bytes.NewReader(marshal(q)), io.Discard); err == nil {
		t.Fatal("QA passed substituted content")
	}
	q.TaskArtifact = taskArtifact(q.Goal)
	q.Developer = &roleresult.Result{Outcome: "completed"}
	if err := FakeWorker(bytes.NewReader(marshal(q)), io.Discard); err == nil {
		t.Fatal("QA received implementation claims")
	}
}
func TestWorkflowExpiredBoundary(t *testing.T) {
	ctx, s, r, spec := fixture(t)
	expiry, _ := time.Parse(time.RFC3339Nano, spec.HardDeadline)
	r.now = func() time.Time { return expiry }
	if _, err := r.Run(ctx, spec.RunID); err == nil {
		t.Fatal("expired Run dispatched")
	}
	w, err := s.GetWorkflow(ctx, spec.RunID)
	if err != nil || w.Current != nil || len(w.Steps) != 0 {
		t.Fatal("deadline rejection created work", err)
	}
	bank, err := s.GetRunTokenBalance(ctx, spec.RunID)
	if err != nil || bank.NormalUsed != 0 {
		t.Fatal(bank, err)
	}
}

type rejectQuestion struct{}

func (rejectQuestion) Write([]byte) (int, error) { return 0, errors.New("question delivery failed") }
