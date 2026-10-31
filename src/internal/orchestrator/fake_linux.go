// Package orchestrator owns the minimal fake workflow. Agents receive value-only
// requests; only this controller retains a StateStore handle.
package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"marc-runtime/internal/identity"
	"marc-runtime/src/internal/roleresult"
)

type Request struct {
	RunID, InvocationID, ExecutionID, AttemptID, Role, Purpose, ContentIdentity, Goal, At, Deadline string
	Step, InvocationOrdinal                                                                         int
	Developer, QA, Reviewer                                                                         *roleresult.Result
	TaskArtifact                                                                                    json.RawMessage
	Behavior                                                                                        string // deterministic fault modes, used only by tests
}
type Response struct {
	Semantic      json.RawMessage
	Artifact      json.RawMessage
	Input, Output *int64
}
type captured struct {
	Response   Response
	DurationNS int64
	At         string
	ReleasedAt string
}

func marshal(v any) []byte { b, _ := json.Marshal(v); return b }
func ptr(n int64) *int64   { return &n }
func taskArtifact(goal string) json.RawMessage {
	return marshal(map[string]string{"goal": goal, "sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(goal)))})
}

// FakeWorker is a closed, deterministic subprocess protocol. It has no database,
// credential, filesystem/tool or network interface and spawns no descendants.
func FakeWorker(input io.Reader, output io.Writer) error {
	raw, err := io.ReadAll(io.LimitReader(input, identity.MaxJSONBytes+1))
	if err != nil {
		return err
	}
	if err = identity.CheckJSONEnvelope(raw); err != nil {
		return err
	}
	var q Request
	if err = json.Unmarshal(raw, &q); err != nil {
		return err
	}
	deadline, err := time.Parse(time.RFC3339Nano, q.Deadline)
	if err != nil {
		return err
	}
	if !time.Now().Before(deadline) {
		return fmt.Errorf("original fake request deadline expired")
	}
	// Independent of the controller's reads and accounting. Supervisor death also
	// terminates this fixed child via Pdeathsig; real process trees remain B5 work.
	timer := time.AfterFunc(time.Until(deadline), func() { os.Exit(124) })
	defer timer.Stop()
	switch q.Behavior {
	case "hang":
		<-make(chan struct{})
	case "oversize":
		_, err = io.CopyN(output, zeroReader{}, identity.MaxJSONBytes+1)
		return err
	case "invalid":
		_, err = output.Write([]byte(`{"Semantic":`))
		return err
	case "exit":
		return fmt.Errorf("fake process failure")
	}
	producedAt := time.Now().UTC().Format(time.RFC3339Nano)
	p := map[string]any{"execution_id": q.ExecutionID, "attempt": 1, "invocation_id": q.InvocationID, "invocation": q.InvocationOrdinal}
	response := Response{Input: ptr(0), Output: ptr(4)}
	if q.Behavior == "overrun" {
		response.Output = ptr(5)
	}
	if q.Behavior == "unknown" {
		response.Input = nil
		response.Output = nil
	}
	if q.Role == "LEAD" {
		qa, review := map[string]any{"status": "not_run"}, map[string]any{"status": "not_run"}
		next := "DEVELOPER"
		purpose := "IMPLEMENT"
		status := "active"
		if q.Developer != nil {
			if q.Developer.Outcome != "completed" {
				status = "blocked"
			}
			next = "QA"
			purpose = "QA"
		}
		if q.QA != nil {
			qa = map[string]any{"status": q.QA.Outcome, "result_ref": map[string]string{"result_id": q.QA.ResultID}}
			next = "REVIEWER"
			purpose = "REVIEW"
			if q.QA.Outcome != "passed" {
				status = "blocked"
			}
		}
		if q.Reviewer != nil {
			review = map[string]any{"status": q.Reviewer.Outcome, "result_ref": map[string]string{"result_id": q.Reviewer.ResultID}}
			if q.Reviewer.Outcome == "approved" && status != "blocked" {
				status = "approved"
			} else {
				status = "blocked"
			}
		}
		state := map[string]any{"schema_version": 1, "lead_state_id": q.InvocationID + "-state", "run_id": q.RunID, "cycle": 1, "producer": p, "content_identity": q.ContentIdentity, "goal": q.Goal, "acceptance_criteria": []string{"The deterministic task artifact matches its exact goal digest."}, "open_findings": []any{}, "resolved_findings": []any{}, "qa": qa, "review": review, "semantic_status": status, "created_at": producedAt}
		var action any
		if status == "active" {
			action = map[string]any{"schema_version": 1, "action_id": q.InvocationID + "-action", "run_id": q.RunID, "cycle": 1, "producer": p, "based_on_lead_state": map[string]string{"lead_state_id": q.InvocationID + "-state"}, "content_identity": q.ContentIdentity, "target_role": next, "purpose": purpose, "finding_ids": []string{}, "intent": "Validate the deterministic task artifact for this stage.", "created_at": producedAt}
		}
		response.Semantic = marshal(struct {
			State  any
			Action any
		}{state, action})
	} else {
		if q.Role == "QA" && (q.Developer != nil || q.QA != nil || q.Reviewer != nil) {
			return fmt.Errorf("QA projection contains semantic claims")
		}
		if q.Role != "DEVELOPER" && string(q.TaskArtifact) != string(taskArtifact(q.Goal)) {
			return fmt.Errorf("independent artifact check failed")
		}
		if q.Role == "REVIEWER" && (q.QA == nil || q.QA.Outcome != "passed") {
			return fmt.Errorf("review lacks positive QA evidence")
		}
		response.Artifact = taskArtifact(q.Goal)
		result := map[string]any{"schema_version": 1, "result_id": roleresult.ResultID(1, q.Step), "result_ordinal": q.Step, "run_id": q.RunID, "cycle": 1, "execution_id": q.ExecutionID, "attempt_id": q.AttemptID, "invocation_id": q.InvocationID, "role": q.Role, "content_identity": q.ContentIdentity, "summary": "Deterministic task artifact verified.", "evidence_refs": []any{map[string]string{"evidence_id": q.InvocationID + "-evidence", "kind": "test_result"}}, "produced_at": producedAt}
		switch q.Role {
		case "DEVELOPER":
			result["outcome"] = "completed"
			result["remaining_work"] = []string{}
		case "QA":
			result["outcome"] = "passed"
			result["finding_occurrences"] = []any{}
			result["checks"] = []any{map[string]any{"check_id": "digest-check", "required": true, "outcome": "passed", "summary": "Exact goal digest checked independently.", "evidence_refs": result["evidence_refs"]}}
			if q.Behavior == "qa-inconclusive" {
				result["outcome"] = "inconclusive"
				result["checks"] = []any{}
			}
		case "REVIEWER":
			result["outcome"] = "approved"
			result["finding_occurrences"] = []any{}
		default:
			return fmt.Errorf("unsupported fake role")
		}
		if q.Behavior == "wrong-content" {
			result["content_identity"] = "foreign-content"
		}
		response.Semantic = marshal(result)
	}
	_, err = output.Write(marshal(response))
	return err
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

type boundedBuffer struct{ bytes.Buffer }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > identity.MaxJSONBytes {
		return 0, fmt.Errorf("fake output exceeds ingress bound")
	}
	return b.Buffer.Write(p)
}
func executeFake(ctx context.Context, q Request, command []string) (captured, error) {
	var out captured
	deadline, err := time.Parse(time.RFC3339Nano, q.Deadline)
	if err != nil {
		return out, err
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Env = []string{"ACP_FAKE_CHILD=1"}
	cmd.Dir = "/"
	cmd.Stdin = bytes.NewReader(marshal(q))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	// Kill the complete owned group, including blocked output readers. No inherited
	// descriptors, transcript, environment credentials or authoritative path.
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 100 * time.Millisecond
	var stdout, stderr boundedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	started := time.Now()
	if err = cmd.Start(); err != nil {
		return out, fmt.Errorf("fake release failed: %w", err)
	}
	out.ReleasedAt = time.Now().UTC().Format(time.RFC3339Nano)
	err = cmd.Wait()
	out.DurationNS = time.Since(started).Nanoseconds()
	out.At = time.Now().UTC().Format(time.RFC3339Nano)
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if err != nil {
		return out, fmt.Errorf("fake request failed: %w", err)
	}
	if err = identity.CheckJSONEnvelope(stdout.Bytes()); err != nil {
		return out, err
	}
	dec := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&out.Response); err != nil {
		return out, err
	}
	return out, nil
}
