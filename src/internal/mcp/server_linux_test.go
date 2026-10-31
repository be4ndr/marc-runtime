package mcp

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"ai-control-plane/src/internal/orchestrator"
	"ai-control-plane/src/internal/roleresult"
	"ai-control-plane/src/internal/statestore"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestMCPProcess(t *testing.T) {
	if os.Getenv("ACP_MCP_TEST_CHILD") != "1" {
		return
	}
	ctx := statestore.WithMutationActor(context.Background(), "ORCHESTRATOR")
	s, err := statestore.Open(ctx, os.Getenv("ACP_MCP_TEST_DATA"), statestore.OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	r, err := orchestrator.New(s)
	if err == nil {
		err = Serve(ctx, r, os.Stdin, os.Stdout)
	}
	err = errors.Join(err, s.Close())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

type testClient struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    *bufio.Reader
	id     int
	stderr bytes.Buffer
}

func client(t *testing.T, data string) *testClient {
	t.Helper()
	c := &testClient{cmd: exec.Command(os.Args[0], "-test.run=^TestMCPProcess$")}
	c.cmd.Env = append(os.Environ(), "ACP_MCP_TEST_CHILD=1", "ACP_MCP_TEST_DATA="+data)
	c.cmd.Stderr = &c.stderr
	var err error
	c.in, err = c.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	c.out = bufio.NewReader(out)
	if err = c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.in.Close()
		if c.cmd.ProcessState == nil {
			c.cmd.Process.Kill()
			c.cmd.Wait()
		}
	})
	c.rpc(t, "initialize", map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "test", "version": "1"}})
	c.write(t, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	return c
}
func (c *testClient) write(t *testing.T, v any) {
	t.Helper()
	if err := json.NewEncoder(c.in).Encode(v); err != nil {
		t.Fatal(err)
	}
}
func (c *testClient) rpc(t *testing.T, method string, params any) json.RawMessage {
	t.Helper()
	c.id++
	c.write(t, map[string]any{"jsonrpc": "2.0", "id": c.id, "method": method, "params": params})
	raw, err := c.out.ReadBytes('\n')
	if err != nil {
		t.Fatal(err, c.stderr.String())
	}
	var r struct {
		ID     int
		Result json.RawMessage
		Error  any
	}
	if err = json.Unmarshal(raw, &r); err != nil || r.ID != c.id || r.Error != nil {
		t.Fatal(string(raw), err)
	}
	return r.Result
}
func (c *testClient) tool(t *testing.T, name string, arg any) (orchestrator.ExternalView, bool) {
	t.Helper()
	raw := c.rpc(t, "tools/call", map[string]any{"name": name, "arguments": arg, "_meta": map[string]any{"progressToken": c.id}})
	var result struct {
		Content []struct{ Text string }
		IsError bool
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		return orchestrator.ExternalView{}, true
	}
	var v orchestrator.ExternalView
	if err := json.Unmarshal([]byte(result.Content[0].Text), &v); err != nil {
		t.Fatal(err)
	}
	return v, false
}
func mcpWorktreeFixture(t *testing.T) (*testClient, string, string) {
	t.Helper()
	data := t.TempDir()
	os.Chmod(data, 0700)
	repo := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		raw, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatal(err, string(raw))
		}
	}
	git("init", "-q")
	os.WriteFile(filepath.Join(repo, "task.txt"), []byte("before\n"), 0600)
	git("add", "task.txt")
	git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
	tree := filepath.Join(t.TempDir(), "worktree")
	git("worktree", "add", "--detach", tree, "HEAD")
	c := client(t, data)
	return c, data, tree
}
func TestMCPWorktreeEndToEndAndResume(t *testing.T) {
	c, data, tree := mcpWorktreeFixture(t)
	var list struct{ Tools []any }
	if err := json.Unmarshal(c.rpc(t, "tools/list", map[string]any{}), &list); err != nil || len(list.Tools) != 4 {
		t.Fatal(err, list)
	}
	start := map[string]any{"run_id": "mcp-run", "goal": "Change task.txt to after.", "worktree_path": tree, "mcp_client_name": "fixture-control", "approval_response": "I approve this task."}
	v, bad := c.tool(t, "workflow_start", start)
	if bad {
		t.Fatal("start failed")
	}
	assertMCPClientInstructions(t, v, "fixture-control")
	if !strings.Contains(v.TaskInstructions, "verifiable acceptance criteria") {
		t.Fatal("MCP start omitted Lead task guidance")
	}
	if _, bad = c.tool(t, "workflow_complete", map[string]any{"run_id": v.RunID}); !bad {
		t.Fatal("premature completion")
	}
	var last map[string]any
	for _, role := range []string{"DEVELOPER", "QA", "REVIEWER", "APPROVE"} {
		submit := func(decision, outcome string) {
			t.Helper()
			s := map[string]any{"invocation_id": v.Step.InvocationID, "ownership_epoch": v.OwnershipEpoch, "actor_revision": v.ActorRevision, "semantic_revision": v.SemanticRevision, "summary": "Observed and verified actual task content."}
			if decision != "" {
				s["decision"] = decision
			} else {
				s["outcome"] = outcome
				s["evidence"] = "Read task.txt: after."
			}
			if v.Step.Role == "QA" {
				s["checks"] = []any{map[string]any{"check_id": "actual-content", "required": true, "outcome": "passed", "summary": "Independently read task.txt and checked exact bytes."}}
			}
			last = map[string]any{"run_id": v.RunID, "submission": s}
			var bad bool
			v, bad = c.tool(t, "workflow_advance", last)
			if bad {
				t.Fatal("submission failed", role)
			}
			assertMCPClientInstructions(t, v, "fixture-control")
			if v.TaskInstructions != "" {
				t.Fatal("completed role retained task guidance")
			}
		}
		submit(role, "")
		if role == "APPROVE" {
			break
		}
		v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID})
		if bad {
			t.Fatal("dispatch failed")
		}
		assertMCPClientInstructions(t, v, "fixture-control")
		if !strings.Contains(v.TaskInstructions, "Acceptance:") {
			t.Fatal("MCP dispatch omitted role task guidance", role)
		}
		if role == "DEVELOPER" {
			os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600)
		}
		if role == "QA" && len(v.AcceptedResults) > 0 {
			t.Fatal("QA received Developer claims")
		}
		submit("", map[string]string{"DEVELOPER": "completed", "QA": "passed", "REVIEWER": "approved"}[role])
		if role == "DEVELOPER" {
			c.in.Close()
			if err := c.cmd.Wait(); err != nil {
				t.Fatal(err, c.stderr.String())
			}
			c = client(t, data)
			// Duplicate acknowledgement after disconnect must not create another step.
			replay, bad := c.tool(t, "workflow_advance", last)
			if bad || replay.Step.InvocationID != v.Step.InvocationID || !replay.StepCompleted {
				t.Fatal("lost ACK replay", replay)
			}
			assertMCPClientInstructions(t, replay, "fixture-control")
		}
		v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID})
		if bad {
			t.Fatal("Lead return failed")
		}
		assertMCPClientInstructions(t, v, "fixture-control")
	}
	v, bad = c.tool(t, "workflow_complete", map[string]any{"run_id": v.RunID})
	if bad || v.Status != "READY_FOR_HUMAN_MERGE" || v.Terminal.ProofID == "" {
		t.Fatal("terminal failed", v)
	}
	assertMCPClientInstructions(t, v, "fixture-control")
	replay, bad := c.tool(t, "workflow_complete", map[string]any{"run_id": v.RunID})
	if bad || replay.Terminal != v.Terminal {
		t.Fatal("terminal replay")
	}
	assertMCPClientInstructions(t, replay, "fixture-control")
	c.in.Close()
	if err := c.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(data, statestore.DatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"provider_calls", "usage_receipts", "invocation_admissions", "run_budget_confirmations"} {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatal("synthetic accounting", table, n, err)
		}
	}
}
func TestMCPRejectsMalformedAndUninitializedCalls(t *testing.T) {
	// Uninitialized calls cannot reach the runtime, so a nil runtime is sufficient.
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workflow_start","arguments":{}}}` + "\n" + `{"jsonrpc":"2.0","id":2,"id":3,"method":"ping"}` + "\n"
	var out bytes.Buffer
	if err := Serve(context.Background(), nil, bytes.NewBufferString(input), &out); err != nil {
		t.Fatal(err)
	}
	var replies []response
	d := json.NewDecoder(&out)
	for {
		var r response
		if err := d.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, r)
	}
	if len(replies) != 2 || replies[0].Error.Code != -32000 || replies[1].Error.Code != -32700 {
		t.Fatal(replies)
	}
}

func TestMCPLongArgumentReturnsCorrelatedError(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 17, "method": "tools/call", "params": map[string]any{"name": "workflow_advance", "arguments": map[string]any{"evidence": strings.Repeat("x", 1025)}}})
	var out bytes.Buffer
	if err := Serve(context.Background(), nil, bytes.NewReader(append(raw, '\n')), &out); err != nil {
		t.Fatal(err)
	}
	var reply response
	if err := json.Unmarshal(out.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if string(reply.ID) != "17" || reply.Error == nil || reply.Error.Code != -32602 {
		t.Fatalf("lost correlation: %s", out.Bytes())
	}
}

func TestMCPNegativeFindingContractAndResume(t *testing.T) {
	for _, role := range []string{"QA", "REVIEWER"} {
		t.Run(role, func(t *testing.T) {
			c, data, tree := mcpWorktreeFixture(t)
			v, bad := c.tool(t, "workflow_start", map[string]any{"run_id": "negative-run", "goal": "Change task.txt and verify it.", "worktree_path": tree, "mcp_client_name": "fixture-control", "approval_response": "I approve this task."})
			if bad {
				t.Fatal("start failed")
			}
			advance := func() {
				t.Helper()
				var bad bool
				v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID})
				if bad {
					t.Fatal("advance failed")
				}
			}
			submit := func(decision, outcome string) {
				t.Helper()
				s := orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Summary: "Actual role result.", Decision: decision, Outcome: outcome}
				if outcome != "" {
					s.Evidence = "Observed frozen task.txt."
				}
				if v.Step.Role == "QA" {
					s.Checks = []orchestrator.ExternalCheck{{CheckID: "required-check", Required: true, Outcome: "passed", Summary: "Required check passed."}}
				}
				var bad bool
				v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": s})
				if bad {
					t.Fatal("submission failed")
				}
			}
			submit("DEVELOPER", "")
			advance()
			if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600); err != nil {
				t.Fatal(err)
			}
			submit("", "completed")
			advance()
			submit("QA", "")
			advance()
			if role == "REVIEWER" {
				submit("", "passed")
				advance()
				submit("REVIEWER", "")
				advance()
			}
			negative := orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Summary: "Confirmed blocking issue.", Outcome: map[string]string{"QA": "failed", "REVIEWER": "changes_required"}[role], Evidence: "Evidence of the reported blocker."}
			if role == "QA" {
				negative.Checks = []orchestrator.ExternalCheck{{CheckID: "required-check", Required: true, Outcome: "failed", Summary: "Required condition failed."}}
			}
			if _, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": negative}); !bad {
				t.Fatal("negative without finding accepted")
			}
			// Reopen after rejection; no immutable invalid capture may survive.
			c.in.Close()
			if err := c.cmd.Wait(); err != nil {
				t.Fatal(err)
			}
			c = client(t, data)
			negative.FindingDefinitions = []roleresult.Finding{{FindingID: "fnd-000001", FindingOrdinal: 1, OriginCycle: v.Step.Cycle, OriginResultID: v.ResultID, Blocking: true, Summary: "Confirmed blocking issue."}}
			negative.FindingOccurrences = []roleresult.Occurrence{{FindingID: "fnd-000001", EvidenceRefs: []roleresult.EvidenceRef{{EvidenceID: v.EvidenceID, Kind: "test_result"}}, Locations: []roleresult.Location{{Path: "task.txt"}}}}
			args := map[string]any{"run_id": v.RunID, "submission": negative}
			v, bad = c.tool(t, "workflow_advance", args)
			if bad || !v.StepCompleted {
				t.Fatal("complete negative verdict rejected")
			}
			c.in.Close()
			if err := c.cmd.Wait(); err != nil {
				t.Fatal(err)
			}
			c = client(t, data)
			if _, bad = c.tool(t, "workflow_advance", args); bad {
				t.Fatal("identical replay rejected")
			}
			advance()
			facts := v.AcceptedResults[len(v.AcceptedResults)-1]
			if len(facts.FindingDefinitions) != 1 || len(facts.FindingOccurrences) != 1 {
				t.Fatal("finding contract lost at MCP", facts)
			}
			submit("BLOCK", "")
			advance()
			if v.Status != "BLOCKED" {
				t.Fatal(v)
			}
			if _, bad = c.tool(t, "workflow_complete", map[string]any{"run_id": v.RunID}); !bad {
				t.Fatal("negative run completed")
			}
			c.in.Close()
			if err := c.cmd.Wait(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", filepath.Join(data, statestore.DatabaseName))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for table, want := range map[string]int{"findings": 1, "finding_occurrences": 1, "provider_calls": 0, "usage_receipts": 0, "invocation_admissions": 0} {
				var n int
				if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != want {
					t.Fatal(table, n, err)
				}
			}
		})
	}
}

func TestMCPFindingSchemaResolvesExistingContracts(t *testing.T) {
	for _, entry := range tools() {
		tool := entry.(map[string]any)
		if tool["name"] != "workflow_advance" {
			continue
		}
		raw, err := json.Marshal(tool["inputSchema"])
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		c := jsonschema.NewCompiler()
		c.DefaultDraft(jsonschema.Draft2020)
		const uri = "https://ai-control-plane.invalid/workflow-advance"
		if err = c.AddResource(uri, doc); err != nil {
			t.Fatal(err)
		}
		schema, err := c.Compile(uri)
		if err != nil {
			t.Fatal(err)
		}
		args := map[string]any{"run_id": "run1", "submission": map[string]any{"invocation_id": "inv1", "ownership_epoch": 1, "actor_revision": 0, "semantic_revision": 0, "summary": "Confirmed blocker.", "outcome": "failed", "finding_definitions": []any{map[string]any{"finding_id": "fnd-000001", "finding_ordinal": 1, "origin_cycle": 1, "origin_result_id": "res-c001-r004", "blocking": true, "summary": "Confirmed blocker."}}, "finding_occurrences": []any{map[string]any{"finding_id": "fnd-000001", "evidence_refs": []any{map[string]any{"evidence_id": "inv1-evidence", "kind": "test_result"}}}}}}
		raw, _ = json.Marshal(args)
		value, _ := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err = schema.Validate(value); err != nil {
			t.Fatal(err)
		}
		args["submission"].(map[string]any)["finding_definitions"].([]any)[0].(map[string]any)["status"] = "open"
		raw, _ = json.Marshal(args)
		value, _ = jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err = schema.Validate(value); err == nil {
			t.Fatal("schema admitted invented Finding field")
		}
	}
}

func TestMCPFindingRawRequiredFields(t *testing.T) {
	base := map[string]any{"finding_id": "fnd-000001", "finding_ordinal": 1, "origin_cycle": 1, "origin_result_id": "res-c001-r004", "blocking": false, "summary": "Nonblocking observation."}
	for _, variant := range []string{"valid-false", "missing-blocking", "null-blocking", "null-collection", "missing-occurrence-evidence", "missing-fence", "missing-check-required", "oversized-finding-text"} {
		t.Run(variant, func(t *testing.T) {
			definition := map[string]any{}
			for k, v := range base {
				definition[k] = v
			}
			submission := map[string]any{"invocation_id": "inv1", "ownership_epoch": 1, "actor_revision": 0, "semantic_revision": 0, "summary": "Nonblocking observation.", "finding_definitions": []any{definition}}
			switch variant {
			case "missing-blocking":
				delete(definition, "blocking")
			case "null-blocking":
				definition["blocking"] = nil
			case "null-collection":
				submission["finding_definitions"] = nil
			case "missing-occurrence-evidence":
				submission["finding_occurrences"] = []any{map[string]any{"finding_id": "fnd-000001"}}
			case "missing-fence":
				delete(submission, "semantic_revision")
			case "missing-check-required":
				submission["checks"] = []any{map[string]any{"check_id": "check", "outcome": "passed", "summary": "Checked."}}
			case "oversized-finding-text":
				definition["summary"] = strings.Repeat("x", 1025)
			}
			raw, _ := json.Marshal(map[string]any{"run_id": "run1", "submission": submission})
			err := validateAdvanceArguments(raw)
			if (err == nil) != (variant == "valid-false") {
				t.Fatalf("required field validation: %v", err)
			}
		})
	}
}

func TestB7MCPRemediationToReadiness(t *testing.T) {
	c, data, tree := mcpWorktreeFixture(t)
	v, bad := c.tool(t, "workflow_start", map[string]any{"run_id": "remediation-run", "goal": "Repair and verify task.txt.", "worktree_path": tree, "mcp_client_name": "fixture-control", "approval_response": "I approve this task."})
	if bad {
		t.Fatal("start")
	}
	advance := func() {
		t.Helper()
		var bad bool
		v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID})
		if bad {
			t.Fatal("advance")
		}
	}
	submit := func(decision, outcome string, negative, resolve bool) {
		t.Helper()
		in := orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Summary: "Check the concrete reported boundary.", Decision: decision, Outcome: outcome}
		if decision != "" {
			in.EscalationReason = "Verify the changed candidate against the reported defect."
		}
		if outcome != "" {
			in.Evidence = "Observed task.txt and checked its actual bytes."
		}
		if v.Step.Role == "QA" {
			check := "passed"
			if negative {
				check = "failed"
			}
			in.Checks = []orchestrator.ExternalCheck{{CheckID: "required", Required: true, Outcome: check, Summary: "Checked task.txt."}}
		}
		if negative {
			in.FindingDefinitions = []roleresult.Finding{{FindingID: "fnd-000001", FindingOrdinal: 1, OriginCycle: v.Step.Cycle, OriginResultID: v.ResultID, Blocking: true, Summary: "Wrong task bytes."}}
			in.FindingOccurrences = []roleresult.Occurrence{{FindingID: "fnd-000001", EvidenceRefs: []roleresult.EvidenceRef{{EvidenceID: v.EvidenceID, Kind: "test_result"}}}}
		}
		if resolve {
			for _, res := range v.AcceptedResults {
				if res.Role == "QA" {
					in.ResolvedFindings = []orchestrator.ExternalResolution{{FindingID: "fnd-000001", EvidenceID: res.EvidenceRefs[0].EvidenceID}}
				}
			}
		}
		var bad bool
		v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": in})
		if bad {
			t.Fatalf("submit %s %s", decision, outcome)
		}
	}
	submit("DEVELOPER", "", false, false)
	advance()
	if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("wrong\n"), 0600); err != nil {
		t.Fatal(err)
	}
	submit("", "completed", false, false)
	advance()
	submit("QA", "", false, false)
	advance()
	submit("", "failed", true, false)
	advance()
	submit("REMEDIATE", "", false, false)
	c.in.Close()
	if err := c.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	c = client(t, data)
	advance()
	if v.Step.Cycle != 2 || v.Step.Purpose != "REMEDIATE" || len(v.FindingIDs) != 1 || v.LeadIntent == "" {
		t.Fatal("lost remediation intent", v)
	}
	if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("fixed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	submit("", "completed", false, false)
	advance()
	submit("QA", "", false, false)
	advance()
	submit("", "passed", false, false)
	advance()
	submit("REVIEWER", "", false, false)
	advance()
	submit("", "approved", false, false)
	advance()
	submit("APPROVE", "", false, true)
	v, bad = c.tool(t, "workflow_complete", map[string]any{"run_id": v.RunID})
	if bad || v.Status != "READY_FOR_HUMAN_MERGE" {
		t.Fatal("completion", v)
	}
	terminal := v.Terminal
	c.in.Close()
	if err := c.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	c = client(t, data)
	v, bad = c.tool(t, "workflow_complete", map[string]any{"run_id": v.RunID})
	if bad || v.Terminal != terminal {
		t.Fatal("terminal replay")
	}
	c.in.Close()
	if err := c.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(data, statestore.DatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"provider_calls", "usage_receipts", "invocation_admissions"} {
		var n int
		if err = db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatal("synthetic accounting", table, n, err)
		}
	}
}
