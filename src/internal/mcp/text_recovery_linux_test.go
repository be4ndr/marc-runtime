package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"ai-control-plane/src/internal/orchestrator"
)

// A nil runtime proves every rejected envelope stops before Orchestrator access.
func TestMCPDescriptiveLengthRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, arguments, pointer string
		correctable              bool
	}{
		{"summary", `{"submission":{"summary":"LONG"}}`, "/params/arguments/submission/summary", true},
		{"array item", `{"submission":{"remaining_work":["ok","LONG"]}}`, "/params/arguments/submission/remaining_work/1", true},
		{"check summary", `{"submission":{"checks":[{"summary":"LONG"}]}}`, "/params/arguments/submission/checks/0/summary", true},
		{"finding summary", `{"submission":{"finding_definitions":[{"summary":"LONG"}]}}`, "/params/arguments/submission/finding_definitions/0/summary", true},
		{"approval", `{"approval_response":"LONG"}`, "/params/arguments/approval_response", false},
		{"identifier", `{"run_id":"LONG"}`, "/params/arguments/run_id", false},
		{"unknown field", `{"unknown":{"summary":"LONG"}}`, "/params/arguments/unknown/summary", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arguments := strings.ReplaceAll(tc.arguments, "LONG", strings.Repeat("😀", 1025))
			raw := `{"jsonrpc":"2.0","id":37,"method":"tools/call","params":{"name":"workflow_advance","arguments":` + arguments + `}}` + "\n"
			var out bytes.Buffer
			if err := Serve(context.Background(), nil, strings.NewReader(raw), &out); err != nil {
				t.Fatal(err)
			}
			var reply response
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil || string(reply.ID) != "37" || reply.Error == nil || reply.Error.Code != -32602 {
				t.Fatal("lost validation correlation", out.String(), err)
			}
			message := reply.Error.Message
			if !strings.Contains(message, tc.pointer) || strings.Contains(message, "😀") {
				t.Fatal("missing field location or reflected rejected value", message)
			}
			if tc.correctable {
				for _, want := range []string{"only the named descriptive field", "at most two", "identifiers", "authority", "exact-match", "essential", "accepted", "Stop"} {
					if !strings.Contains(message, want) {
						t.Fatalf("missing recovery constraint %q: %s", want, message)
					}
				}
			} else if strings.Contains(message, "retry") {
				t.Fatal("non-correctable field received retry guidance", message)
			}
		})
	}
}

func TestMCPTextCorrectionPreservesFencesAndReplay(t *testing.T) {
	c, _, tree := mcpWorktreeFixture(t)
	v, bad := c.tool(t, "workflow_start", map[string]any{"run_id": "text-recovery", "goal": "Verify task.txt contains exact bytes before followed by newline.", "worktree_path": tree, "mcp_client_name": "fixture-control", "approval_response": "I approve this task."})
	if bad || v.HardDeadline == "" {
		t.Fatal("workflow start failed or hid its deadline", v)
	}
	const essential = `Verify exact bytes "before\n" in task.txt; preserve unrelated files.`
	in := orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Decision: "DEVELOPER", Summary: essential + strings.Repeat("😀", 1025)}
	args := map[string]any{"run_id": v.RunID, "submission": in}
	// Original failure and first corrected retry remain oversized. The client
	// retains the same authority tuple and succeeds on its second corrected retry.
	for attempt := 0; attempt < 2; attempt++ {
		in.Summary = essential + strings.Repeat("😀", 1025-attempt)
		args["submission"] = in
		c.id++
		c.write(t, map[string]any{"jsonrpc": "2.0", "id": c.id, "method": "tools/call", "params": map[string]any{"name": "workflow_advance", "arguments": args}})
		raw, err := c.out.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var reply struct {
			ID    int
			Error *rpcError
		}
		if err := json.Unmarshal(raw, &reply); err != nil || reply.ID != c.id || reply.Error == nil || !strings.Contains(reply.Error.Message, "at most two corrected retries") {
			t.Fatal("missing bounded correction advice", string(raw), err)
		}
		unchanged, bad := c.tool(t, "workflow_inspect", map[string]any{"run_id": v.RunID})
		if bad || !reflect.DeepEqual(v, unchanged) {
			t.Fatal("rejected length changed workflow or fences", unchanged)
		}
	}
	in.Summary = essential + strings.Repeat("😀", 1024-len(essential))
	args["submission"] = in
	stale := in
	stale.OwnershipEpoch++
	if _, bad := c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": stale}); !bad {
		t.Fatal("text correction bypassed authority fence")
	}
	accepted, bad := c.tool(t, "workflow_advance", args)
	if bad || !accepted.StepCompleted || accepted.Step.InvocationID != v.Step.InvocationID {
		t.Fatal("field-only correction was not accepted", accepted)
	}
	replay, bad := c.tool(t, "workflow_advance", args)
	if bad || !reflect.DeepEqual(accepted, replay) {
		t.Fatal("identical corrected replay changed accepted state")
	}
	in.Summary = "changed"
	if _, bad := c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": in}); !bad {
		t.Fatal("accepted text was rewritten")
	}
	developer, bad := c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID})
	if bad || developer.LeadIntent != args["submission"].(orchestrator.ExternalSubmission).Summary {
		t.Fatal("accepted exact-match text was altered", developer)
	}
}

func TestMCPLengthRecoveryDoesNotMaskMalformedEnvelope(t *testing.T) {
	for _, tail := range []string{`,"summary":"again"}}}}`, `}}}} {}`, `,"other":"\ud800"}}}}`} {
		raw := `{"jsonrpc":"2.0","id":37,"method":"tools/call","params":{"name":"workflow_advance","arguments":{"submission":{"summary":"` + strings.Repeat("x", 1025) + `"` + tail + "\n"
		var out bytes.Buffer
		if err := Serve(context.Background(), nil, strings.NewReader(raw), &out); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "at most two") {
			t.Fatal("malformed envelope received length-retry guidance", out.String())
		}
	}
}
