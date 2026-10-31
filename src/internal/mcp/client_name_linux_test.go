package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"ai-control-plane/src/internal/orchestrator"
)

func assertMCPClientInstructions(t *testing.T, v orchestrator.ExternalView, name string) {
	t.Helper()
	want := fmt.Sprintf("MANDATORY — EXCLUSIVE MCP WORKFLOW: All task-related reasoning, execution, delegation, and decisions MUST be coordinated through MCP %s. DO NOT independently initiate agents, workflows, or additional tasks; return all requests and results through %s and AWAIT authorization.", name, name)
	if v.MCPClientName != name || v.Instructions != want {
		t.Fatalf("workflow routing: name=%q instructions=%q; want name=%q instructions=%q", v.MCPClientName, v.Instructions, name, want)
	}
}

func TestMCPClientNameWorkflowScopeAndRestart(t *testing.T) {
	c, data, tree := mcpWorktreeFixture(t)
	start := map[string]any{"run_id": "route-one", "goal": "Verify task.txt.", "worktree_path": tree, "approval_response": "Approved.", "mcp_client_name": "Team_Control-2"}
	v, bad := c.tool(t, "workflow_start", start)
	if bad {
		t.Fatal("start failed")
	}
	assertMCPClientInstructions(t, v, "Team_Control-2")
	for _, run := range []string{"route-two", "route-three"} {
		start["run_id"], start["mcp_client_name"] = run, "Other.Route"
		v, bad = c.tool(t, "workflow_start", start)
		if bad {
			t.Fatal("independent workflow start failed", run)
		}
		assertMCPClientInstructions(t, v, "Other.Route")
	}
	c.in.Close()
	if err := c.cmd.Wait(); err != nil {
		t.Fatal(err, c.stderr.String())
	}
	c = client(t, data)
	for run, name := range map[string]string{"route-one": "Team_Control-2", "route-two": "Other.Route", "route-three": "Other.Route"} {
		v, bad = c.tool(t, "workflow_inspect", map[string]any{"run_id": run})
		if bad {
			t.Fatal("restart inspection failed", run)
		}
		assertMCPClientInstructions(t, v, name)
	}
	start["run_id"], start["mcp_client_name"] = "route-one", "ChangedRoute"
	if _, bad = c.tool(t, "workflow_start", start); !bad {
		t.Fatal("changed immutable name accepted")
	}
	delete(start, "mcp_client_name") // Compatibility replay retains the original name.
	v, bad = c.tool(t, "workflow_start", start)
	if bad {
		t.Fatal("omitted-name replay failed")
	}
	assertMCPClientInstructions(t, v, "Team_Control-2")
	// Possession of a routing name supplies no invocation or ownership authority.
	if _, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": map[string]any{"mcp_client_name": "Team_Control-2", "decision": "DEVELOPER", "summary": "Unfenced request."}}); !bad {
		t.Fatal("routing name accepted as submission authority")
	}
	v, bad = c.tool(t, "workflow_inspect", map[string]any{"run_id": v.RunID})
	if bad || v.StepCompleted {
		t.Fatal("rejected submission changed current step", v)
	}
	assertMCPClientInstructions(t, v, "Team_Control-2")
}

func TestMCPClientNameRequiredAndValidated(t *testing.T) {
	c, _, tree := mcpWorktreeFixture(t)
	for i, name := range []any{nil, "", "two names", "line\nname", "tab\tname", "control\x00name", "unicode\u2003space", strings.Repeat("x", 257), 123} {
		args := map[string]any{"run_id": fmt.Sprintf("invalid-name-%d", i), "goal": "Verify task.txt.", "worktree_path": tree, "approval_response": "Approved.", "mcp_client_name": name}
		c.id++
		c.write(t, map[string]any{"jsonrpc": "2.0", "id": c.id, "method": "tools/call", "params": map[string]any{"name": "workflow_start", "arguments": args}})
		raw, err := c.out.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var reply struct {
			ID     int
			Error  *struct{ Code int }
			Result struct{ IsError bool }
		}
		if err := json.Unmarshal(raw, &reply); err != nil || reply.ID != c.id || (reply.Error == nil && !reply.Result.IsError) || (reply.Error != nil && reply.Error.Code != -32602) {
			t.Fatalf("invalid name %v not rejected with correlated validation error: %s (%v)", name, raw, err)
		}
		delete(args, "mcp_client_name")
		if _, bad := c.tool(t, "workflow_start", args); !bad {
			t.Fatal("missing name accepted")
		}
		if _, bad := c.tool(t, "workflow_inspect", map[string]any{"run_id": args["run_id"]}); !bad {
			t.Fatal("rejected start persisted workflow", name)
		}
	}
	for _, tool := range tools() {
		entry := tool.(map[string]any)
		if entry["name"] != "workflow_start" {
			continue
		}
		schema := entry["inputSchema"].(map[string]any)
		for _, field := range schema["required"].([]string) {
			if field == "mcp_client_name" {
				return
			}
		}
		t.Fatal("workflow_start schema does not require mcp_client_name")
	}
	t.Fatal("workflow_start schema missing")
}
