package mcp

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"marc-runtime/src/internal/orchestrator"
	"marc-runtime/src/internal/statestore"
)

func TestMCPPublishedTextLimits(t *testing.T) {
	for _, entry := range tools() {
		tool := entry.(map[string]any)
		name := tool["name"].(string)
		if name != "workflow_start" && name != "workflow_advance" {
			continue
		}
		raw, _ := json.Marshal(tool["inputSchema"])
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		c := jsonschema.NewCompiler()
		c.DefaultDraft(jsonschema.Draft2020)
		const uri = "https://ai-control-plane.invalid/text-limits"
		if err = c.AddResource(uri, doc); err != nil {
			t.Fatal(err)
		}
		sch, err := c.Compile(uri)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{256, 257, 1024, 1025} {
			for _, char := range []string{"x", "😀"} {
				text := strings.Repeat(char, n)
				var args map[string]any
				if name == "workflow_start" {
					args = map[string]any{"run_id": "run", "goal": text, "worktree_path": "/tmp/tree", "approval_response": text, "mcp_client_name": "control"}
				} else {
					args = map[string]any{"run_id": "run", "submission": map[string]any{"invocation_id": "inv", "ownership_epoch": 1, "actor_revision": 1, "semantic_revision": 0, "summary": text, "blocker": text, "evidence": text, "remaining_work": []string{text}, "escalation_reason": text, "checks": []any{map[string]any{"check_id": "check", "required": true, "outcome": "passed", "summary": text}}, "finding_definitions": []any{map[string]any{"finding_id": "fnd-000001", "finding_ordinal": 1, "origin_cycle": 1, "origin_result_id": "res-c001-r001", "blocking": true, "summary": text}}}}
				}
				raw, _ = json.Marshal(args)
				value, _ := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
				if (sch.Validate(value) == nil) != (n <= 1024) {
					t.Fatalf("%s description bound %d", name, n)
				}
			}
		}
		// Published structural limits must still reject 257-code-point strings.
		for _, field := range []string{"run_id", "mcp_client_name", "worktree_path", "invocation_id", "outcome"} {
			args := map[string]any{"run_id": "run", "goal": "goal", "worktree_path": "/tmp/tree", "approval_response": "approve", "mcp_client_name": "control"}
			if name == "workflow_advance" {
				if field == "mcp_client_name" || field == "worktree_path" {
					continue
				}
				sub := map[string]any{"invocation_id": "inv", "ownership_epoch": 1, "actor_revision": 1, "semantic_revision": 0, "summary": "summary"}
				args = map[string]any{"run_id": "run", "submission": sub}
				if field != "run_id" {
					sub[field] = strings.Repeat("x", 257)
				} else {
					args[field] = strings.Repeat("x", 257)
				}
			} else {
				if field == "invocation_id" || field == "outcome" {
					continue
				}
				args[field] = strings.Repeat("x", 257)
			}
			raw, _ = json.Marshal(args)
			value, _ := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
			if sch.Validate(value) == nil {
				t.Fatalf("%s structural %s widened", name, field)
			}
		}
	}
}

func TestMCPDescriptiveTextPersistenceAndReplay(t *testing.T) {
	for _, char := range []string{"x", "😀"} {
		for _, n := range []int{256, 257, 1024} {
			t.Run(fmt.Sprintf("%s-%d", char, n), func(t *testing.T) {
				c, data, tree := mcpWorktreeFixture(t)
				text := strings.Repeat(char, n)
				start := map[string]any{"run_id": "long-text", "goal": text, "worktree_path": tree, "approval_response": text, "mcp_client_name": "fixture-control"}
				v, bad := c.tool(t, "workflow_start", start)
				if bad {
					t.Fatal("start rejected")
				}
				in := orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Decision: "DEVELOPER", Summary: text}
				args := map[string]any{"run_id": v.RunID, "submission": in}
				v, bad = c.tool(t, "workflow_advance", args)
				if bad || !v.StepCompleted {
					t.Fatal("Lead submission rejected")
				}
				c.in.Close()
				if err := c.cmd.Wait(); err != nil {
					t.Fatal(err)
				}
				c = client(t, data)
				replay, bad := c.tool(t, "workflow_start", start)
				if bad || replay.Step.InvocationID != v.Step.InvocationID {
					t.Fatal("start replay changed step")
				}
				replay, bad = c.tool(t, "workflow_advance", args)
				if bad || replay.Step.InvocationID != v.Step.InvocationID || !replay.StepCompleted {
					t.Fatal("Lead replay failed")
				}
				in.Summary = "!" + text[len(char):]
				if _, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": in}); !bad {
					t.Fatal("conflicting replay accepted")
				}
				v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID})
				if bad || v.Step.Role != "DEVELOPER" {
					t.Fatal("handoff failed")
				}
				in = orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Outcome: "blocked", Summary: text, Evidence: text, Blocker: text, RemainingWork: []string{text}}
				v, bad = c.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": in})
				if bad || !v.StepCompleted {
					t.Fatal("Worker text rejected")
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
				for _, query := range []string{
					`SELECT json_extract(spec_json,'$.Goal') FROM workflow_runs`,
					`SELECT json_extract(spec_json,'$.Confirmation.Response') FROM workflow_runs`,
					`SELECT goal FROM lead_states`, `SELECT intent FROM next_actions`,
					`SELECT summary FROM role_results`, `SELECT summary FROM developer_blockers`,
					`SELECT summary FROM developer_remaining_work`,
				} {
					var got string
					if err := db.QueryRow(query).Scan(&got); err != nil || got != text {
						t.Fatalf("persisted text: %s: %v", query, err)
					}
				}
			})
		}
	}
}
