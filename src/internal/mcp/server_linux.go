// Package mcp transports untrusted intents to Orchestrator; it never holds StateStore.
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"marc-runtime/internal/identity"
	"marc-runtime/schemas"
	"marc-runtime/src/internal/orchestrator"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}
type call struct {
	Meta      json.RawMessage `json:"_meta,omitempty"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}
type advance struct {
	RunID      string                           `json:"run_id"`
	Submission *orchestrator.ExternalSubmission `json:"submission,omitempty"`
}
type runArg struct {
	RunID string `json:"run_id"`
}
type inspectArg struct {
	RunID   string                               `json:"run_id"`
	History *orchestrator.ExternalHistoryRequest `json:"history,omitempty"`
}

func decode(raw []byte, v any) error {
	if err := identity.CheckJSONEnvelope(raw); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	return d.Decode(v)
}

// Recovery is advice for a rejected call, not a server-side retry or mutation.
// Only known prose fields are eligible; approval responses and unknown fields
// may carry exact-match or authority-bearing text even at the descriptive limit.
func descriptiveLengthMessage(err error) string {
	var limit *identity.JSONStringLimitError
	if !errors.As(err, &limit) || limit.Limit != identity.MaxDescriptionCodePoints {
		return err.Error()
	}
	field := strings.TrimPrefix(limit.Pointer, "/params/arguments/")
	correctable := false
	switch field {
	case "goal", "submission/summary", "submission/evidence", "submission/blocker", "submission/escalation_reason":
		correctable = true
	default:
		parts := strings.Split(field, "/")
		if len(parts) >= 3 && parts[0] == "submission" {
			_, indexErr := strconv.Atoi(parts[2])
			correctable = indexErr == nil && ((len(parts) == 3 && parts[1] == "remaining_work") ||
				(len(parts) == 4 && parts[3] == "summary" && (parts[1] == "checks" || parts[1] == "finding_definitions")))
		}
	}
	if !correctable {
		return err.Error()
	}
	return err.Error() + ". Rejected before workflow mutation. Shorten only the named descriptive field to at most 1024 Unicode code points, preserving all essential information. Keep every other field, identifiers, authority fields, and exact-match content unchanged; never rewrite accepted submissions or immutable Finding definitions. Make at most two corrected retries for this call. Stop and report a blocker if the text cannot fit without information loss, any protected content would change, or both retries fail."
}

func schema(properties map[string]any, required ...string) map[string]any {
	return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
}
func tools() []any {
	// Descriptions allow 1024 code points; structural strings retain 256.
	str := map[string]any{"type": "string", "maxLength": identity.MaxStringCodePoints}
	text := map[string]any{"type": "string", "maxLength": identity.MaxDescriptionCodePoints}
	integer := map[string]any{"type": "integer"}
	// Publish the existing contract inline; clients need no repository-relative
	// schema resolver. StateStore still performs reference/aggregate validation.
	raw, _ := schemas.Files.ReadFile("role-result-common.schema.json")
	var common map[string]any
	_ = json.Unmarshal(raw, &common)
	boundSchemaText(common)
	submission := schema(map[string]any{"invocation_id": str, "ownership_epoch": integer, "actor_revision": integer, "semantic_revision": integer, "decision": map[string]any{"type": "string", "enum": []string{"DEVELOPER", "QA", "REVIEWER", "REMEDIATE", "APPROVE", "BLOCK"}}, "outcome": str, "summary": text, "evidence": text, "remaining_work": map[string]any{"type": "array", "items": text}, "blocker": text, "checks": map[string]any{"type": "array", "items": schema(map[string]any{"check_id": str, "required": map[string]any{"type": "boolean"}, "outcome": str, "summary": text}, "check_id", "required", "outcome", "summary")}}, "invocation_id", "ownership_epoch", "actor_revision", "semantic_revision", "summary")
	properties := submission["properties"].(map[string]any)
	properties["escalation_reason"] = text
	properties["finding_ids"] = map[string]any{"type": "array", "uniqueItems": true, "items": str}
	properties["resolved_findings"] = map[string]any{"type": "array", "items": schema(map[string]any{"finding_id": str, "evidence_id": str}, "finding_id", "evidence_id")}
	properties["finding_definitions"] = map[string]any{"type": "array", "items": map[string]any{"$ref": "#/$defs/finding"}}
	properties["finding_occurrences"] = map[string]any{"type": "array", "items": map[string]any{"$ref": "#/$defs/findingOccurrence"}}
	advanceSchema := schema(map[string]any{"run_id": str, "submission": submission}, "run_id")
	advanceSchema["$defs"] = common["$defs"]
	list := []any{
		map[string]any{"name": "workflow_start", "description": "Start an explicitly human-approved external Git-worktree workflow. Supply mcp_client_name as the MCP registration name used to reach this server, not initialize clientInfo.name. Follow the returned workflow instructions. Codex coding/model requests remain outside Orchestrator accounting. Call advance to submit each role's own structured decision/result; use fresh role contexts, never transcripts. No commit/merge/push.", "inputSchema": schema(map[string]any{"run_id": str, "goal": text, "worktree_path": str, "approval_response": text, "hard_window_seconds": integer, "mcp_client_name": map[string]any{"type": "string", "minLength": 1, "maxLength": 256, "description": "Workflow-scoped MCP registration name; no whitespace or control characters. Routing reference only, not identity or authorization."}}, "run_id", "goal", "worktree_path", "approval_response", "mcp_client_name")},
		map[string]any{"name": "workflow_advance", "description": "Submit the current role with returned fences. QA failed/Reviewer changes_required need occurrences of blocking Findings; a failed check alone is insufficient. Supply new definitions with returned result_id and step.Cycle; use returned evidence_id for this submission's evidence. Never invent findings. Lead follows next_decision; REMEDIATE targets open finding_ids and needs escalation_reason. Resolve explicitly with finding_id/evidence_id pairs. Requested model/effort is client configuration, not attested. Restart/replay never reexecutes work.", "inputSchema": advanceSchema},
		map[string]any{"name": "workflow_inspect", "description": "Read current workflow, role-scoped facts and context health. Lead may opt into a bounded history page with history.limit (1..4) and optional exclusive before_step cursor; follow history.next_before_step for older decisions/evidence only when relevant. History is omitted by default and unavailable to Workers. No state mutation. QA receives no Developer semantic claims.", "inputSchema": schema(map[string]any{"run_id": str, "history": schema(map[string]any{"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": orchestrator.MaxExternalHistoryEntries}, "before_step": map[string]any{"type": "integer", "minimum": 0}}, "limit")}, "run_id")},
		map[string]any{"name": "workflow_complete", "description": "Request readiness after final Lead approval. Orchestrator verifies current worktree and independent positive Developer/QA/Reviewer proof chain; never commits, merges or pushes.", "inputSchema": schema(map[string]any{"run_id": str}, "run_id")},
	}
	for _, entry := range list {
		tool := entry.(map[string]any)
		tool["annotations"] = map[string]any{"readOnlyHint": tool["name"] == "workflow_inspect", "destructiveHint": false, "openWorldHint": false, "idempotentHint": tool["name"] != "workflow_advance"}
	}
	return list
}

func boundSchemaText(v any) {
	switch v := v.(type) {
	case map[string]any:
		if v["type"] == "string" {
			// Preserve explicit field-specific bounds from the public contracts.
			if _, ok := v["maxLength"]; !ok {
				v["maxLength"] = identity.MaxStringCodePoints
			}
		}
		for _, child := range v {
			boundSchemaText(child)
		}
	case []any:
		for _, child := range v {
			boundSchemaText(child)
		}
	}
}

var advanceSchemaOnce sync.Once
var compiledAdvanceSchema *jsonschema.Schema
var advanceSchemaError error

// Validate the advertised wire schema before mapping Go zero values. In
// particular, missing/null blocking flags must never become nonblocking facts.
func validateAdvanceArguments(raw []byte) error {
	advanceSchemaOnce.Do(func() {
		encoded, err := json.Marshal(tools()[1].(map[string]any)["inputSchema"])
		if err != nil {
			advanceSchemaError = err
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
		if err != nil {
			advanceSchemaError = err
			return
		}
		compiler := jsonschema.NewCompiler()
		compiler.DefaultDraft(jsonschema.Draft2020)
		const uri = "https://ai-control-plane.invalid/workflow-advance"
		if err = compiler.AddResource(uri, doc); err != nil {
			advanceSchemaError = err
			return
		}
		compiledAdvanceSchema, advanceSchemaError = compiler.Compile(uri)
	})
	if advanceSchemaError != nil {
		return advanceSchemaError
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	return compiledAdvanceSchema.Validate(value)
}

// Serve implements bounded newline-delimited stdio MCP, without HTTP or a gateway.
func Serve(ctx context.Context, r *orchestrator.Runtime, input io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), identity.MaxJSONBytes+1)
	encoder := json.NewEncoder(output)
	initialized, ready := false, false
	for ctx.Err() == nil && scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var q request
		if err := decode(scanner.Bytes(), &q); err != nil || q.JSONRPC != "2.0" {
			id := json.RawMessage("null")
			code, message := -32700, "invalid JSON-RPC message"
			// After rejection, recover only a valid raw correlation ID. No rejected
			// arguments are decoded or forwarded to Orchestrator. Duplicate keys keep
			// a null ID because their correlation is ambiguous.
			if err != nil && utf8.Valid(scanner.Bytes()) && json.Valid(scanner.Bytes()) && !strings.Contains(err.Error(), "duplicate JSON") {
				var envelope struct {
					ID json.RawMessage `json:"id"`
				}
				if json.Unmarshal(scanner.Bytes(), &envelope) == nil && identity.CheckJSONEnvelope(envelope.ID) == nil {
					var value any
					if json.Unmarshal(envelope.ID, &value) == nil {
						switch value.(type) {
						case string, float64:
							id = envelope.ID
							code, message = -32602, descriptiveLengthMessage(err)
						}
					}
				}
			}
			if err := encoder.Encode(response{JSONRPC: "2.0", ID: id, Error: &rpcError{code, message}}); err != nil {
				return err
			}
			continue
		}
		if len(q.ID) == 0 {
			if q.Method == "notifications/initialized" && initialized {
				ready = true
			}
			continue
		}
		var id any
		if err := json.Unmarshal(q.ID, &id); err != nil {
			return err
		}
		switch id.(type) {
		case string, float64:
		default:
			if err := encoder.Encode(response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{-32600, "request ID must be string or number"}}); err != nil {
				return err
			}
			continue
		}
		reply := response{JSONRPC: "2.0", ID: q.ID}
		switch q.Method {
		case "initialize":
			var p struct {
				ProtocolVersion string         `json:"protocolVersion"`
				Capabilities    map[string]any `json:"capabilities"`
				ClientInfo      map[string]any `json:"clientInfo"`
			}
			if initialized || decode(q.Params, &p) != nil {
				reply.Error = &rpcError{-32602, "invalid initialize"}
				break
			}
			version := p.ProtocolVersion
			switch version {
			case "2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25":
			default:
				version = "2025-11-25"
			}
			initialized = true
			reply.Result = map[string]any{"protocolVersion": version, "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "marc", "version": "0.1.0"}, "instructions": "Supply the MCP registration name as mcp_client_name when starting a workflow, not clientInfo.name. Follow the workflow-scoped instructions returned by workflow tools. Use only workflow tools for state transitions. Perform coding externally. Never fabricate usage; internal Codex requests are excluded. Use fresh role contexts and structured results, no transcript propagation."}
		case "ping":
			reply.Result = map[string]any{}
		case "tools/list":
			if !ready {
				reply.Error = &rpcError{-32000, "initialize first"}
			} else {
				reply.Result = map[string]any{"tools": tools()}
			}
		case "tools/call":
			if !ready {
				reply.Error = &rpcError{-32000, "initialize first"}
				break
			}
			var c call
			if err := decode(q.Params, &c); err != nil {
				reply.Error = &rpcError{-32602, "invalid tool call"}
				break
			}
			var result orchestrator.ExternalView
			var err error
			switch c.Name {
			case "workflow_start":
				var p orchestrator.ExternalStart
				if err = decode(c.Arguments, &p); err == nil {
					result, err = r.ExternalStart(ctx, p)
				}
			case "workflow_advance":
				var p advance
				if err = decode(c.Arguments, &p); err == nil {
					if err = validateAdvanceArguments(c.Arguments); err == nil {
						result, err = r.ExternalAdvance(ctx, p.RunID, p.Submission)
					}
				}
			case "workflow_inspect":
				var p inspectArg
				if err = decode(c.Arguments, &p); err == nil {
					if p.History == nil {
						result, err = r.ExternalInspect(ctx, p.RunID)
					} else {
						result, err = r.ExternalInspectHistory(ctx, p.RunID, *p.History)
					}
				}
			case "workflow_complete":
				var p runArg
				if err = decode(c.Arguments, &p); err == nil {
					result, err = r.ExternalComplete(ctx, p.RunID)
				}
			default:
				reply.Error = &rpcError{-32602, "unknown tool"}
			}
			if reply.Error != nil {
				break
			}
			text := ""
			if err != nil {
				text = err.Error()
			} else {
				raw, e := json.Marshal(result)
				if e != nil {
					return e
				}
				text = string(raw)
			}
			reply.Result = map[string]any{"content": []any{map[string]string{"type": "text", "text": text}}, "isError": err != nil}
		default:
			reply.Error = &rpcError{-32601, fmt.Sprintf("unsupported method %s", q.Method)}
		}
		if err := encoder.Encode(reply); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return scanner.Err()
}
