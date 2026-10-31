package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"ai-control-plane/src/internal/orchestrator"
	"ai-control-plane/src/internal/statestore"
)

// In-process stdio transport includes MCP parsing, Orchestrator, SQLite, Git and
// artifact work in CPU/allocation profiles. It never invokes a model.
type benchmarkClient struct {
	input     io.WriteCloser
	output    *bufio.Reader
	id        int
	latencies map[string][]time.Duration
}

func (c *benchmarkClient) rpc(b testing.TB, method string, params any) json.RawMessage {
	b.Helper()
	c.id++
	if err := json.NewEncoder(c.input).Encode(map[string]any{"jsonrpc": "2.0", "id": c.id, "method": method, "params": params}); err != nil {
		b.Fatal(err)
	}
	raw, err := c.output.ReadBytes('\n')
	if err != nil {
		b.Fatal(err)
	}
	var reply struct {
		ID     int
		Result json.RawMessage
		Error  any
	}
	if err = json.Unmarshal(raw, &reply); err != nil || reply.ID != c.id || reply.Error != nil {
		b.Fatal(string(raw), err)
	}
	return reply.Result
}
func (c *benchmarkClient) tool(b testing.TB, name string, args any) orchestrator.ExternalView {
	b.Helper()
	start := time.Now()
	raw := c.rpc(b, "tools/call", map[string]any{"name": name, "arguments": args})
	var result struct {
		IsError bool
		Content []struct{ Text string }
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.IsError {
		b.Fatal(string(raw), err)
	}
	var view orchestrator.ExternalView
	if err := json.Unmarshal([]byte(result.Content[0].Text), &view); err != nil {
		b.Fatal(err)
	}
	c.latencies[name] = append(c.latencies[name], time.Since(start))
	return view
}

func benchmarkWorkflowFixture(b testing.TB, files int) (*benchmarkClient, *statestore.StateStore, string, string) {
	b.Helper()
	base := os.Getenv("ACP_B6_BENCH_ROOT")
	var root string
	if base == "" {
		root = b.TempDir()
	} else {
		var err error
		root, err = os.MkdirTemp(base, "workflow-")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(root) })
	}
	repo, tree, data := filepath.Join(root, "repo"), filepath.Join(root, "worktree"), filepath.Join(root, "data")
	if err := os.Mkdir(repo, 0700); err != nil {
		b.Fatal(err)
	}
	git := func(args ...string) {
		b.Helper()
		raw, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput()
		if err != nil {
			b.Fatal(err, string(raw))
		}
	}
	git("init", "-q")
	for n := 0; n < files; n++ {
		name := fmt.Sprintf("source-%04d.go", n)
		raw := append([]byte("package fixture\n// "), bytes.Repeat([]byte("x"), 480)...)
		raw = append(raw, '\n')
		if err := os.WriteFile(filepath.Join(repo, name), raw, 0600); err != nil {
			b.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "task.txt"), []byte("before\n"), 0600); err != nil {
		b.Fatal(err)
	}
	git("add", ".")
	git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "benchmark fixture")
	git("worktree", "add", "--detach", tree, "HEAD")
	ctx := statestore.WithMutationActor(context.Background(), "ORCHESTRATOR")
	s, err := statestore.Open(ctx, data, statestore.OpenOptions{AllowVolatileForTests: base == ""})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	r, err := orchestrator.New(s)
	if err != nil {
		b.Fatal(err)
	}
	requestReader, requestWriter := io.Pipe()
	responseReader, responseWriter := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, r, requestReader, responseWriter); responseWriter.Close() }()
	b.Cleanup(func() {
		requestWriter.Close()
		responseReader.Close()
		if err := <-done; err != nil {
			b.Error(err)
		}
	})
	c := &benchmarkClient{input: requestWriter, output: bufio.NewReader(responseReader), latencies: map[string][]time.Duration{}}
	c.rpc(b, "initialize", map[string]any{"protocolVersion": "2025-11-25", "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "benchmark", "version": "1"}})
	if err := json.NewEncoder(c.input).Encode(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}); err != nil {
		b.Fatal(err)
	}
	return c, s, tree, data
}

func benchmarkWorkflow(b testing.TB, c *benchmarkClient, tree, run string) orchestrator.ExternalView {
	b.Helper()
	v := c.tool(b, "workflow_start", map[string]any{"run_id": run, "goal": "Set task.txt to after newline; independently check and review it.", "worktree_path": tree, "mcp_client_name": "fixture-control", "approval_response": "I approve this deterministic benchmark fixture."})
	submit := func(decision, outcome string) {
		b.Helper()
		in := orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Summary: "Verified actual fixture content.", Decision: decision, Outcome: outcome}
		if outcome != "" {
			in.Evidence = "Read task.txt and inspected its exact change."
		}
		if v.Step.Role == "QA" {
			raw, err := os.ReadFile(filepath.Join(tree, "task.txt"))
			if err != nil || string(raw) != "after\n" {
				b.Fatal("actual QA check failed", err)
			}
			in.Checks = []orchestrator.ExternalCheck{{CheckID: "exact-content", Required: true, Outcome: "passed", Summary: "Exact bytes independently checked."}}
		}
		v = c.tool(b, "workflow_advance", map[string]any{"run_id": run, "submission": in})
	}
	for _, role := range []string{"DEVELOPER", "QA", "REVIEWER"} {
		submit(role, "")
		v = c.tool(b, "workflow_advance", map[string]any{"run_id": run})
		if role == "DEVELOPER" {
			if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600); err != nil {
				b.Fatal(err)
			}
		}
		submit("", map[string]string{"DEVELOPER": "completed", "QA": "passed", "REVIEWER": "approved"}[role])
		v = c.tool(b, "workflow_advance", map[string]any{"run_id": run})
	}
	submit("APPROVE", "")
	v = c.tool(b, "workflow_complete", map[string]any{"run_id": run})
	if v.Status != "READY_FOR_HUMAN_MERGE" || v.Terminal.ProofID == "" {
		b.Fatal(v)
	}
	return v
}

func reportLatencies(b *testing.B, name string, values []time.Duration) {
	values = slices.Clone(values)
	slices.Sort(values)
	for _, q := range []struct {
		name    string
		percent int
	}{{"p50", 50}, {"p95", 95}, {"p99", 99}} {
		index := (len(values)*q.percent+99)/100 - 1
		if index >= 0 {
			b.ReportMetric(float64(values[index])/float64(time.Millisecond), name+"-"+q.name+"-ms")
		}
	}
}

func BenchmarkMCPWorkflow(b *testing.B) {
	for _, files := range []int{1, 64, 512} {
		b.Run(fmt.Sprintf("files-%d", files), func(b *testing.B) {
			c, _, tree, _ := benchmarkWorkflowFixture(b, files)
			c.latencies = map[string][]time.Duration{}
			b.ReportAllocs()
			b.ResetTimer()
			var times []time.Duration
			for n := 0; n < b.N; n++ {
				start := time.Now()
				benchmarkWorkflow(b, c, tree, fmt.Sprintf("workflow-%d", n))
				times = append(times, time.Since(start))
			}
			b.StopTimer()
			reportLatencies(b, "workflow", times)
			for name, values := range c.latencies {
				reportLatencies(b, name, values)
			}
		})
	}
}

func BenchmarkMCPInspectAndReplay(b *testing.B) {
	for _, files := range []int{1, 64, 512} {
		for _, tool := range []string{"workflow_inspect", "workflow_complete"} {
			b.Run(fmt.Sprintf("files-%d/%s", files, tool), func(b *testing.B) {
				c, _, tree, _ := benchmarkWorkflowFixture(b, files)
				v := benchmarkWorkflow(b, c, tree, "replay-run")
				c.latencies = map[string][]time.Duration{}
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					next := c.tool(b, tool, map[string]any{"run_id": v.RunID})
					if next.Terminal != v.Terminal {
						b.Fatal("replay changed proof")
					}
				}
				b.StopTimer()
				reportLatencies(b, tool, c.latencies[tool])
			})
		}
	}
}

func BenchmarkWorkflowRestart(b *testing.B) {
	for _, runs := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("runs-%d", runs), func(b *testing.B) {
			c, store, tree, data := benchmarkWorkflowFixture(b, 1)
			for n := 0; n < runs; n++ {
				benchmarkWorkflow(b, c, tree, fmt.Sprintf("restart-%d", n))
			}
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(data, statestore.DatabaseName))
			if err != nil {
				b.Fatal(err)
			}
			ctx := statestore.WithMutationActor(context.Background(), "ORCHESTRATOR")
			b.ReportAllocs()
			b.ResetTimer()
			var times []time.Duration
			for n := 0; n < b.N; n++ {
				start := time.Now()
				s, err := statestore.Open(ctx, data)
				if err != nil {
					b.Fatal(err)
				}
				if err = s.Close(); err != nil {
					b.Fatal(err)
				}
				times = append(times, time.Since(start))
			}
			b.StopTimer()
			b.ReportMetric(float64(info.Size())/1024, "database-KiB")
			reportLatencies(b, "restart", times)
		})
	}
}
