package mcp

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"ai-control-plane/src/internal/orchestrator"
	"ai-control-plane/src/internal/statestore"
)

func TestMCPSharedDatabasePeerExitAndRestart(t *testing.T) {
	a, data, tree := mcpWorktreeFixture(t)
	v, bad := a.tool(t, "workflow_start", map[string]any{"run_id": "shared-mcp", "goal": "Change task.txt to after.", "worktree_path": tree, "mcp_client_name": "fixture-control", "approval_response": "I approve this task."})
	if bad {
		t.Fatal("start rejected")
	}
	b := client(t, data)
	inspect := func(c *testClient) {
		t.Helper()
		got, bad := c.tool(t, "workflow_inspect", map[string]any{"run_id": v.RunID})
		if bad || !reflect.DeepEqual(got, v) {
			t.Fatalf("divergent authoritative view:\n got: %+v\nwant: %+v", got, v)
		}
	}
	closeClient := func(c *testClient) {
		t.Helper()
		if err := c.in.Close(); err != nil {
			t.Fatal(err)
		}
		if err := c.cmd.Wait(); err != nil {
			t.Fatal(err, c.stderr.String())
		}
	}
	advance := func(c *testClient, submission *orchestrator.ExternalSubmission) {
		t.Helper()
		args := map[string]any{"run_id": v.RunID}
		if submission != nil {
			args["submission"] = submission
		}
		var bad bool
		v, bad = c.tool(t, "workflow_advance", args)
		if bad {
			t.Fatal("advance rejected")
		}
	}
	inspect(a)
	inspect(b)
	stale := &orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Decision: "DEVELOPER", Summary: "Implement the approved task."}
	advance(a, stale)
	inspect(b)
	// Closing one independent MCP process must leave its peer on the same WAL.
	closeClient(b)
	advance(a, nil)
	b = client(t, data)
	inspect(b)
	if _, bad := b.tool(t, "workflow_advance", map[string]any{"run_id": v.RunID, "submission": &orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision - 1, SemanticRevision: v.SemanticRevision, Outcome: "completed", Summary: "Stale revision", Evidence: "stale"}}); !bad {
		t.Fatal("stale cross-process revision accepted")
	}
	inspect(a)
	inspect(b)
	if err := os.WriteFile(filepath.Join(tree, "task.txt"), []byte("after\n"), 0600); err != nil {
		t.Fatal(err)
	}
	advance(b, &orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Outcome: "completed", Summary: "Changed task.txt.", Evidence: "Read task.txt: after."})
	inspect(a)
	if err := b.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := b.cmd.Wait(); err == nil {
		t.Fatal("SIGKILL unexpectedly succeeded")
	}
	inspect(a)
	advance(a, nil)
	closeClient(a)
	// All MCP connections are gone. A complete restart must retain exactly the
	// acknowledged state, including accepted result/evidence and current fences.
	a = client(t, data)
	b = client(t, data)
	inspect(a)
	inspect(b)
	for _, name := range []string{statestore.DatabaseName, statestore.DatabaseName + "-wal", statestore.DatabaseName + "-shm"} {
		info, err := os.Lstat(filepath.Join(data, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			t.Fatalf("SQLite file security: %s: %v %v", name, info, err)
		}
	}
	closeClient(a)
	closeClient(b)
	db, err := sql.Open("sqlite", filepath.Join(data, statestore.DatabaseName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var integrity string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity_check: %s: %v", integrity, err)
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Error("foreign_key_check returned a violation")
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatal(err)
	}
}

type unreadMCPInput struct{ t *testing.T }

func (r unreadMCPInput) Read([]byte) (int, error) {
	r.t.Fatal("shutdown accepted another read")
	return 0, nil
}

func TestMCPCanceledBeforeRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Serve(ctx, nil, unreadMCPInput{t}, os.Stdout); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
