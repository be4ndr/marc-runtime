package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"marc-runtime/src/internal/orchestrator"
	"marc-runtime/src/internal/statestore"
	"golang.org/x/sys/unix"
)

// Probe from a separate process: opening/closing these files in the MCP
// process would itself release the locks this regression is checking.
func TestMCPWorktreeSQLiteLockProbeProcess(t *testing.T) {
	dir := os.Getenv("ACP_QA_SQLITE_LOCK_DIR")
	if dir == "" {
		return
	}
	var owners []int32
	for _, suffix := range []string{"", "-shm"} {
		fd, err := unix.Open(filepath.Join(dir, statestore.DatabaseName+suffix), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: unix.SEEK_SET}
		err = unix.FcntlFlock(uintptr(fd), unix.F_GETLK, &lock)
		closeErr := unix.Close(fd)
		if err != nil || closeErr != nil {
			fmt.Fprintln(os.Stderr, err, closeErr)
			os.Exit(1)
		}
		owners = append(owners, lock.Pid)
	}
	if err := json.NewEncoder(os.Stdout).Encode(owners); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func TestMCPProtectedDataPreservesSQLiteLocks(t *testing.T) {
	repo := t.TempDir()
	if err := os.Chmod(repo, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) {
		t.Helper()
		if raw, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git: %v: %s", err, raw)
		}
	}
	git("init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "task.txt"), []byte("fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "task.txt")
	git("-c", "user.name=QA", "-c", "user.email=qa@example.invalid", "commit", "-qm", "fixture")
	data := filepath.Join(repo, "state")
	c := client(t, data)
	assertLocks := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMCPWorktreeSQLiteLockProbeProcess$")
		cmd.Env = append(os.Environ(), "ACP_QA_SQLITE_LOCK_DIR="+data)
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("lock probe: %v: %s", err, raw)
		}
		var owners []int32
		if err := json.Unmarshal(raw, &owners); err != nil || len(owners) != 2 {
			t.Fatalf("lock probe reply %s: %v", raw, err)
		}
		for i, owner := range owners {
			if owner != int32(c.cmd.Process.Pid) {
				t.Fatalf("worktree capture released SQLite lock %d: owner=%d, want MCP pid=%d", i, owner, c.cmd.Process.Pid)
			}
		}
	}
	assertLocks()
	v, bad := c.tool(t, "workflow_start", map[string]any{"run_id": "protected-layout", "goal": "Verify fixture without reading protected data.", "worktree_path": repo, "mcp_client_name": "fixture-control", "approval_response": "Approved QA fixture."})
	if bad {
		t.Fatal("worktree containing protected data was rejected")
	}
	assertLocks()
	peer := client(t, data)
	inspect := func(p *testClient) {
		t.Helper()
		got, bad := p.tool(t, "workflow_inspect", map[string]any{"run_id": v.RunID})
		if bad || !reflect.DeepEqual(v, got) {
			t.Fatalf("divergent protected workflow: got=%+v want=%+v", got, v)
		}
	}
	advance := func(sub *orchestrator.ExternalSubmission) {
		t.Helper()
		args := map[string]any{"run_id": v.RunID}
		if sub != nil {
			args["submission"] = sub
		}
		v, bad = c.tool(t, "workflow_advance", args)
		if bad {
			t.Fatal("protected workflow advance failed")
		}
	}
	input := func() *orchestrator.ExternalSubmission {
		return &orchestrator.ExternalSubmission{InvocationID: v.Step.InvocationID, OwnershipEpoch: v.OwnershipEpoch, ActorRevision: v.ActorRevision, SemanticRevision: v.SemanticRevision, Summary: "Independently verified task.txt fixture."}
	}
	inspect(peer)
	sub := input()
	sub.Decision = "DEVELOPER"
	advance(sub)
	inspect(peer)
	if err := peer.in.Close(); err != nil {
		t.Fatal(err)
	}
	if err := peer.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	assertLocks()
	advance(nil)
	peer = client(t, data)
	inspect(peer) // Reproduce the original peer-exit/restart boundary.
	for _, role := range []string{"DEVELOPER", "QA", "REVIEWER"} {
		if v.Step.Role == "LEAD" {
			sub = input()
			sub.Decision = role
			advance(sub)
			advance(nil)
		}
		sub = input()
		sub.Outcome = map[string]string{"DEVELOPER": "completed", "QA": "passed", "REVIEWER": "approved"}[role]
		sub.Evidence = "Read task.txt and verified fixture bytes."
		if role == "QA" {
			sub.Checks = []orchestrator.ExternalCheck{{CheckID: "fixture", Required: true, Outcome: "passed", Summary: "Checked fixture bytes."}}
		}
		advance(sub)
		inspect(peer)
		advance(nil)
	}
	sub = input()
	sub.Decision = "APPROVE"
	advance(sub)
	v, bad = c.tool(t, "workflow_complete", map[string]any{"run_id": v.RunID})
	if bad || v.Status != "READY_FOR_HUMAN_MERGE" {
		t.Fatal("protected workflow completion failed", v)
	}
	inspect(peer)
	for _, p := range []*testClient{peer, c} {
		if err := p.in.Close(); err != nil {
			t.Fatal(err)
		}
		if err := p.cmd.Wait(); err != nil {
			t.Fatal(err)
		}
	}
	c = client(t, data)
	inspect(c) // Complete restart retains the same proof and fences.
	assertLocks()
	if err := c.in.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
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
	if err := rows.Err(); err != nil {
		t.Error(err)
	}
	if err := rows.Close(); err != nil {
		t.Error(err)
	}
}
