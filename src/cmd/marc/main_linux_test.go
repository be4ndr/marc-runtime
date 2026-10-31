package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"marc-runtime/src/internal/app"
	"marc-runtime/src/internal/orchestrator"
	"marc-runtime/src/internal/statestore"
)

type cancelWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *cancelWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.cancel()
	return n, err
}

func TestCommandStartupAndClose(t *testing.T) {
	ctx, cancel := context.WithCancel(statestore.WithMutationActor(context.Background(), "ORCHESTRATOR"))
	defer cancel()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	out := &cancelWriter{cancel: cancel}
	if err := run(ctx, []string{"--data-dir", dir}, out, statestore.OpenOptions{AllowVolatileForTests: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Persistence ready:") {
		t.Fatal("no readiness after validation")
	}
	s, err := app.Start(statestore.WithMutationActor(context.Background(), "ORCHESTRATOR"), dir, statestore.OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		t.Fatal("closed command database cannot restart", err)
	}
	s.Close()
}

func TestCommandFailsBeforeReadiness(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, statestore.DatabaseName), []byte("not a database"), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run(statestore.WithMutationActor(context.Background(), "ORCHESTRATOR"), []string{"--data-dir", dir}, &output); err == nil {
		t.Fatal("corrupt database accepted")
	}
	if strings.Contains(output.String(), "Persistence ready:") {
		t.Fatal("failed startup reported ready")
	}
}

func TestCommandRequiresExplicitOrchestratorActor(t *testing.T) {
	for _, ctx := range []context.Context{context.Background(), statestore.WithMutationActor(context.Background(), "QA")} {
		dir := filepath.Join(t.TempDir(), "not-created")
		var output bytes.Buffer
		if err := run(ctx, []string{"--data-dir", dir}, &output); !errors.Is(err, statestore.ErrMutationAuthority) {
			t.Fatalf("startup promoted an unauthorized caller: %v", err)
		}
		if output.Len() != 0 {
			t.Fatal("unauthorized startup reported readiness")
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("unauthorized startup created state: %v", err)
		}
	}
}

func TestMain(m *testing.M) {
	if os.Getenv("ACP_FAKE_CHILD") == "1" {
		if err := orchestrator.FakeWorker(os.Stdin, os.Stdout); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
func TestCommandFakeWorkflowAndResume(t *testing.T) {
	ctx := statestore.WithMutationActor(context.Background(), "ORCHESTRATOR")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	args := []string{"--data-dir", dir, "--fake-run", "cli-fake", "--goal", "Verify this fake task.", "--normal-tokens", "1000", "--approve-budget", "I approve 1000 normal WORK tokens."}
	if err := run(ctx, args, &output, statestore.OpenOptions{AllowVolatileForTests: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "READY_FOR_HUMAN_MERGE") {
		t.Fatal(output.String())
	}
	output.Reset()
	if err := run(ctx, []string{"--data-dir", dir, "--resume", "cli-fake"}, &output, statestore.OpenOptions{AllowVolatileForTests: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "READY_FOR_HUMAN_MERGE") {
		t.Fatal(output.String())
	}
}
