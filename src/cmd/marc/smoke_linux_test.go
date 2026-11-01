package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"marc-runtime/src/internal/statestore"
)

// CI builds once before the suite. Normal startup storage checks remain enabled.
func TestCLIExecutableSmoke(t *testing.T) {
	binary := os.Getenv("MARC_TEST_BINARY")
	if binary == "" {
		t.Skip("set MARC_TEST_BINARY to the absolute path of a built marc executable")
	}
	dataDir := func(t *testing.T) string {
		t.Helper()
		dir := t.TempDir()
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	for _, signal := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(signal.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "--data-dir", dataDir(t))
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cancel()
				if cmd.ProcessState == nil {
					_ = cmd.Wait()
				}
			})
			line, err := bufio.NewReader(out).ReadString('\n')
			if err != nil || !strings.HasPrefix(line, "Persistence ready:") {
				cancel()
				_ = cmd.Wait()
				t.Fatalf("startup: %q: %v: %s", line, err, &stderr)
			}
			if err := cmd.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Wait(); err != nil {
				t.Fatalf("shutdown: %v: %s", err, &stderr)
			}
		})
	}
	t.Run("fake workflow and resume", func(t *testing.T) {
		dir := dataDir(t)
		var terminal statestore.ProofReference
		for i, args := range [][]string{
			{"--fake-run", "cli-smoke", "--goal", "Verify the deterministic smoke artifact.", "--normal-tokens", "1000", "--approve-budget", "I approve 1000 normal WORK tokens."},
			{"--resume", "cli-smoke"},
		} {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			cmd := exec.CommandContext(ctx, binary, append([]string{"--data-dir", dir}, args...)...)
			raw, err := cmd.CombinedOutput()
			cancel()
			if err != nil {
				t.Fatalf("workflow: %v: %s", err, raw)
			}
			// CLI readiness and the initial approval prompt precede the JSON report.
			lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
			var report struct {
				RunID, Status        string
				Terminal             statestore.ProofReference
				AccountingIncomplete bool
				AccountingStatus     string
			}
			if err := json.Unmarshal(lines[len(lines)-1], &report); err != nil {
				t.Fatalf("report: %v: %s", err, raw)
			}
			if report.RunID != "cli-smoke" || report.Status != "READY_FOR_HUMAN_MERGE" || report.Terminal.ProofID == "" || report.AccountingIncomplete || report.AccountingStatus != "complete" {
				t.Fatalf("unexpected report: %+v", report)
			}
			if i == 0 {
				terminal = report.Terminal
			} else if report.Terminal != terminal {
				t.Fatalf("resume changed terminal proof: %+v != %+v", report.Terminal, terminal)
			}
		}
	})
}
