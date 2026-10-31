package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"ai-control-plane/src/internal/statestore"
	"golang.org/x/sys/unix"
)

// Exercise run's real stdio shutdown and StateStore ownership. Check descriptors
// before exiting: OS process cleanup must not hide a leaked SQLite pool/root.
func TestMCPCommandShutdownProcess(t *testing.T) {
	dir := os.Getenv("ACP_COMMAND_SHUTDOWN_DATA")
	if dir == "" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	ctx = statestore.WithMutationActor(ctx, "ORCHESTRATOR")
	err := run(ctx, []string{"--mcp", "--data-dir", dir}, os.Stdout, statestore.OpenOptions{AllowVolatileForTests: true})
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, entry := range entries {
		target, _ := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if target == dir || strings.HasPrefix(target, dir+"/") {
			fmt.Fprintln(os.Stderr, "StateStore descriptor leaked after shutdown:", target)
			os.Exit(1)
		}
	}
	os.Exit(0)
}

func TestMCPCommandGracefulShutdownAndRestart(t *testing.T) {
	for _, exit := range []string{"eof", "SIGINT", "SIGTERM", "SIGTERM-backpressure"} {
		t.Run(exit, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			for restart := 0; restart < 2; restart++ {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMCPCommandShutdownProcess$")
				cmd.Env = append(os.Environ(), "ACP_COMMAND_SHUTDOWN_DATA="+dir)
				cmd.Stderr = os.Stderr
				in, err := cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				out, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					cancel()
					in.Close()
					if cmd.ProcessState == nil {
						_ = cmd.Wait()
					}
				})
				if _, err := io.WriteString(in, "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},\"clientInfo\":{}}}\n"); err != nil {
					t.Fatal(err)
				}
				var reply struct {
					ID     int
					Result json.RawMessage
					Error  any
				}
				if err := json.NewDecoder(out).Decode(&reply); err != nil || reply.ID != 1 || reply.Error != nil || len(reply.Result) == 0 {
					t.Fatalf("startup: %+v: %v", reply, err)
				}
				var written chan struct{}
				if exit == "SIGTERM-backpressure" {
					written = make(chan struct{})
					go func() {
						defer close(written)
						_, _ = io.WriteString(in, strings.Repeat("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n", 4096))
					}()
					fd := out.(*os.File).Fd()
					capacity, err := unix.FcntlInt(fd, unix.F_GETPIPE_SZ, 0)
					if err != nil {
						t.Fatal(err)
					}
					ticker := time.NewTicker(5 * time.Millisecond)
					defer ticker.Stop()
					for {
						available, err := unix.IoctlGetInt(int(fd), unix.TIOCINQ)
						if err != nil {
							t.Fatal(err)
						}
						if available >= capacity-capacity/16 {
							break // Leave the child's stdout full and unread.
						}
						select {
						case <-ticker.C:
						case <-ctx.Done():
							t.Fatal("stdout did not fill before deadline")
						}
					}
				}
				if exit == "eof" {
					if err := in.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					sig := syscall.SIGTERM
					if exit == "SIGINT" {
						sig = syscall.SIGINT
					}
					if err := cmd.Process.Signal(sig); err != nil {
						t.Fatal(err)
					}
				}
				if err := cmd.Wait(); err != nil {
					t.Fatalf("graceful %s shutdown: %v", exit, err)
				}
				if written != nil {
					<-written
				}
			}
		})
	}
}
