package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"marc-runtime/src/internal/statestore"
)

func TestStartupAutomaticallyInitializes(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_DATA_HOME", base)
	s, err := Start(statestore.WithMutationActor(context.Background(), "ORCHESTRATOR"), "", statestore.OpenOptions{AllowVolatileForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	want := filepath.Join(base, "ai-control-plane", statestore.DatabaseName)
	if s.Info().Path != want {
		t.Fatalf("path %q, want %q", s.Info().Path, want)
	}
	if _, err := os.Stat(want); err != nil {
		t.Fatal(err)
	}
}

func TestDataDirResolution(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("HOME", t.TempDir())
	want := filepath.Join(os.Getenv("HOME"), ".local", "share", "ai-control-plane")
	if got, err := ResolveDataDir(""); err != nil || got != want {
		t.Fatalf("default %q: %v", got, err)
	}
	t.Setenv("XDG_DATA_HOME", "relative")
	if _, err := ResolveDataDir(""); err == nil {
		t.Fatal("relative XDG_DATA_HOME accepted")
	}
	explicit := t.TempDir()
	if got, err := ResolveDataDir(explicit); err != nil || got != explicit {
		t.Fatal("explicit directory lost", err)
	}
}
