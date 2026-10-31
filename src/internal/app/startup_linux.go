// Package app owns startup and passes the private StateStore only to control-plane code.
package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"ai-control-plane/src/internal/statestore"
)

func ResolveDataDir(explicit string) (string, error) {
	if explicit != "" {
		return filepath.Abs(explicit)
	}
	base := os.Getenv("XDG_DATA_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		base = filepath.Join(home, ".local", "share")
	}
	if !filepath.IsAbs(base) {
		return "", fmt.Errorf("XDG_DATA_HOME must be absolute")
	}
	return filepath.Join(base, "ai-control-plane"), nil
}

func Start(ctx context.Context, explicitDataDir string, options ...statestore.OpenOptions) (*statestore.StateStore, error) {
	dir, err := ResolveDataDir(explicitDataDir)
	if err != nil {
		return nil, err
	}
	return statestore.Open(ctx, dir, options...)
}
