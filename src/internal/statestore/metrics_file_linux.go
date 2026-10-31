package statestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// MetricsProjectionError means the authoritative mutation has committed, but
// its observational refresh failed. Retry RefreshMetrics, not the mutation.
type MetricsProjectionError struct {
	RunID string
	Err   error
}

func (e *MetricsProjectionError) Error() string {
	return fmt.Sprintf("run %s state committed; metrics refresh failed: %v", e.RunID, e.Err)
}
func (e *MetricsProjectionError) Unwrap() error { return e.Err }

type metricsBinding struct {
	mu  sync.Mutex // Snapshot and publication order for this Run only.
	dir string     // Changed under both binding.mu and StateStore.metricsMu.
}

// EnableMetricsProjection binds an existing run to its controller-configured
// artifact directory and immediately refreshes from SQLite. Bindings are local
// configuration, re-established after restart; metrics files are never imported.
// A failed initial refresh leaves the binding enabled for a later refresh.
func (s *StateStore) EnableMetricsProjection(ctx context.Context, runID, runDir string) error {
	if err := requireIDs(struct{ name, value string }{"run_id", runID}); err != nil {
		return err
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, "SELECT 1 FROM runs WHERE run_id=?", runID).Scan(&exists); err != nil {
		return err
	}
	dir, err := prepareDataDir(runDir)
	if err != nil {
		return err
	}
	s.metricsMu.Lock()
	if s.metricsDirs == nil {
		s.metricsDirs = map[string]*metricsBinding{}
	}
	binding := s.metricsDirs[runID]
	if binding == nil {
		binding = &metricsBinding{}
		s.metricsDirs[runID] = binding
	}
	s.metricsMu.Unlock()
	binding.mu.Lock()
	defer binding.mu.Unlock()
	s.metricsMu.Lock()
	for other, bound := range s.metricsDirs {
		if other != runID && bound.dir == dir {
			s.metricsMu.Unlock()
			return fmt.Errorf("metrics directory already bound to run %s", other)
		}
	}
	binding.dir = dir
	s.metricsMu.Unlock()
	return s.projectMetrics(ctx, runID, dir)
}

// RefreshMetrics regenerates an enabled projection. Success or failure cannot
// change authoritative state. No automatic workflow or retry is performed.
func (s *StateStore) RefreshMetrics(ctx context.Context, runID string) error {
	if err := requireIDs(struct{ name, value string }{"run_id", runID}); err != nil {
		return err
	}
	return s.refreshMetrics(ctx, runID, true, s.projectMetrics)
}

func (s *StateStore) refreshEnabledMetrics(ctx context.Context, runID string) error {
	return s.refreshMetrics(ctx, runID, false, s.projectMetrics)
}

func (s *StateStore) refreshMetrics(ctx context.Context, runID string, required bool, project func(context.Context, string, string) error) error {
	s.metricsMu.Lock()
	binding := s.metricsDirs[runID]
	s.metricsMu.Unlock()
	if binding != nil {
		binding.mu.Lock()
		defer binding.mu.Unlock()
		if binding.dir != "" {
			return project(ctx, runID, binding.dir)
		}
	}
	if required {
		return fmt.Errorf("metrics projection is not enabled for run %s", runID)
	}
	return nil
}

func (s *StateStore) projectMetrics(ctx context.Context, runID, dir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Lock before taking the snapshot, through replacement. Separate StateStore
	// instances cannot publish an older snapshot over a newer one. Contention is
	// a refresh failure, not a blocking workflow or an authoritative failure.
	lock, err := os.OpenFile(filepath.Join(dir, ".metrics.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return fmt.Errorf("open metrics lock: %w", err)
	}
	defer lock.Close()
	if info, err := lock.Stat(); err != nil || !info.Mode().IsRegular() {
		if err != nil {
			return err
		}
		return fmt.Errorf("metrics lock must be a regular file")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("lock metrics projection: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	source, err := s.metricsSnapshot(ctx, runID)
	if err != nil {
		return fmt.Errorf("read metrics snapshot: %w", err)
	}
	raw, err := renderMetrics(source, time.Now())
	if err != nil {
		return fmt.Errorf("render metrics: %w", err)
	}
	return replaceMetricsFile(ctx, dir, raw, os.Rename)
}

// Replacement stays on one filesystem. Sync the complete temporary file before
// rename and the directory afterward. Failures before rename preserve old bytes;
// a directory-sync failure may leave the new complete projection visible.
func replaceMetricsFile(ctx context.Context, dir string, raw []byte, rename func(string, string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".metrics-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	defer f.Close()
	if _, err := f.Write(raw); err != nil {
		return fmt.Errorf("write metrics temporary file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync metrics temporary file: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := rename(tmp, filepath.Join(dir, "metrics.json")); err != nil {
		return fmt.Errorf("replace metrics: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync metrics directory: %w", err)
	}
	return nil
}
