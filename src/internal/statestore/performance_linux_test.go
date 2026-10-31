package statestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

// Managed fake history exercises existing accounting paths. These rows are
// fixture-owned calls, never representations of external Codex requests.
func benchmarkManagedHistory(b *testing.B, n int) *StateStore {
	b.Helper()
	base := os.Getenv("ACP_B6_BENCH_ROOT")
	dir := b.TempDir()
	if base != "" {
		var err error
		dir, err = os.MkdirTemp(base, "managed-")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { os.RemoveAll(dir) })
	}
	s, err := Open(accountingContext, dir, OpenOptions{AllowVolatileForTests: base == ""})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	exec := func(q string, args ...any) {
		b.Helper()
		if _, err := s.db.Exec(q, args...); err != nil {
			b.Fatal(err)
		}
	}
	exec("INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES('bench-run','RUNNING',1,?,?)", semanticTimestamp, semanticTimestamp)
	cycles := min(n, 10)
	for c := 1; c <= cycles; c++ {
		exec("INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES('bench-run',?,'RUNNING',?)", c, semanticTimestamp)
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("bench-%d", i)
		cycle := i%cycles + 1
		exec("INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES(?,'bench-run',?,'DEVELOPER','IMPLEMENT','RUNNING',?)", id, cycle, semanticTimestamp)
		exec("INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES(?,?,'bench-run',?,1,'RUNNING',?)", id, id, cycle, semanticTimestamp)
		exec("INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch) VALUES(?,?,?,'bench-run',?,1,'DEVELOPER','IMPLEMENT','fake','benchmark','none','RUNNING',?,1)", id, id, id, cycle, semanticTimestamp)
		exec("INSERT INTO provider_calls(provider_call_id,invocation_id,call_ordinal,provider,model,reasoning_effort,started_at) VALUES(?,?,1,'fake','benchmark','none',?)", id, id, semanticTimestamp)
	}
	return s
}

// Measure an unbound Run's committed mutation while another Run publishes its
// real 1,000-call fake-history projection. The filesystem lock identifies the
// publication boundary without adding delays to production code.
func BenchmarkMetricsCrossRun(b *testing.B) {
	s := benchmarkManagedHistory(b, 1000)
	if _, err := s.db.Exec("INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES('other-run','CREATED',1,?,?)", semanticTimestamp, semanticTimestamp); err != nil {
		b.Fatal(err)
	}
	dir := filepath.Join(filepath.Dir(s.Info().Path), "projection")
	if err := s.EnableMetricsProjection(accountingContext, "bench-run", dir); err != nil {
		b.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".metrics.lock"), os.O_RDWR, 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer lock.Close()
	before := s.db.Stats()
	b.ReportAllocs()
	b.ResetTimer()
	var times []time.Duration
	for n := 0; n < b.N; n++ {
		done := make(chan error, 1)
		go func() { done <- s.RefreshMetrics(accountingContext, "bench-run") }()
		deadline := time.Now().Add(10 * time.Second)
		for {
			err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if errors.Is(err, syscall.EWOULDBLOCK) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
				b.Fatal(err)
			}
			select {
			case err := <-done:
				b.Fatal("publication ended before observation", err)
			default:
			}
			if time.Now().After(deadline) {
				b.Fatal("publication did not acquire file lock")
			}
			time.Sleep(time.Millisecond)
		}
		start := time.Now()
		if err := s.numericMutation(accountingContext, "benchmark", "other-run", nil, func(*sql.Conn) error { return nil }); err != nil {
			b.Fatal(err)
		}
		times = append(times, time.Since(start))
		if err := <-done; err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	slices.Sort(times)
	for _, q := range []struct {
		label   string
		percent int
	}{{"p50", 50}, {"p95", 95}, {"p99", 99}} {
		b.ReportMetric(float64(times[(len(times)*q.percent+99)/100-1])/float64(time.Millisecond), "other-run-"+q.label+"-ms")
	}
	b.ReportMetric(float64(s.db.Stats().WaitDuration-before.WaitDuration)/float64(time.Millisecond), "pool-wait-ms")
}

func BenchmarkStateStoreHistory(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("calls-%d", n), func(b *testing.B) {
			s := benchmarkManagedHistory(b, n)
			for _, query := range []string{`SELECT p.provider_call_id,i.invocation_id,c.receipt_state FROM provider_calls p JOIN invocations i USING(invocation_id) LEFT JOIN provider_call_usage c USING(provider_call_id) WHERE i.run_id='bench-run' ORDER BY p.invocation_id,p.call_ordinal`, `SELECT * FROM v_run_token_balance WHERE run_id='bench-run'`} {
				b.Log("EXPLAIN QUERY PLAN", query)
				plans, err := s.db.Query("EXPLAIN QUERY PLAN " + query)
				if err != nil {
					b.Fatal(err)
				}
				for plans.Next() {
					var a, c, d int
					var detail string
					if err := plans.Scan(&a, &c, &d, &detail); err != nil {
						b.Fatal(err)
					}
					b.Log(detail)
				}
				plans.Close()
			}
			for _, kind := range []string{"validate", "mutation", "snapshot", "render"} {
				b.Run(kind, func(b *testing.B) {
					source, err := s.metricsSnapshot(context.Background(), "bench-run")
					if err != nil {
						b.Fatal(err)
					}
					stats := s.db.Stats()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch kind {
						case "validate":
							err = validateRunNumbers(context.Background(), s.db, "bench-run")
						case "mutation":
							err = s.numericMutation(accountingContext, "benchmark", "bench-run", nil, func(*sql.Conn) error { return nil })
						case "snapshot":
							_, err = s.metricsSnapshot(context.Background(), "bench-run")
						case "render":
							_, err = renderMetrics(source, metricsTestTime)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					after := s.db.Stats()
					b.ReportMetric(float64(after.WaitDuration-stats.WaitDuration)/float64(time.Millisecond), "pool-wait-ms")
					if kind == "validate" || kind == "mutation" {
						b.ReportMetric(float64(1+min(n, 10)+3*n), "data-rows/validation-pass")
					}
				})
			}
		})
	}
}

// Concurrent read demand measures the connection/writer occupancy of actual
// conflict diagnostics. Timing is evidence, not a correctness threshold.
func TestB6ConflictLockMeasurement(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "lock-measurement")
	claim(t, s, d)
	changed := d
	caps, call := *d.Capabilities, *d.ProviderCall
	changed.Capabilities, changed.ProviderCall = &caps, &call
	changed.ProviderCall.Model, changed.Capabilities.Model = "changed-model", "changed-model"
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				var n int
				_ = s.db.QueryRow("SELECT 1").Scan(&n)
			}
		}
	}()
	before := s.db.Stats()
	var samples []time.Duration
	for i := 0; i < 20; i++ {
		start := time.Now()
		created, err := s.ClaimAdmissionDispatch(accountingContext, changed, accountingOptions())
		samples = append(samples, time.Since(start))
		if created || !errors.Is(err, ErrDispatchClaimConflict) {
			t.Fatal(created, err)
		}
	}
	close(stop)
	<-done
	after := s.db.Stats()
	slices.Sort(samples)
	t.Logf("durable conflict p50=%s p95=%s p99=%s pool-wait-total=%s waits=%d", samples[9], samples[18], samples[19], after.WaitDuration-before.WaitDuration, after.WaitCount-before.WaitCount)
	if resultCount(t, s, "provider_calls") != 1 || resultCount(t, s, "admission_dispatch_claims") != 1 {
		t.Fatal("conflict authorized new work")
	}
	if dir, err := filepath.Abs(s.dumpDir); err == nil {
		t.Log("diagnostic root", dir)
	}
}
