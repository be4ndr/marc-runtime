package statestore

import (
	"context"

	"encoding/json"
	"errors"
	"math"
	"path/filepath"

	"strings"
	"testing"
	"time"
)

func providerTiming(t *testing.T, s *StateStore, id string) (string, *int64) {
	t.Helper()
	var start string
	var duration *int64
	if err := s.db.QueryRow(`SELECT started_at,duration_ns FROM provider_calls WHERE provider_call_id=?`, id).Scan(&start, &duration); err != nil {
		t.Fatal(err)
	}
	return start, duration
}

func TestProviderCallMeasuredTimingAndRecovery(t *testing.T) {
	dir := privateTempDir(t)
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	seedInvocation(t, s)
	start, duration := providerTiming(t, s, "i1-call")
	if start != semanticTimestamp || duration != nil {
		t.Fatal(start, duration)
	}
	_, initial := metricsFind(t, metricsRead(t, s), "i1")
	if initial.ProviderCalls[0].DurationNS != nil || initial.ProviderCalls[0].FinishedAt != nil {
		t.Fatal(initial.ProviderCalls)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	_, duration = providerTiming(t, s, "i1-call")
	if duration != nil {
		t.Fatal("restart invented physical completion")
	}
	before := metricsRead(t, s)
	opts := accountingOptions()
	opts.At = "1999-01-01T00:00:00Z" // Audit time cannot determine completion legality.
	opts.Audit = accountingAudit()
	for _, invalid := range []int64{-1, math.MinInt64, math.MaxInt64} {
		if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", invalid, opts); err == nil {
			t.Fatal("accepted invalid duration", invalid)
		}
		if _, err := s.db.Exec(`UPDATE provider_calls SET duration_ns=? WHERE provider_call_id='i1-call'`, invalid); err == nil {
			t.Fatal("SQL accepted invalid duration", invalid)
		}
	}
	_, duration = providerTiming(t, s, "i1-call")
	if duration != nil || resultCount(t, s, "events") != 0 || metricsRead(t, s).SourceSnapshot != before.SourceSnapshot {
		t.Fatal("invalid duration changed state or audit")
	}
	const measured int64 = 1_234_567_890
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", measured, opts); err != nil {
		t.Fatal(err)
	}
	_, duration = providerTiming(t, s, "i1-call")
	if duration == nil || *duration != measured {
		t.Fatal(duration)
	}
	after := metricsRead(t, s)
	_, invocation := metricsFind(t, after, "i1")
	call := invocation.ProviderCalls[0]
	parsed, err := time.Parse(time.RFC3339Nano, start)
	if err != nil {
		t.Fatal(err)
	}
	want := parsed.Add(time.Duration(measured)).Format(time.RFC3339Nano)
	if call.StartedAt != start || call.DurationNS == nil || *call.DurationNS != measured || call.FinishedAt == nil || *call.FinishedAt != want {
		t.Fatal("incorrect timing projection", call)
	}
	if before.SourceSnapshot == after.SourceSnapshot {
		t.Fatal("duration missing from source identity")
	}
	snapshot, err := s.metricsSnapshot(accountingContext, "r1")
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonical), "finished_at") {
		t.Fatal("derived finish entered source identity")
	}
	raw, err := renderMetrics(snapshot, metricsTestTime.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if metricsDecode(t, raw).SourceSnapshot != after.SourceSnapshot {
		t.Fatal("generated_at changed source identity")
	}
	// Retry metadata can precede the previous start. Only accepted duration sequences calls.
	next := ProviderCall{ProviderCallID: "retry", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "fallback"}
	if err := s.CreateProviderCall(accountingContext, next, AccountingOptions{At: "1980-01-01T00:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	for _, repeated := range []int64{0, measured + 1} {
		if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", repeated, opts); err != nil {
			t.Fatal(err)
		}
	}
	_, duration = providerTiming(t, s, "i1-call")
	_, retryDuration := providerTiming(t, s, "retry")
	if duration == nil || *duration != measured || retryDuration != nil || resultCount(t, s, "events") != 1 {
		t.Fatal("repeat overwrote first duration, affected successor or duplicated audit")
	}
	for _, stmt := range []string{
		`UPDATE provider_calls SET duration_ns=2 WHERE provider_call_id='i1-call'`,
		`UPDATE provider_calls SET started_at='1970-01-01T00:00:00Z' WHERE provider_call_id='i1-call'`,
	} {
		if _, err := s.db.Exec(stmt); err == nil {
			t.Fatal("rewrote accepted timing", stmt)
		}
	}
	// Zero is a known measured duration, distinct from NULL.
	if err := s.FinishProviderCall(accountingContext, "r1", "retry", 0, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	_, retry := metricsFind(t, metricsRead(t, s), "i1")
	if c := retry.ProviderCalls[1]; c.DurationNS == nil || *c.DurationNS != 0 || c.FinishedAt == nil || *c.FinishedAt != c.StartedAt {
		t.Fatal(c)
	}
	marker := metricsRead(t, s).SourceSnapshot
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	_, duration = providerTiming(t, s, "i1-call")
	if duration == nil || *duration != measured || metricsRead(t, s).SourceSnapshot != marker {
		t.Fatal("restart lost measured duration or stable identity")
	}
}

func TestConcurrentMeasuredFinishAndRegistration(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	other, err := openTest(accountingContext, filepath.Dir(s.Info().Path))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx, cancel := context.WithTimeout(accountingContext, 5*time.Second)
	defer cancel()
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	start := make(chan struct{})
	results := make(chan error, 2)
	for n, store := range []*StateStore{s, other} {
		go func(store *StateStore, duration int64) {
			<-start
			results <- store.FinishProviderCall(ctx, "r1", "i1-call", duration, opts)
		}(store, int64(n+1))
	}
	close(start)
	for n := 0; n < 2; n++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	_, duration := providerTiming(t, s, "i1-call")
	if duration == nil || (*duration != 1 && *duration != 2) || resultCount(t, s, "events") != 1 {
		t.Fatal("concurrent finish did not accept exactly one duration/audit", duration)
	}
	next := ProviderCall{ProviderCallID: "retry", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "fake"}
	if err := s.CreateProviderCall(ctx, next, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	next.ProviderCallID = "fallback"
	start = make(chan struct{})
	finishResult, createResult := make(chan error, 1), make(chan error, 1)
	go func() { <-start; finishResult <- s.FinishProviderCall(ctx, "r1", "retry", 3, accountingOptions()) }()
	go func() { <-start; createResult <- other.CreateProviderCall(ctx, next, accountingOptions()) }()
	close(start)
	if err := <-finishResult; err != nil {
		t.Fatal(err)
	}
	if err := <-createResult; err != nil {
		// Registration may acquire the writer lock before completion and fail closed.
		if err := other.CreateProviderCall(ctx, next, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	}
	var active int
	if err := s.db.QueryRow(`SELECT count(*) FROM provider_calls WHERE invocation_id='i1' AND duration_ns IS NULL`).Scan(&active); err != nil || active != 1 {
		t.Fatal("finish/register broke sequential invariant", active, err)
	}
	_, duration = providerTiming(t, s, "retry")
	if duration == nil || *duration != 3 {
		t.Fatal("finish/register lost accepted measurement")
	}
}

// A display timestamp can be unrepresentable without rejecting physical completion.
func TestDerivedFinishProjectionFailureDoesNotRollbackDuration(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", 0, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	call := ProviderCall{ProviderCallID: "future", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "fake"}
	if err := s.CreateProviderCall(accountingContext, call, AccountingOptions{At: "9999-12-31T23:59:59Z"}); err != nil {
		t.Fatal(err)
	}
	if err := s.EnableMetricsProjection(context.Background(), "r1", privateTempDir(t)); err != nil {
		t.Fatal(err)
	}
	err := s.FinishProviderCall(accountingContext, "r1", "future", int64(2*time.Second), accountingOptions())
	var projectionError *MetricsProjectionError
	if !errors.As(err, &projectionError) {
		t.Fatal("projection failure not observable as committed state", err)
	}
	_, duration := providerTiming(t, s, "future")
	if duration == nil || *duration != int64(2*time.Second) {
		t.Fatal("projection failure rolled back accepted measurement")
	}
}

func TestProviderCallDurationRetainsExactInt64(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	const measured int64 = math.MaxInt64 - 1
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", measured, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	start, duration := providerTiming(t, s, "i1-call")
	if duration == nil || *duration != measured {
		t.Fatal("duration lost int64 precision", duration)
	}
	_, invocation := metricsFind(t, metricsRead(t, s), "i1")
	call := invocation.ProviderCalls[0]
	parsed, err := time.Parse(time.RFC3339Nano, start)
	if err != nil {
		t.Fatal(err)
	}
	if call.DurationNS == nil || *call.DurationNS != measured || call.FinishedAt == nil || *call.FinishedAt != parsed.Add(time.Duration(measured)).Format(time.RFC3339Nano) {
		t.Fatal("projection lost exact duration", call)
	}
}
