package statestore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"ai-control-plane/internal/numeric"
	"ai-control-plane/schemas"
)

var metricsTestTime = time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)

func metricsInvocationFixture(t *testing.T, s *StateStore, id, role, purpose, status string, cycle int) {
	t.Helper()
	execSQL(t, s.db, `INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES ('r1',?,'fixture',?) ON CONFLICT DO NOTHING`, cycle, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES (?,'r1',?,?,?, ?,?)`, id+"-e", cycle, role, purpose, status, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES (?,?,'r1',?,1,?,?)`, id+"-a", id+"-e", cycle, status, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch) VALUES (?,?,?,'r1',?,1,?,?,'fake','reported-model','high',?,?,1)`, id, id+"-a", id+"-e", cycle, role, purpose, status, semanticTimestamp)
	fixtureProviderCall(t, s, id, "r1", "reported-model")
}

func metricsReceipt(t *testing.T, s *StateStore, id, invocation, state string, input, cache, output, reasoning *int64, at string) UsageReceipt {
	t.Helper()
	r := UsageReceipt{ReceiptID: id, RunID: "r1", InvocationID: invocation, ProviderCallID: invocation + "-call", Completeness: state, InputTokens: input, CachedInputTokens: cache, OutputTokens: output, ReasoningTokens: reasoning, ReportedAt: at}
	if err := s.AcceptUsageReceipt(orchestratorContext(), r, AccountingOptions{At: at}); err != nil {
		t.Fatal(err)
	}
	return r
}

func metricsRead(t *testing.T, s *StateStore) metricsDocument {
	t.Helper()
	source, err := s.metricsSnapshot(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := renderMetrics(source, metricsTestTime)
	if err != nil {
		t.Fatal(err)
	}
	return metricsDecode(t, raw)
}

func metricsDecode(t *testing.T, raw []byte) metricsDocument {
	t.Helper()
	if err := schemas.Validate("metrics.schema.json", raw); err != nil {
		t.Fatal(err)
	}
	var doc metricsDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.GeneratedAt == "" || len(doc.SourceSnapshot) != 71 {
		t.Fatal("missing snapshot provenance", doc.GeneratedAt, doc.SourceSnapshot)
	}
	return doc
}

func metricsFind(t *testing.T, doc metricsDocument, id string) (string, metricsInvocation) {
	t.Helper()
	for _, c := range doc.Cycles {
		for _, r := range c.Roles {
			for _, i := range r.Invocations {
				if i.ID == id {
					return r.Role, i
				}
			}
		}
	}
	t.Fatal("missing invocation", id)
	return "", metricsInvocation{}
}

func requireMetric(t *testing.T, actual metricsValue, state string, value *int64) {
	t.Helper()
	if actual.State != state || !reflect.DeepEqual(actual.Value, value) {
		t.Fatalf("metric %+v, want %s %v", actual, state, value)
	}
}

func TestMetricsOrchestratorPartialAndExecutionIndependence(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	execSQL(t, s.db, `INSERT INTO run_active_actor VALUES ('r1','DEVELOPER','i1',1,?,0)`, semanticTimestamp)
	doc := metricsRead(t, s)
	if len(doc.Cycles[0].Roles) != 1 || doc.Cycles[0].Roles[0].Role != "DEVELOPER" {
		t.Fatal("synthesized orchestration usage", doc.Cycles)
	}
	metricsInvocationFixture(t, s, "controller", "ORCHESTRATOR", "RECOVER", "COMPLETED", 1)
	metricsInvocationFixture(t, s, "empty-partial", "QA", "QA", "BLOCKED", 1)
	metricsReceipt(t, s, "partial-null", "empty-partial", "partial", nil, nil, nil, nil, semanticTimestamp)
	metricsReceipt(t, s, "controller-final", "controller", "final", counter(10), counter(0), counter(2), counter(1), semanticTimestamp)
	metricsReceipt(t, s, "still-running-final", "i1", "final", counter(20), nil, counter(3), nil, semanticTimestamp)
	doc = metricsRead(t, s)
	role, controller := metricsFind(t, doc, "controller")
	if role != "ORCHESTRATOR" || controller.Source != "fake" || controller.Model != "reported-model" || controller.ReasoningEffort != "high" || controller.State.Current {
		t.Fatal(controller, role)
	}
	requireMetric(t, controller.Metrics.Usage.Work, "final", counter(12))
	_, partial := metricsFind(t, doc, "empty-partial")
	if partial.Metrics.Usage.ReceiptState != "partial" || partial.State.InvocationStatus != "BLOCKED" || partial.ReceiptID == nil || *partial.ReceiptID != "partial-null" {
		t.Fatal(partial)
	}
	for _, m := range usageValues(&partial.Metrics.Usage) {
		requireMetric(t, *m, "partial", nil)
	}
	_, running := metricsFind(t, doc, "i1")
	if running.State.InvocationStatus != "RUNNING" || !running.State.Current || running.Metrics.Usage.ReceiptState != "final" || doc.State.Actor == nil || *doc.State.Actor.Invocation != "i1" {
		t.Fatal("receipt changed execution state", running, doc.State)
	}
	requireMetric(t, running.Metrics.Usage.Work, "final", counter(23))
	requireMetric(t, running.Metrics.Usage.Spend, "unknown", nil)
	if doc.Totals.Usage.ReceiptState != "partial" {
		t.Fatal("partial NULL completeness collapsed", doc.Totals)
	}
	for _, m := range doc.Totals.Telemetry {
		requireMetric(t, m, "unknown", nil)
	}
	for _, m := range doc.Totals.Events {
		requireMetric(t, m, "unknown", nil)
	}
	// A completed invocation without a receipt remains unknown, independently of
	// whether the controller finished it or selected it as current.
	metricsInvocationFixture(t, s, "completed-no-receipt", "REVIEWER", "REVIEW", "COMPLETED", 1)
	_, completed := metricsFind(t, metricsRead(t, s), "completed-no-receipt")
	if completed.State.InvocationStatus != "COMPLETED" || completed.Metrics.Usage.ReceiptState != "unknown" || completed.ReceiptID != nil {
		t.Fatal(completed)
	}
}

func TestMetricsCumulativeReceiptsAndFinalNull(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	metricsReceipt(t, s, "p1", "i1", "partial", counter(10), counter(2), counter(1), nil, "2026-10-05T01:00:00Z")
	metricsReceipt(t, s, "p2", "i1", "partial", counter(20), counter(5), counter(3), counter(1), "2026-10-05T02:00:00Z")
	_, i := metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "partial", counter(23))
	requireMetric(t, i.Metrics.Usage.Fresh, "partial", counter(15))
	requireMetric(t, i.Metrics.Usage.Spend, "partial", counter(18))
	if *i.ReceiptID != "p2" {
		t.Fatal("wrong cumulative snapshot", i)
	}
	metricsReceipt(t, s, "final", "i1", "final", counter(30), nil, counter(4), nil, "2026-10-05T03:00:00Z")
	// A later partial is rejected and cannot promote the final's missing cache.
	// Earlier numeric evidence remains known and partial; it cannot settle SPEND.
	late := UsageReceipt{ReceiptID: "late-partial", RunID: "r1", InvocationID: "i1", ProviderCallID: "i1-call", Completeness: "partial", InputTokens: counter(30), CachedInputTokens: counter(6), OutputTokens: counter(4), ReasoningTokens: counter(1), ReportedAt: "2026-10-05T04:00:00Z"}
	if err := s.AcceptUsageReceipt(orchestratorContext(), late, AccountingOptions{At: late.ReportedAt}); err == nil {
		t.Fatal("new PARTIAL accepted after FINAL")
	}
	_, i = metricsFind(t, metricsRead(t, s), "i1")
	if *i.ReceiptID != "final" || i.Metrics.Usage.ReceiptState != "final" {
		t.Fatal(i)
	}
	requireMetric(t, i.Metrics.Usage.Work, "final", counter(34))
	requireMetric(t, i.Metrics.Usage.Cache, "partial", counter(5))
	requireMetric(t, i.Metrics.Usage.Fresh, "partial", counter(15))
	requireMetric(t, i.Metrics.Usage.Spend, "partial", counter(18))
	requireMetric(t, i.Metrics.Usage.Reasoning, "partial", counter(1))
	if countSchemaObject(t, s, "resource_reservations") != 0 || resultCount(t, s, "usage_receipts") != 3 {
		t.Fatal("projection changed accounting authority")
	}
}

func TestMetricsCompletenessRollupsAndKnownZero(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	metricsInvocationFixture(t, s, "unknown", "DEVELOPER", "IMPLEMENT", "RUNNING", 1)
	metricsInvocationFixture(t, s, "zero", "QA", "QA", "COMPLETED", 2)
	metricsReceipt(t, s, "known", "i1", "final", counter(100), counter(40), counter(10), counter(3), semanticTimestamp)
	metricsReceipt(t, s, "known-zero", "zero", "final", counter(0), counter(0), counter(0), counter(0), semanticTimestamp)
	doc := metricsRead(t, s)
	requireMetric(t, doc.Totals.Usage.Input, "partial", counter(100))
	requireMetric(t, doc.Totals.Usage.Cache, "partial", counter(40))
	requireMetric(t, doc.Totals.Usage.Fresh, "partial", counter(60))
	requireMetric(t, doc.Totals.Usage.Output, "partial", counter(10))
	requireMetric(t, doc.Totals.Usage.Work, "partial", counter(110))
	requireMetric(t, doc.Totals.Usage.Spend, "partial", counter(70))
	requireMetric(t, doc.Cycles[0].Roles[0].Metrics.Usage.Work, "partial", counter(110))
	if doc.Cycles[1].Cycle != 2 || doc.Cycles[1].Metrics.Usage.ReceiptState != "final" {
		t.Fatal(doc.Cycles)
	}
	for _, m := range usageValues(&doc.Cycles[1].Metrics.Usage) {
		requireMetric(t, *m, "final", counter(0))
	}
	execSQL(t, s.db, `INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES ('r1',3,'fixture',?)`, semanticTimestamp)
	doc = metricsRead(t, s)
	if len(doc.Cycles[2].Roles) != 0 {
		t.Fatal("invented empty-cycle invocations")
	}
	for _, m := range usageValues(&doc.Cycles[2].Metrics.Usage) {
		requireMetric(t, *m, "unknown", nil)
	}
}

func TestMetricsSnapshotConsistencyAndStableMarker(t *testing.T) {
	ctx := orchestratorContext()
	dir := privateTempDir(t)
	s, err := openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedInvocation(t, s)
	execSQL(t, s.db, `INSERT INTO run_active_actor VALUES ('r1','DEVELOPER','i1',1,?,0)`, semanticTimestamp)
	writer, err := openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	c, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	before, err := readMetricsSource(ctx, c, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.authoritativeMutation(ctx, "r1", nil, func(c *sql.Conn) error {
		_, err := c.ExecContext(ctx, `UPDATE runs SET status='BLOCKED',revision=revision+1 WHERE run_id='r1';
 UPDATE invocations SET status='COMPLETED',completed_at='2026-10-06T00:00:00Z' WHERE invocation_id='i1';
 UPDATE run_active_actor SET active_invocation_id=NULL,revision=revision+1 WHERE run_id='r1';
 INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,completeness,input_tokens,cached_input_tokens,output_tokens,work_tokens,spend_tokens,reported_at,accepted_at)
 VALUES ('new','i1','i1-call','final',10,0,2,12,12,'2026-10-06T00:00:00Z','2026-10-06T00:00:00Z');
 UPDATE provider_calls SET duration_ns=1234567890 WHERE provider_call_id='i1-call';`)
		if err != nil {
			return err
		}
		return advanceProviderCallUsage(ctx, c, UsageReceipt{ReceiptID: "new", ProviderCallID: "i1-call", Completeness: "final", InputTokens: counter(10), CachedInputTokens: counter(0), OutputTokens: counter(2), ReportedAt: "2026-10-06T00:00:00Z"})
	}); err != nil {
		t.Fatal(err)
	}
	after, err := readMetricsSource(ctx, c, "r1")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("mixed SQLite snapshots", err)
	}
	raw1, err := renderMetrics(before, metricsTestTime)
	if err != nil {
		t.Fatal(err)
	}
	raw2, err := renderMetrics(after, metricsTestTime.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	d1, d2 := metricsDecode(t, raw1), metricsDecode(t, raw2)
	if d1.SourceSnapshot != d2.SourceSnapshot || d1.GeneratedAt == d2.GeneratedAt {
		t.Fatal("unstable snapshot marker")
	}
	if _, err := c.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	fresh := metricsRead(t, s)
	_, invocation := metricsFind(t, fresh, "i1")
	if fresh.SourceSnapshot == d1.SourceSnapshot || fresh.State.Status != "BLOCKED" || invocation.State.Current || invocation.State.InvocationStatus != "COMPLETED" || invocation.Metrics.Usage.ReceiptState != "final" {
		t.Fatal("fresh snapshot missed committed state", fresh, invocation)
	}
	_, oldInvocation := metricsFind(t, d1, "i1")
	if invocation.ProviderCalls[0].DurationNS == nil || *invocation.ProviderCalls[0].DurationNS != 1234567890 || invocation.ProviderCalls[0].FinishedAt == nil || oldInvocation.ProviderCalls[0].DurationNS != nil || oldInvocation.ProviderCalls[0].FinishedAt != nil {
		t.Fatal("timing crossed SQLite snapshots")
	}
	if d1.State.Status != "RUNNING" || !oldInvocation.State.Current || oldInvocation.Metrics.Usage.ReceiptState != "unknown" {
		t.Fatal("old snapshot contains new state")
	}
}

func TestMetricsAtomicReplacementAndFailureCleanup(t *testing.T) {
	dir := privateTempDir(t)
	path := filepath.Join(dir, "metrics.json")
	old, newBytes := []byte("old complete projection\n"), []byte("new complete projection\n")
	if err := os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected rename failure")
	err := replaceMetricsFile(orchestratorContext(), dir, newBytes, func(from, to string) error {
		data, err := os.ReadFile(from)
		if err != nil || !bytes.Equal(data, newBytes) {
			t.Fatal("incomplete temporary file", err)
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, old) {
		t.Fatal("failed replace lost old file", err)
	}
	temps, err := filepath.Glob(filepath.Join(dir, ".metrics-*"))
	if err != nil || len(temps) != 0 {
		t.Fatal("temporary files leaked", temps, err)
	}
	handle, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err := replaceMetricsFile(orchestratorContext(), dir, newBytes, os.Rename); err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(handle)
	if err != nil || !bytes.Equal(data, old) {
		t.Fatal("replacement modified old inode", err)
	}
	data, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(data, newBytes) {
		t.Fatal("new readers missed complete replacement", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("unsafe projection mode", info, err)
	}
	ctx, cancel := context.WithCancel(orchestratorContext())
	cancel()
	if err := replaceMetricsFile(ctx, dir, old, os.Rename); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	data, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(data, newBytes) {
		t.Fatal("cancelled replacement touched projection", err)
	}
}

func TestMetricsPostCommitFailureRefreshAndRestart(t *testing.T) {
	ctx := orchestratorContext()
	dataDir, runDir := privateTempDir(t), privateTempDir(t)
	s, err := openTest(ctx, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	seedInvocation(t, s)
	if err := s.EnableMetricsProjection(ctx, "r1", runDir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(runDir, "metrics.json")
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	metricsDecode(t, old)
	lockPath := filepath.Join(runDir, ".metrics.lock")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lockPath, 0700); err != nil {
		t.Fatal(err)
	}
	r := UsageReceipt{ReceiptID: "final", RunID: "r1", InvocationID: "i1", ProviderCallID: "i1-call", Completeness: "final", InputTokens: counter(10), CachedInputTokens: counter(0), OutputTokens: counter(2), ReportedAt: semanticTimestamp}
	err = s.AcceptUsageReceipt(ctx, r, AccountingOptions{At: semanticTimestamp, Audit: accountingAudit()})
	var projectionErr *MetricsProjectionError
	if !errors.As(err, &projectionErr) || projectionErr.RunID != "r1" {
		t.Fatal("missing committed-state error", err)
	}
	if resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("refresh failure rolled back state/audit")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, old) {
		t.Fatal("failed refresh changed old projection", err)
	}
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMetrics(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	final := metricsDecode(t, data)
	requireMetric(t, final.Totals.Usage.Work, "final", counter(12))
	if final.SourceSnapshot == metricsDecode(t, old).SourceSnapshot {
		t.Fatal("receipt did not change snapshot marker")
	}
	// Rejected and idempotent submissions do not refresh or append audit rows.
	if err := s.AcceptUsageReceipt(ctx, r, AccountingOptions{At: semanticTimestamp, Audit: accountingAudit()}); err != nil {
		t.Fatal(err)
	}
	r.InputTokens = counter(-1)
	if err := s.AcceptUsageReceipt(ctx, r, accountingOptions()); err == nil {
		t.Fatal("invalid receipt accepted")
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, unchanged) || resultCount(t, s, "events") != 1 {
		t.Fatal("rejection/retry refreshed or mutated audit", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt metrics are not authority"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err = openTest(ctx, dataDir)
	if err != nil {
		t.Fatal("metrics blocked restart", err)
	}
	defer s.Close()
	if err := s.EnableMetricsProjection(ctx, "r1", runDir); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restarted := metricsDecode(t, data)
	if restarted.SourceSnapshot != final.SourceSnapshot || resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("restart imported metrics or changed source marker")
	}
}

func TestMetricsOverflowRejectedBeforeCommit(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	metricsInvocationFixture(t, s, "second", "DEVELOPER", "IMPLEMENT", "RUNNING", 1)
	dir := privateTempDir(t)
	path := filepath.Join(dir, "metrics.json")
	if err := s.EnableMetricsProjection(context.Background(), "r1", dir); err != nil {
		t.Fatal(err)
	}
	metricsReceipt(t, s, "huge1", "i1", "final", counter(math.MaxInt64), counter(0), counter(0), nil, semanticTimestamp)
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc := metricsDecode(t, old)
	requireMetric(t, doc.Totals.Usage.Work, "partial", counter(math.MaxInt64))
	r := UsageReceipt{ReceiptID: "huge2", RunID: "r1", InvocationID: "second", ProviderCallID: "second-call", Completeness: "final", InputTokens: counter(1), CachedInputTokens: counter(0), OutputTokens: counter(0), ReportedAt: semanticTimestamp}
	err = s.AcceptUsageReceipt(orchestratorContext(), r, accountingOptions())
	var invariantErr *numeric.Error
	if !errors.As(err, &invariantErr) || resultCount(t, s, "usage_receipts") != 1 {
		t.Fatal("overflow corrupted authoritative state", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, old) {
		t.Fatal("overflow corrupted projection", err)
	}
}

func TestMetricsBindingsAndSchemaCompatibility(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	ctx := orchestratorContext()
	dir := privateTempDir(t)
	if err := s.EnableMetricsProjection(ctx, "missing", dir); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if err := s.RefreshMetrics(ctx, "r1"); err == nil {
		t.Fatal("unconfigured refresh succeeded")
	}
	if err := s.EnableMetricsProjection(ctx, "r1", dir); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s.db, `INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES ('r2','CREATED',1,?,?)`, semanticTimestamp, semanticTimestamp)
	if err := s.EnableMetricsProjection(ctx, "r2", dir); err == nil {
		t.Fatal("two runs bound to same output")
	}
	otherDir := privateTempDir(t)
	if err := s.EnableMetricsProjection(ctx, "r2", otherDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(otherDir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	doc := metricsDecode(t, data)
	if len(doc.Cycles) != 0 {
		t.Fatal("invented cycles", doc)
	}
	for _, m := range usageValues(&doc.Totals.Usage) {
		requireMetric(t, *m, "unknown", nil)
	}
	// Optional projection metadata preserves validation of historical v1 exports.
	var historical map[string]any
	if err := json.Unmarshal(data, &historical); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"generated_at", "source_snapshot", "execution_state", "accounting_status", "accounting_incomplete"} {
		delete(historical, key)
	}
	raw, err := json.Marshal(historical)
	if err != nil {
		t.Fatal(err)
	}
	if err := schemas.Validate("metrics.schema.json", raw); err != nil {
		t.Fatal("v1 compatibility lost", err)
	}
	if err := s.RefreshMetrics(ctx, "r1"); err != nil {
		t.Fatal(fmt.Errorf("refresh after other-run failure: %w", err))
	}
}

func TestB6AccountingReporting(t *testing.T) {
	for _, state := range []string{"unknown", "partial", "final-null", "final-zero"} {
		t.Run(state, func(t *testing.T) {
			s := testStore(t)
			seedInvocation(t, s)
			spec := workflowSpec()
			spec.RunID = "r1"
			raw, err := json.Marshal(spec)
			if err != nil {
				t.Fatal(err)
			}
			execSQL(t, s.db, "INSERT INTO workflow_runs(run_id,spec_json) VALUES('r1',?)", string(raw))
			if state == "partial" {
				metricsReceipt(t, s, "receipt", "i1", "partial", nil, nil, nil, nil, semanticTimestamp)
			} else if state == "final-null" {
				metricsReceipt(t, s, "receipt", "i1", "final", nil, nil, nil, nil, semanticTimestamp)
			} else if state == "final-zero" {
				metricsReceipt(t, s, "receipt", "i1", "final", counter(0), counter(0), counter(0), nil, semanticTimestamp)
			}
			for _, status := range []string{"RUNNING", "BLOCKED"} {
				execSQL(t, s.db, "UPDATE runs SET status=? WHERE run_id='r1'", status)
				before := mutationDatabaseSnapshot(t, s)
				w, err := s.GetWorkflow(accountingContext, "r1")
				if err != nil {
					t.Fatal(err)
				}
				doc := metricsRead(t, s)
				want := "incomplete"
				if state == "final-zero" {
					want = "complete"
				}
				if w.AccountingStatus != want || doc.AccountingStatus != want || w.AccountingIncomplete != (want != "complete") || doc.AccountingIncomplete != w.AccountingIncomplete {
					t.Fatalf("state=%s status=%s workflow=%+v metrics=%+v", state, status, w, doc)
				}
				if !bytes.Equal(before, mutationDatabaseSnapshot(t, s)) {
					t.Fatal("reporting changed authority")
				}
			}
		})
	}
	s := testStore(t)
	spec := workflowSpec()
	if err := s.CreateWorkflow(accountingContext, spec); err != nil {
		t.Fatal(err)
	}
	w, err := s.GetWorkflow(accountingContext, spec.RunID)
	if err != nil || w.AccountingStatus != "unknown" || !w.AccountingIncomplete {
		t.Fatal(w, err)
	}
}

func TestB6MetricsSlowPublicationDoesNotBlockOtherRuns(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	ctx := orchestratorContext()
	for _, run := range []string{"r2", "r3"} {
		execSQL(t, s.db, "INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES(?,'CREATED',1,?,?)", run, semanticTimestamp, semanticTimestamp)
	}
	dir := privateTempDir(t)
	if err := s.EnableMetricsProjection(ctx, "r1", dir); err != nil {
		t.Fatal(err)
	}
	if err := s.EnableMetricsProjection(ctx, "r2", privateTempDir(t)); err != nil {
		t.Fatal(err)
	}
	before := mutationDatabaseSnapshot(t, s)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		done <- s.refreshMetrics(ctx, "r1", true, func(ctx context.Context, run, dir string) error {
			if _, err := s.metricsSnapshot(ctx, run); err != nil {
				return err
			}
			close(entered)
			<-release // Simulated slow filesystem after the database snapshot.
			return s.projectMetrics(ctx, run, dir)
		})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher did not start")
	}
	other := make(chan error, 1)
	go func() {
		for _, run := range []string{"r2", "r3"} {
			if err := s.numericMutation(ctx, "test", run, nil, func(*sql.Conn) error { return nil }); err != nil {
				other <- err
				return
			}
		}
		other <- s.EnableMetricsProjection(ctx, "r3", dir)
	}()
	select {
	case err := <-other:
		if err == nil || !strings.Contains(err.Error(), "already bound") {
			t.Fatal("directory collision was not rejected", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow metrics blocked unrelated enabled/disabled Runs")
	}
	same := make(chan error, 1)
	go func() { same <- s.RefreshMetrics(ctx, "r1") }()
	select {
	case err := <-same:
		t.Fatal("same Run publication bypassed ordering", err)
	case <-time.After(20 * time.Millisecond):
	}
	rebound := make(chan error, 1)
	newDir := privateTempDir(t)
	go func() { rebound <- s.EnableMetricsProjection(ctx, "r1", newDir) }()
	select {
	case err := <-rebound:
		t.Fatal("rebinding bypassed in-flight publication", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	for _, ch := range []chan error{done, same, rebound} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("refresh did not finish")
		}
	}
	if !bytes.Equal(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("publication changed authoritative state")
	}
	if err := os.Remove(filepath.Join(dir, "metrics.json")); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMetrics(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "metrics.json")); !os.IsNotExist(err) {
		t.Fatal("refresh published to old binding", err)
	}
	if _, err := os.Stat(filepath.Join(newDir, "metrics.json")); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsCrossStoreLockAndCancelledRefresh(t *testing.T) {
	ctx := orchestratorContext()
	dbDir, runDir := privateTempDir(t), privateTempDir(t)
	s, err := openTest(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedInvocation(t, s)
	if err := s.EnableMetricsProjection(ctx, "r1", runDir); err != nil {
		t.Fatal(err)
	}
	other, err := openTest(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	lock, err := os.OpenFile(filepath.Join(runDir, ".metrics.lock"), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	// The second binding stays enabled even though the initial refresh is busy.
	if err := other.EnableMetricsProjection(ctx, "r1", runDir); err == nil {
		t.Fatal("concurrent publisher bypassed the file lock")
	}
	old, err := os.ReadFile(filepath.Join(runDir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	r := UsageReceipt{ReceiptID: "locked-final", RunID: "r1", InvocationID: "i1", ProviderCallID: "i1-call", Completeness: "final", InputTokens: counter(3), CachedInputTokens: counter(0), OutputTokens: counter(2), ReportedAt: semanticTimestamp}
	err = other.AcceptUsageReceipt(ctx, r, accountingOptions())
	var projectionErr *MetricsProjectionError
	if !errors.As(err, &projectionErr) || resultCount(t, other, "usage_receipts") != 1 {
		t.Fatal("lock failure affected commit", err)
	}
	data, err := os.ReadFile(filepath.Join(runDir, "metrics.json"))
	if err != nil || !bytes.Equal(data, old) {
		t.Fatal("locked publisher replaced the projection", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMetrics(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(runDir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	doc := metricsDecode(t, data)
	requireMetric(t, doc.Totals.Usage.Work, "final", counter(5))
	// Cancellation of an observational refresh cannot touch authority or output.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := other.refreshEnabledMetrics(cancelled, "r1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	unchanged, err := os.ReadFile(filepath.Join(runDir, "metrics.json"))
	if err != nil || !bytes.Equal(data, unchanged) {
		t.Fatal("cancelled refresh touched output", err)
	}
}
