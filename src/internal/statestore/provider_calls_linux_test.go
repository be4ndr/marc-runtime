package statestore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"marc-runtime/internal/numeric"
)

func addProviderCall(t *testing.T, s *StateStore, id, model string) {
	t.Helper()
	var active string
	if err := s.db.QueryRow(`SELECT provider_call_id FROM provider_calls WHERE invocation_id='i1' AND duration_ns IS NULL`).Scan(&active); err == nil {
		if err := s.FinishProviderCall(accountingContext, "r1", active, 1_000_000, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if err := s.CreateProviderCall(accountingContext, ProviderCall{ProviderCallID: id, RunID: "r1", InvocationID: "i1", Provider: "fake", Model: model}, accountingOptions()); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyProviderCallRegistrationEnforcesSharedIdentityAndTextPolicy(t *testing.T) {
	s, _ := accountingFixture(t)
	beforeCalls, beforeEvents := resultCount(t, s, "provider_calls"), resultCount(t, s, "events")
	for _, tc := range []struct {
		name string
		call ProviderCall
	}{
		{"identifier punctuation", ProviderCall{ProviderCallID: "bad_call", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "model"}},
		{"model too long", ProviderCall{ProviderCallID: "long-model", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: strings.Repeat("m", 257)}},
		{"invalid UTF-8 reasoning", ProviderCall{ProviderCallID: "bad-reasoning", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "model", ReasoningEffort: string([]byte{0xff})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.CreateProviderCall(accountingContext, tc.call, accountingOptions()); err == nil {
				t.Fatal("invalid ProviderCall accepted")
			}
			if resultCount(t, s, "provider_calls") != beforeCalls || resultCount(t, s, "events") != beforeEvents {
				t.Fatal("invalid ProviderCall partially persisted")
			}
		})
	}
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	valid := ProviderCall{ProviderCallID: "unicode-valid", RunID: "r1", InvocationID: "i1", Provider: strings.Repeat("界", 256), Model: strings.Repeat("型", 256), ReasoningEffort: strings.Repeat("深", 256)}
	if err := s.CreateProviderCall(accountingContext, valid, accountingOptions()); err != nil {
		t.Fatalf("valid Unicode boundary values rejected: %v", err)
	}
	var provider, model, reasoning string
	if err := s.db.QueryRow(`SELECT provider,model,reasoning_effort FROM provider_calls WHERE provider_call_id=?`, valid.ProviderCallID).Scan(&provider, &model, &reasoning); err != nil || provider != valid.Provider || model != valid.Model || reasoning != valid.ReasoningEffort {
		t.Fatalf("persisted text changed: %v", err)
	}
}
func callReceipt(id, call, state string, input *int64) UsageReceipt {
	r := receipt(id, state)
	r.ProviderCallID = call
	r.InputTokens = input
	if input != nil {
		r.CachedInputTokens = counter(0)
		r.OutputTokens = counter(0)
	}
	return r
}
func readCallUsage(t *testing.T, s *StateStore, id string) (usageSummary, string, int64) {
	t.Helper()
	var u usageSummary
	var receipt sql.NullString
	var rev int64
	row := s.db.QueryRow(`SELECT receipt_id,revision,`+currentUsageColumns+` FROM provider_call_usage WHERE provider_call_id=?`, id)
	if err := scanCurrentUsage(row.Scan, &u, &receipt, &rev); err != nil {
		t.Fatal(err)
	}
	return u, receipt.String, rev
}

func TestProviderCallsRetriesFallbackAndCurrentAccounting(t *testing.T) {
	s, _ := accountingFixture(t)

	addProviderCall(t, s, "retry", "same-model")
	addProviderCall(t, s, "fallback", "alternate-model")
	acceptUsage(t, s, callReceipt("first-final", "i1-call", "final", counter(100)))
	acceptUsage(t, s, callReceipt("retry-final", "retry", "final", counter(80)))
	acceptUsage(t, s, callReceipt("fallback-partial", "fallback", "partial", nil))
	_, i := metricsFind(t, metricsRead(t, s), "i1")
	if len(i.ProviderCalls) != 3 || i.ProviderCalls[2].Model != "alternate-model" || i.ReceiptID != nil || i.ReportedAt != nil {
		t.Fatal(i)
	}
	requireMetric(t, i.Metrics.Usage.Work, "partial", counter(180))
	requireMetric(t, i.Metrics.Usage.Spend, "partial", counter(180))
	acceptUsage(t, s, callReceipt("fallback-known", "fallback", "partial", counter(40)))
	_, i = metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "partial", counter(220))
	acceptUsage(t, s, callReceipt("fallback-final", "fallback", "final", counter(50)))

	_, i = metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "final", counter(230))
	// Physical call completion permits another controller-selected retry, but
	// neither completion nor all-final usage proves invocation settlement safety.
	addProviderCall(t, s, "another-retry", "fake")
	if resultCount(t, s, "provider_calls") != 4 {
		t.Fatal("sequential retry was not admitted")
	}
}

func TestProviderCallCurrentProgressionIgnoresTimestampsAndReceiptIDs(t *testing.T) {
	dir := privateTempDir(t)
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	seedInvocation(t, s)
	for n, at := range []string{semanticTimestamp, semanticTimestamp, "2020-01-01T00:00:00Z"} {
		ids := []string{"z-first", "a-second", "0-backdated"}
		r := callReceipt(ids[n], "i1-call", "partial", counter(int64(10*(n+1))))
		r.ReportedAt = at
		if err := s.AcceptUsageReceipt(accountingContext, r, AccountingOptions{At: at}); err != nil {
			t.Fatal(err)
		}
		u, id, rev := readCallUsage(t, s, "i1-call")
		requireMetric(t, u.Work, "partial", r.InputTokens)
		if id != ids[n] || rev != int64(n+1) {
			t.Fatal(id, rev)
		}
		_, i := metricsFind(t, metricsRead(t, s), "i1")
		requireMetric(t, i.Metrics.Usage.Work, "partial", r.InputTokens)
	}
	final := callReceipt("final", "i1-call", "final", counter(40))
	acceptUsage(t, s, final)
	if err := s.AcceptUsageReceipt(accountingContext, callReceipt("zz-later-partial", "i1-call", "partial", counter(40)), accountingOptions()); err == nil {
		t.Fatal("new partial accepted after FINAL")
	}
	before := metricsRead(t, s)
	u, id, rev := readCallUsage(t, s, "i1-call")
	requireMetric(t, u.Work, "final", counter(40))
	if id != "final" || rev != 4 {
		t.Fatal(id, rev)
	}
	s.Close()
	s, err = openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	after := metricsRead(t, s)
	if before.SourceSnapshot != after.SourceSnapshot {
		t.Fatal("restart changed source identity")
	}
	u, id, rev = readCallUsage(t, s, "i1-call")
	requireMetric(t, u.Work, "final", counter(40))
	if id != "final" || rev != 4 {
		t.Fatal(id, rev)
	}
	// Metrics never even reads usage_receipts. The current table owns selection.
	conn, err := s.db.Conn(accountingContext)
	if err != nil {
		t.Fatal(err)
	}
	// A connection-local shadow audit table is a bounded proof that projection does
	// not query immutable receipts. Main durable state is unchanged.
	if _, err := conn.ExecContext(accountingContext, `CREATE TEMP TABLE usage_receipts (unrelated TEXT)`); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	projected := metricsRead(t, s)
	if projected.SourceSnapshot != after.SourceSnapshot {
		t.Fatal("audit affected current-state projection")
	}
}

func TestProviderCallUnknownAndInterruptedUsage(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	// No receipt, even after an interruption, means unknown rather than zero.
	execSQL(t, s.db, `UPDATE invocations SET status='INTERRUPTED' WHERE invocation_id='i1'`)
	_, i := metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "unknown", nil)
	if i.State.InvocationStatus != "INTERRUPTED" {
		t.Fatal(i.State)
	}
	acceptUsage(t, s, callReceipt("empty-partial", "i1-call", "partial", nil))
	addProviderCall(t, s, "unobserved-retry", "fake")
	_, i = metricsFind(t, metricsRead(t, s), "i1")
	if i.Metrics.Usage.ReceiptState != "partial" {
		t.Fatal(i.Metrics)
	}
	for _, v := range usageValues(&i.Metrics.Usage) {
		requireMetric(t, *v, "partial", nil)
	}
	// The explicitly requested 100 final + 40 partial example.
	acceptUsage(t, s, callReceipt("known-retry", "unobserved-retry", "final", counter(100)))
	acceptUsage(t, s, callReceipt("known-partial", "i1-call", "partial", counter(40)))
	_, i = metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "partial", counter(140))
}

func TestProviderCallReceiptCurrentUsageAtomicRollback(t *testing.T) {
	s, _ := accountingFixture(t)

	acceptUsage(t, s, callReceipt("old", "i1-call", "partial", counter(10)))
	before, id, rev := readCallUsage(t, s, "i1-call")
	// Force a failure after the immutable INSERT but before the current update.
	execSQL(t, s.db, `CREATE TEMP TRIGGER fail_current BEFORE UPDATE ON provider_call_usage BEGIN SELECT RAISE(ABORT,'injected current failure'); END`)
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	if err := s.AcceptUsageReceipt(accountingContext, callReceipt("new", "i1-call", "partial", counter(20)), opts); err == nil {
		t.Fatal("injected update accepted")
	}
	execSQL(t, s.db, `DROP TRIGGER fail_current`)
	// Audit failure happens after both accounting writes and rolls them both back.
	opts.Audit[0].EventType = ""
	if err := s.AcceptUsageReceipt(accountingContext, callReceipt("new", "i1-call", "partial", counter(20)), opts); err == nil {
		t.Fatal("bad audit accepted")
	}
	opts.Audit = accountingAudit()
	for _, r := range []UsageReceipt{callReceipt("lower", "i1-call", "partial", counter(5)), callReceipt("overflow", "i1-call", "partial", counter(math.MaxInt64)), callReceipt("wrong-call", "absent", "partial", counter(20))} {
		if r.ReceiptID == "overflow" {
			r.OutputTokens = counter(1)
		}
		if err := s.AcceptUsageReceipt(accountingContext, r, opts); err == nil {
			t.Fatal("invalid receipt accepted", r.ReceiptID)
		}
	}
	after, id2, rev2 := readCallUsage(t, s, "i1-call")
	if !reflect.DeepEqual(before, after) || id != id2 || rev != rev2 || resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 0 {
		t.Fatal("rollback lost atomicity", after, id2, rev2)
	}

	// Immutable evidence and call boundaries cannot be reassigned or removed.
	for _, statement := range []string{`DELETE FROM usage_receipts`, `UPDATE usage_receipts SET provider_call_id='absent'`, `DELETE FROM provider_calls`, `UPDATE provider_calls SET invocation_id='absent'`, `DELETE FROM provider_call_usage`, `UPDATE provider_call_usage SET work_amount=0,revision=revision+1`} {
		if _, err := s.db.Exec(statement); err == nil {
			t.Fatal("invalid write accepted", statement)
		}
	}
}

func TestProviderCallConcurrentSnapshotAndAcceptance(t *testing.T) {
	ctx, cancel := context.WithTimeout(accountingContext, 10*time.Second)
	defer cancel()
	dir := privateTempDir(t)
	s, err := openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedInvocation(t, s)
	writer, err := openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	done := make(chan error, 1)
	go func() {
		for n := int64(1); n <= 40; n++ {
			r := callReceipt(fmt.Sprintf("concurrent-%d", n), "i1-call", "partial", counter(n))
			err := writer.authoritativeMutation(ctx, "r1", nil, func(c *sql.Conn) error {
				if _, err := c.ExecContext(ctx, `UPDATE runs SET revision=? WHERE run_id='r1'`, n); err != nil {
					return err
				}
				if _, err := c.ExecContext(ctx, `INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,completeness,input_tokens,cached_input_tokens,output_tokens,reported_at,accepted_at) VALUES (?,'i1','i1-call','partial',?,0,0,?,?)`, r.ReceiptID, n, semanticTimestamp, semanticTimestamp); err != nil {
					return err
				}
				return advanceProviderCallUsage(ctx, c, r)
			})
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for n := 0; n < 80; n++ {
		source, err := s.metricsSnapshot(ctx, "r1")
		if err != nil {
			t.Fatal(err)
		}
		call := source.ProviderCalls[0]
		if call.Revision != source.State.Revision || call.Usage.Work.Value != nil && *call.Usage.Work.Value != source.State.Revision {
			t.Fatal("mixed source snapshot", source.State, call)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	u, _, rev := readCallUsage(t, s, "i1-call")
	requireMetric(t, u.Work, "partial", counter(40))
	if rev != 40 {
		t.Fatal(rev)
	}
}

func TestProviderCallCreationProvenanceAndAtomicAudit(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	for _, call := range []ProviderCall{
		{ProviderCallID: "", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "fake"},
		{ProviderCallID: "foreign", RunID: "r2", InvocationID: "i1", Provider: "fake", Model: "fake"},
		{ProviderCallID: "missing", RunID: "r1", InvocationID: "absent", Provider: "fake", Model: "fake"},
	} {
		if err := s.CreateProviderCall(accountingContext, call, accountingOptions()); err == nil {
			t.Fatal("invalid call accepted", call)
		}
	}
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	opts.Audit[0].EventType = ""
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", 1_000_000, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateProviderCall(accountingContext, ProviderCall{ProviderCallID: "rolled-back", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "fake"}, opts); err == nil {
		t.Fatal("invalid audit accepted")
	}
	if resultCount(t, s, "provider_calls") != 1 || resultCount(t, s, "provider_call_usage") != 1 {
		t.Fatal("call creation not atomic")
	}
	r := callReceipt("missing-boundary", "", "partial", nil)
	if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); err == nil {
		t.Fatal("receipt synthesized call")
	}
	addAccountingInvocation(t, s, "other", "r1", "DEVELOPER", "IMPLEMENT")
	r.ProviderCallID = "other-call"
	if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); err == nil {
		t.Fatal("cross invocation receipt accepted")
	}
	r = callReceipt("immutable", "i1-call", "partial", counter(10))
	acceptUsage(t, s, r)
	addProviderCall(t, s, "second", "fake")
	r.ProviderCallID = "second"
	if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); err == nil {
		t.Fatal("receipt identity rebound to another call")
	}
}

func TestProviderCallProjectionFailureKeepsCurrentUsage(t *testing.T) {
	// The filesystem error includes this TMPDIR name. It must not be mistaken
	// for SQLite overflow by any public mutation's deferred error enrichment.
	tmp := filepath.Join(privateTempDir(t), "integer overflow")
	if err := os.Mkdir(tmp, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", tmp)
	s := testStore(t)
	seedInvocation(t, s)
	dir := privateTempDir(t)
	if err := s.EnableMetricsProjection(accountingContext, "r1", dir); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(dir, ".metrics.lock")
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lock, 0700); err != nil {
		t.Fatal(err)
	}
	var projection *MetricsProjectionError
	assertProjection := func(err error) {
		t.Helper()
		var failure *numeric.Error
		var pathError *os.PathError
		if !errors.As(err, &projection) || !errors.As(err, &pathError) || errors.As(err, &failure) {
			t.Fatal("projection/filesystem error misclassified", err)
		}
	}
	err = s.FinishProviderCall(accountingContext, "r1", "i1-call", 1_000_000, accountingOptions())
	assertProjection(err)
	if !errors.As(err, &projection) {
		t.Fatal("finish projection failure not observable", err)
	}
	err = s.CreateProviderCall(accountingContext, ProviderCall{ProviderCallID: "retry", InvocationID: "i1", RunID: "r1", Provider: "fake", Model: "alternate"}, accountingOptions())
	assertProjection(err)
	if !errors.As(err, &projection) {
		t.Fatal("creation failure not observable", err)
	}
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	err = s.AcceptUsageReceipt(accountingContext, callReceipt("committed", "retry", "partial", counter(80)), opts)
	assertProjection(err)
	if !errors.As(err, &projection) {
		t.Fatal("receipt failure not observable", err)
	}
	u, id, rev := readCallUsage(t, s, "retry")
	requireMetric(t, u.Work, "partial", counter(80))
	if id != "committed" || rev != 1 || resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("projection rolled back authority")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil || !bytes.Equal(raw, old) {
		t.Fatal("failure changed prior projection", err)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMetrics(accountingContext, "r1"); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	_, i := metricsFind(t, metricsDecode(t, raw), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "partial", counter(80))
}

func TestProviderCallOmittedCountersRetainPartialEvidence(t *testing.T) {
	s, _ := accountingFixture(t)

	r := callReceipt("first-known", "i1-call", "partial", counter(100))
	r.CachedInputTokens = counter(20)
	r.OutputTokens = counter(10)
	acceptUsage(t, s, r)
	r = callReceipt("omitted", "i1-call", "partial", nil)
	acceptUsage(t, s, r)
	_, i := metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "partial", counter(110))
	requireMetric(t, i.Metrics.Usage.Spend, "partial", counter(90))
	r = callReceipt("new-parents", "i1-call", "partial", counter(120))
	r.CachedInputTokens = nil
	r.OutputTokens = counter(20)
	acceptUsage(t, s, r)
	_, i = metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "partial", counter(140))
	requireMetric(t, i.Metrics.Usage.Fresh, "partial", counter(80))
	requireMetric(t, i.Metrics.Usage.Spend, "partial", counter(90))
	// Fresh/SPEND are the last coherently reported amounts, not invented by mixing
	// newer input/output with older cache evidence. Their evidence is explicit.
	evidence := i.ProviderCalls[0].UsageEvidence
	if *evidence["spend_tokens"] != "first-known" || *evidence["work_tokens"] != "new-parents" {
		t.Fatal(evidence)
	}
	r = callReceipt("final-missing-cache", "i1-call", "final", counter(130))
	r.CachedInputTokens = nil
	r.OutputTokens = counter(30)
	acceptUsage(t, s, r)
	_, i = metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "final", counter(160))
	requireMetric(t, i.Metrics.Usage.Cache, "partial", counter(20))
	requireMetric(t, i.Metrics.Usage.Spend, "partial", counter(90))

}
