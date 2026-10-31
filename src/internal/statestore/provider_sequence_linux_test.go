package statestore

import (
	"context"
	"fmt"
	"math"
	"path/filepath"

	"testing"
	"time"
)

func TestProviderCallSerialActivityAndRestart(t *testing.T) {
	dir := privateTempDir(t)
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	seedInvocation(t, s)
	next := ProviderCall{ProviderCallID: "retry", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "fallback"}
	acceptUsage(t, s, callReceipt("final-first", "i1-call", "final", counter(10)))
	if err := s.CreateProviderCall(accountingContext, next, accountingOptions()); err == nil {
		t.Fatal("FINAL usage ended physical activity")
	}
	for _, run := range []string{"r2", "missing"} {
		if err := s.FinishProviderCall(accountingContext, run, "i1-call", 1_000_000, accountingOptions()); err == nil {
			t.Fatal("foreign controller finish accepted")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateProviderCall(accountingContext, next, accountingOptions()); err == nil {
		t.Fatal("restart lost active call")
	}
	before := metricsRead(t, s)
	bad := accountingOptions()
	bad.Audit = accountingAudit()
	bad.Audit[0].EventType = ""
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", 1_000_000, bad); err == nil {
		t.Fatal("bad audit accepted")
	}
	if err := s.CreateProviderCall(accountingContext, next, accountingOptions()); err == nil {
		t.Fatal("failed finish freed active slot")
	}
	good := accountingOptions()
	good.Audit = accountingAudit()
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", 1_000_000, good); err != nil {
		t.Fatal(err)
	}
	after := metricsRead(t, s)
	if before.SourceSnapshot == after.SourceSnapshot {
		t.Fatal("finish absent from source identity")
	}
	_, i := metricsFind(t, after, "i1")
	if i.ProviderCalls[0].FinishedAt == nil {
		t.Fatal("missing finish projection")
	}
	requireMetric(t, i.Metrics.Usage.Work, "final", counter(10))
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", 1_000_000, good); err != nil {
		t.Fatal(err)
	}
	if resultCount(t, s, "events") != 1 {
		t.Fatal("duplicate finish appended audit")
	}
	for _, stmt := range []string{
		`UPDATE provider_calls SET duration_ns=NULL WHERE provider_call_id='i1-call'`,
		`UPDATE provider_calls SET model='changed' WHERE provider_call_id='i1-call'`,
		`DELETE FROM provider_calls WHERE provider_call_id='i1-call'`,
	} {
		if _, err := s.db.Exec(stmt); err == nil {
			t.Fatal("call identity/activity rewritten", stmt)
		}
	}
	if err := s.CreateProviderCall(accountingContext, next, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishProviderCall(accountingContext, "r1", "retry", 1_000_000, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	// An interrupted request can finish without any receipt. Its usage stays unknown.
	_, i = metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.ProviderCalls[1].Usage.Work, "unknown", nil)
	requireMetric(t, i.Metrics.Usage.Work, "partial", counter(10))
	marker := metricsRead(t, s).SourceSnapshot
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	if metricsRead(t, s).SourceSnapshot != marker {
		t.Fatal("restart changed activity/usage snapshot")
	}
	next.ProviderCallID = "fallback"
	if err := s.CreateProviderCall(accountingContext, next, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO provider_calls(provider_call_id,invocation_id,call_ordinal,provider,model,reasoning_effort,started_at,duration_ns) VALUES ('bypass','i1',4,'fake','fake','',?,?)`, semanticTimestamp, int64(1)); err == nil {
		t.Fatal("registered pre-finished call")
	}
}

func TestProviderCallConcurrentRegistrationPerInvocation(t *testing.T) {
	for _, independent := range []bool{false, true} {
		t.Run(fmt.Sprint(independent), func(t *testing.T) {
			s := testStore(t)
			seedInvocation(t, s)
			if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", 1_000_000, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			if independent {
				addAccountingInvocation(t, s, "i2", "r1", "DEVELOPER", "IMPLEMENT")
				if err := s.FinishProviderCall(accountingContext, "r1", "i2-call", 1_000_000, accountingOptions()); err != nil {
					t.Fatal(err)
				}
			}
			other, err := openTest(accountingContext, filepath.Dir(s.Info().Path))
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			ctx, cancel := context.WithTimeout(accountingContext, 5*time.Second)
			defer cancel()
			results := make(chan error, 2)
			start := make(chan struct{})
			for n, store := range []*StateStore{s, other} {
				go func(n int, store *StateStore) {
					<-start
					invocation := "i1"
					if independent && n == 1 {
						invocation = "i2"
					}
					results <- store.CreateProviderCall(ctx, ProviderCall{ProviderCallID: fmt.Sprintf("race-%d", n), RunID: "r1", InvocationID: invocation, Provider: "fake", Model: "fake"}, accountingOptions())
				}(n, store)
			}
			close(start)
			accepted := 0
			for n := 0; n < 2; n++ {
				if <-results == nil {
					accepted++
				}
			}
			want := 1
			if independent {
				want = 2
			}
			if accepted != want {
				t.Fatal(accepted, want)
			}
			var active, total, usage int
			if err := s.db.QueryRow(`SELECT count(*) FROM provider_calls WHERE duration_ns IS NULL`).Scan(&active); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM provider_calls`).Scan(&total); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM provider_call_usage`).Scan(&usage); err != nil {
				t.Fatal(err)
			}
			if active != want || total != 2*want || usage != total {
				t.Fatal(active, total, usage)
			}
			var ordinals string
			if err := s.db.QueryRow(`SELECT group_concat(call_ordinal) FROM (SELECT call_ordinal FROM provider_calls WHERE invocation_id='i1' ORDER BY call_ordinal)`).Scan(&ordinals); err != nil || ordinals != "1,2" {
				t.Fatal(ordinals, err)
			}
		})
	}
}

// An arbitrary existing lifecycle label is not the missing controller decision.

// Registration and settlement may overlap; neither transaction order releases the hold.

func TestProviderCallInvalidAggregateRejectsWholeMutation(t *testing.T) {
	s, _ := accountingFixture(t)

	first := callReceipt("max-final", "i1-call", "final", counter(math.MaxInt64))
	first.CachedInputTokens = counter(math.MaxInt64)
	acceptUsage(t, s, first)
	addProviderCall(t, s, "retry", "fake")
	second := callReceipt("one-final", "retry", "final", counter(1))
	second.CachedInputTokens = counter(1)
	if err := s.AcceptUsageReceipt(accountingContext, second, accountingOptions()); err == nil {
		t.Fatal("invalid aggregate accepted")
	}
	// No dimension of that mutation is accepted. A valid zero observation can
	// still establish independently known SPEND without manufacturing usage.
	second.InputTokens = counter(0)
	second.CachedInputTokens = counter(0)
	acceptUsage(t, s, second)

	spend, err := invocationCurrentDimension(accountingContext, s.db, "i1", "SPEND")
	if err != nil {
		t.Fatal(err)
	}
	requireMetric(t, spend, "final", counter(0))

}

func TestProviderCallMultiCallDimensionKnowledge(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	first := callReceipt("first-final", "i1-call", "final", counter(10))
	first.CachedInputTokens = nil
	first.OutputTokens = counter(2)
	acceptUsage(t, s, first)
	addProviderCall(t, s, "retry", "fake")
	second := callReceipt("retry-final", "retry", "final", counter(3))
	second.CachedInputTokens = counter(1)
	second.OutputTokens = counter(1)
	acceptUsage(t, s, second)
	work, err := invocationCurrentDimension(accountingContext, s.db, "i1", "WORK")
	if err != nil {
		t.Fatal(err)
	}
	requireMetric(t, work, "final", counter(16))
	spend, err := invocationCurrentDimension(accountingContext, s.db, "i1", "SPEND")
	if err != nil {
		t.Fatal(err)
	}
	requireMetric(t, spend, "partial", counter(3))
	_, i := metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, i.Metrics.Usage.Work, "final", counter(16))
	requireMetric(t, i.Metrics.Usage.Spend, "partial", counter(3))

	if spend.State != "partial" {
		t.Fatal("incomplete accounting became final", spend)
	}
}
