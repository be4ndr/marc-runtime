package statestore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"testing"
)

func TestSemanticRequestExactIntegerConversion(t *testing.T) {
	for _, tc := range []struct {
		name, resource, key, text string
		want                      int64
		valid                     bool
	}{
		{"large-work", "WORK", "work_tokens", "18014398509481985", (1 << 54) + 1, true},
		{"max-work", "WORK", "work_tokens", "9223372036854775807", math.MaxInt64, true},
		{"integer-exponent", "WORK", "work_tokens", "1e2", 100, true},
		{"overflow-work", "WORK", "work_tokens", "9223372036854775808", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testStore(t)
			state, opts := semanticFixture(t, s)
			if err := s.AcceptLeadState(accountingContext, wire(t, state), opts); err != nil {
				t.Fatal(err)
			}
			opts.ExpectedSemanticRevision = 1
			doc := requestFixture(state)
			doc["requested"] = map[string]any{tc.key: json.Number(tc.text)}
			err := s.AcceptResourceRequest(accountingContext, wire(t, doc), opts)
			if (err == nil) != tc.valid {
				t.Fatal(err)
			}
			if !tc.valid {
				if resultCount(t, s, "resource_requests") != 0 {
					t.Fatal("request partially committed")
				}
				return
			}
			var amount int64
			var kind string
			if err := s.db.QueryRow(`SELECT requested_amount,typeof(requested_amount) FROM resource_request_amounts WHERE resource_type=?`, tc.resource).Scan(&amount, &kind); err != nil || amount != tc.want || kind != "integer" {
				t.Fatal(amount, kind, err)
			}
		})
	}
}

func TestUsageCumulativeProgression(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after *int64
		completeness  string
		valid         bool
	}{
		{"equal-final", counter(100), counter(100), "final", true},
		{"increasing-final", counter(100), counter(120), "final", true},
		{"lower-final", counter(100), counter(80), "final", false},
		{"unknown-to-known", nil, counter(100), "final", true},
		{"known-to-final-null", counter(100), nil, "final", true},
		{"increasing-partial", counter(100), counter(120), "partial", true},
		{"equal-partial", counter(100), counter(100), "partial", true},
		{"lower-partial", counter(100), counter(80), "partial", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := accountingFixture(t)

			p := receipt("old", "partial")
			p.InputTokens = tc.before
			acceptUsage(t, s, p)
			n := receipt("new", tc.completeness)
			n.InputTokens = tc.after
			n.OutputTokens = counter(0)
			opts := accountingOptions()
			opts.Audit = accountingAudit()
			err := s.AcceptUsageReceipt(accountingContext, n, opts)
			if (err == nil) != tc.valid {
				t.Fatal(err)
			}

			if !tc.valid {
				if resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 0 {
					t.Fatal("contradiction mutated authority")
				}
				return
			}
			// Retry of earlier partial remains idempotent after later progression.
			if err := s.AcceptUsageReceipt(accountingContext, p, opts); err != nil {
				t.Fatal("exact historical retry rejected", err)
			}
			if resultCount(t, s, "usage_receipts") != 2 || resultCount(t, s, "events") != 1 {
				t.Fatal("retry duplicated state/audit")
			}

		})
	}
}

func TestUsagePrimitiveAndSubsetConsistency(t *testing.T) {
	for column := 0; column < 4; column++ {
		t.Run(fmt.Sprint(column), func(t *testing.T) {
			s, _ := accountingFixture(t)
			p := receipt("p1", "partial")
			values := []**int64{&p.InputTokens, &p.CachedInputTokens, &p.OutputTokens, &p.ReasoningTokens}
			*values[column] = counter(100)
			acceptUsage(t, s, p)
			n := receipt("p2", "partial")
			values = []**int64{&n.InputTokens, &n.CachedInputTokens, &n.OutputTokens, &n.ReasoningTokens}
			*values[column] = counter(80)
			if err := s.AcceptUsageReceipt(accountingContext, n, accountingOptions()); err == nil {
				t.Fatal("primitive regression", column)
			}
		})
	}
	s, _ := accountingFixture(t)
	p := receipt("cache", "partial")
	p.CachedInputTokens = counter(20)
	p.ReasoningTokens = counter(10)
	acceptUsage(t, s, p)
	n := receipt("parent", "final")
	n.InputTokens = counter(15)
	if err := s.AcceptUsageReceipt(accountingContext, n, accountingOptions()); err == nil {
		t.Fatal("parent excludes established cached subset")
	}
	n.InputTokens = counter(30)
	n.OutputTokens = counter(5)
	if err := s.AcceptUsageReceipt(accountingContext, n, accountingOptions()); err == nil {
		t.Fatal("parent excludes established reasoning subset")
	}
	n.OutputTokens = counter(15)
	acceptUsage(t, s, n)
	n = receipt("after-final", "partial")
	n.InputTokens = counter(31)
	if err := s.AcceptUsageReceipt(accountingContext, n, accountingOptions()); err == nil {
		t.Fatal("observation exceeded accepted final")
	}
	// Cache attribution can increase while derived SPEND decreases.
	s2, _ := accountingFixture(t)
	p = receipt("partial", "partial")
	p.InputTokens = counter(100)
	p.OutputTokens = counter(10)
	p.CachedInputTokens = counter(0)
	acceptUsage(t, s2, p)
	n = receipt("final", "final")
	n.InputTokens = counter(100)
	n.OutputTokens = counter(10)
	n.CachedInputTokens = counter(90)
	acceptUsage(t, s2, n)
	var spend int64
	if err := s2.db.QueryRow(`SELECT spend_tokens FROM usage_receipts WHERE receipt_id='final'`).Scan(&spend); err != nil || spend != 20 {
		t.Fatal(spend, err)
	}
}

func TestFinalUnknownCacheNeverUsesKnownPartialActual(t *testing.T) {
	s, _ := accountingFixture(t)

	p := receipt("partial", "partial")
	p.InputTokens = counter(80)
	p.OutputTokens = counter(10)
	p.CachedInputTokens = counter(50)
	acceptUsage(t, s, p)
	f := receipt("final", "final")
	f.InputTokens = counter(90)
	f.OutputTokens = counter(10)
	acceptUsage(t, s, f)

	var cache, actual sql.NullInt64
	if err := s.db.QueryRow(`SELECT cached_input_tokens,spend_tokens FROM usage_receipts WHERE receipt_id='final'`).Scan(&cache, &actual); err != nil || cache.Valid || actual.Valid {
		t.Fatal("final NULL backfilled", cache, actual, err)
	}
}

func TestIncreasingPartialsAreNeverSummedAndNullDoesNotEraseEvidence(t *testing.T) {
	s, _ := accountingFixture(t)

	for i, n := range []int64{100, 120, 140} {
		r := receipt(fmt.Sprint("partial", i), "partial")
		r.InputTokens = counter(n)
		acceptUsage(t, s, r)
	}
	acceptUsage(t, s, receipt("gap", "partial"))
	lower := receipt("different-logical-id", "partial")
	lower.InputTokens = counter(130)
	if err := s.AcceptUsageReceipt(accountingContext, lower, accountingOptions()); err == nil {
		t.Fatal("NULL snapshot erased established counter")
	}
	f := receipt("final", "final")
	f.InputTokens = counter(150)
	f.OutputTokens = counter(0)
	acceptUsage(t, s, f)

}

func TestUsageConcurrentConsistency(t *testing.T) {
	for _, kind := range []string{"identical-id", "identical-observation", "conflicting-id", "contradictory-observation"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := accountingFixture(t)
			other, err := openTest(accountingContext, filepath.Dir(s.Info().Path))
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			// Different counters cross: either order must reject the second observation.
			r1 := receipt("one", "partial")
			r1.InputTokens = counter(100)
			r1.OutputTokens = counter(80)
			r2 := r1
			if kind == "identical-observation" || kind == "contradictory-observation" {
				r2.ReceiptID = "two"
			}
			if kind == "conflicting-id" || kind == "contradictory-observation" {
				r2.InputTokens = counter(80)
				r2.OutputTokens = counter(100)
			}
			start := make(chan struct{})
			results := make(chan error, 2)
			opts := accountingOptions()
			opts.Audit = accountingAudit()
			for i, store := range []*StateStore{s, other} {
				go func(i int, store *StateStore) {
					<-start
					r := r1
					if i == 1 {
						r = r2
					}
					results <- store.AcceptUsageReceipt(accountingContext, r, opts)
				}(i, store)
			}
			close(start)
			winners := 0
			for i := 0; i < 2; i++ {
				if <-results == nil {
					winners++
				}
			}
			wantWinners, wantRows := 2, 1
			if kind == "identical-observation" {
				wantRows = 2
			}
			if kind == "conflicting-id" || kind == "contradictory-observation" {
				wantWinners = 1
			}
			if winners != wantWinners || resultCount(t, s, "usage_receipts") != wantRows || resultCount(t, s, "events") != wantRows {
				t.Fatal(winners, resultCount(t, s, "usage_receipts"), resultCount(t, s, "events"))
			}
		})
	}
}
