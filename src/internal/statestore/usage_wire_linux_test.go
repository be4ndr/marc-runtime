package statestore

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"marc-runtime/internal/numeric"
)

func usageJSONFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../../schemas/testdata/usage-receipts/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestUsageReceiptJSONFixturesAndAtomicRejection(t *testing.T) {
	var cases []struct {
		File     string `json:"file"`
		Accepted bool   `json:"accepted"`
	}
	if err := json.Unmarshal(usageJSONFixture(t, "manifest.json"), &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.File, func(t *testing.T) {
			s := testStore(t)
			seedInvocation(t, s)
			before, _, revision := readCallUsage(t, s, "i1-call")
			opts := accountingOptions()
			opts.Audit = accountingAudit()
			err := s.AcceptUsageReceiptJSON(accountingContext, usageJSONFixture(t, tc.File), opts)
			if (err == nil) != tc.Accepted {
				t.Fatalf("accepted=%v: %v", tc.Accepted, err)
			}
			after, _, rev := readCallUsage(t, s, "i1-call")
			if !tc.Accepted && (!reflect.DeepEqual(before, after) || rev != revision || resultCount(t, s, "usage_receipts") != 0 || resultCount(t, s, "events") != 0) {
				t.Fatal("rejected wire changed usage/receipts/audit")
			}
			if tc.Accepted {
				var w usageReceiptWire
				if err := json.Unmarshal(usageJSONFixture(t, tc.File), &w); err != nil {
					t.Fatal(err)
				}
				if after.ReceiptState != w.State || !reflect.DeepEqual(after.Input.Value, w.InputTokens) || !reflect.DeepEqual(after.Cache.Value, w.CachedInputTokens) || !reflect.DeepEqual(after.Output.Value, w.OutputTokens) || !reflect.DeepEqual(after.Reasoning.Value, w.ReasoningTokens) {
					t.Fatal("wire mapping lost nullable counters/completeness", after)
				}
				if w.State == "partial" && w.InputTokens == nil && after.Work.State != "partial" || w.State == "unknown" && w.InputTokens != nil && after.Input.State != "partial" {
					t.Fatal("receipt state and knowledge were conflated", after)
				}
				switch tc.File {
				case "final-missing-cache.json":
					requireMetric(t, after.Work, "final", counter(135))
					requireMetric(t, after.Spend, "unknown", nil)
				case "unknown-known.json":
					requireMetric(t, after.Work, "partial", counter(100))
					requireMetric(t, after.Fresh, "unknown", nil)
					requireMetric(t, after.Spend, "unknown", nil)
				case "final-null.json":
					for _, v := range usageValues(&after) {
						requireMetric(t, *v, "unknown", nil)
					}
				}
				if resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 1 {
					t.Fatal("receipt/audit not accepted once")
				}
			}
		})
	}
}

func TestUnicodeProviderCallMetadataRoundTripsThroughReceiptAndMetrics(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	provider, model := strings.Repeat("供", 256), strings.Repeat("模型", 128)
	d := capabilityClaim(a, "unicode-receipt-claim")
	d.ProviderCall.Provider, d.Capabilities.Provider = provider, provider
	d.ProviderCall.Model, d.Capabilities.Model = model, model
	claim(t, s, d)
	raw := string(usageJSONFixture(t, "unknown.json"))
	raw = strings.Replace(raw, `"run_id": "r1"`, `"run_id": "`+a.RunID+`"`, 1)
	raw = strings.Replace(raw, `"invocation_id": "i1"`, `"invocation_id": "`+a.Binding.InvocationID+`"`, 1)
	raw = strings.Replace(raw, `"provider_call_id": "i1-call"`, `"provider_call_id": "`+d.ProviderCall.ProviderCallID+`"`, 1)
	raw = strings.Replace(raw, `"source": "fake"`, `"source": "`+provider+`"`, 1)
	raw = strings.Replace(raw, `"model": "fake-model"`, `"model": "`+model+`"`, 1)
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(raw), accountingOptions()); err != nil {
		t.Fatalf("receipt rejected matching Unicode provenance: %v", err)
	}
	metricsDir := privateTempDir(t)
	if err := s.EnableMetricsProjection(accountingContext, a.RunID, metricsDir); err != nil {
		t.Fatalf("metrics projection rejected matching Unicode provenance: %v", err)
	}
	projected, err := os.ReadFile(filepath.Join(metricsDir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	metrics := metricsDecode(t, projected)
	_, invocation := metricsFind(t, metrics, a.Binding.InvocationID)
	if len(invocation.ProviderCalls) != 1 || invocation.ProviderCalls[0].Source != provider || invocation.ProviderCalls[0].Model != model {
		t.Fatalf("projection changed provider metadata: %+v", invocation.ProviderCalls)
	}
}

func TestUsageReceiptJSONProgressionReplayAndRestart(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	partial := usageJSONFixture(t, "partial-known.json")
	if err := s.AcceptUsageReceiptJSON(accountingContext, partial, opts); err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct{ old, new string }{
		{`"source": "fake"`, `"source": "different"`},
		{`"model": "fake-model"`, `"model": "different"`},
		{`"invocation_id": "i1"`, `"invocation_id": "different"`},
		{`"provider_call_id": "i1-call"`, `"provider_call_id": "absent"`},
		{`"run_id": "r1"`, `"run_id": "different"`},
		{`"input_tokens": 100`, `"input_tokens": 101`},
	} {
		if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(strings.Replace(string(partial), change.old, change.new, 1)), opts); err == nil {
			t.Fatal("conflicting replay/provenance accepted", change.old)
		}
	}
	// Equivalent timestamp offsets and JSON field/whitespace order replay exactly.
	var doc map[string]any
	if err := json.Unmarshal(partial, &doc); err != nil {
		t.Fatal(err)
	}
	doc["reported_at"] = "2026-10-01T13:00:00+01:00"
	replay, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AcceptUsageReceiptJSON(accountingContext, replay, opts); err != nil {
		t.Fatal(err)
	}
	_, _, rev := readCallUsage(t, s, "i1-call")
	if rev != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("replay advanced usage/audit")
	}
	newPartial := strings.Replace(string(partial), "wire-receipt", "new-partial", 1)
	newPartial = strings.Replace(newPartial, `"input_tokens": 100`, `"input_tokens": 110`, 1)
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(newPartial), opts); err != nil {
		t.Fatal(err)
	}
	regression := strings.Replace(strings.Replace(newPartial, "new-partial", "bad-partial", 1), `"input_tokens": 110`, `"input_tokens": 109`, 1)
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(regression), opts); err == nil {
		t.Fatal("regressive partial accepted")
	}
	final := strings.Replace(string(usageJSONFixture(t, "final.json")), "wire-receipt", "new-final", 1)
	final = strings.Replace(final, "2026-10-01T12:00:00Z", "2020-01-01T00:00:00Z", 1)
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(final), opts); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		strings.Replace(final, "new-final", "second-final", 1),
		strings.Replace(strings.Replace(final, "new-final", "regression", 1), `"input_tokens": 120`, `"input_tokens": 119`, 1),
	} {
		if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(invalid), opts); err == nil {
			t.Fatal("invalid final accepted")
		}
	}
	// An earlier exact partial replay remains valid after FINAL. No receipt selection.
	if err := s.AcceptUsageReceiptJSON(accountingContext, partial, opts); err != nil {
		t.Fatal(err)
	}
	before, id, revision := readCallUsage(t, s, "i1-call")
	if id != "new-final" || before.Work.State != "final" || *before.Work.Value != 135 || *before.Spend.Value != 105 {
		t.Fatal(before, id)
	}
	dir := filepath.Dir(s.Info().Path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.AcceptUsageReceiptJSON(accountingContext, []byte(final), opts); err != nil {
		t.Fatal(err)
	}
	after, afterID, afterRev := readCallUsage(t, restarted, "i1-call")
	if !reflect.DeepEqual(before, after) || id != afterID || revision != afterRev || resultCount(t, restarted, "events") != 3 {
		t.Fatal("restart changed current usage or replay audit")
	}
}

func TestUsageReceiptJSONMultiCallExactActualsAndAggregateRollback(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	// Decimal integer parsing preserves values beyond float64's exact range.
	raw := string(usageJSONFixture(t, "partial-null.json"))
	raw = strings.Replace(raw, `"input_tokens": null`, `"input_tokens": 18014398509481985`, 1)
	raw = strings.Replace(raw, `"output_tokens": null`, `"output_tokens": 0`, 1)
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(raw), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	u, _, _ := readCallUsage(t, s, "i1-call")
	if u.Input.Value == nil || *u.Input.Value != (1<<54)+1 || u.Work.Value == nil || *u.Work.Value != (1<<54)+1 || u.Spend.Value != nil {
		t.Fatal("inexact parsing or NULL cache inference", u)
	}
	addProviderCall(t, s, "fallback-call", "fallback-model")
	fallback := strings.Replace(string(usageJSONFixture(t, "final.json")), "i1-call", "fallback-call", 1)
	fallback = strings.Replace(fallback, "fake-model", "fallback-model", 1)
	fallback = strings.Replace(fallback, "wire-receipt", "fallback-receipt", 1)
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(fallback), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	_, invocation := metricsFind(t, metricsRead(t, s), "i1")
	requireMetric(t, invocation.Metrics.Usage.Work, "partial", counter((1<<54)+1+135))
	requireMetric(t, invocation.Metrics.Usage.Spend, "partial", counter(105))
	before, id, revision := readCallUsage(t, s, "fallback-call")
	// The single receipt is representable, but its affected Run input total is not.
	overflow := strings.Replace(string(usageJSONFixture(t, "partial-null.json")), "i1-call", "fallback-call", 1)
	overflow = strings.Replace(overflow, "fake-model", "fallback-model", 1)
	overflow = strings.Replace(overflow, "wire-receipt", "aggregate-overflow", 1)
	overflow = strings.Replace(overflow, `"input_tokens": null`, `"input_tokens": 9223372036854775807`, 1)
	// FINAL already froze fallback usage; use a new sequential call to exercise aggregate validation.
	addProviderCall(t, s, "later-call", "fallback-model")
	overflow = strings.Replace(overflow, "fallback-call", "later-call", 1)
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	err := s.AcceptUsageReceiptJSON(accountingContext, []byte(overflow), opts)
	var invariant *numeric.Error
	if !errors.As(err, &invariant) {
		t.Fatal("aggregate overflow not rejected", err)
	}
	after, afterID, afterRev := readCallUsage(t, s, "fallback-call")
	_, _, laterRev := readCallUsage(t, s, "later-call")
	if !reflect.DeepEqual(before, after) || id != afterID || revision != afterRev || laterRev != 0 || resultCount(t, s, "usage_receipts") != 2 || resultCount(t, s, "events") != 0 {
		t.Fatal("aggregate numeric failure committed state/audit")
	}
}

func TestUsageReceiptJSONCanonicalTimeAndNullableReplay(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	base := string(usageJSONFixture(t, "partial-null.json"))
	invalid := strings.Replace(base, "2026-10-01T12:00:00Z", "9999-12-31T23:30:00-01:00", 1)
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(invalid), accountingOptions()); err == nil {
		t.Fatal("unpersistable normalized timestamp accepted")
	}
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(base), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	replay := strings.Replace(base, `"state": "partial"`, `"state": "partial", "reasoning_tokens": null, "provider_receipt_id": null`, 1)
	if err := s.AcceptUsageReceiptJSON(accountingContext, []byte(replay), accountingOptions()); err != nil {
		t.Fatal("omitted/null optional counters changed evidence identity", err)
	}
	_, _, rev := readCallUsage(t, s, "i1-call")
	if rev != 1 || resultCount(t, s, "usage_receipts") != 1 {
		t.Fatal("nullable replay inserted/updated evidence")
	}
}

func TestUsageReceiptJSONTruthfulOverrunAndHeldSettlement(t *testing.T) {
	s, _ := accountingFixture(t)

	if err := s.AcceptUsageReceiptJSON(accountingContext, usageJSONFixture(t, "final.json"), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	u, _, _ := readCallUsage(t, s, "i1-call")
	if u.Work.Value == nil || *u.Work.Value != 135 {
		t.Fatal("actual overrun rejected/clamped", u)
	}

}

func TestUsageReceiptJSONNumericFailureAndAuditRollback(t *testing.T) {
	for _, state := range []string{"final", "partial"} {
		t.Run(state, func(t *testing.T) {
			s := testStore(t)
			seedInvocation(t, s)
			raw := strings.Replace(string(usageJSONFixture(t, "final.json")), `"input_tokens": 120`, `"input_tokens": 9223372036854775807`, 1)
			raw = strings.Replace(raw, `"cached_input_tokens": 30`, `"cached_input_tokens": 9223372036854775807`, 1)
			raw = strings.Replace(raw, `"state": "final"`, `"state": "`+state+`"`, 1)
			opts := accountingOptions()
			opts.Audit = accountingAudit()
			err := s.AcceptUsageReceiptJSON(accountingContext, []byte(raw), opts)
			var invariant *numeric.Error
			if !errors.As(err, &invariant) {
				t.Fatal("overflow not classified", err)
			}
			u, _, revision := readCallUsage(t, s, "i1-call")
			if revision != 0 || u.Spend.Value != nil || resultCount(t, s, "usage_receipts") != 0 || resultCount(t, s, "events") != 0 {
				t.Fatal("overflow partly accepted")
			}
		})
	}
	s := testStore(t)
	seedInvocation(t, s)
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	opts.Audit[0].Details = json.RawMessage(`{`)
	if err := s.AcceptUsageReceiptJSON(accountingContext, usageJSONFixture(t, "final.json"), opts); err == nil {
		t.Fatal("invalid audit accepted")
	}
	_, _, rev := readCallUsage(t, s, "i1-call")
	if rev != 0 || resultCount(t, s, "usage_receipts") != 0 {
		t.Fatal("audit failure committed usage")
	}
}
