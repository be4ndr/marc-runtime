package statestore

import (
	"encoding/json"
	"reflect"
	"testing"
)

func usageWireFor(t *testing.T, r UsageReceipt, source, model string) []byte {
	t.Helper()
	raw, err := json.Marshal(usageReceiptWire{SchemaVersion: 1, ReceiptID: r.ReceiptID, RunID: r.RunID, InvocationID: r.InvocationID,
		ProviderCallID: r.ProviderCallID, ProviderReceiptID: r.ProviderReceiptID, Source: source, Model: model,
		ReportedAt: r.ReportedAt, State: r.Completeness, InputTokens: r.InputTokens, CachedInputTokens: r.CachedInputTokens,
		OutputTokens: r.OutputTokens, ReasoningTokens: r.ReasoningTokens})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type usageMutationSnapshot struct {
	Usage                   usageSummary
	Receipt                 string
	Revision                int64
	Bank                    RunTokenBalance
	Receipts, Events, Stops int
}

func snapshotUsageMutation(t *testing.T, s *StateStore, d AdmissionDispatchClaim) usageMutationSnapshot {
	t.Helper()
	u, id, rev := readCallUsage(t, s, d.ProviderCall.ProviderCallID)
	bank, err := s.GetRunTokenBalance(accountingContext, d.RunID)
	if err != nil {
		t.Fatal(err)
	}
	return usageMutationSnapshot{u, id, rev, bank, resultCount(t, s, "usage_receipts"), resultCount(t, s, "events"), resultCount(t, s, "run_hard_stops")}
}

func requireUsageMutationUnchanged(t *testing.T, s *StateStore, d AdmissionDispatchClaim, before usageMutationSnapshot) {
	t.Helper()
	if after := snapshotUsageMutation(t, s, d); !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected/replayed receipt changed state: before=%+v after=%+v", before, after)
	}
}

func TestUsageFinalClosesReceiptSequence(t *testing.T) {
	for _, known := range []bool{true, false} {
		t.Run(map[bool]string{true: "known", false: "null"}[known], func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 100)
			admit(t, s, a)
			d := tokenClaim(a, "closed-call", 20)
			claim(t, s, d)
			var amount *int64
			if known {
				amount = counter(10)
			}
			partial := admissionReceipt(d, "initial-partial", "partial", amount)
			final := admissionReceipt(d, "close-final", "final", amount)
			opts := accountingOptions()
			opts.Audit = []AuditRow{{EventType: "usage-finality", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp, Details: json.RawMessage(`{}`)}}
			for _, r := range []UsageReceipt{partial, final} {
				if err := s.AcceptUsageReceipt(accountingContext, r, opts); err != nil {
					t.Fatal("PARTIAL -> FINAL failed", err)
				}
			}
			before := snapshotUsageMutation(t, s, d)
			if before.Usage.ReceiptState != "final" || before.Revision != 2 || before.Events != 2 || before.Receipts != 2 {
				t.Fatal("FINAL not durably accepted", before)
			}
			if !known {
				if before.Usage.Work.Value != nil || before.Bank.NormalUsed != 0 || before.Bank.NormalAvailable != 100 {
					t.Fatal("FINAL NULL invented known consumption", before)
				}
			}
			for _, state := range []string{"partial", "unknown", "final"} {
				late := admissionReceipt(d, "late-"+state, state, counter(10))
				for _, wire := range []bool{false, true} {
					var err error
					if wire {
						err = s.AcceptUsageReceiptJSON(accountingContext, usageWireFor(t, late, "fake", "fake-model"), opts)
					} else {
						err = s.AcceptUsageReceipt(accountingContext, late, opts)
					}
					if err == nil {
						t.Fatal("new receipt accepted after FINAL", state, wire)
					}
					requireUsageMutationUnchanged(t, s, d, before)
				}
			}
			conflict := final
			conflict.InputTokens = counter(11)
			if err := s.AcceptUsageReceipt(accountingContext, conflict, opts); err == nil {
				t.Fatal("conflicting FINAL identity accepted")
			}
			// Both the final and a previously accepted partial replay exactly.
			for _, r := range []UsageReceipt{final, partial} {
				if err := s.AcceptUsageReceipt(accountingContext, r, opts); err != nil {
					t.Fatal("exact replay rejected after FINAL", err)
				}
				if err := s.AcceptUsageReceiptJSON(accountingContext, usageWireFor(t, r, "fake", "fake-model"), opts); err != nil {
					t.Fatal("wire replay rejected after FINAL", err)
				}
			}
			requireUsageMutationUnchanged(t, s, d, before)
			s = restartAgentStore(t, s)
			if err := s.AcceptUsageReceipt(accountingContext, final, opts); err != nil {
				t.Fatal("restart rejected FINAL replay", err)
			}
			if err := s.AcceptUsageReceipt(accountingContext, admissionReceipt(d, "restart-late", "partial", counter(10)), opts); err == nil {
				t.Fatal("restart reopened receipt sequence")
			}
			requireUsageMutationUnchanged(t, s, d, before)
			var duration *int64
			if err := s.db.QueryRow(`SELECT duration_ns FROM provider_calls WHERE provider_call_id=?`, d.ProviderCall.ProviderCallID).Scan(&duration); err != nil || duration != nil {
				t.Fatal("FINAL supplied physical completion", duration, err)
			}
		})
	}
}

func TestUsageFinalityPreservesDistinctCallCumulativeAccounting(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 200)
	admit(t, s, a)
	for n, amounts := range [][2]int64{{10, 20}, {15, 40}} {
		id := []string{"original", "fallback"}[n]
		d := tokenClaim(a, id, 40)
		claim(t, s, d)
		acceptUsage(t, s, admissionReceipt(d, id+"-partial", "partial", counter(amounts[0])))
		final := admissionReceipt(d, id+"-final", "final", counter(amounts[1]))
		acceptUsage(t, s, final)
		acceptUsage(t, s, final)
		if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
			t.Fatal(err)
		}
	}
	// Receipt totals are 85; authoritative distinct-call WORK is 20+40=60.
	requireTokenBank(t, s, a.RunID, RunTokenBalance{200, 60, 140, 20, 0, 20, 220})
	third := tokenClaim(a, "null-final", 20)
	claim(t, s, third)
	acceptUsage(t, s, admissionReceipt(third, "unknown-final", "final", nil))
	before := snapshotUsageMutation(t, s, third)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{200, 60, 140, 20, 0, 20, 220})
	s = restartAgentStore(t, s)
	late := admissionReceipt(third, "cannot-correct-final-null", "partial", counter(20))
	if err := s.AcceptUsageReceipt(accountingContext, late, accountingOptions()); err == nil {
		t.Fatal("FINAL NULL was corrected after restart")
	}
	requireUsageMutationUnchanged(t, s, third, before)

}

func TestUsageFinalNullDoesNotErasePreviouslyConfirmedUsage(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "known-then-null", 20)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "known-before-final", "partial", counter(10)))
	acceptUsage(t, s, admissionReceipt(d, "final-missing-counters", "final", nil))
	u, id, rev := readCallUsage(t, s, d.ProviderCall.ProviderCallID)
	if id != "final-missing-counters" || rev != 2 || u.ReceiptState != "final" {
		t.Fatal("FINAL with missing counters did not close the sequence", u, id, rev)
	}
	requireMetric(t, u.Work, "partial", counter(10))
	var reported, work *int64
	if err := s.db.QueryRow(`SELECT input_tokens,work_tokens FROM usage_receipts WHERE receipt_id=?`, id).Scan(&reported, &work); err != nil || reported != nil || work != nil {
		t.Fatal("FINAL NULL receipt was rewritten/backfilled", reported, work, err)
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 10, 90, 10, 0, 10, 110})
	before := snapshotUsageMutation(t, s, d)
	if err := s.AcceptUsageReceipt(accountingContext, admissionReceipt(d, "post-final-correction", "partial", counter(15)), accountingOptions()); err == nil {
		t.Fatal("FINAL NULL reopened confirmed usage")
	}
	requireUsageMutationUnchanged(t, s, d, before)
}

func TestUsageAuthorityRejectsUntrustedProvenanceAndUnclaimedCalls(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "authorized-call", 20)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "authorized-partial", "partial", counter(10)))
	before := snapshotUsageMutation(t, s, d)
	opts := accountingOptions()
	opts.Audit = []AuditRow{{EventType: "unauthorized-usage", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp, Details: json.RawMessage(`{}`)}}
	for _, change := range []func(*UsageReceipt){
		func(r *UsageReceipt) { r.RunID = "wrong-run" },
		func(r *UsageReceipt) { r.InvocationID = "wrong-invocation" },
		func(r *UsageReceipt) { r.ProviderCallID = "unregistered-call" },
	} {
		r := admissionReceipt(d, "unauthorized", "partial", counter(15))
		change(&r)
		if err := s.AcceptUsageReceipt(accountingContext, r, opts); err == nil {
			t.Fatal("unauthorized ancestry accepted")
		}
		requireUsageMutationUnchanged(t, s, d, before)
	}
	for _, sourceModel := range [][2]string{{"different-provider", "fake-model"}, {"fake", "different-model"}} {
		r := admissionReceipt(d, "unauthorized-wire", "partial", counter(15))
		if err := s.AcceptUsageReceiptJSON(accountingContext, usageWireFor(t, r, sourceModel[0], sourceModel[1]), opts); err == nil {
			t.Fatal("unauthorized provider/model accepted")
		}
		requireUsageMutationUnchanged(t, s, d, before)
	}
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	// A registered row alone is not controller dispatch authority in a budgeted Run.
	execSQL(t, s.db, `INSERT INTO provider_calls(provider_call_id,invocation_id,call_ordinal,provider,model,reasoning_effort,started_at)
 VALUES('unclaimed-call',?,2,'fake','fake-model','high',?)`, a.Binding.InvocationID, semanticTimestamp)
	r := admissionReceipt(d, "unclaimed-usage", "partial", counter(15))
	r.ProviderCallID = "unclaimed-call"
	before = snapshotUsageMutation(t, s, d)
	for _, wire := range []bool{false, true} {
		var err error
		if wire {
			err = s.AcceptUsageReceiptJSON(accountingContext, usageWireFor(t, r, "fake", "fake-model"), opts)
		} else {
			err = s.AcceptUsageReceipt(accountingContext, r, opts)
		}
		if err == nil {
			t.Fatal("unclaimed call obtained usage/accounting authority", wire)
		}
		requireUsageMutationUnchanged(t, s, d, before)
	}
	u, _, rev := readCallUsage(t, s, "unclaimed-call")
	if rev != 0 || u.Work.Value != nil {
		t.Fatal("unauthorized mutation changed unclaimed current usage", u, rev)
	}
}

func TestUsageOriginalAuthorityAllowsLateFinalAfterOwnershipChange(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "previous-owner-call", 20)
	claim(t, s, d)
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	seedAdmissionInvocation(t, s, "new-current-agent", "new-current-execution")
	// This is an Orchestrator observation about the original physical call, not a
	// stale agent submission seeking semantic ownership or another dispatch.
	acceptUsage(t, s, admissionReceipt(d, "late-authorized-final", "final", counter(20)))
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 20, 80, 10, 0, 10, 110})
	if err := s.AcceptUsageReceipt(accountingContext, admissionReceipt(d, "late-owner-correction", "partial", counter(20)), accountingOptions()); err == nil {
		t.Fatal("ownership change reopened FINAL sequence")
	}
}

func TestUsageFinalClosureSQLiteGuardAndHistoricalEvidence(t *testing.T) {
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	s, err := openTestMigrations(accountingContext, privateTempDir(t), migrations)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	if err := s.ConfigureRunAdmission(accountingContext, admissionPolicy(100), accountingOptions()); err != nil {
		t.Fatal(err)
	}
	a := admissionFor(t, s, seedAdmissionInvocation(t, s, "historical-final", "historical-final-execution"), pools, 20)
	admit(t, s, a)
	d := tokenClaim(a, "historical-final-call", 20)
	claim(t, s, d)
	acceptUsage(t, s, admissionReceipt(d, "historical-final-receipt", "final", counter(10)))
	before := snapshotUsageMutation(t, s, d)
	s = restartAgentStore(t, s)
	requireUsageMutationUnchanged(t, s, d, before)
	if _, err := s.db.Exec(`INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,completeness,input_tokens,cached_input_tokens,output_tokens,reported_at,accepted_at)
 VALUES('new-post-final',?,?,'partial',10,0,0,?,?)`, a.Binding.InvocationID, d.ProviderCall.ProviderCallID, semanticTimestamp, semanticTimestamp); err == nil {
		t.Fatal("SQLite allowed post-FINAL insertion")
	}
	requireUsageMutationUnchanged(t, s, d, before)
}
