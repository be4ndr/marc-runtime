package statestore

import (
	"encoding/json"
	"math"
	"testing"
)

var accountingContext = orchestratorContext()

func accountingOptions() AccountingOptions { return AccountingOptions{At: semanticTimestamp} }
func accountingAudit() []AuditRow {
	return []AuditRow{{EventType: "opaque-accounting-fixture", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp, Details: json.RawMessage(`{"fixture":true}`)}}
}
func counter(n int64) *int64 { return &n }
func runAccount(pool, resource string) ResourceAccount {
	return ResourceAccount{PoolID: pool, RunID: "r1", Scope: "RUN", ResourceType: resource}
}
func createPool(t *testing.T, s *StateStore, a ResourceAccount, role, purpose string) {
	t.Helper()
	if err := s.CreateResourcePool(accountingContext, ResourcePool{Account: a, Eligibility: primaryTokenEligibility()}, accountingOptions()); err != nil {
		t.Fatal(err)
	}
}

func receipt(id, completeness string) UsageReceipt {
	return UsageReceipt{ReceiptID: id, RunID: "r1", InvocationID: "i1", ProviderCallID: "i1-call", Completeness: completeness, ReportedAt: semanticTimestamp}
}
func acceptUsage(t *testing.T, s *StateStore, r UsageReceipt) {
	t.Helper()
	if err := s.AcceptUsageReceipt(accountingContext, r, accountingOptions()); err != nil {
		t.Fatal(err)
	}
}
func accountingFixture(t *testing.T) (*StateStore, ResourceAccount) {
	s := testStore(t)
	seedInvocation(t, s)
	return s, runAccount("fixture-unused", "WORK")
}
func addAccountingInvocation(t *testing.T, s *StateStore, id, run, role, purpose string) {
	t.Helper()
	execSQL(t, s.db, `INSERT OR IGNORE INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES (?,'RUNNING',1,?,?)`, run, semanticTimestamp, semanticTimestamp)
	execSQL(t, s.db, `INSERT OR IGNORE INTO cycles(run_id,cycle_no,status,created_at) VALUES (?,1,'RUNNING',?)`, run, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES (?,?,1,?,?,'RUNNING',?)`, id+"-execution", run, role, purpose, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES (?,?,?,1,1,'RUNNING',?)`, id+"-attempt", id+"-execution", run, semanticTimestamp)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch) VALUES (?,?,?,?,1,1,?,?,'fake','fake-model','high','RUNNING',?,1)`, id, id+"-attempt", id+"-execution", run, role, purpose, semanticTimestamp)
	fixtureProviderCall(t, s, id, run, "fake-model")
}

func TestAccountingReceiptIdempotencyAndConsistency(t *testing.T) {
	s, _ := accountingFixture(t)
	r := receipt("receipt", "final")
	r.InputTokens = counter(10)
	r.CachedInputTokens = counter(2)
	r.OutputTokens = counter(5)
	r.ReasoningTokens = counter(3)
	r.ProviderRaw = json.RawMessage(`{"b":2,"a":1}`)
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	if err := s.AcceptUsageReceipt(accountingContext, r, opts); err != nil {
		t.Fatal(err)
	}
	r.ReportedAt = "2026-10-05T08:00:00-04:00"
	r.ProviderRaw = json.RawMessage(`{ "a":1, "b":2 }`)
	opts.At = "2026-10-05T12:01:00Z"
	if err := s.AcceptUsageReceipt(accountingContext, r, opts); err != nil {
		t.Fatal(err)
	}
	if resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("retry duplicated receipt/audit")
	}
	var work, spend int64
	if err := s.db.QueryRow(`SELECT work_tokens,spend_tokens FROM usage_receipts`).Scan(&work, &spend); err != nil || work != 15 || spend != 13 {
		t.Fatal(work, spend, err)
	}
	r.OutputTokens = counter(6)
	if err := s.AcceptUsageReceipt(accountingContext, r, opts); err == nil {
		t.Fatal("conflicting receipt content")
	}
	r.ReceiptID = "another-final"
	if err := s.AcceptUsageReceipt(accountingContext, r, opts); err == nil {
		t.Fatal("second final")
	}
	for name, change := range map[string]func(*UsageReceipt){
		"negative":         func(r *UsageReceipt) { r.InputTokens = counter(-1) },
		"cache subset":     func(r *UsageReceipt) { r.InputTokens = counter(1); r.CachedInputTokens = counter(2) },
		"reasoning subset": func(r *UsageReceipt) { r.OutputTokens = counter(1); r.ReasoningTokens = counter(2) },
		"overflow":         func(r *UsageReceipt) { r.InputTokens = counter(math.MaxInt64); r.OutputTokens = counter(1) },
		"cross run":        func(r *UsageReceipt) { r.RunID = "another-run" },
		"missing invocation": func(r *UsageReceipt) {
			r.InvocationID = "missing"
			r.ProviderCallID = "missing-call"
		},
		"invalid completeness": func(r *UsageReceipt) { r.Completeness = "done" },
		"invalid metadata":     func(r *UsageReceipt) { r.ProviderRaw = json.RawMessage(`{`) },
		"invalid time":         func(r *UsageReceipt) { r.ReportedAt = "now" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := receipt("bad", "final")
			change(&bad)
			if err := s.AcceptUsageReceipt(accountingContext, bad, opts); err == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
	// Unknown zero is distinct from a reported zero, even for the same identity.
	addProviderCall(t, s, "null-call", "fake-model")
	unknown := receipt("null", "unknown")
	unknown.ProviderCallID = "null-call"
	acceptUsage(t, s, unknown)
	zero := unknown
	zero.InputTokens = counter(0)
	if err := s.AcceptUsageReceipt(accountingContext, zero, opts); err == nil {
		t.Fatal("NULL treated as zero")
	}
	for _, sql := range []string{`UPDATE usage_receipts SET input_tokens=0`, `DELETE FROM usage_receipts`} {
		if _, err := s.db.Exec(sql); err == nil {
			t.Fatal("immutable object mutated", sql)
		}
	}
}

// Explicit physical fake-call fixture; no call is inferred during receipt acceptance.
func fixtureProviderCall(t *testing.T, s *StateStore, invocation, run, model string) {
	t.Helper()
	if err := s.CreateProviderCall(accountingContext, ProviderCall{ProviderCallID: invocation + "-call", RunID: run, InvocationID: invocation, Provider: "fake", Model: model, ReasoningEffort: "high"}, accountingOptions()); err != nil {
		t.Fatal(err)
	}
}
