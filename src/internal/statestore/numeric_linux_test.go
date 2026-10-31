package statestore

import (
	"encoding/json"
	"errors"
	"math"

	"reflect"
	"sync"
	"testing"

	"ai-control-plane/internal/numeric"
	"ai-control-plane/src/internal/roleresult"
)

func requireNumeric(t *testing.T, err error, domain string) *numeric.Error {
	t.Helper()
	var failure *numeric.Error
	if !errors.As(err, &failure) || failure.Domain != domain {
		t.Fatalf("want numeric error domain %s, got %v", domain, err)
	}
	return failure
}

func TestReceiptNumericInvariantRollbackAndRestart(t *testing.T) {
	for _, state := range []string{"partial", "final"} {
		t.Run(state, func(t *testing.T) {
			dir := privateTempDir(t)
			s, err := openTest(accountingContext, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			seedInvocation(t, s)
			baseline := callReceipt("valid", "i1-call", "partial", counter(10))
			acceptUsage(t, s, baseline)
			before, receipt, revision := readCallUsage(t, s, "i1-call")
			r := callReceipt("invalid", "i1-call", state, counter(math.MaxInt64))
			r.CachedInputTokens = counter(math.MaxInt64)
			r.OutputTokens = counter(1)
			opts := accountingOptions()
			opts.Audit = accountingAudit()
			failure := requireNumeric(t, s.AcceptUsageReceipt(accountingContext, r, opts), "WORK")
			if failure.Operation != "AcceptUsageReceipt" || failure.RunID != "r1" || failure.InvocationID != "i1" || failure.ProviderCallID != "i1-call" {
				t.Fatal(failure)
			}
			assertUnchanged := func() {
				t.Helper()
				after, id, rev := readCallUsage(t, s, "i1-call")
				if !reflect.DeepEqual(after, before) || id != receipt || rev != revision || resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 0 {
					t.Fatal("invalid receipt or partial SPEND accepted", after, id, rev)
				}
			}
			assertUnchanged()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = openTest(accountingContext, dir)
			if err != nil {
				t.Fatal(err)
			}
			assertUnchanged()
		})
	}
}

func TestAggregateNumericInvariantBeforeCommit(t *testing.T) {
	for _, state := range []string{"partial", "final"} {
		t.Run(state, func(t *testing.T) {
			dir := privateTempDir(t)
			s, err := openTest(accountingContext, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			seedInvocation(t, s)
			first := callReceipt("first", "i1-call", "final", counter(math.MaxInt64))
			first.CachedInputTokens = counter(math.MaxInt64)
			acceptUsage(t, s, first)
			addProviderCall(t, s, "retry", "fallback")
			before, id, rev := readCallUsage(t, s, "retry")
			r := callReceipt("invalid-total", "retry", state, counter(0))
			r.CachedInputTokens = counter(0)
			r.OutputTokens = counter(1)
			opts := accountingOptions()
			opts.Audit = accountingAudit()
			// No metrics binding: accounting validity cannot depend on projection.
			failure := requireNumeric(t, s.AcceptUsageReceipt(accountingContext, r, opts), "WORK")
			if failure.ExecutionID != "e1" || failure.AttemptID != "a1" {
				t.Fatal("missing ancestry", failure)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = openTest(accountingContext, dir)
			if err != nil {
				t.Fatal(err)
			}
			after, newID, newRev := readCallUsage(t, s, "retry")
			if !reflect.DeepEqual(before, after) || newID != id || newRev != rev || resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 0 {
				t.Fatal("aggregate mutation committed", after, newID, newRev)
			}
		})
	}
}

func TestConcurrentAggregateOverflowRejectsOneMutation(t *testing.T) {
	dir := privateTempDir(t)
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedInvocation(t, s)
	metricsInvocationFixture(t, s, "other", "DEVELOPER", "IMPLEMENT", "RUNNING", 1)
	other, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for n, store := range []*StateStore{s, other} {
		wg.Add(1)
		go func(n int, store *StateStore) {
			defer wg.Done()
			<-start
			invocation, call := "i1", "i1-call"
			if n == 1 {
				invocation, call = "other", "other-call"
			}
			r := callReceipt(invocation, call, "final", counter(math.MaxInt64/2+1))
			r.InvocationID = invocation
			opts := accountingOptions()
			opts.Audit = accountingAudit()
			results <- store.AcceptUsageReceipt(accountingContext, r, opts)
		}(n, store)
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else {
			requireNumeric(t, err, "input_tokens")
		}
	}
	if success != 1 || resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("concurrent aggregate escaped validation", success)
	}
}

func TestExtremeResourceInputNeverPanicsOrPersists(t *testing.T) {
	s := testStore(t)
	state, opts := semanticFixture(t, s)
	if err := s.AcceptLeadState(accountingContext, wire(t, state), opts); err != nil {
		t.Fatal(err)
	}
	opts.ExpectedSemanticRevision = 1
	opts.Audit = fixtureAudit()
	for _, value := range []string{"1e1000000000", "1e-1000000000", "1e9223372036854775808", "1e-9223372036854775808", "9223372036854775808"} {
		doc := requestFixture(state)
		doc["requested"] = map[string]any{"work_tokens": json.Number(value)}
		if err := s.AcceptResourceRequest(accountingContext, wire(t, doc), opts); err == nil {
			t.Fatal("invalid numeric input accepted", value)
		}
	}
	if resultCount(t, s, "resource_requests") != 0 || resultCount(t, s, "events") != 0 {
		t.Fatal("invalid external numeric input persisted")
	}
}

func TestCanonicalTimestampRangeAndReplay(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	for _, at := range []string{"9999-12-31T23:59:59-23:00", "0000-01-01T00:00:00+23:00"} {
		r := callReceipt("invalid", "i1-call", "partial", counter(1))
		r.ReportedAt = at
		requireNumeric(t, s.AcceptUsageReceipt(accountingContext, r, opts), "timestamp")
		if resultCount(t, s, "usage_receipts") != 0 || resultCount(t, s, "events") != 0 {
			t.Fatal("invalid normalized timestamp persisted")
		}
	}
	r := callReceipt("valid", "i1-call", "partial", counter(1))
	r.ReportedAt = "2026-10-05T13:00:00+01:00"
	if err := s.AcceptUsageReceipt(accountingContext, r, opts); err != nil {
		t.Fatal(err)
	}
	r.ReportedAt = semanticTimestamp
	if err := s.AcceptUsageReceipt(accountingContext, r, opts); err != nil {
		t.Fatal("normalized replay failed", err)
	}
	if resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("replay appended evidence/audit")
	}
}

func TestOwnershipNumericExhaustion(t *testing.T) {
	for _, domain := range []string{"ownership_epoch", "actor_revision"} {
		t.Run(domain, func(t *testing.T) {
			s := testStore(t)
			seedInvocation(t, s)
			epoch, revision := int64(1), int64(0)
			if domain == "ownership_epoch" {
				epoch = math.MaxInt64
			} else {
				revision = math.MaxInt64
			}
			execSQL(t, s.db, "UPDATE invocations SET ownership_epoch=? WHERE invocation_id='i1'", epoch)
			execSQL(t, s.db, "INSERT INTO run_active_actor VALUES ('r1','DEVELOPER','i1',?,?,?)", epoch, semanticTimestamp, revision)
			actor, err := s.GetActiveActor(accountingContext, "r1")
			if err != nil {
				t.Fatal(err)
			}
			requireNumeric(t, s.RecoverSemanticOwnershipToLead(accountingContext, "r1", ownerOptions(actor)), domain)
			after, err := s.GetActiveActor(accountingContext, "r1")
			if err != nil || !reflect.DeepEqual(actor, after) || resultCount(t, s, "events") != 0 {
				t.Fatal("ownership changed", after, err)
			}
		})
	}
}

// Corruption fixtures may bypass one guard only while seeding the impossible
// successor. Restore its exact definition before testing production operations.
func seedRevision(t *testing.T, s *StateStore, trigger, statement string) {
	t.Helper()
	var definition string
	if err := s.db.QueryRow("SELECT sql FROM sqlite_schema WHERE name=?", trigger).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s.db, "DROP TRIGGER "+trigger)
	execSQL(t, s.db, statement, math.MaxInt64)
	execSQL(t, s.db, definition)
}

func TestMutableRevisionExhaustion(t *testing.T) {
	t.Run("semantic", func(t *testing.T) {
		s := testStore(t)
		state, opts := semanticFixture(t, s)
		execSQL(t, s.db, "UPDATE run_current_semantic_state SET revision=? WHERE run_id='run1'", int64(math.MaxInt64))
		opts.ExpectedSemanticRevision = math.MaxInt64
		opts.Audit = fixtureAudit()
		requireNumeric(t, s.AcceptLeadState(accountingContext, wire(t, state), opts), "semantic_revision")
		if resultCount(t, s, "lead_states") != 0 || resultCount(t, s, "events") != 0 {
			t.Fatal("exhausted semantic revision committed")
		}
	})
	t.Run("current_usage", func(t *testing.T) {
		s := testStore(t)
		seedInvocation(t, s)
		acceptUsage(t, s, callReceipt("partial", "i1-call", "partial", counter(10)))
		seedRevision(t, s, "provider_call_usage_update", "UPDATE provider_call_usage SET revision=? WHERE provider_call_id='i1-call'")
		opts := accountingOptions()
		opts.Audit = accountingAudit()
		requireNumeric(t, s.AcceptUsageReceipt(accountingContext, callReceipt("new", "i1-call", "partial", counter(11)), opts), "provider_call_usage_revision")
		u, id, rev := readCallUsage(t, s, "i1-call")
		if id != "partial" || rev != math.MaxInt64 || *u.Input.Value != 10 || resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 0 {
			t.Fatal("exhausted usage revision committed")
		}
	})

}

func TestProviderCallOrdinalExhaustion(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	if err := s.FinishProviderCall(accountingContext, "r1", "i1-call", 0, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s.db, `INSERT INTO provider_calls(provider_call_id,invocation_id,call_ordinal,provider,model,reasoning_effort,started_at) VALUES ('last','i1',?,'fake','fake','high',?)`, int64(math.MaxInt64), semanticTimestamp)
	if err := s.FinishProviderCall(accountingContext, "r1", "last", 0, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	requireNumeric(t, s.CreateProviderCall(accountingContext, ProviderCall{ProviderCallID: "bad", RunID: "r1", InvocationID: "i1", Provider: "fake", Model: "fake"}, opts), "provider_call_ordinal")
	if resultCount(t, s, "provider_calls") != 2 || resultCount(t, s, "events") != 0 {
		t.Fatal("ordinal exhausted but call accepted")
	}
}

func TestJournalSequenceExhaustionRollsBackMutation(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	execSQL(t, s.db, `INSERT INTO events(event_seq,run_id,event_type,actor_role,occurred_at) VALUES (?,'r1','opaque','ORCHESTRATOR',?)`, int64(math.MaxInt64), semanticTimestamp)
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	requireNumeric(t, s.AcceptUsageReceipt(accountingContext, callReceipt("bad", "i1-call", "partial", counter(10)), opts), "event_sequence")
	_, id, revision := readCallUsage(t, s, "i1-call")
	if id != "" || revision != 0 || resultCount(t, s, "usage_receipts") != 0 || resultCount(t, s, "events") != 1 {
		t.Fatal("journal exhaustion committed mutation")
	}
}

func TestFindingOccurrenceExhaustionRollsBackResultAndOwnership(t *testing.T) {
	s := testStore(t)
	raw := resultFixture(t, "qa-failed")
	r, err := roleresult.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	seedResultBinding(t, s, r, 1)
	if err := s.AcceptRoleResult(accountingContext, raw, []roleresult.Finding{newFinding(r)}, ResultFence{1, 0}); err != nil {
		t.Fatal(err)
	}
	execSQL(t, s.db, `INSERT INTO findings SELECT run_id,'fnd-000006',6,origin_cycle,origin_result_id,0,'exhaustion fixture',created_at FROM findings WHERE finding_id='fnd-000005'`)
	execSQL(t, s.db, `INSERT INTO finding_occurrences SELECT ?,run_id,'fnd-000006',result_id,observed_at FROM finding_occurrences WHERE finding_id='fnd-000005'`, int64(math.MaxInt64))
	var doc map[string]any
	if err := json.Unmarshal(resultFixture(t, "reviewer-changes-required"), &doc); err != nil {
		t.Fatal(err)
	}
	doc["result_ordinal"] = 4
	doc["result_id"] = roleresult.ResultID(2, 4)
	doc["execution_id"] = "later-execution"
	doc["attempt_id"] = "later-attempt"
	doc["invocation_id"] = "later-invocation"
	raw = wire(t, doc)
	later, err := roleresult.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	seedResultBinding(t, s, later, 3)
	before, err := s.GetActiveActor(accountingContext, later.RunID)
	if err != nil {
		t.Fatal(err)
	}
	requireNumeric(t, s.AcceptRoleResultAndReturnToLead(accountingContext, raw, nil, ResultReturnOptions{Fence: ResultFence{3, 2}, ReturnedAt: semanticTimestamp, Audit: fixtureAudit()}), "finding_occurrence_id")
	after, err := s.GetActiveActor(accountingContext, later.RunID)
	if err != nil || !reflect.DeepEqual(before, after) || resultCount(t, s, "role_results") != 1 || resultCount(t, s, "finding_occurrences") != 2 || resultCount(t, s, "events") != 0 {
		t.Fatal("occurrence exhaustion partially accepted result or ownership", after, err)
	}
}
