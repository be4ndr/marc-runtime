package statestore

import (
	"bytes"
	"database/sql"

	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestB6SlowConflictDumpsReleaseDatabase(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "slow-dump")
	claim(t, s, d)
	changed := d
	caps, call := *d.Capabilities, *d.ProviderCall
	changed.Capabilities, changed.ProviderCall = &caps, &call
	changed.ProviderCall.Model, changed.Capabilities.Model = "changed-model", "changed-model"
	before := mutationDatabaseSnapshot(t, s)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		created, err := s.claimAdmissionDispatch(accountingContext, changed, accountingOptions(), func(dir string, c *dispatchClaimConflict, d AdmissionDispatchClaim) (string, string, error) {
			close(entered)
			<-release
			return writeDispatchConflictDumps(dir, c, d)
		})
		if created {
			done <- fmt.Errorf("conflict created a claim")
			return
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("diagnostic did not start")
	}
	other := make(chan error, 1)
	go func() {
		other <- s.numericMutation(accountingContext, "test-concurrent-mutation", a.RunID, nil, func(*sql.Conn) error { return nil })
	}()
	select {
	case err := <-other:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("diagnostic retained database connection/writer lock")
	}
	if !bytes.Equal(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("rejected claim changed authority before diagnostics")
	}
	unblock()
	select {
	case err := <-done:
		var conflict *DispatchClaimConflictError
		if !errors.As(err, &conflict) || conflict.ReportReference == "" || conflict.DiagnosticError != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("conflict did not finish")
	}
	if resultCount(t, s, "provider_calls") != 1 || resultCount(t, s, "admission_dispatch_claims") != 1 {
		t.Fatal("diagnostic changed authorization")
	}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err != nil {
		t.Fatal("original replay changed", created, err)
	}
}

func TestB6ConflictReportSurvivesProjectionFailure(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "projection-conflict")
	claim(t, s, d)
	dir := privateTempDir(t)
	if err := s.EnableMetricsProjection(accountingContext, a.RunID, dir); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(dir, ".metrics.lock")
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lock, 0700); err != nil {
		t.Fatal(err)
	}
	changed := d
	caps, call := *d.Capabilities, *d.ProviderCall
	changed.Capabilities, changed.ProviderCall = &caps, &call
	changed.Capabilities.Model, changed.ProviderCall.Model = "changed-model", "changed-model"
	created, err := s.ClaimAdmissionDispatch(accountingContext, changed, accountingOptions())
	var conflict *DispatchClaimConflictError
	var projection *MetricsProjectionError
	if created || !errors.As(err, &conflict) || conflict.ReportReference == "" || !errors.As(conflict.DiagnosticError, &projection) {
		t.Fatal(created, err)
	}
	var sequence int64
	if _, err := fmt.Sscanf(conflict.ReportReference, "events:%d", &sequence); err != nil {
		t.Fatal(err)
	}
	events, err := s.ListEvents(accountingContext, a.RunID, sequence-1, 1)
	if err != nil || len(events) != 1 || events[0].EventType != "dispatch_claim_conflict" {
		t.Fatal(events, err)
	}
	if resultCount(t, s, "provider_calls") != 1 || resultCount(t, s, "admission_dispatch_claims") != 1 {
		t.Fatal("report failure changed release authority")
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshMetrics(accountingContext, a.RunID); err != nil {
		t.Fatal(err)
	}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err != nil {
		t.Fatal("identical replay failed", created, err)
	}
}

// Seed the immutable claim shape accepted before capability snapshots were
// required. No production validator is bypassed to authorize a new release;
// these rows model evidence already on disk when upgrading the executable.

func TestP18QAReplayProtectsEveryCapabilityAndBound(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "digest-qa")
	claim(t, s, d)
	for _, tc := range []struct {
		name   string
		change func(*AdmissionDispatchClaim)
	}{
		{"context", func(d *AdmissionDispatchClaim) { d.Capabilities.ContextWindowTokens = 101 }},
		{"output capability", func(d *AdmissionDispatchClaim) {
			d.Capabilities.ContextWindowTokens = 101
			d.Capabilities.MaxOutputTokens = 61
		}},
		{"input capability", func(d *AdmissionDispatchClaim) { d.Capabilities.MaxInputTokens = counter(41) }},
		{"omit input capability", func(d *AdmissionDispatchClaim) { d.Capabilities.MaxInputTokens = nil }},
		{"provider identity", func(d *AdmissionDispatchClaim) {
			d.Capabilities.Provider = "different"
			d.ProviderCall.Provider = "different"
		}},
		{"model identity", func(d *AdmissionDispatchClaim) {
			d.Capabilities.Model = "different"
			d.ProviderCall.Model = "different"
		}},
		{"WORK bound", func(d *AdmissionDispatchClaim) { setDispatchBound(d, "WORK", 101) }},
		{"input bound", func(d *AdmissionDispatchClaim) { setDispatchBound(d, "INPUT_TOKENS", 39) }},
		{"output bound", func(d *AdmissionDispatchClaim) { setDispatchBound(d, "OUTPUT_TOKENS", 59) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := d
			caps, call := *d.Capabilities, *d.ProviderCall
			changed.Capabilities, changed.ProviderCall = &caps, &call
			changed.Bounds = append([]DispatchBound(nil), d.Bounds...)
			tc.change(&changed)
			var beforeDigest string
			if err := s.db.QueryRow(`SELECT contract_digest FROM admission_dispatch_claims WHERE claim_id=?`, d.ClaimID).Scan(&beforeDigest); err != nil {
				t.Fatal(err)
			}
			if created, err := s.ClaimAdmissionDispatch(accountingContext, changed, accountingOptions()); created || !errors.Is(err, ErrDispatchClaimConflict) {
				t.Fatalf("valid but altered replay must conflict: created=%t err=%v", created, err)
			} else {
				var conflict *DispatchClaimConflictError
				if !errors.As(err, &conflict) || conflict.ReportReference == "" || conflict.StoredDigest != beforeDigest {
					t.Fatalf("conflict diagnostics missing: %#v", conflict)
				}
			}
			var afterDigest string
			if err := s.db.QueryRow(`SELECT contract_digest FROM admission_dispatch_claims WHERE claim_id=?`, d.ClaimID).Scan(&afterDigest); err != nil || afterDigest != beforeDigest {
				t.Fatal("conflicting replay changed claim digest", afterDigest, err)
			}
			if got := resultCount(t, s, "provider_calls"); got != 1 {
				t.Fatalf("conflicting replay registered another ProviderCall: %d", got)
			}
		})
	}
	before := mutationDatabaseSnapshot(t, s)
	if err := s.CreateProviderCall(accountingContext, *claimFor(a, "bare-bypass", 100).ProviderCall, accountingOptions()); err == nil {
		t.Fatal("bare ProviderCall registration bypassed budgeted admission")
	}
	if !bytes.Equal(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("bare registration rejection changed evidence")
	}
}

func TestP18QAConflictReportIsDurableRedactedAndReferenced(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	s.workingDir = t.TempDir()
	s.dumpDir = filepath.Join(s.workingDir, "dump")
	admit(t, s, a)
	d := capabilityClaim(a, "diagnostic-claim")
	claim(t, s, d)
	var storedDigest string
	if err := s.db.QueryRow(`SELECT contract_digest FROM admission_dispatch_claims WHERE claim_id=?`, d.ClaimID).Scan(&storedDigest); err != nil {
		t.Fatal(err)
	}
	changed := d
	caps, call := *d.Capabilities, *d.ProviderCall
	changed.Capabilities, changed.ProviderCall = &caps, &call
	changed.Bounds = append([]DispatchBound(nil), d.Bounds...)
	changed.ProviderCall.Model = "fake-model-next"
	changed.Capabilities.Model = changed.ProviderCall.Model
	changed.Capabilities.ContextWindowTokens++
	setDispatchBound(&changed, "INPUT_TOKENS", 35)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, changed, accountingOptions()); created || !errors.Is(err, ErrDispatchClaimConflict) {
		t.Fatalf("conflict not rejected: created=%t err=%v", created, err)
	} else {
		var conflict *DispatchClaimConflictError
		if !errors.As(err, &conflict) || conflict.ReportReference == "" || conflict.DiagnosticError != nil {
			t.Fatalf("missing durable diagnostic reference: %#v", conflict)
		}
		var sequence int64
		if _, err := fmt.Sscanf(conflict.ReportReference, "events:%d", &sequence); err != nil || sequence <= 0 {
			t.Fatalf("invalid event reference %q: %v", conflict.ReportReference, err)
		}
		entries, err := s.ListEvents(accountingContext, a.RunID, sequence-1, 1)
		if err != nil || len(entries) != 1 || entries[0].Sequence != sequence || entries[0].EventType != "dispatch_claim_conflict" {
			t.Fatalf("diagnostic event not available by reference: entries=%+v err=%v", entries, err)
		}
		var report map[string]any
		if err := json.Unmarshal(entries[0].Details, &report); err != nil {
			t.Fatal(err)
		}
		if report["kind"] != "dispatch_claim_conflict" || report["diagnostic_available"] != true || report["full_report"] == nil || report["safe_report"] == nil {
			t.Fatalf("incomplete conflict report references: %#v", report)
		}
		raw, _ := json.Marshal(report)
		if bytes.Contains(raw, []byte("fake-model-next")) || bytes.Contains(raw, []byte("parameter_differences")) {
			t.Fatalf("journal copied full diagnostic content: %s", raw)
		}
		var fullSeen, safeSeen bool
		for _, key := range []string{"full_report", "safe_report"} {
			name, ok := report[key].(string)
			if !ok || !strings.HasPrefix(name, "dump/") {
				t.Fatalf("invalid dump reference %s: %#v", key, report[key])
			}
			path := filepath.Join(s.workingDir, name)
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("diagnostic file mode/path: %s %#v %v", path, info, err)
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if key == "full_report" {
				fullSeen = bytes.Contains(contents, []byte("fake-model-next"))
			}
			if key == "safe_report" {
				safeSeen = bytes.Contains(contents, []byte("fake-model-next"))
			}
			if key == "safe_report" && bytes.Contains(contents, []byte("fake-model-next")) {
				t.Fatalf("safe dump leaked full values: %s", contents)
			}
		}
		if !fullSeen || safeSeen {
			t.Fatalf("full/safe dump separation failed: full=%v safe=%v", fullSeen, safeSeen)
		}
		if info, err := os.Stat(s.dumpDir); err != nil || info.Mode().Perm() != 0700 {
			t.Fatalf("dump directory mode: %#v %v", info, err)
		}
	}
	if got := resultCount(t, s, "provider_calls"); got != 1 || resultCount(t, s, "admission_dispatch_claims") != 1 {
		t.Fatal("conflict changed ProviderCall/claim persistence")
	}
	var afterDigest string
	if err := s.db.QueryRow(`SELECT contract_digest FROM admission_dispatch_claims WHERE claim_id=?`, d.ClaimID).Scan(&afterDigest); err != nil || afterDigest != storedDigest {
		t.Fatal("conflict rewrote stored digest", afterDigest, err)
	}

	// The report insert is diagnostic only. Its failure cannot make the request
	// look like an identical replay or create a ProviderCall.
	execSQL(t, s.db, `CREATE TEMP TRIGGER reject_dispatch_conflict_report BEFORE INSERT ON main.events
WHEN NEW.event_type='dispatch_claim_conflict' BEGIN SELECT RAISE(ABORT,'diagnostic failure fixture'); END`)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, changed, accountingOptions()); created || !errors.Is(err, ErrDispatchClaimConflict) {
		t.Fatalf("diagnostic failure weakened rejection: created=%t err=%v", created, err)
	} else {
		var conflict *DispatchClaimConflictError
		if !errors.As(err, &conflict) || conflict.ReportReference != "" || conflict.DiagnosticError == nil {
			t.Fatalf("diagnostic failure not reported safely: %#v", conflict)
		}
	}
	if got := resultCount(t, s, "provider_calls"); got != 1 || resultCount(t, s, "admission_dispatch_claims") != 1 {
		t.Fatal("failed diagnostic registered a ProviderCall or altered claim")
	}
}

func TestP18QAConflictDumpFailureStillRejectsAndJournals(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "dump-failure-claim")
	claim(t, s, d)
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	s.dumpDir = filepath.Join(blocked, "dump")
	changed := d
	caps, call := *d.Capabilities, *d.ProviderCall
	changed.Capabilities, changed.ProviderCall = &caps, &call
	changed.ProviderCall.Model, changed.Capabilities.Model = "changed-model", "changed-model"
	if created, err := s.ClaimAdmissionDispatch(accountingContext, changed, accountingOptions()); created || !errors.Is(err, ErrDispatchClaimConflict) {
		t.Fatalf("dump failure weakened conflict rejection: created=%t err=%v", created, err)
	}
	entries, err := s.ListEvents(accountingContext, a.RunID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range entries {
		if event.EventType == "dispatch_claim_conflict" {
			found = true
			var detail map[string]any
			if json.Unmarshal(event.Details, &detail) != nil || detail["diagnostic_available"] != false {
				t.Fatalf("missing unavailable diagnostic metadata: %s", event.Details)
			}
		}
	}
	if !found {
		t.Fatal("conflict metadata was not journaled")
	}
}

func TestP18QAInvalidDispatchIsFullyAtomic(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*AdmissionDispatchClaim, *AccountingOptions)
	}{
		{"missing capabilities", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.Capabilities = nil }},
		{"negative context", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.Capabilities.ContextWindowTokens = -1 }},
		{"zero output capability", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.Capabilities.MaxOutputTokens = 0 }},
		{"negative output capability", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.Capabilities.MaxOutputTokens = -1 }},
		{"negative optional input", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.Capabilities.MaxInputTokens = counter(-1) }},
		{"zero optional input exceeded", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.Capabilities.MaxInputTokens = counter(0) }},
		{"invalid capability UTF8", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.Capabilities.Model = string([]byte{255}) }},
		{"invalid provider call identifier", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.ProviderCall.ProviderCallID = "bad_call" }},
		{"provider metadata exceeds code-point bound", func(d *AdmissionDispatchClaim, _ *AccountingOptions) {
			d.ProviderCall.Model = strings.Repeat("m", 257)
			d.Capabilities.Model = d.ProviderCall.Model
		}},
		{"duplicate bound", func(d *AdmissionDispatchClaim, _ *AccountingOptions) {
			d.Bounds = append(d.Bounds, DispatchBound{"WORK", 100})
		}},
		{"negative bound", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { setDispatchBound(d, "INPUT_TOKENS", -1) }},
		{"missing WORK", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { removeDispatchBound(d, "WORK") }},
		{"overflow", func(d *AdmissionDispatchClaim, _ *AccountingOptions) {
			setDispatchBound(d, "INPUT_TOKENS", math.MaxInt64)
			setDispatchBound(d, "OUTPUT_TOKENS", 1)
		}},
		{"ancestry mismatch", func(d *AdmissionDispatchClaim, _ *AccountingOptions) { d.ProviderCall.InvocationID = "other" }},
		{"post-insert audit failure", func(_ *AdmissionDispatchClaim, o *AccountingOptions) {
			o.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 1000)
			admit(t, s, a)
			d, opts := capabilityClaim(a, "atomic-qa"), accountingOptions()
			tc.change(&d, &opts)
			before := mutationDatabaseSnapshot(t, s)
			if created, err := s.ClaimAdmissionDispatch(accountingContext, d, opts); created || err == nil {
				t.Fatalf("invalid claim accepted: created=%t err=%v", created, err)
			}
			if !bytes.Equal(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("rejected dispatch changed persisted state")
			}
		})
	}
}

func TestP18QACapabilityArithmeticBoundaries(t *testing.T) {
	call := ProviderCall{Provider: "fake", Model: "fake-model"}
	caps := &ProviderModelCapabilities{Provider: call.Provider, Model: call.Model, ContextWindowTokens: math.MaxInt64, MaxOutputTokens: math.MaxInt64}
	bounds := map[string]int64{"WORK": math.MaxInt64, "INPUT_TOKENS": math.MaxInt64 - 1, "OUTPUT_TOKENS": 1}
	if err := validateProviderRequestCapabilities(call, bounds, caps); err != nil {
		t.Fatalf("representable exact boundary rejected: %v", err)
	}
	bounds["OUTPUT_TOKENS"] = 2
	if err := validateProviderRequestCapabilities(call, bounds, caps); err == nil {
		t.Fatal("context sum overflow accepted")
	}
	caps.MaxInputTokens = counter(0)
	bounds["INPUT_TOKENS"], bounds["OUTPUT_TOKENS"] = 0, 1
	if err := validateProviderRequestCapabilities(call, bounds, caps); err != nil {
		t.Fatalf("explicit zero input cap rejected despite zero input: %v", err)
	}
}

func TestP18QAOverrunLateFinalAfterOwnershipAndRestart(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "late-qa")
	claim(t, s, d)
	partial := admissionReceipt(d, "qa-partial", "partial", counter(500))
	partial.InputTokens = counter(500)
	partial.OutputTokens = counter(1)
	acceptUsage(t, s, partial) // WORK 501 exceeds both dispatch and agent limits.
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	seedAdmissionInvocation(t, s, "replacement-owner", "replacement-execution")
	s = restartAgentStore(t, s)
	final := admissionReceipt(d, "qa-late-final", "final", counter(1140))
	final.InputTokens = counter(1140)
	final.OutputTokens, final.CachedInputTokens, final.ReasoningTokens = counter(60), counter(1000), counter(50)
	acceptUsage(t, s, final) // Subsets do not add to WORK; accept beyond absolute ceiling.
	requireTokenBank(t, s, a.RunID, RunTokenBalance{1000, 1200, -200, 100, 200, -100, 1100})
	requireStopClass(t, s, "DISPATCH_BOUND_EXCEEDED", "OBSERVED_DISPATCH_BOUND_EXCEEDED")
	before := mutationDatabaseSnapshot(t, s)
	acceptUsage(t, s, final)
	if !bytes.Equal(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("exact final replay changed accounting or stop evidence")
	}
	s = restartAgentStore(t, s)
	acceptUsage(t, s, final)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{1000, 1200, -200, 100, 200, -100, 1100})
}
