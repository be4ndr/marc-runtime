package statestore

import (
	"math"
	"path/filepath"
	"testing"
)

func capabilityClaim(a InvocationAdmission, id string) AdmissionDispatchClaim {
	d := claimFor(a, id, 100)
	setDispatchBound(&d, "INPUT_TOKENS", 40)
	setDispatchBound(&d, "OUTPUT_TOKENS", 60)
	d.Capabilities = &ProviderModelCapabilities{
		Provider: "fake", Model: "fake-model", ContextWindowTokens: 100,
		MaxOutputTokens: 60, MaxInputTokens: counter(40),
	}
	return d
}

func TestProviderModelCapabilitiesValidateBeforeProviderRegistration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(*AdmissionDispatchClaim)
		wantErr bool
	}{
		{name: "context and output boundaries", change: func(d *AdmissionDispatchClaim) { d.Capabilities.MaxInputTokens = nil }},
		{name: "optional input capability", change: func(*AdmissionDispatchClaim) {}},
		{name: "missing capabilities", change: func(d *AdmissionDispatchClaim) { d.Capabilities = nil }, wantErr: true},
		{name: "provider mismatch", change: func(d *AdmissionDispatchClaim) { d.Capabilities.Provider = "other" }, wantErr: true},
		{name: "model mismatch", change: func(d *AdmissionDispatchClaim) { d.Capabilities.Model = "other-model" }, wantErr: true},
		{name: "missing context window", change: func(d *AdmissionDispatchClaim) { d.Capabilities.ContextWindowTokens = 0 }, wantErr: true},
		{name: "output capability exceeds context", change: func(d *AdmissionDispatchClaim) { d.Capabilities.MaxOutputTokens = 101 }, wantErr: true},
		{name: "input capability exceeds context", change: func(d *AdmissionDispatchClaim) { d.Capabilities.MaxInputTokens = counter(101) }, wantErr: true},
		{name: "missing input boundary", change: func(d *AdmissionDispatchClaim) { removeDispatchBound(d, "INPUT_TOKENS") }, wantErr: true},
		{name: "missing output boundary", change: func(d *AdmissionDispatchClaim) { removeDispatchBound(d, "OUTPUT_TOKENS") }, wantErr: true},
		{name: "context boundary exceeded", change: func(d *AdmissionDispatchClaim) { setDispatchBound(d, "INPUT_TOKENS", 41) }, wantErr: true},
		{name: "per-call WORK boundary exceeded", change: func(d *AdmissionDispatchClaim) {
			d.Capabilities.ContextWindowTokens = 200
			setDispatchBound(d, "INPUT_TOKENS", 41)
		}, wantErr: true},
		{name: "output capability exceeded", change: func(d *AdmissionDispatchClaim) {
			d.Capabilities.ContextWindowTokens = 200
			setDispatchBound(d, "INPUT_TOKENS", 39)
			setDispatchBound(d, "OUTPUT_TOKENS", 61)
		}, wantErr: true},
		{name: "optional input capability exceeded", change: func(d *AdmissionDispatchClaim) {
			d.Capabilities.ContextWindowTokens = 200
			setDispatchBound(d, "INPUT_TOKENS", 41)
			setDispatchBound(d, "OUTPUT_TOKENS", 59)
		}, wantErr: true},
		{name: "capability arithmetic overflow", change: func(d *AdmissionDispatchClaim) {
			setDispatchBound(d, "INPUT_TOKENS", math.MaxInt64)
			setDispatchBound(d, "OUTPUT_TOKENS", 1)
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 1000)
			admit(t, s, a)
			d := capabilityClaim(a, "capability-claim")
			tc.change(&d)
			created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions())
			if tc.wantErr {
				if err == nil || created {
					t.Fatalf("invalid request dispatched: created=%v err=%v", created, err)
				}
				if got := resultCount(t, s, "provider_calls"); got != 0 {
					t.Fatalf("rejected request registered %d ProviderCalls", got)
				}
			} else if err != nil || !created {
				t.Fatalf("valid request rejected: created=%v err=%v", created, err)
			}
		})
	}
}

func removeDispatchBound(d *AdmissionDispatchClaim, dimension string) {
	for i := range d.Bounds {
		if d.Bounds[i].Dimension == dimension {
			d.Bounds = append(d.Bounds[:i], d.Bounds[i+1:]...)
			return
		}
	}
}

func TestProviderCapabilityOverrunReceiptPersistsStopsAndReplaysAfterRestart(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "capability-overrun")
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); err != nil || !created {
		t.Fatalf("boundary request dispatch failed: created=%v err=%v", created, err)
	}
	actual := admissionReceipt(d, "capability-overrun-partial", "partial", counter(40))
	actual.InputTokens = counter(40)
	actual.OutputTokens = counter(61) // exceeds output and combined context boundaries
	if err := s.AcceptUsageReceipt(accountingContext, actual, accountingOptions()); err != nil {
		t.Fatalf("truthful overrun receipt rejected: %v", err)
	}
	requireStopClass(t, s, "DISPATCH_BOUND_EXCEEDED", "OBSERVED_DISPATCH_BOUND_EXCEEDED")
	if created, err := s.ClaimAdmissionDispatch(accountingContext, capabilityClaim(a, "after-capability-stop"), accountingOptions()); created || err == nil {
		t.Fatalf("dispatch passed durable overrun stop: created=%v err=%v", created, err)
	}
	dir := filepath.Dir(s.info.Path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.AcceptUsageReceipt(accountingContext, actual, accountingOptions()); err != nil {
		t.Fatalf("exact receipt replay after restart failed: %v", err)
	}
	requireStopClass(t, s, "DISPATCH_BOUND_EXCEEDED", "OBSERVED_DISPATCH_BOUND_EXCEEDED")
	if got := resultCount(t, s, "provider_calls"); got != 1 {
		t.Fatalf("provider call count after replay/restart=%d, want 1", got)
	}
}

func TestProviderCapabilityClaimDigestProtectsReplayAcrossRestart(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 1000)
	admit(t, s, a)
	d := capabilityClaim(a, "capability-replay")
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); err != nil || !created {
		t.Fatalf("initial claim failed: created=%v err=%v", created, err)
	}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); err != nil || created {
		t.Fatalf("exact replay changed release: created=%v err=%v", created, err)
	}
	changed := d
	changed.Capabilities = &ProviderModelCapabilities{
		Provider: d.Capabilities.Provider, Model: d.Capabilities.Model,
		ContextWindowTokens: 101, MaxOutputTokens: d.Capabilities.MaxOutputTokens,
	}
	if created, err := s.ClaimAdmissionDispatch(accountingContext, changed, accountingOptions()); err == nil || created {
		t.Fatalf("changed capability snapshot replay accepted: created=%v err=%v", created, err)
	}
	dir := filepath.Dir(s.info.Path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := openTest(accountingContext, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); err != nil || created {
		t.Fatalf("exact replay after restart changed release: created=%v err=%v", created, err)
	}
	if got := resultCount(t, s, "provider_calls"); got != 1 {
		t.Fatalf("provider call count after replay=%d, want 1", got)
	}
}
