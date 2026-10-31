package statestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func rejectedMutationActors() map[string]context.Context {
	actors := map[string]context.Context{"missing": context.Background(), "nil-context": nil,
		"wrong-type": context.WithValue(context.Background(), mutationActorKey{}, 1)}
	for _, role := range []string{"LEAD", "DEVELOPER", "QA", "REVIEWER", "", "unknown", "orchestrator", " ORCHESTRATOR "} {
		actors["role="+role] = WithMutationActor(context.Background(), role)
	}
	return actors
}

// Snapshot every persisted table, including audit sequence and bootstrap history,
// so rejection cannot hide an unrelated row change behind a resource subtotal.
func mutationDatabaseSnapshot(t *testing.T, s *StateStore) []byte {
	t.Helper()
	rows, err := s.db.Query(`SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := map[string][][]any{}
	for _, name := range tables {
		rows, err := s.db.Query(`SELECT * FROM "` + name + `" ORDER BY rowid`)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for n := range values {
				ptrs[n] = &values[n]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			for n, v := range values {
				if b, ok := v.([]byte); ok {
					values[n] = append([]byte{}, b...)
				}
			}
			snapshot[name] = append(snapshot[name], values)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestMutationActorRejectsEveryDomainMutationAtomically(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "authority-call", 20)
	claim(t, s, d)
	receipt := admissionReceipt(d, "authority-receipt", "partial", counter(5))
	state, semanticOpts := semanticFixture(t, s)
	stateRaw, actionRaw, requestRaw := wire(t, state), wire(t, actionFixture(state)), wire(t, requestFixture(state))
	resultRaw := resultFixture(t, "developer-completed")
	opts := accountingOptions()
	// A claimed Orchestrator audit actor must never supply initiating authority.
	opts.Audit = accountingAudit()
	semanticOpts.Audit = fixtureAudit()
	own := OwnershipOptions{Expected: a.Fence.Owner, ChangedAt: semanticTimestamp, Audit: opts.Audit}

	pool := ResourcePool{Account: a.Resources[0].Account, Eligibility: primaryTokenEligibility()}
	grant := ResourceGrant{Account: a.Resources[0].Account, AdmissionID: a.AdmissionID, AllocationID: "authority-topup", Source: "controller", Amount: 1}
	proof := recoveryContract(t, s, a, "authority-proof")
	human := HumanReadinessDisposition{DispositionID: "human-authority", RunID: a.RunID, StopID: "stop", Classification: "OBSERVED_DISPATCH_BOUND_EXCEEDED", ContentIdentity: a.Binding.ContentIdentity, InteractionID: "interaction", Prompt: "prompt", Response: "response", Decision: "ALLOW_TERMINAL_EVALUATION", Boundary: proof.Boundary, Evidence: proof.Recovery.ContainmentEvidence}
	operations := map[string]func(context.Context) error{
		"IncreaseRunBudget": func(ctx context.Context) error {
			_, err := s.IncreaseRunBudget(ctx, a.RunID, 10, approvedBudget("actor-increment"), opts)
			return err
		},
		"RecordHardLimitRequest": func(ctx context.Context) error {
			_, err := s.RecordHardLimitRequest(ctx, HardLimitRequest{RequestID: "actor-hard", RunID: a.RunID, Justification: "More work remains.", PresentedAt: opts.At, Grant: grant}, opts)
			return err
		},
		"DecideHardLimitRequest": func(ctx context.Context) error {
			_, err := s.DecideHardLimitRequest(ctx, a.RunID, "actor-hard", nil, opts)
			return err
		},
		"CloseAdmission": func(ctx context.Context) error {
			_, err := s.CloseAdmission(ctx, a.RunID, a.AdmissionID, ProofReference{ProofID: proof.ProofID, SHA256: strings.Repeat("a", 64)}, opts)
			return err
		},
		"AuthorizeAdmissionRelease": func(ctx context.Context) error {
			_, err := s.AuthorizeAdmissionRelease(ctx, d.RunID, d.ClaimID, opts)
			return err
		},
		"DenyResourceRequest":   func(ctx context.Context) error { return s.DenyResourceRequest(ctx, a.RunID, "actor-request", opts) },
		"CreateResourcePool":    func(ctx context.Context) error { return s.CreateResourcePool(ctx, pool, opts) },
		"GrantResources":        func(ctx context.Context) error { return s.GrantResources(ctx, a.RunID, []ResourceGrant{grant}, opts) },
		"ConfigureRunAdmission": func(ctx context.Context) error { return s.ConfigureRunAdmission(ctx, admissionPolicy(100), opts) },
		"AdmitInvocation":       func(ctx context.Context) error { _, err := s.AdmitInvocation(ctx, a, opts); return err },
		"DenyTokenTopUp": func(ctx context.Context) error {
			_, err := s.DenyTokenTopUp(ctx, a.RunID, a.AdmissionID, "decision", "reason", opts)
			return err
		},
		"ClaimAdmissionDispatch": func(ctx context.Context) error { _, err := s.ClaimAdmissionDispatch(ctx, d, opts); return err },
		"SealAdmissionNonDispatch": func(ctx context.Context) error {
			_, err := s.SealAdmissionNonDispatch(ctx, a.RunID, a.AdmissionID, "non-dispatch", "NEVER_RELEASED", opts)
			return err
		},
		"RecordControllerReleaseEvidence": func(ctx context.Context) error {
			_, err := s.RecordControllerReleaseEvidence(ctx, ControllerReleaseEvidence{RunID: a.RunID, ClaimID: d.ClaimID, EvidenceID: "release", ReleasedAt: semanticTimestamp}, opts)
			return err
		},
		"CreateProviderCall": func(ctx context.Context) error { return s.CreateProviderCall(ctx, *d.ProviderCall, opts) },
		"FinishProviderCall": func(ctx context.Context) error {
			return s.FinishProviderCall(ctx, a.RunID, d.ProviderCall.ProviderCallID, 1, opts)
		},
		"AcceptUsageReceipt": func(ctx context.Context) error { return s.AcceptUsageReceipt(ctx, receipt, opts) },
		"AcceptUsageReceiptJSON": func(ctx context.Context) error {
			return s.AcceptUsageReceiptJSON(ctx, usageWireFor(t, receipt, "fake", "fake-model"), opts)
		},
		"AcceptLeadState":       func(ctx context.Context) error { return s.AcceptLeadState(ctx, stateRaw, semanticOpts) },
		"AcceptNextAction":      func(ctx context.Context) error { return s.AcceptNextAction(ctx, actionRaw, semanticOpts) },
		"AcceptResourceRequest": func(ctx context.Context) error { return s.AcceptResourceRequest(ctx, requestRaw, semanticOpts) },
		"AcceptRoleResult":      func(ctx context.Context) error { return s.AcceptRoleResult(ctx, resultRaw, nil, ResultFence{}) },
		"AcceptRoleResultAndReturnToLead": func(ctx context.Context) error {
			return s.AcceptRoleResultAndReturnToLead(ctx, resultRaw, nil, ResultReturnOptions{ReturnedAt: semanticTimestamp, Audit: opts.Audit})
		},
		"TransferSemanticOwnership":       func(ctx context.Context) error { return s.TransferSemanticOwnership(ctx, a.RunID, "LEAD", nil, own) },
		"BindActiveInvocation":            func(ctx context.Context) error { return s.BindActiveInvocation(ctx, a.RunID, a.Binding, own) },
		"ClearActiveInvocation":           func(ctx context.Context) error { return s.ClearActiveInvocation(ctx, a.RunID, own) },
		"RecoverSemanticOwnershipToLead":  func(ctx context.Context) error { return s.RecoverSemanticOwnershipToLead(ctx, a.RunID, own) },
		"SealAuthorityProof":              func(ctx context.Context) error { _, err := s.SealAuthorityProof(ctx, proof, opts); return err },
		"RecordHumanReadinessDisposition": func(ctx context.Context) error { return s.RecordHumanReadinessDisposition(ctx, human, opts) },
	}
	before := mutationDatabaseSnapshot(t, s)
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			for actor, ctx := range rejectedMutationActors() {
				t.Run(actor, func(t *testing.T) {
					if err := operation(ctx); !errors.Is(err, ErrMutationAuthority) {
						t.Fatalf("want authority rejection, got %v", err)
					}
					if after := mutationDatabaseSnapshot(t, s); !bytes.Equal(before, after) {
						t.Fatal("unauthorized mutation changed persisted state or audit")
					}
				})
			}
		})
	}
}

func TestMutationActorStartupAndRestart(t *testing.T) {
	for actor, ctx := range rejectedMutationActors() {
		t.Run(actor, func(t *testing.T) {
			dir := filepath.Join(privateTempDir(t), "not-created")
			s, err := openTest(ctx, dir)
			if s != nil || !errors.Is(err, ErrMutationAuthority) {
				t.Fatalf("unauthorized startup: store=%v err=%v", s, err)
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("unauthorized startup created directory: %v", err)
			}
		})
	}
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "restart-authority", 10)
	claim(t, s, d)
	before := mutationDatabaseSnapshot(t, s)
	dir := filepath.Dir(s.Info().Path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := openTest(context.Background(), dir); reopened != nil || !errors.Is(err, ErrMutationAuthority) {
		t.Fatalf("unauthorized restart: %v %v", reopened, err)
	}
	s, err := openTest(orchestratorContext(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if after := mutationDatabaseSnapshot(t, s); !bytes.Equal(before, after) {
		t.Fatal("restart changed persisted state")
	}
	receipt := admissionReceipt(d, "restart-final", "final", counter(7))
	if err := s.AcceptUsageReceipt(context.Background(), receipt, accountingOptions()); !errors.Is(err, ErrMutationAuthority) {
		t.Fatal(err)
	}
	if err := s.AcceptUsageReceipt(orchestratorContext(), receipt, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	accepted := mutationDatabaseSnapshot(t, s)
	if err := s.AcceptUsageReceipt(WithMutationActor(context.Background(), "DEVELOPER"), receipt, accountingOptions()); !errors.Is(err, ErrMutationAuthority) {
		t.Fatalf("unauthorized FINAL replay: %v", err)
	}
	if err := s.AcceptUsageReceipt(orchestratorContext(), receipt, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if created, err := s.ClaimAdmissionDispatch(WithMutationActor(context.Background(), "QA"), d, accountingOptions()); created || !errors.Is(err, ErrMutationAuthority) {
		t.Fatalf("unauthorized claim replay: %v %v", created, err)
	}
	if created, err := s.ClaimAdmissionDispatch(orchestratorContext(), d, accountingOptions()); created || err != nil {
		t.Fatalf("authorized claim replay: %v %v", created, err)
	}
	if after := mutationDatabaseSnapshot(t, s); !bytes.Equal(accepted, after) {
		t.Fatal("receipt/claim replays changed persisted state")
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 7, 93, 10, 0, 10, 110})
	if err := s.AcceptUsageReceipt(orchestratorContext(), admissionReceipt(d, "after-final", "partial", counter(8)), accountingOptions()); err == nil {
		t.Fatal("actor authority reopened FINAL")
	}
}

func TestMutationActorIsSeparateFromSemanticAndAuditRoles(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	opts := AccountingOptions{At: semanticTimestamp, Audit: []AuditRow{{EventType: "controller-persisted-worker-intent", ActorRole: "DEVELOPER", OccurredAt: semanticTimestamp}}}
	// The affected owner is LEAD and the audit subject is a Worker. The initiator
	// is still the runtime Orchestrator, and must not be inferred from either.
	if created, err := s.AdmitInvocation(orchestratorContext(), a, opts); !created || err != nil {
		t.Fatal(created, err)
	}
	before := mutationDatabaseSnapshot(t, s)
	own := OwnershipOptions{Expected: a.Fence.Owner, ChangedAt: semanticTimestamp}
	own.Expected.ActorRevision++
	if err := s.ClearActiveInvocation(orchestratorContext(), a.RunID, own); err == nil || errors.Is(err, ErrMutationAuthority) {
		t.Fatalf("actor authority bypassed stale ownership fence: %v", err)
	}
	if after := mutationDatabaseSnapshot(t, s); !bytes.Equal(before, after) {
		t.Fatal("stale ownership mutation changed state")
	}
	own.Expected = a.Fence.Owner
	if err := s.ClearActiveInvocation(orchestratorContext(), a.RunID, own); err != nil {
		t.Fatal(err)
	}
}
