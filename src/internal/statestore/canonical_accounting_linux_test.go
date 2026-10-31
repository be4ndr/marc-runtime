package statestore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestB3BudgetConfirmationAndHumanIncrement(t *testing.T) {
	s := testStore(t)
	seedAdmissionRun(t, s)
	pools := admissionPools(t, s, "normal")
	p := admissionPolicy(100)
	p.Confirmation = HumanBudgetApproval{}
	before := mutationDatabaseSnapshot(t, s)
	if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err == nil {
		t.Fatal("missing human confirmation accepted")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("unconfirmed configuration changed authority")
	}
	p.Confirmation = approvedBudget("original-confirmation")
	p.AgentTokenProfiles = []AgentTokenProfile{{Role: "LEAD", SoftPercent: counter(12), HardPercent: counter(15)}}
	if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigureRunAdmission(accountingContext, p, accountingOptions()); err != nil {
		t.Fatal("configuration replay", err)
	}
	a := admissionFor(t, s, seedAdmissionInvocation(t, s, "budget-agent", "budget-execution"), pools, 0)
	admit(t, s, a)
	d := tokenClaim(a, "budget-first-call", 2)
	claim(t, s, d)
	r := admissionReceipt(d, "budget-first-final", "final", counter(2))
	acceptUsage(t, s, r)
	opts := accountingOptions()
	opts.Audit = accountingAudit()
	h := approvedBudget("increment-one")
	if created, err := s.IncreaseRunBudget(accountingContext, a.RunID, 9, h, opts); err != nil || !created {
		t.Fatal(created, err)
	}
	if created, err := s.IncreaseRunBudget(accountingContext, a.RunID, 9, h, opts); err != nil || created {
		t.Fatal("increment replay", created, err)
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{109, 2, 107, 10, 0, 10, 119})
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 12, HardLimit: 15, Used: 2})
	var original, profile int64
	if err := s.db.QueryRow(`SELECT hard_limit FROM run_resource_budgets WHERE run_id=?`, a.RunID).Scan(&original); err != nil || original != 100 {
		t.Fatal(original, err)
	}
	if err := s.db.QueryRow(`SELECT hard_limit FROM run_agent_token_profiles WHERE run_id=? AND role='LEAD'`, a.RunID).Scan(&profile); err != nil || profile != 15 {
		t.Fatal(profile, err)
	}
	if resultCount(t, s, "resource_allocations") != 1 || resultCount(t, s, "usage_receipts") != 1 || resultCount(t, s, "events") != 1 {
		t.Fatal("increment rewrote history or replayed audit")
	}
	before = mutationDatabaseSnapshot(t, s)
	changed := h
	changed.Response = "a different approval"
	if created, err := s.IncreaseRunBudget(accountingContext, a.RunID, 9, changed, opts); created || err == nil {
		t.Fatal("conflicting increment replay", created, err)
	}
	denied := approvedBudget("increment-denied")
	denied.Decision = "DENY"
	if created, err := s.IncreaseRunBudget(accountingContext, a.RunID, 9, denied, opts); created || err == nil {
		t.Fatal("denial increased budget", created, err)
	}
	bad := opts
	bad.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: opts.At}}
	if created, err := s.IncreaseRunBudget(accountingContext, a.RunID, 10, approvedBudget("increment-rollback"), bad); created || err == nil {
		t.Fatal("invalid audit committed increment", created, err)
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("rejected increment changed state")
	}
	s = restartAgentStore(t, s)
	requireTokenBank(t, s, a.RunID, RunTokenBalance{109, 2, 107, 10, 0, 10, 119})
	if created, err := s.IncreaseRunBudget(accountingContext, a.RunID, 9, h, opts); err != nil || created {
		t.Fatal("restart replay", created, err)
	}
	if _, err := s.IncreaseRunBudget(accountingContext, a.RunID, math.MaxInt64, approvedBudget("overflow-increment"), opts); err == nil {
		t.Fatal("overflow admitted")
	}
	for _, stmt := range []string{`UPDATE run_budget_increases SET amount=1`, `DELETE FROM run_budget_increases`, `UPDATE run_resource_budgets SET hard_limit=110`, `UPDATE run_budget_confirmations SET confirmed_at='later'`} {
		if _, err := s.db.Exec(stmt); err == nil {
			t.Fatal("SQL changed immutable budget history", stmt)
		}
	}
	raw, _ := json.Marshal(HumanBudgetApproval{ApprovalID: "sql-missing-response", Decision: "APPROVE"})
	if _, err := s.db.Exec(`INSERT INTO run_budget_increases VALUES(?,?,1,?,?,?)`, "sql-missing-response", a.RunID, string(raw), opts.At, stringsRepeat64()); err == nil {
		t.Fatal("SQL allowed missing human response")
	}
	if _, err := s.db.Exec(`INSERT INTO run_budget_increases VALUES(?,?,?, ?,?,?)`, "sql-overflow", a.RunID, int64(math.MaxInt64), string(mustApprovalJSON(approvedBudget("sql-overflow"))), opts.At, stringsRepeat64()); err == nil {
		t.Fatal("SQL overflow admitted")
	}
	if err := validateDatabase(accountingContext, s.db); err != nil {
		t.Fatal(err)
	}
}
func stringsRepeat64() string {
	return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
func mustApprovalJSON(h HumanBudgetApproval) []byte { b, _ := json.Marshal(h); return b }
func shiftedOptions(delta time.Duration) AccountingOptions {
	t, _ := time.Parse(time.RFC3339Nano, semanticTimestamp)
	return AccountingOptions{At: t.Add(delta).Format(time.RFC3339Nano)}
}

func hardRequestFixture(t *testing.T, receiptState ...string) (*StateStore, InvocationAdmission, HardLimitRequest) {
	t.Helper()
	s, a := configuredAdmissionFixture(t, 100)
	a.Resources[0].Amount = 10
	a.SoftLimit = counter(5)
	admit(t, s, a)
	d := tokenClaim(a, "hard-first", 10)
	claim(t, s, d)
	state := "final"
	if len(receiptState) > 0 {
		state = receiptState[0]
	}
	acceptUsage(t, s, admissionReceipt(d, "hard-first-usage", state, counter(10)))
	if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	p := HardLimitRequest{RequestID: "hard-proposal", RunID: a.RunID, Justification: "Lead assessed remaining work and requests ten tokens.", PresentedAt: semanticTimestamp, Grant: ResourceGrant{Account: a.Resources[0].Account, AdmissionID: a.AdmissionID, AllocationID: "hard-extension", Source: "lead-hard-proposal", Amount: 10}}
	if created, err := s.RecordHardLimitRequest(accountingContext, p, accountingOptions()); err != nil || !created {
		t.Fatal(created, err)
	}
	return s, a, p
}
func TestB3HardApprovalRejectionTimeoutAndRestart(t *testing.T) {
	for _, mode := range []string{"approve", "deny", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			s, a, p := hardRequestFixture(t)
			if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{p.Grant}, accountingOptions()); err == nil {
				t.Fatal("bare Hard top-up authorized")
			}
			if created, err := s.RecordHardLimitRequest(accountingContext, p, accountingOptions()); err != nil || created {
				t.Fatal("proposal replay", created, err)
			}
			s = restartAgentStore(t, s)
			if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, nil, shiftedOptions(4*time.Minute)); out.Created || err == nil {
				t.Fatal("early timeout", out, err)
			}
			var h *HumanBudgetApproval
			opts := shiftedOptions(time.Minute)
			if mode != "timeout" {
				v := approvedBudget("hard-response")
				if mode == "deny" {
					v.Decision = "DENY"
				}
				h = &v
			} else {
				opts = shiftedOptions(5 * time.Minute)
			}
			if mode == "approve" {
				before := mutationDatabaseSnapshot(t, s)
				bad := opts
				bad.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: opts.At}}
				if failed, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, h, bad); failed.Created || failed.Applied || err == nil {
					t.Fatal("invalid audit committed approval", failed, err)
				}
				if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
					t.Fatal("grant/decision/audit did not roll back together")
				}
			}
			out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, h, opts)
			if err != nil || !out.Created || out.Applied != (mode != "deny") {
				t.Fatal(out, err)
			}
			replay, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, h, opts)
			if err != nil || replay.Created || replay.Applied != out.Applied {
				t.Fatal("decision replay", replay, err)
			}
			if mode == "deny" {
				requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 5, HardLimit: 10, Used: 10, CompletionOnly: true})
				if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, nil, shiftedOptions(5*time.Minute)); out.Created || err == nil {
					t.Fatal("rejection replaced by timeout", out, err)
				}
			} else {
				requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 15, HardLimit: 20, Used: 10})
				if resultCount(t, s, "resource_allocations") != 2 {
					t.Fatal("extension duplicated")
				}
				claim(t, s, tokenClaim(a, "after-hard-extension", 2))
			}
			requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 10, 90, 10, 0, 10, 110})
			for _, stmt := range []string{`UPDATE hard_limit_requests SET deadline='2099-01-01T00:00:00Z'`, `DELETE FROM hard_limit_decisions`} {
				if _, err := s.db.Exec(stmt); err == nil {
					t.Fatal("SQL changed immutable decision", stmt)
				}
			}
		})
	}
}
func TestB3HardDecisionRaceAndConstraintRevalidation(t *testing.T) {
	s, a, p := hardRequestFixture(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for _, human := range []bool{true, false} {
		go func(human bool) {
			defer wg.Done()
			<-start
			var h *HumanBudgetApproval
			opts := shiftedOptions(5 * time.Minute)
			if human {
				v := approvedBudget("racing-rejection")
				v.Decision = "DENY"
				h = &v
				opts = shiftedOptions(5*time.Minute - time.Nanosecond)
			}
			_, _ = s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, h, opts)
		}(human)
	}
	close(start)
	wg.Wait()
	if resultCount(t, s, "hard_limit_decisions") != 1 || resultCount(t, s, "resource_allocations") > 2 {
		t.Fatal("decision race applied twice")
	}
	s2, a2, p2 := hardRequestFixture(t)
	expired := accountingOptions()
	expired.At = admissionDeadline
	out, err := s2.DecideHardLimitRequest(accountingContext, a2.RunID, p2.RequestID, nil, expired)
	if err != nil || !out.Created || out.Applied {
		t.Fatal("expired execution received timeout grant", out, err)
	}
	if resultCount(t, s2, "resource_allocations") != 1 {
		t.Fatal("expired grant persisted")
	}
}

func TestB3ReleaseRevalidatesImmutableHardWindow(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	admit(t, s, a)
	d := tokenClaim(a, "delayed-release", 2)
	if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); err != nil || !created {
		t.Fatal(created, err)
	}
	s = restartAgentStore(t, s)
	expired := accountingOptions()
	expired.At = admissionDeadline
	before := mutationDatabaseSnapshot(t, s)
	if created, err := s.AuthorizeAdmissionRelease(accountingContext, d.RunID, d.ClaimID, expired); created || err == nil {
		t.Fatal("post-deadline release", created, err)
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("denied release changed authority")
	}
	if _, err := s.db.Exec(`INSERT INTO admission_release_authorizations VALUES(?,?,?)`, d.ClaimID, admissionDeadline, stringsRepeat64()); err == nil {
		t.Fatal("SQL allowed release at deadline")
	}
	valid := accountingOptions()
	dt, _ := time.Parse(time.RFC3339Nano, admissionDeadline)
	valid.At = dt.Add(-time.Nanosecond).Format(time.RFC3339Nano)
	if created, err := s.AuthorizeAdmissionRelease(accountingContext, d.RunID, d.ClaimID, valid); err != nil || !created {
		t.Fatal("pre-deadline release", created, err)
	}
	if created, err := s.AuthorizeAdmissionRelease(accountingContext, d.RunID, d.ClaimID, valid); err != nil || created {
		t.Fatal("release replay", created, err)
	}
}

func closeTestAdmission(t *testing.T, s *StateStore, a InvocationAdmission, id string) {
	t.Helper()
	ref := sealProof(t, s, recoveryContract(t, s, a, id+"-containment"))
	if created, err := s.CloseAdmission(accountingContext, a.RunID, a.AdmissionID, ref, accountingOptions()); err != nil || !created {
		t.Fatal("closure", created, err)
	}
}
func positiveTerminalFixture(t *testing.T, unknown bool) (*authorityWorkflow, AuthorityProof) {
	t.Helper()
	mode := "known"
	if unknown {
		mode = "unknown"
	}
	w := proofWorkflowFixture(t, mode)
	closeTestAdmission(t, w.s, w.workerAdmission, "worker-closed")
	qa, qad := w.addWorker(t, "qa-passed", "qa-ready", 4)
	qd := tokenClaim(qad, "qa-ready-call", 2)
	claim(t, w.s, qd)
	acceptUsage(t, w.s, admissionReceipt(qd, "qa-ready-final", "final", counter(2)))
	if err := w.s.FinishProviderCall(accountingContext, qa.RunID, qd.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	w.accept(t, qa)
	w.bindLead(t, "lead-qa-ready", &qa, nil, false)
	closeTestAdmission(t, w.s, qad, "qa-closed")
	review, ad := w.addWorker(t, "reviewer-approved", "review-ready", 5)
	cp := sealProof(t, w.s, validationContract(t, w, w.worker, "review-ready-completion"))
	recovery := sealProof(t, w.s, recoveryContract(t, w.s, w.workerAdmission, "review-ready-containment"))
	rp := AuthorityProof{ProofID: "review-ready-admission", RunID: review.RunID, Kind: "REVIEW_ADMISSION", ContentIdentity: review.ContentIdentity, Verdict: "ELIGIBLE", Reason: "prior calls closed; remaining evidence validated", Boundary: authorityBoundary(t, w.s, review.RunID, ad.Binding), Review: &ReviewAdmissionProof{Completion: cp, Containment: recovery, AdmissionID: ad.AdmissionID}}
	reviewRef := sealProof(t, w.s, rp)
	rd := tokenClaim(ad, "review-ready-call", 2)
	claim(t, w.s, rd)
	acceptUsage(t, w.s, admissionReceipt(rd, "review-ready-final", "final", counter(2)))
	if err := w.s.FinishProviderCall(accountingContext, review.RunID, rd.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
		t.Fatal(err)
	}
	w.accept(t, review)
	w.bindLead(t, "lead-terminal-ready", &qa, &review, true)
	closeTestAdmission(t, w.s, ad, "review-closed")
	cp = sealProof(t, w.s, validationContract(t, w, w.worker, "ready-completion"))
	qp := sealProof(t, w.s, validationContract(t, w, qa, "ready-qa"))
	vp := sealProof(t, w.s, validationContract(t, w, review, "ready-review"))
	containment := sealProof(t, w.s, recoveryContract(t, w.s, w.workerAdmission, "ready-containment"))
	p := AuthorityProof{ProofID: "ready-terminal", RunID: review.RunID, Kind: "TERMINAL", ContentIdentity: review.ContentIdentity, Verdict: "SUPPORTS_READINESS", Reason: "all no-further-call and semantic obligations resolved", Boundary: authorityBoundary(t, w.s, review.RunID, w.lead), Terminal: &TerminalProof{Completion: cp, QAValidation: qp, ReviewValidation: vp, ReviewAdmission: reviewRef, Containment: containment, LeadStateID: w.state["lead_state_id"].(string)}}
	return w, p
}
func TestB3TokenOnlyTerminalWithFinalUnknownAccounting(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "known", true: "unknown"}[unknown], func(t *testing.T) {
			w, p := positiveTerminalFixture(t, unknown)
			r, err := w.s.SealAuthorityProof(accountingContext, p, accountingOptions())
			if err != nil || r.AccountingIncomplete != unknown {
				t.Fatal(r, err)
			}
			if r.Verdict != "SUPPORTS_READINESS" {
				t.Fatal(r)
			}
			if countSchemaObject(t, w.s, "resource_reservations") != 0 || countSchemaObject(t, w.s, "v_unsettled_usage") != 0 {
				t.Fatal("ghost accounting objects remain")
			}
			if unknown {
				var amount *int64
				if err := w.s.db.QueryRow(`SELECT work_tokens FROM usage_receipts WHERE receipt_id='worker-usage'`).Scan(&amount); err != nil || amount != nil {
					t.Fatal("FINAL NULL rewritten", amount, err)
				}
			}
			w.s = restartAgentStore(t, w.s)
			got, err := w.s.ValidateCurrentAuthorityProof(accountingContext, p.RunID, ProofReference{r.ProofID, r.SHA256})
			if err != nil || !reflect.DeepEqual(got, r) {
				t.Fatal("restart readiness/completeness", got, err)
			}
			d := claimFor(w.workerAdmission, "forbidden-after-closure", 2)
			if created, err := w.s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); created || err == nil {
				t.Fatal("closed admission dispatched", created, err)
			}
			var status string
			if err := w.s.db.QueryRow(`SELECT status FROM runs WHERE run_id=?`, p.RunID).Scan(&status); err != nil || status != "RUNNING" {
				t.Fatal("proof implemented B4 transition", status, err)
			}
		})
	}
}

func TestB3HardDecisionClosureAndSQLGuards(t *testing.T) {
	s, a, p := hardRequestFixture(t)
	before := mutationDatabaseSnapshot(t, s)
	h := approvedBudget("early-response")
	if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, &h, shiftedOptions(-time.Nanosecond)); out.Created || err == nil {
		t.Fatal("response before presentation", out, err)
	}
	for _, tc := range []struct {
		decision, response, at string
		applied                bool
	}{
		{"APPROVE", `{"Decision":"APPROVE"}`, semanticTimestamp, false},
		{"TIMEOUT", "", semanticTimestamp, false},
		{"TIMEOUT", "", admissionDeadline, true},
	} {
		var response any
		if tc.response != "" {
			response = tc.response
		}
		if _, err := s.db.Exec(`INSERT INTO hard_limit_decisions VALUES(?,?,?,?,?,?,?)`, p.RequestID, tc.decision, response, tc.at, stringsRepeat64(), tc.applied, "SQL guard test"); err == nil {
			t.Fatal("invalid direct-SQL Hard decision accepted", tc)
		}
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("invalid decisions changed history")
	}
	closeTestAdmission(t, s, a, "hard-proposal-closed")
	var decision string
	var applied bool
	if err := s.db.QueryRow(`SELECT decision,applied FROM hard_limit_decisions WHERE request_id=?`, p.RequestID).Scan(&decision, &applied); err != nil || decision != "EXPIRED" || applied {
		t.Fatal(decision, applied, err)
	}
	if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, nil, shiftedOptions(5*time.Minute)); out.Created || out.Applied || err == nil {
		t.Fatal("closure left timeout authority", out, err)
	}
	if resultCount(t, s, "resource_allocations") != 1 {
		t.Fatal("closure created allocation")
	}
}

func TestB3ReleaseRevalidatesLateUsageAndState(t *testing.T) {
	for _, mode := range []string{"late-usage-at-soft", "physically-finished", "owner-fence"} {
		t.Run(mode, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 100)
			a.SoftLimit = counter(5)
			admit(t, s, a)
			first := tokenClaim(a, "first-release", 6)
			claim(t, s, first)
			acceptUsage(t, s, admissionReceipt(first, "first-partial", "partial", counter(2)))
			if err := s.FinishProviderCall(accountingContext, a.RunID, first.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
				t.Fatal(err)
			}
			d := tokenClaim(a, "delayed-after-first", 2)
			if created, err := s.ClaimAdmissionDispatch(accountingContext, d, accountingOptions()); err != nil || !created {
				t.Fatal(created, err)
			}
			switch mode {
			case "late-usage-at-soft":
				acceptUsage(t, s, admissionReceipt(first, "first-late-partial", "partial", counter(5)))
			case "physically-finished":
				if err := s.FinishProviderCall(accountingContext, a.RunID, d.ProviderCall.ProviderCallID, 1, accountingOptions()); err != nil {
					t.Fatal(err)
				}
			case "owner-fence":
				execSQL(t, s.db, `UPDATE run_current_semantic_state SET revision=revision+1 WHERE run_id=?`, a.RunID)
			}
			before := mutationDatabaseSnapshot(t, s)
			if created, err := s.AuthorizeAdmissionRelease(accountingContext, a.RunID, d.ClaimID, accountingOptions()); created || err == nil {
				t.Fatal("changed state released", created, err)
			}
			if _, err := s.db.Exec(`INSERT INTO admission_release_authorizations VALUES(?,?,?)`, d.ClaimID, semanticTimestamp, stringsRepeat64()); err == nil {
				t.Fatal("SQL released changed state")
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("release rejection changed authority")
			}
		})
	}
}

func TestB3SQLRequestCapabilityConstraints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*AdmissionDispatchClaim)
	}{
		{"zero-work", func(d *AdmissionDispatchClaim) { setDispatchBound(d, "WORK", 0) }},
		{"zero-output", func(d *AdmissionDispatchClaim) { setDispatchBound(d, "OUTPUT_TOKENS", 0) }},
		{"zero-context", func(d *AdmissionDispatchClaim) { d.Capabilities.ContextWindowTokens = 0 }},
		{"over-model-output", func(d *AdmissionDispatchClaim) { d.Capabilities.MaxOutputTokens = 1 }},
		{"missing-completion-capabilities", func(d *AdmissionDispatchClaim) { d.CompletionOnly = true }},
		{"completion-tools-enabled", func(d *AdmissionDispatchClaim) {
			d.CompletionOnly = true
			d.CompletionCapabilities = &CompletionCapabilities{ToolsEnabled: true, FinalResponseOnly: true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a := configuredAdmissionFixture(t, 1000)
			admit(t, s, a)
			d := capabilityClaim(a, "sql-capabilities")
			if err := s.numericMutation(accountingContext, "SQL fixture", a.RunID, nil, func(c *sql.Conn) error {
				return insertProviderCall(accountingContext, c, *d.ProviderCall, semanticTimestamp)
			}); err != nil {
				t.Fatal(err)
			}
			valid := d
			d.Bounds = append([]DispatchBound(nil), d.Bounds...)
			caps := *d.Capabilities
			d.Capabilities = &caps
			tc.change(&d)
			stmt := `INSERT INTO admission_dispatch_claims VALUES(?,?,?,1,?,?,?,?)`
			var fractional map[string]any
			if err := json.Unmarshal(mustClaimJSON(valid), &fractional); err != nil {
				t.Fatal(err)
			}
			fractional["Capabilities"].(map[string]any)["ContextWindowTokens"] = 100.5
			raw, _ := json.Marshal(fractional)
			if _, err := s.db.Exec(stmt, valid.ClaimID, valid.AdmissionID, valid.ProviderCall.ProviderCallID, semanticTimestamp, stringsRepeat64(), false, string(raw)); err == nil {
				t.Fatal("SQL accepted fractional model capability")
			}
			modeMismatch := valid
			modeMismatch.CompletionOnly = true
			modeMismatch.CompletionCapabilities = &CompletionCapabilities{FinalResponseOnly: true}
			if _, err := s.db.Exec(stmt, valid.ClaimID, valid.AdmissionID, valid.ProviderCall.ProviderCallID, semanticTimestamp, admissionDigest(modeMismatch), false, string(mustClaimJSON(modeMismatch))); err == nil {
				t.Fatal("SQL mode differs from immutable request")
			}

			if _, err := s.db.Exec(stmt, d.ClaimID, d.AdmissionID, d.ProviderCall.ProviderCallID, semanticTimestamp, admissionDigest(d), d.CompletionOnly, string(mustClaimJSON(d))); err == nil {
				t.Fatal("SQL accepted invalid request")
			}
			if _, err := s.db.Exec(stmt, valid.ClaimID, valid.AdmissionID, valid.ProviderCall.ProviderCallID, semanticTimestamp, admissionDigest(valid), false, string(mustClaimJSON(valid))); err != nil {
				t.Fatal("same SQL fixture rejected valid request", err)
			}
			if _, err := s.db.Exec(`INSERT INTO admission_dispatch_bounds VALUES(?,'WORK',99)`, valid.ClaimID); err == nil {
				t.Fatal("bound disagreed with request JSON")
			}
			for _, b := range valid.Bounds {
				execSQL(t, s.db, `INSERT INTO admission_dispatch_bounds VALUES(?,?,?)`, valid.ClaimID, b.Dimension, b.Amount)
			}
			if err := validateDatabase(accountingContext, s.db); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestB3ResourceRequestDenialIsFinalAndIdempotent(t *testing.T) {
	s := testStore(t)
	doc, opts := semanticFixture(t, s)
	if err := s.AcceptLeadState(accountingContext, wire(t, doc), opts); err != nil {
		t.Fatal(err)
	}
	opts.ExpectedSemanticRevision = 1
	r := requestFixture(doc)
	if err := s.AcceptResourceRequest(accountingContext, wire(t, r), opts); err != nil {
		t.Fatal(err)
	}
	id := r["request_id"].(string)
	op := accountingOptions()
	op.Audit = fixtureAudit()
	if err := s.DenyResourceRequest(accountingContext, "run1", id, op); err != nil {
		t.Fatal(err)
	}
	before := mutationDatabaseSnapshot(t, s)
	if err := s.DenyResourceRequest(accountingContext, "run1", id, op); err != nil {
		t.Fatal("denial replay", err)
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("denial replay duplicated state/audit")
	}
	if _, err := s.db.Exec(`UPDATE resource_requests SET status='PENDING',revision=revision+1 WHERE request_id=?`, requestStorageKey("run1", id)); err == nil {
		t.Fatal("SQL reopened denied request")
	}
}

func TestB3CheckedArithmeticAndFinalUnknownDimensions(t *testing.T) {
	for _, tc := range []struct {
		a, b     int64
		subtract bool
	}{
		{math.MaxInt64, 1, false}, {math.MinInt64, -1, false}, {math.MinInt64, 1, true}, {math.MaxInt64, -1, true},
	} {
		var err error
		if tc.subtract {
			_, err = checkedSubtract(tc.a, tc.b)
		} else {
			_, err = checkedAdd(tc.a, tc.b)
		}
		if err == nil {
			t.Fatal("unchecked overflow", tc)
		}
	}
	for _, tc := range []struct {
		name                              string
		input, output, cache, work, spend *int64
	}{
		{name: "all unknown"},
		{name: "output unknown", input: counter(10), cache: counter(0)},
		{name: "input unknown", output: counter(5), cache: counter(0)},
		{name: "cache unknown", input: counter(10), output: counter(5), work: counter(15)},
		{name: "cache zero", input: counter(10), output: counter(5), cache: counter(0), work: counter(15), spend: counter(15)},
		{name: "cache reported", input: counter(10), output: counter(5), cache: counter(4), work: counter(15), spend: counter(11)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := accountingFixture(t)
			r := receipt("final", "final")
			r.InputTokens = tc.input
			r.OutputTokens = tc.output
			r.CachedInputTokens = tc.cache
			acceptUsage(t, s, r)
			var work, spend *int64
			var workState, spendState, sequence string
			if err := s.db.QueryRow(`SELECT work_amount,spend_amount,work_state,spend_state,receipt_state FROM provider_call_usage WHERE provider_call_id=?`, r.ProviderCallID).Scan(&work, &spend, &workState, &spendState, &sequence); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(work, tc.work) || !reflect.DeepEqual(spend, tc.spend) || sequence != "final" {
				t.Fatal(work, spend, sequence)
			}
			if (workState == "final") != (work != nil) || (spendState == "final") != (spend != nil) {
				t.Fatal("NULL acquired final numeric knowledge", workState, spendState)
			}
		})
	}
}

func TestB3ConcurrentBudgetIncrementReplay(t *testing.T) {
	s, a := configuredAdmissionFixture(t, 100)
	other, err := openTest(accountingContext, filepath.Dir(s.Info().Path))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	start := make(chan struct{})
	out := make(chan bool, 2)
	failures := make(chan error, 2)
	for _, store := range []*StateStore{s, other} {
		go func(store *StateStore) {
			<-start
			created, err := store.IncreaseRunBudget(accountingContext, a.RunID, 10, approvedBudget("racing-increment"), accountingOptions())
			out <- created
			failures <- err
		}(store)
	}
	close(start)
	created := 0
	for range 2 {
		if <-out {
			created++
		}
		if err := <-failures; err != nil {
			t.Fatal(err)
		}
	}
	if created != 1 || resultCount(t, s, "run_budget_increases") != 1 {
		t.Fatal("concurrent approval increased twice", created)
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{110, 0, 110, 11, 0, 11, 121})
	h := approvedBudget("reused-interaction")
	h.InteractionID = "racing-increment-interaction"
	before := mutationDatabaseSnapshot(t, s)
	if created, err := s.IncreaseRunBudget(accountingContext, a.RunID, 10, h, accountingOptions()); created || err == nil {
		t.Fatal("interaction approved twice", created, err)
	}
	if _, err := s.db.Exec(`INSERT INTO run_budget_increases VALUES(?,?,10,?,?,?)`, h.ApprovalID, a.RunID, string(mustApprovalJSON(h)), semanticTimestamp, stringsRepeat64()); err == nil {
		t.Fatal("SQL reused human interaction")
	}
	if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
		t.Fatal("reused interaction changed history")
	}
}

func TestB3LateOverrunInvalidatesHardProposalAndBudgetIncreasePreservesStop(t *testing.T) {
	s, a, p := hardRequestFixture(t, "partial")
	d := tokenClaim(a, "hard-first", 10)
	acceptUsage(t, s, admissionReceipt(d, "hard-late-final", "final", counter(11)))
	stops, err := s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil || len(stops) == 0 {
		t.Fatal(stops, err)
	}
	out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, nil, shiftedOptions(5*time.Minute))
	if err != nil || !out.Created || out.Applied {
		t.Fatal("late overrun received timeout grant", out, err)
	}
	if resultCount(t, s, "resource_allocations") != 1 {
		t.Fatal("invalidated proposal allocated")
	}
	if created, err := s.IncreaseRunBudget(accountingContext, a.RunID, 10, approvedBudget("after-stop-increment"), shiftedOptions(6*time.Minute)); err != nil || !created {
		t.Fatal(created, err)
	}
	after, err := s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil || !reflect.DeepEqual(stops, after) {
		t.Fatal("increase rewrote stop evidence", after, err)
	}
	requireTokenBank(t, s, a.RunID, RunTokenBalance{110, 11, 99, 11, 0, 11, 121})
	requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 5, HardLimit: 10, Used: 11, BudgetExceeded: true})
	if created, err := s.ClaimAdmissionDispatch(accountingContext, tokenClaim(a, "after-stopped-increase", 1), accountingOptions()); created || err == nil {
		t.Fatal("increase restored stopped dispatch", created, err)
	}
	s = restartAgentStore(t, s)
	after, err = s.ListRunHardStops(accountingContext, a.RunID)
	if err != nil || !reflect.DeepEqual(stops, after) {
		t.Fatal("restart rewrote stop evidence", after, err)
	}
	if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{p.Grant}, accountingOptions()); err == nil {
		t.Fatal("increase bypassed immutable overrun")
	}
}

func TestB3HardDenialDecisionIDCollision(t *testing.T) {
	for _, priorDenial := range []bool{false, true} {
		t.Run(fmt.Sprintf("prior-denial-%t", priorDenial), func(t *testing.T) {
			s, a, p := hardRequestFixture(t)
			if priorDenial {
				if created, err := s.DenyTokenTopUp(accountingContext, a.RunID, a.AdmissionID, p.RequestID, "Orchestrator denies current top-up", accountingOptions()); err != nil || !created {
					t.Fatal(created, err)
				}
			}
			before := mutationDatabaseSnapshot(t, s)
			h := approvedBudget("human-hard-rejection")
			h.Decision = "DENY"
			h.Prompt = "Approve the proposed ten-token Hard extension?"
			h.Response = "No, I reject this ten-token extension."
			opts := shiftedOptions(time.Minute)
			bad := opts
			bad.Audit = []AuditRow{{EventType: "", ActorRole: "ORCHESTRATOR", OccurredAt: opts.At}}
			if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, &h, bad); out.Created || err == nil {
				t.Fatal("denial committed with invalid audit", out, err)
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("failed denial changed history")
			}
			opts.Audit = accountingAudit()
			if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, &h, opts); err != nil || !out.Created || out.Applied {
				t.Fatal("valid human rejection failed", out, err)
			}
			count := 0
			if priorDenial {
				count = 1
			}
			if resultCount(t, s, "admission_token_topup_denials") != count || resultCount(t, s, "hard_limit_decisions") != 1 || resultCount(t, s, "resource_allocations") != 1 || resultCount(t, s, "events") != 1 {
				t.Fatal("denial copied or changed unrelated history")
			}
			s = restartAgentStore(t, s)
			if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, &h, opts); err != nil || out.Created || out.Applied {
				t.Fatal("denial replay", out, err)
			}
			before = mutationDatabaseSnapshot(t, s)
			if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, nil, shiftedOptions(5*time.Minute)); err == nil || out.Created || out.Applied {
				t.Fatal("timeout replaced human rejection", out, err)
			}
			if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{p.Grant}, accountingOptions()); err == nil {
				t.Fatal("rejected proposal funded grant")
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("rejected continuation changed history")
			}
			requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 5, HardLimit: 10, Used: 10, CompletionOnly: true})
			requireTokenBank(t, s, a.RunID, RunTokenBalance{100, 10, 90, 10, 0, 10, 110})
			var restricted bool
			if err := s.db.QueryRow(`SELECT completion_only FROM v_release_token_capacity WHERE admission_id=?`, a.AdmissionID).Scan(&restricted); err != nil || !restricted {
				t.Fatal("SQL lost human denial restriction", restricted, err)
			}
		})
	}
}

func TestB3SQLHardDecisionRequiresCapturedDecision(t *testing.T) {
	for _, invalid := range []string{"missing", "null", "mismatch", "number"} {
		t.Run(invalid, func(t *testing.T) {
			s, a, p := hardRequestFixture(t)
			h := approvedBudget("sql-hard-response")
			h.Prompt = "Approve the proposed ten-token Hard extension?"
			h.Response = "Yes, I approve the ten-token extension."
			var response map[string]any
			if err := json.Unmarshal(mustApprovalJSON(h), &response); err != nil {
				t.Fatal(err)
			}
			switch invalid {
			case "missing":
				delete(response, "Decision")
			case "null":
				response["Decision"] = nil
			case "mismatch":
				response["Decision"] = "DENY"
			case "number":
				response["Decision"] = 1
			}
			raw, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			before := mutationDatabaseSnapshot(t, s)
			if _, err := s.db.Exec(`INSERT INTO hard_limit_decisions VALUES(?,'APPROVE',?,?,?,?,?)`, p.RequestID, string(raw), shiftedOptions(time.Minute).At, stringsRepeat64(), true, ""); err == nil {
				t.Fatal("SQL accepted incomplete human APPROVE")
			}
			g := p.Grant
			g.ApprovalID = p.RequestID
			if err := s.GrantResources(accountingContext, a.RunID, []ResourceGrant{g}, shiftedOptions(time.Minute)); err == nil {
				t.Fatal("incomplete response provided Go grant authority")
			}
			if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
				t.Fatal("invalid human response changed history")
			}
			s = restartAgentStore(t, s)
			if out, err := s.DecideHardLimitRequest(accountingContext, a.RunID, p.RequestID, &h, shiftedOptions(time.Minute)); err != nil || !out.Created || !out.Applied {
				t.Fatal("complete human APPROVE rejected", out, err)
			}
			requireAgentLimits(t, s, a, AgentTokenLimits{SoftLimit: 15, HardLimit: 20, Used: 10})
		})
	}
}

func TestB3ReleaseRequiresCompleteNormalizedBounds(t *testing.T) {
	for _, work := range []int64{2, 20} {
		for _, missing := range []string{"all", "WORK", "INPUT_TOKENS", "OUTPUT_TOKENS"} {
			t.Run(fmt.Sprintf("work-%d-missing-%s", work, missing), func(t *testing.T) {
				s, a := configuredAdmissionFixture(t, 100)
				a.Resources[0].Amount = 10
				admit(t, s, a)
				d := tokenClaim(a, "incomplete-bounds", work)
				if err := s.numericMutation(accountingContext, "test-fixture", a.RunID, nil, func(c *sql.Conn) error {
					return insertProviderCall(accountingContext, c, *d.ProviderCall, semanticTimestamp)
				}); err != nil {
					t.Fatal(err)
				}
				execSQL(t, s.db, `INSERT INTO admission_dispatch_claims VALUES(?,?,?,1,?,?,0,?)`, d.ClaimID, d.AdmissionID, d.ProviderCall.ProviderCallID, semanticTimestamp, admissionDigest(d), string(mustClaimJSON(d)))
				for _, b := range d.Bounds {
					if missing != "all" && b.Dimension != missing {
						execSQL(t, s.db, `INSERT INTO admission_dispatch_bounds VALUES(?,?,?)`, d.ClaimID, b.Dimension, b.Amount)
					}
				}
				s = restartAgentStore(t, s)
				before := mutationDatabaseSnapshot(t, s)
				if created, err := s.AuthorizeAdmissionRelease(accountingContext, a.RunID, d.ClaimID, accountingOptions()); created || err == nil {
					t.Fatal("Go released incomplete normalized bounds", created, err)
				}
				if _, err := s.db.Exec(`INSERT INTO admission_release_authorizations VALUES(?,?,?)`, d.ClaimID, semanticTimestamp, stringsRepeat64()); err == nil {
					t.Fatal("SQL released incomplete normalized bounds")
				}
				if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
					t.Fatal("rejected release changed history")
				}
				for _, b := range d.Bounds {
					if missing == "all" || b.Dimension == missing {
						execSQL(t, s.db, `INSERT INTO admission_dispatch_bounds VALUES(?,?,?)`, d.ClaimID, b.Dimension, b.Amount)
					}
				}
				if work > 10 {
					before = mutationDatabaseSnapshot(t, s)
					if created, err := s.AuthorizeAdmissionRelease(accountingContext, a.RunID, d.ClaimID, accountingOptions()); created || err == nil {
						t.Fatal("Go released above Hard", created, err)
					}
					if _, err := s.db.Exec(`INSERT INTO admission_release_authorizations VALUES(?,?,?)`, d.ClaimID, semanticTimestamp, stringsRepeat64()); err == nil {
						t.Fatal("SQL released above Hard")
					}
					if !reflect.DeepEqual(before, mutationDatabaseSnapshot(t, s)) {
						t.Fatal("over-capacity release changed history")
					}
					return
				}
				if created, err := s.AuthorizeAdmissionRelease(accountingContext, a.RunID, d.ClaimID, accountingOptions()); err != nil || !created {
					t.Fatal("complete bounds rejected", created, err)
				}
				s = restartAgentStore(t, s)
				if created, err := s.AuthorizeAdmissionRelease(accountingContext, a.RunID, d.ClaimID, accountingOptions()); err != nil || created {
					t.Fatal("release replay", created, err)
				}
			})
		}
	}
}
