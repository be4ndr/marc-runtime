package statestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"marc-runtime/internal/identity"
)

// These contracts accept explicit controller configuration, not agent requests.
// Units are the existing resource units. A soft limit records a cooperative
// boundary; it does not manufacture a hard-stop or select management policy.
type RunResourceBudget struct {
	ResourceType string
	HardLimit    int64
	SoftLimit    *int64
}
type RunAdmissionPolicy struct {
	RunID, PolicyID, HardDeadline string
	Budgets                       []RunResourceBudget
	Confirmation                  HumanBudgetApproval
	AgentTokenProfiles            []AgentTokenProfile
}
type AdmissionFence struct {
	Owner                                                                                 SemanticFence
	SemanticRevision, RunRevision, ExecutionRevision, AttemptRevision, InvocationRevision int64
}
type InvocationAdmission struct {
	AdmissionID, RunID string
	Kind               string // NORMAL or explicit controller CONTAINMENT (ORCHESTRATOR/RECOVER)
	Binding            InvocationBinding
	Fence              AdmissionFence
	HardDeadline       string // original execution deadline, never renewed on continuation
	// One original WORK spending limit; grants do not reserve bank capacity.
	Resources []AdmissionResource
	// Nonempty only for the Orchestrator's own minimal emergency completion operations.
	ContingencyReason string
	SoftLimit         *int64 `json:",omitempty"` // controller-supplied initial boundary when no role profile exists
}
type DispatchBound struct {
	Dimension string
	Amount    int64
}

// ProviderModelCapabilities is the normalized capability snapshot used to
// validate one provider request before release. ContextWindowTokens includes
// both input and requested output. MaxInputTokens is optional because some
// models expose only a combined context window and an output limit.
type ProviderModelCapabilities struct {
	Provider            string
	Model               string
	ContextWindowTokens int64
	MaxOutputTokens     int64
	MaxInputTokens      *int64 `json:",omitempty"`
}

// A completion request has no tools or workspace writes and returns only its
// final structured response. The future adapter must enforce these capabilities.
type CompletionCapabilities struct{ ToolsEnabled, WorkspaceWriteEnabled, FinalResponseOnly bool }
type AdmissionDispatchClaim struct {
	ClaimID, AdmissionID, RunID string
	ProviderCall                *ProviderCall              // nil for controller tool/execution release
	Bounds                      []DispatchBound            // exact semantics, never an estimate or usage observation
	Capabilities                *ProviderModelCapabilities `json:",omitempty"`
	CompletionCapabilities      *CompletionCapabilities    `json:",omitempty"`
	CompletionOnly              bool                       `json:",omitempty"` // trusted controller restriction after a denied top-up
}
type AdmissionDeniedError struct{ RunID, Reason string }

func (e *AdmissionDeniedError) Error() string {
	return "admission denied: " + e.Reason + " (" + e.RunID + ")"
}

var errAdmissionReplay = errors.New("identical admission evidence replay")
var budgetResources = []string{"WORK"}

// JSON fingerprints require valid, bounded UTF-8; encoding/json otherwise
// replaces invalid bytes, making conflicting controller identities compare equal.
func admissionUTF8(values ...string) error {
	for _, v := range values {
		if !identity.ValidText(v) {
			return fmt.Errorf("admission text must be valid UTF-8 and at most 256 Unicode code points")
		}
	}
	return nil
}

// descriptionUTF8 preserves exact UTF-8 before fingerprinting descriptive text.
// Structural strings continue to use admissionUTF8's 256-code-point bound.
func descriptionUTF8(values ...string) error {
	for _, v := range values {
		if err := identity.CheckDescription("description", v); err != nil {
			return err
		}
	}
	return nil
}

func requireIDs(fields ...struct{ name, value string }) error {
	for _, field := range fields {
		if err := identity.CheckID(field.name, field.value); err != nil {
			return err
		}
	}
	return nil
}
func optionalAdmissionUTF8(values ...*string) error {
	for _, v := range values {
		if v != nil {
			if err := admissionUTF8(*v); err != nil {
				return err
			}
		}
	}
	return nil
}

func admissionDigest(v any) string {
	b, _ := json.Marshal(v)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
func policyExists(ctx context.Context, q querier, run string) (bool, error) {
	var n int
	err := q.QueryRowContext(ctx, `SELECT count(*) FROM run_admission_policy WHERE run_id=?`, run).Scan(&n)
	return n == 1, err
}
func beforeDeadline(at, deadline string) bool {
	a, ea := time.Parse(time.RFC3339Nano, at)
	d, ed := time.Parse(time.RFC3339Nano, deadline)
	return ea == nil && ed == nil && a.Before(d)
}

// ConfigureRunAdmission installs the finite, immutable budget once. Historical
// consuming state has no admission provenance and cannot be silently adopted.
// Human-approved increments are separate immutable evidence; deadlines never extend.
func (s *StateStore) ConfigureRunAdmission(ctx context.Context, p RunAdmissionPolicy, opts AccountingOptions) error {
	var err error
	if err = admissionUTF8(p.RunID, p.PolicyID); err != nil {
		return err
	}
	if err = requireIDs(struct{ name, value string }{"run_id", p.RunID}, struct{ name, value string }{"policy_id", p.PolicyID}); err != nil {
		return err
	}
	if err := validateBudgetApproval(p.Confirmation, true); err != nil {
		return err
	}
	if strings.TrimSpace(p.RunID) == "" || strings.TrimSpace(p.PolicyID) == "" {
		return fmt.Errorf("invalid Run admission policy identity")
	}
	if p.HardDeadline, err = canonicalTime(p.HardDeadline); err != nil {
		return numericFailure("ConfigureRunAdmission", p.RunID, "", "", err)
	}
	if opts.At, err = canonicalTime(opts.At); err != nil {
		return numericFailure("ConfigureRunAdmission", p.RunID, "", "", err)
	}

	if len(p.Budgets) != len(budgetResources) {
		return fmt.Errorf("Run requires one finite WORK token budget")
	}
	seen := map[string]bool{}
	for _, b := range p.Budgets {
		if b.ResourceType != "WORK" || seen[b.ResourceType] || b.HardLimit < 0 || b.SoftLimit != nil && (*b.SoftLimit < 0 || *b.SoftLimit > b.HardLimit) {
			return fmt.Errorf("invalid finite Run budget")
		}
		seen[b.ResourceType] = true
		if _, err := checkedAdd(b.HardLimit, b.HardLimit/10); err != nil {
			return numericFailure("ConfigureRunAdmission", p.RunID, "", "", err)
		}
	}
	if err := resolveProfilePercentages(&p); err != nil {
		return err
	}
	roles := map[string]bool{}
	for _, profile := range p.AgentTokenProfiles {
		if !primaryAgentRole(profile.Role) || roles[profile.Role] || profile.SoftLimit < 0 || profile.SoftLimit > profile.HardLimit || profile.HardLimit > p.Budgets[0].HardLimit {
			return fmt.Errorf("invalid agent token role profile")
		}
		roles[profile.Role] = true
	}
	err = s.numericMutation(ctx, "ConfigureRunAdmission", p.RunID, opts.Audit, func(c *sql.Conn) error {
		var old string
		digest := admissionDigest(p)
		err := c.QueryRowContext(ctx, `SELECT contract_digest FROM run_budget_confirmations WHERE run_id=?`, p.RunID).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting confirmed Run policy replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if !beforeDeadline(opts.At, p.HardDeadline) {
			return fmt.Errorf("Run deadline must be in the future at configuration")
		}
		if err := approvalUnused(ctx, c, p.Confirmation); err != nil {
			return err
		}
		var historical int
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_calls p JOIN invocations i USING(invocation_id) WHERE i.run_id=?) OR EXISTS(SELECT 1 FROM resource_allocations WHERE run_id=?)`, p.RunID, p.RunID).Scan(&historical); err != nil {
			return err
		}
		if historical != 0 {
			return fmt.Errorf("historical consumption/commitments require explicit reconciliation; cannot invent admission provenance")
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO run_admission_policy VALUES(?,?,?,?)`, p.RunID, p.PolicyID, p.HardDeadline, opts.At); err != nil {
			return err
		}
		for _, b := range p.Budgets {
			if _, err := c.ExecContext(ctx, `INSERT INTO run_resource_budgets VALUES(?,?,?,?)`, p.RunID, b.ResourceType, b.HardLimit, b.SoftLimit); err != nil {
				return err
			}
		}
		for _, profile := range p.AgentTokenProfiles {
			if _, err := c.ExecContext(ctx, `INSERT INTO run_agent_token_profiles VALUES(?,?,?,?)`, p.RunID, profile.Role, profile.SoftLimit, profile.HardLimit); err != nil {
				return err
			}
		}
		raw, _ := json.Marshal(p.Confirmation)
		if _, err := c.ExecContext(ctx, `INSERT INTO run_budget_confirmations VALUES(?,?,?,?,?)`, p.RunID, p.Confirmation.ApprovalID, string(raw), opts.At, digest); err != nil {
			return err
		}

		return nil
	})
	if errors.Is(err, errAdmissionReplay) {
		return nil
	}
	return err
}

// Run availability depends only on confirmed current WORK, never grants or holds.
// Each physical call contributes once; NULL is zero only for this calculation.
type runCharge struct{ Total, Known int64 }

// A reported component may prove a dispatch-bound violation without fabricating
// a normalized WORK observation. It is never used to charge the Run bank.
func knownResourceMinimum(amount, left, right *int64) *int64 {
	for _, component := range []*int64{left, right} {
		if component != nil && (amount == nil || *component > *amount) {
			amount = component
		}
	}
	return amount
}

func admissionCharge(ctx context.Context, q querier, run, resource string) (runCharge, error) {
	if resource != "WORK" {
		return runCharge{}, fmt.Errorf("WORK is the only budget quantity")
	}
	b, err := runTokenBalance(ctx, q, run)
	return runCharge{Total: b.NormalUsed, Known: b.NormalUsed}, err
}

func validateAdmissionFence(ctx context.Context, c *sql.Conn, run string, b InvocationBinding, f AdmissionFence) error {
	if f.Owner.InvocationID != nil && f.Owner.ContentIdentity == nil {
		return fmt.Errorf("bound admission owner requires content fence")
	}
	actor, err := validateOwner(ctx, c, run, f.Owner)
	if err != nil {
		return err
	}
	var role, purpose, execution, attempt, runStatus string
	var content *string
	var cycle, currentCycle int
	var epoch, srev, rrev, erev, arev, irev int64
	var completed, executionCompleted, attemptCompleted *string
	err = c.QueryRowContext(ctx, `SELECT i.role,i.purpose,i.execution_id,i.attempt_id,i.cycle_no,i.content_identity,i.ownership_epoch,i.completed_at,
 r.current_cycle,r.status,r.revision,e.revision,t.revision,i.revision,s.revision,e.completed_at,t.completed_at
 FROM invocations i JOIN runs r ON r.run_id=i.run_id JOIN executions e ON e.execution_id=i.execution_id
 JOIN attempts t ON t.attempt_id=i.attempt_id JOIN run_current_semantic_state s ON s.run_id=i.run_id
 WHERE i.run_id=? AND i.invocation_id=?`, run, b.InvocationID).Scan(&role, &purpose, &execution, &attempt, &cycle, &content, &epoch, &completed, &currentCycle, &runStatus, &rrev, &erev, &arev, &irev, &srev, &executionCompleted, &attemptCompleted)
	if err != nil {
		return err
	}
	if execution != b.ExecutionID || attempt != b.AttemptID || cycle != b.Cycle || cycle != currentCycle || purpose != b.Purpose || epoch != actor.OwnershipEpoch || completed != nil || executionCompleted != nil || attemptCompleted != nil || runStatus != "RUNNING" ||
		rrev != f.RunRevision || erev != f.ExecutionRevision || arev != f.AttemptRevision || irev != f.InvocationRevision || srev != f.SemanticRevision {
		return fmt.Errorf("stale admission ancestry/lifecycle/fencing")
	}
	if role == "ORCHESTRATOR" && purpose == "RECOVER" {
		if content == nil && b.ContentIdentity != "" || content != nil && *content != b.ContentIdentity {
			return fmt.Errorf("admission content mismatch")
		}
	} else {
		if role != actor.Role || actor.InvocationID == nil || *actor.InvocationID != b.InvocationID || content == nil || *content != b.ContentIdentity || b.ContentIdentity == "" {
			return fmt.Errorf("admission requires current bound semantic invocation/content")
		}
	}
	var accepted int
	if err = c.QueryRowContext(ctx, `SELECT count(*) FROM role_results WHERE invocation_id=?`, b.InvocationID).Scan(&accepted); err != nil {
		return err
	}
	if accepted != 0 {
		return fmt.Errorf("completed result cannot authorize continuation")
	}
	return nil
}

// AdmitInvocation grants tokens to active work and commits their provenance
// under the same writer lock as ownership and Run-wide capacity validation.
// The bool reports new evidence; replay never grants a second execution release.
func (s *StateStore) AdmitInvocation(ctx context.Context, a InvocationAdmission, opts AccountingOptions) (newRecord bool, resultErr error) {
	defer func() { resultErr = numericFailure("AdmitInvocation", a.RunID, a.Binding.InvocationID, "", resultErr) }()
	var err error
	if a.Kind == "" {
		a.Kind = "NORMAL"
	}
	if a.Kind != "NORMAL" && a.Kind != "CONTAINMENT" {
		return false, fmt.Errorf("invalid admission kind")
	}
	if a.AdmissionID == "" || a.RunID == "" || len(a.Resources) != len(budgetResources) {
		return false, fmt.Errorf("admission requires identity and one explicit WORK spending limit")
	}
	if opts.At, err = canonicalTime(opts.At); err != nil {
		return false, numericFailure("AdmitInvocation", a.RunID, a.Binding.InvocationID, "", err)
	}
	if a.HardDeadline, err = canonicalTime(a.HardDeadline); err != nil {
		return false, numericFailure("AdmitInvocation", a.RunID, a.Binding.InvocationID, "", err)
	}
	a.Resources = append([]AdmissionResource(nil), a.Resources...)
	sort.Slice(a.Resources, func(i, j int) bool { return a.Resources[i].Account.ResourceType < a.Resources[j].Account.ResourceType })
	if err = admissionUTF8(a.AdmissionID, a.RunID, a.Binding.InvocationID, a.Binding.ExecutionID, a.Binding.AttemptID, a.Binding.ContentIdentity); err != nil {
		return false, err
	}
	if err := descriptionUTF8(a.ContingencyReason); err != nil {
		return false, err
	}
	if err = requireIDs(
		struct{ name, value string }{"admission_id", a.AdmissionID},
		struct{ name, value string }{"run_id", a.RunID},
		struct{ name, value string }{"invocation_id", a.Binding.InvocationID},
		struct{ name, value string }{"execution_id", a.Binding.ExecutionID},
		struct{ name, value string }{"attempt_id", a.Binding.AttemptID},
	); err != nil {
		return false, err
	}
	if a.Binding.ContentIdentity != "" {
		if err = identity.CheckID("content_identity", a.Binding.ContentIdentity); err != nil {
			return false, err
		}
	}
	if err = optionalAdmissionUTF8(a.Fence.Owner.InvocationID, a.Fence.Owner.ContentIdentity); err != nil {
		return false, err
	}
	for _, r := range a.Resources {
		if err := validateAccount(r.Account); err != nil {
			return false, err
		}
	}
	digest := admissionDigest(a)
	created := false
	var denied error
	err = s.numericMutation(ctx, "AdmitInvocation", a.RunID, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT contract_digest FROM invocation_admissions WHERE admission_id=?`, a.AdmissionID).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting admission replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var runDeadline string
		if err = c.QueryRowContext(ctx, `SELECT hard_deadline FROM run_admission_policy WHERE run_id=?`, a.RunID).Scan(&runDeadline); err != nil {
			return fmt.Errorf("finite Run budgets required for admission: %w", err)
		}
		if !beforeDeadline(opts.At, runDeadline) {
			if err = observeHardStop(ctx, c, a.RunID, "", "UNPROVEN_CONTINUATION", "deadline:run", nil, nil, opts.At); err != nil {
				return err
			}
			denied = &AdmissionDeniedError{a.RunID, "original Run hard deadline expired"}
			return nil
		}
		if !beforeDeadline(opts.At, a.HardDeadline) || beforeDeadline(runDeadline, a.HardDeadline) {
			return fmt.Errorf("expired or extended hard deadline")
		}
		if err = validateAdmissionFence(ctx, c, a.RunID, a.Binding, a.Fence); err != nil {
			return err
		}
		var stops int
		if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM v_run_token_hard_stops WHERE 1=1 AND run_id=?)`, a.RunID).Scan(&stops); err != nil {
			return err
		}
		if stops != 0 && a.Kind == "NORMAL" {
			denied = &AdmissionDeniedError{a.RunID, "durable hard stop"}
			return nil
		}
		var preexisting int
		if err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_calls WHERE invocation_id=?)`, a.Binding.InvocationID).Scan(&preexisting); err != nil {
			return err
		}
		if preexisting != 0 {
			return fmt.Errorf("fresh admission cannot adopt prior physical calls or commitments")
		}
		var executionDeadline string
		err = c.QueryRowContext(ctx, `SELECT hard_deadline FROM execution_admission_deadlines WHERE execution_id=? AND run_id=?`, a.Binding.ExecutionID, a.RunID).Scan(&executionDeadline)
		if err == nil {
			if executionDeadline != a.HardDeadline {
				return fmt.Errorf("continuation cannot renew the original execution deadline")
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			if _, err = c.ExecContext(ctx, `INSERT INTO execution_admission_deadlines VALUES(?,?,?)`, a.Binding.ExecutionID, a.RunID, a.HardDeadline); err != nil {
				return err
			}
		} else {
			return err
		}
		var content *string
		if a.Binding.ContentIdentity != "" {
			content = &a.Binding.ContentIdentity
		}
		f := a.Fence
		b := a.Binding
		var role string
		if err = c.QueryRowContext(ctx, `SELECT role FROM invocations WHERE invocation_id=?`, b.InvocationID).Scan(&role); err != nil {
			return err
		}
		if a.Kind == "CONTAINMENT" && (role != "ORCHESTRATOR" || b.Purpose != "RECOVER") {
			return fmt.Errorf("containment requires controller RECOVER identity")
		}
		if a.Kind == "NORMAL" && primaryAgentRole(role) && a.ContingencyReason == "" {
			if err := resolveAgentTokenProfile(ctx, c, &a, role); err != nil {
				return err
			}
		} else if a.SoftLimit != nil {
			return fmt.Errorf("agent Soft limit requires normal primary-agent admission")
		}
		_, err = c.ExecContext(ctx, `INSERT INTO invocation_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, a.AdmissionID, a.RunID, b.InvocationID, b.ExecutionID, b.AttemptID, b.Cycle, role, b.Purpose, a.Kind, content, f.Owner.OwnershipEpoch, f.Owner.ActorRevision, f.Owner.Role, f.Owner.InvocationID, f.Owner.ContentIdentity, f.SemanticRevision, f.RunRevision, f.ExecutionRevision, f.AttemptRevision, f.InvocationRevision, opts.At, digest)
		if err != nil {
			return err
		}
		for n, r := range a.Resources {
			if r.Account.ResourceType != budgetResources[n] || r.Account.RunID != a.RunID || r.Amount < 0 {
				return fmt.Errorf("invalid admission resource identity/dimension")
			}
			balance, err := resourceBalance(ctx, c, r.Account)
			if err != nil {
				return err
			}
			var eligible int
			if err = c.QueryRowContext(ctx, `SELECT count(*) FROM resource_pool_eligibility e JOIN resource_pools p USING(pool_id) WHERE p.pool_id=? AND e.role=? AND e.purpose=? AND (p.execution_id IS NULL OR p.execution_id=?) AND (p.attempt_id IS NULL OR p.attempt_id=?)`, r.Account.PoolID, role, b.Purpose, b.ExecutionID, b.AttemptID).Scan(&eligible); err != nil {
				return err
			}
			if eligible != 1 {
				return fmt.Errorf("ineligible admission pool/ancestry")
			}
			allocation := ""
			if a.ContingencyReason != "" {
				if a.Kind != "CONTAINMENT" || role != "ORCHESTRATOR" || b.Purpose != "RECOVER" || strings.TrimSpace(a.ContingencyReason) == "" {
					return fmt.Errorf("contingency requires explicit Orchestrator minimal-terminal authorization")
				}
				if err := validateContingencyWork(ctx, c, a); err != nil {
					return err
				}
				if _, err := c.ExecContext(ctx, `INSERT INTO admission_contingency_authorizations VALUES(?,?)`, a.AdmissionID, a.ContingencyReason); err != nil {
					return err
				}
				balance.Granted = 0
			}
			if tokenResource(r.Account.ResourceType) {
				if err := validateNormalTokenAccount(ctx, c, r.Account); err != nil {
					return err
				}
				if a.ContingencyReason == "" && (r.Amount > 0 || a.Kind == "NORMAL" && primaryAgentRole(role)) {
					if err := validateActiveTokenWork(ctx, c, a); err != nil {
						return err
					}

					if _, err := c.ExecContext(ctx, `INSERT INTO admission_agent_token_limits VALUES(?,?,?)`, a.AdmissionID, *a.SoftLimit, r.Amount); err != nil {
						return err
					}

					if r.Amount > 0 {
						allocation = tokenGrantID(a.AdmissionID, r.Account.ResourceType)
						if _, err := c.ExecContext(ctx, `INSERT INTO resource_allocations(allocation_id,run_id,resource_type,amount,source,created_at,pool_id) VALUES(?,?,?,?,'active-token-grant',?,?)`, allocation, a.RunID, r.Account.ResourceType, r.Amount, opts.At, r.Account.PoolID); err != nil {
							return err
						}
						balance.Granted = r.Amount
					}
				}
			}
			if _, err = c.ExecContext(ctx, `INSERT INTO admission_resources VALUES(?,?,?,?)`, a.AdmissionID, r.Account.ResourceType, r.Account.PoolID, r.Amount); err != nil {
				return err
			}
			if tokenResource(r.Account.ResourceType) {
				if allocation != "" {
					if _, err := c.ExecContext(ctx, `INSERT INTO admission_grants VALUES(?,?,?)`, a.AdmissionID, r.Account.ResourceType, allocation); err != nil {
						return err
					}
				}
			}
			bank, err := runTokenBalance(ctx, c, a.RunID)
			if err != nil {
				return err
			}
			available := bank.NormalAvailable
			if a.ContingencyReason != "" {
				available = bank.ContingencyAvailable
			}
			if r.Amount > 0 && (available <= 0 || r.Amount > available || bank.contingencyExhausted()) {
				return &AdmissionDeniedError{a.RunID, "current token bank cannot fund active spending limit"}
			}

		}
		created = true
		return nil
	})
	if errors.Is(err, errAdmissionReplay) {
		return false, nil
	}
	var projection *MetricsProjectionError
	if errors.As(err, &projection) {
		if denied != nil {
			err = errors.Join(denied, err)
		}
		return created, err
	}
	if err != nil {
		return false, err
	}
	return created, denied
}

// Token grants belong to an admitted, currently bound primary agent. Recovery's
// unbound-controller exception in validateAdmissionFence grants no token authority.
func validateActiveTokenWork(ctx context.Context, c *sql.Conn, a InvocationAdmission) error {
	if a.Kind != "NORMAL" || a.Fence.Owner.InvocationID == nil || *a.Fence.Owner.InvocationID != a.Binding.InvocationID || !semanticPurpose(a.Fence.Owner.Role, a.Binding.Purpose) {
		return fmt.Errorf("normal token grant requires the active primary workflow agent")
	}
	if err := validateAdmissionFence(ctx, c, a.RunID, a.Binding, a.Fence); err != nil {
		return err
	}
	return validateSequentialAgent(ctx, c, a.RunID, a.Binding.InvocationID)
}

func tokenGrantID(admission, resource string) string {
	return admissionDigest([]string{"active-token-grant", admission, resource})
}

// Old admission_grants were snapshots of preallocated pool envelopes. Initial
// active-admission limits or the original deterministic grant binding supply
// provenance, including a legitimate zero limit, independently of grant amount.
func activeTokenGrant(ctx context.Context, q querier, admission, resource string) (int64, error) {
	var initial bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admission_agent_token_limits WHERE admission_id=?)`, admission).Scan(&initial); err != nil {
		return 0, err
	}
	if !initial {
		return 0, nil
	}
	rows, err := q.QueryContext(ctx, `SELECT g.allocation_id,g.amount FROM admission_grants b JOIN resource_allocations g USING(allocation_id) WHERE b.admission_id=? AND b.resource_type=?`, admission, resource)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var id string
		var amount int64
		if err := rows.Scan(&id, &amount); err != nil {
			return 0, err
		}
		total, err = checkedAdd(total, amount)
		if err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if !initial {
		return 0, nil
	}
	return total, nil
}

// Emergency work has an explicit controller bound, never an agent allocation.
// Historical controller allocations remain evidence and cannot increase it.
func admissionTokenLimit(ctx context.Context, q querier, a InvocationAdmission) (int64, error) {
	if a.ContingencyReason == "" {
		return activeTokenGrant(ctx, q, a.AdmissionID, "WORK")
	}
	var limit int64
	err := q.QueryRowContext(ctx, `SELECT initial_limit FROM admission_resources WHERE admission_id=? AND resource_type='WORK'`, a.AdmissionID).Scan(&limit)
	return limit, err
}

func validateNormalTokenAccount(ctx context.Context, c *sql.Conn, a ResourceAccount) error {
	if a.Scope != "RUN" || a.ExecutionID != nil || a.AttemptID != nil {
		return fmt.Errorf("normal tokens require a shared Run accounting address")
	}
	var eligible, different int
	if err := c.QueryRowContext(ctx, `SELECT count(*) FROM resource_pool_eligibility WHERE pool_id=? AND role<>'ORCHESTRATOR'`, a.PoolID).Scan(&eligible); err != nil {
		return err
	}
	// All seven primary role/purpose pairs share the same bank; no purpose pool.
	if eligible != 7 {
		return fmt.Errorf("normal token bank cannot restrict capacity by workflow purpose")
	}
	if err := c.QueryRowContext(ctx, `SELECT count(*) FROM admission_resources r JOIN invocation_admissions a USING(admission_id) WHERE a.run_id=? AND r.resource_type=? AND r.pool_id<>?`, a.RunID, a.ResourceType, a.PoolID).Scan(&different); err != nil {
		return err
	}
	if different != 0 {
		return fmt.Errorf("normal tokens must use the same Run accounting address")
	}
	return nil
}

func validateTokenTopUp(ctx context.Context, c *sql.Conn, run string, g ResourceGrant, at string, authorizedHard bool) error {
	if g.AdmissionID == "" {
		return fmt.Errorf("normal tokens require active admission; pool pre-allocation is prohibited")
	}
	a, err := readAdmission(ctx, c, run, g.AdmissionID)
	if err != nil {
		return err
	}
	if err := validateActiveTokenWork(ctx, c, a); err != nil {
		return err
	}
	if g.RequestID != nil {
		var matches int
		if err := c.QueryRowContext(ctx, `SELECT count(*) FROM resource_requests WHERE request_id=? AND run_id=? AND target_execution_id=? AND target_attempt_id=? AND target_role=? AND target_purpose=? AND status='PENDING'`, requestStorageKey(run, *g.RequestID), run, a.Binding.ExecutionID, a.Binding.AttemptID, a.Fence.Owner.Role, a.Binding.Purpose).Scan(&matches); err != nil {
			return err
		}
		if matches != 1 {
			return fmt.Errorf("token top-up request does not target the active agent")
		}
	}
	if err := validateNormalTokenAccount(ctx, c, g.Account); err != nil {
		return err
	}
	var pool string
	if err := c.QueryRowContext(ctx, `SELECT pool_id FROM admission_resources WHERE admission_id=? AND resource_type=?`, g.AdmissionID, g.Account.ResourceType).Scan(&pool); err != nil {
		return err
	}
	if err := admissionClosed(ctx, c, g.AdmissionID); err != nil {
		return err
	}
	if pool != g.Account.PoolID || !beforeDeadline(at, a.HardDeadline) {
		return fmt.Errorf("top-up requires original address and deadline")
	}
	grant, err := activeTokenGrant(ctx, c, g.AdmissionID, g.Account.ResourceType)
	if err != nil {
		return err
	}
	if err := ensureAgentTokenLimits(ctx, c, a, grant); err != nil {
		return err
	}
	var stops int
	if err := c.QueryRowContext(ctx, `SELECT count(*) FROM v_run_token_hard_stops WHERE 1=1 AND run_id=?`, run).Scan(&stops); err != nil {
		return err
	}
	if stops != 0 {
		return &AdmissionDeniedError{run, "durable hard stop"}
	}
	bank, err := runTokenBalance(ctx, c, run)
	if err != nil {
		return err
	}
	used, _, err := invocationActual(ctx, c, a.Binding.InvocationID, "WORK")
	if err != nil {
		return err
	}
	if used > grant {
		return &AdmissionDeniedError{run, "agent budget exceeded; human decision required"}
	}
	if used >= grant && grant > 0 && !authorizedHard {
		var allowed bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM hard_limit_requests r JOIN hard_limit_decisions d USING(request_id) WHERE r.request_id=? AND r.run_id=? AND r.admission_id=? AND r.allocation_id=? AND r.amount=? AND r.initial_hard_limit=? AND d.applied=1 AND d.decision IN ('APPROVE','TIMEOUT'))`, g.ApprovalID, run, g.AdmissionID, g.AllocationID, g.Amount, grant).Scan(&allowed); err != nil {
			return err
		}
		if !allowed {
			return &AdmissionDeniedError{run, "Hard continuation needs the specific durable approval decision"}
		}
	}
	limit, err := checkedAdd(grant, g.Amount)
	if err != nil {
		return err
	}
	remaining, err := checkedSubtract(limit, used)
	if err != nil {
		return err
	}
	if bank.NormalAvailable <= 0 || remaining > bank.NormalAvailable || bank.contingencyExhausted() {
		return &AdmissionDeniedError{run, "normal Run token bank exhausted"}
	}
	return nil
}
