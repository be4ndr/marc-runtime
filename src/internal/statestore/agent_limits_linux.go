package statestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Profiles are trusted Orchestrator policy. Percentages, when supplied, resolve
// against the original normal Run budget, not contingency or remaining grants.
type AgentTokenProfile struct {
	Role                     string
	SoftLimit, HardLimit     int64
	SoftPercent, HardPercent *int64
}

func primaryAgentRole(role string) bool {
	return role == "LEAD" || role == "DEVELOPER" || role == "QA" || role == "REVIEWER"
}

func resolveProfilePercentages(p *RunAdmissionPolicy) error {
	p.AgentTokenProfiles = append([]AgentTokenProfile(nil), p.AgentTokenProfiles...)
	for n := range p.AgentTokenProfiles {
		r := &p.AgentTokenProfiles[n]
		if r.SoftPercent == nil && r.HardPercent == nil {
			continue
		}
		if r.SoftPercent == nil || r.HardPercent == nil || *r.SoftPercent < 0 || *r.HardPercent > 100 || *r.SoftPercent > *r.HardPercent || r.SoftLimit != 0 || r.HardLimit != 0 {
			return fmt.Errorf("invalid agent token profile percentages")
		}
		budget := p.Budgets[0].HardLimit
		// Divide before multiplication to retain exact whole tokens without overflow.
		percent := func(v int64) int64 { return (budget/100)*v + ((budget%100)*v)/100 }
		r.SoftLimit, r.HardLimit = percent(*r.SoftPercent), percent(*r.HardPercent)
	}
	return nil
}

func resolveAgentTokenProfile(ctx context.Context, c *sql.Conn, a *InvocationAdmission, role string) error {
	var soft, hard int64
	err := c.QueryRowContext(ctx, `SELECT soft_limit,hard_limit FROM run_agent_token_profiles WHERE run_id=? AND role=?`, a.RunID, role).Scan(&soft, &hard)
	if errors.Is(err, sql.ErrNoRows) {
		var configured int
		if err := c.QueryRowContext(ctx, `SELECT count(*) FROM run_agent_token_profiles WHERE run_id=?`, a.RunID).Scan(&configured); err != nil {
			return err
		}
		if configured != 0 {
			return fmt.Errorf("active role has no configured token profile")
		}
		// Existing controller callers retain explicit Hard grants. Omitted Soft
		// means Soft=Hard; the Run soft threshold never supplies an agent limit.
		hard, soft = a.Resources[0].Amount, a.Resources[0].Amount
		if a.SoftLimit != nil {
			soft = *a.SoftLimit
		}
	} else if err != nil {
		return err
	} else if a.Resources[0].Amount != 0 && a.Resources[0].Amount != hard || a.SoftLimit != nil && *a.SoftLimit != soft {
		return fmt.Errorf("initial limits conflict with configured role profile")
	}
	if soft < 0 || hard < soft {
		return fmt.Errorf("invalid agent Soft/Hard limits")
	}
	a.Resources[0].Amount, a.SoftLimit = hard, &soft
	return nil
}

// Initial limit provenance is created with the admission, including zero limits.
// A top-up must never adopt a missing historical or speculative limit record.
func ensureAgentTokenLimits(ctx context.Context, c *sql.Conn, a InvocationAdmission, grant int64) error {
	var initial int64
	if err := c.QueryRowContext(ctx, `SELECT initial_hard_limit FROM admission_agent_token_limits WHERE admission_id=?`, a.AdmissionID).Scan(&initial); err != nil {
		return err
	}
	if grant < initial {
		return fmt.Errorf("agent grant is below initial limit")
	}
	return nil
}

// Derived from immutable initial limits, approved allocations and confirmed
// cumulative ProviderCall WORK. This is not another token balance or lifecycle.
type AgentTokenLimits struct {
	SoftLimit, HardLimit, Used     int64
	CompletionOnly, BudgetExceeded bool
}

func agentTokenLimits(ctx context.Context, q querier, a InvocationAdmission) (AgentTokenLimits, error) {
	var limits AgentTokenLimits
	hard, err := activeTokenGrant(ctx, q, a.AdmissionID, "WORK")
	if err != nil {
		return limits, err
	}
	limits.HardLimit, limits.SoftLimit = hard, hard
	var baseSoft, baseHard int64
	err = q.QueryRowContext(ctx, `SELECT initial_soft_limit,initial_hard_limit FROM admission_agent_token_limits WHERE admission_id=?`, a.AdmissionID).Scan(&baseSoft, &baseHard)
	if err == nil {
		increase, err := checkedSubtract(hard, baseHard)
		if err != nil || increase < 0 {
			return limits, fmt.Errorf("agent grant is below its initial Hard limit")
		}
		limits.SoftLimit, err = checkedAdd(baseSoft, increase)
		if err != nil {
			return limits, err
		}
	} else {
		return limits, err
	}
	limits.Used, _, err = invocationActual(ctx, q, a.Binding.InvocationID, "WORK")
	if err != nil {
		return limits, err
	}
	limits.BudgetExceeded = limits.Used > hard
	err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admission_token_topup_denials WHERE admission_id=?1 AND hard_limit=?2)
 OR EXISTS(SELECT 1 FROM hard_limit_requests r JOIN hard_limit_decisions d USING(request_id)
 WHERE r.admission_id=?1 AND r.initial_hard_limit=?2 AND d.decision='DENY')`, a.AdmissionID, hard).Scan(&limits.CompletionOnly)
	return limits, err
}

func (s *StateStore) GetAgentTokenLimits(ctx context.Context, run, admission string) (_ AgentTokenLimits, resultErr error) {
	defer func() { resultErr = numericFailure("GetAgentTokenLimits", run, "", "", resultErr) }()
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"admission_id", admission}); err != nil {
		return AgentTokenLimits{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AgentTokenLimits{}, err
	}
	defer tx.Rollback()
	a, err := readAdmission(ctx, tx, run, admission)
	if err != nil {
		return AgentTokenLimits{}, err
	}
	if a.Kind != "NORMAL" || !primaryAgentRole(a.Fence.Owner.Role) {
		return AgentTokenLimits{}, fmt.Errorf("not a primary-agent admission")
	}
	return agentTokenLimits(ctx, tx, a)
}

// DenyTokenTopUp persists an explicit Orchestrator decision. It allocates nothing
// and permits only controller-marked completion calls within existing Hard/bank
// capacity. A later explicit approval increases both limits and supersedes the
// denial at the old Hard limit. This API does not coordinate the runtime request.
func (s *StateStore) DenyTokenTopUp(ctx context.Context, run, admission, decision, reason string, opts AccountingOptions) (bool, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"admission_id", admission}, struct{ name, value string }{"decision_id", decision}); err != nil {
		return false, err
	}
	if strings.TrimSpace(decision) == "" || strings.TrimSpace(reason) == "" {
		return false, fmt.Errorf("top-up denial requires decision identity and reason")
	}
	if err := admissionUTF8(run, admission, decision); err != nil {
		return false, err
	}
	if err := descriptionUTF8(reason); err != nil {
		return false, err
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return false, err
	}
	created := false
	err = s.numericMutation(ctx, "DenyTokenTopUp", run, opts.Audit, func(c *sql.Conn) error {
		var oldAdmission, oldReason, oldRun string
		err := c.QueryRowContext(ctx, `SELECT d.admission_id,d.reason,a.run_id FROM admission_token_topup_denials d JOIN invocation_admissions a USING(admission_id) WHERE d.decision_id=?`, decision).Scan(&oldAdmission, &oldReason, &oldRun)
		if err == nil {
			if oldAdmission != admission || oldReason != reason || oldRun != run {
				return fmt.Errorf("conflicting top-up denial replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		a, err := readAdmission(ctx, c, run, admission)
		if err != nil {
			return err
		}
		if err := validateActiveTokenWork(ctx, c, a); err != nil {
			return err
		}
		if !beforeDeadline(at, a.HardDeadline) {
			return fmt.Errorf("original agent deadline expired")
		}
		limits, err := agentTokenLimits(ctx, c, a)
		if err != nil {
			return err
		}
		var active, closed bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_calls WHERE invocation_id=? AND duration_ns IS NULL),
 EXISTS(SELECT 1 FROM admission_non_dispatch_proofs WHERE admission_id=?) OR
 EXISTS(SELECT 1 FROM admission_closures WHERE admission_id=?)`, a.Binding.InvocationID, admission, admission).Scan(&active, &closed); err != nil {
			return err
		}
		if active || closed {
			return fmt.Errorf("top-up denial requires live admission between ProviderCalls")
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO admission_token_topup_denials VALUES(?,?,?,?,?)`, decision, admission, limits.HardLimit, reason, at); err != nil {
			return err
		}
		created = true
		return nil
	})
	if errors.Is(err, errAdmissionReplay) {
		return false, nil
	}
	return created, err
}

func refreshAgentTokenStops(ctx context.Context, c *sql.Conn, run, at string) error {
	rows, err := c.QueryContext(ctx, `SELECT admission_id FROM invocation_admissions WHERE run_id=? AND kind='NORMAL' AND role IN ('LEAD','DEVELOPER','QA','REVIEWER') ORDER BY admission_id`, run)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		a, err := readAdmission(ctx, c, run, id)
		if err != nil {
			return err
		}
		limits, err := agentTokenLimits(ctx, c, a)
		if err != nil {
			return err
		}
		source := "agent:" + id
		if limits.HardLimit > 0 && limits.BudgetExceeded {
			if err := observeHardStop(ctx, c, run, "WORK", "BUDGET_EXCEEDED", source, &limits.Used, &limits.HardLimit, at); err != nil {
				return err
			}
		}
		// Original stop evidence remains authoritative if limits later changed.
		stop := stopIdentity(run, "BUDGET_EXCEEDED", source)
		var class string
		var amount, hard int64
		if err := c.QueryRowContext(ctx, `SELECT COALESCE(d.classification,'PENDING'),s.observed_amount,s.applicable_limit FROM run_hard_stops s LEFT JOIN hard_stop_decisions d ON d.stop_id=s.stop_id AND d.ordinal=1 WHERE s.stop_id=? AND s.resource_type='WORK' AND s.observed_amount>s.applicable_limit`, stop).Scan(&class, &amount, &hard); errors.Is(err, sql.ErrNoRows) {
			continue
		} else if err != nil {
			return err
		}
		if class == "BOUNDED_TERMINAL_OVERRUN" {
			// Preserve the immutable historical decision, but append a correctly
			// classified stop for the same original cumulative Hard violation.
			source = "agent-hard:" + id
			if err := observeHardStop(ctx, c, run, "WORK", "BUDGET_EXCEEDED", source, &amount, &hard, at); err != nil {
				return err
			}
			stop = stopIdentity(run, "BUDGET_EXCEEDED", source)
		}
		classification, err := observedBoundClassification(ctx, c, run)
		if err != nil {
			return err
		}
		// Per-call bounds cannot waive the cumulative agent bound. Existing
		// neutral bound-anomaly/human disposition handling applies.
		if err := resolveObservedHardStop(ctx, c, stop, classification, at); err != nil {
			return err
		}
	}
	return nil
}
