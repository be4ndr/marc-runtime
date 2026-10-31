package statestore

import (
	"context"
	"database/sql"
	"fmt"
)

// Balances are checked projections of immutable policy and current per-call
// evidence. Grants are spending limits; they do not debit either bank.
type RunTokenBalance struct {
	NormalBudget, NormalUsed, NormalAvailable                int64
	ContingencyBudget, ContingencyUsed, ContingencyAvailable int64
	AbsoluteLimit                                            int64
}

// A consumed contingency allowance at zero blocks all new consuming work.
// Small normal budgets can have no whole contingency token at all; that initial
// zero allowance alone must not prevent ordinary normal-bank work.
func (b RunTokenBalance) contingencyExhausted() bool {
	return b.ContingencyUsed > 0 && b.ContingencyAvailable <= 0
}

func runTokenBalance(ctx context.Context, q querier, run string) (RunTokenBalance, error) {
	var b RunTokenBalance
	if err := q.QueryRowContext(ctx, `SELECT normal_budget FROM v_run_token_balance WHERE run_id=?`, run).Scan(&b.NormalBudget); err != nil {
		return b, err
	}
	// Tokens are integral. A fractional final token in the 10% allowance cannot
	// be spent; truncation never enlarges the configured absolute ceiling.
	b.ContingencyBudget = b.NormalBudget / 10
	var err error
	b.AbsoluteLimit, err = checkedAdd(b.NormalBudget, b.ContingencyBudget)
	if err != nil {
		return b, err
	}
	rows, err := q.QueryContext(ctx, `SELECT u.work_amount, EXISTS(SELECT 1 FROM invocation_admissions a JOIN admission_contingency_authorizations t USING(admission_id) WHERE a.invocation_id=p.invocation_id)
 FROM provider_calls p JOIN invocations i USING(invocation_id) JOIN provider_call_usage u USING(provider_call_id) WHERE i.run_id=? ORDER BY p.invocation_id,p.call_ordinal`, run)
	if err != nil {
		return b, err
	}
	defer rows.Close()
	for rows.Next() {
		var amount *int64
		var terminal bool
		if err = rows.Scan(&amount, &terminal); err != nil {
			return b, err
		}
		if amount == nil {
			continue
		}
		target := &b.NormalUsed
		if terminal {
			target = &b.ContingencyUsed
		}
		*target, err = checkedAdd(*target, *amount)
		if err != nil {
			return b, err
		}
	}
	if err = rows.Err(); err != nil {
		return b, err
	}
	b.NormalAvailable, err = checkedSubtract(b.NormalBudget, b.NormalUsed)
	if err != nil {
		return b, err
	}
	if b.NormalAvailable < 0 {
		b.ContingencyUsed, err = checkedAdd(b.ContingencyUsed, -b.NormalAvailable)
		if err != nil {
			return b, err
		}
	}
	b.ContingencyAvailable, err = checkedSubtract(b.ContingencyBudget, b.ContingencyUsed)
	return b, err
}

func (s *StateStore) GetRunTokenBalance(ctx context.Context, run string) (_ RunTokenBalance, resultErr error) {
	defer func() { resultErr = numericFailure("GetRunTokenBalance", run, "", "", resultErr) }()
	if err := requireIDs(struct{ name, value string }{"run_id", run}); err != nil {
		return RunTokenBalance{}, err
	}
	// Policy and current usage are read from one statement snapshot in the view.
	var b RunTokenBalance
	resultErr = s.db.QueryRowContext(ctx, `SELECT normal_budget,normal_used,normal_available,contingency_budget,contingency_used,contingency_available,absolute_limit FROM v_run_token_balance WHERE run_id=?`, run).Scan(&b.NormalBudget, &b.NormalUsed, &b.NormalAvailable, &b.ContingencyBudget, &b.ContingencyUsed, &b.ContingencyAvailable, &b.AbsoluteLimit)
	return b, resultErr
}

func validateSequentialAgent(ctx context.Context, q querier, run, invocation string) error {
	var active int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_calls p JOIN invocations i USING(invocation_id) WHERE i.run_id=? AND p.duration_ns IS NULL AND p.invocation_id<>?`, run, invocation).Scan(&active); err != nil {
		return err
	}
	if active != 0 {
		return fmt.Errorf("another agent's ProviderCall remains physically unresolved")
	}
	return nil
}

func validateContingencyWork(ctx context.Context, c *sql.Conn, a InvocationAdmission) error {
	if a.Kind != "CONTAINMENT" || a.ContingencyReason == "" || a.Binding.Purpose != "RECOVER" || a.Fence.Owner.InvocationID != nil {
		return fmt.Errorf("no explicit minimal-terminal contingency authorization")
	}
	if err := validateAdmissionFence(ctx, c, a.RunID, a.Binding, a.Fence); err != nil {
		return err
	}
	return validateSequentialAgent(ctx, c, a.RunID, a.Binding.InvocationID)
}
