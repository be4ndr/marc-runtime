package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"marc-runtime/src/internal/statestore"
)

// PresentHardProposal records successful delivery only after the bounded exact
// question is written. Its justification is supplied by Lead, not invented here.
func (r *Runtime) PresentHardProposal(ctx context.Context, p statestore.HardLimitRequest, output io.Writer) error {
	old, _, err := r.store.WorkflowHardRequest(ctx, p.RunID, p.RequestID)
	if err == nil {
		p.PresentedAt = old.PresentedAt
		_, err = r.store.RecordHardLimitRequest(ctx, p, statestore.AccountingOptions{At: p.PresentedAt})
		return err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	limits, err := r.store.GetAgentTokenLimits(ctx, p.RunID, p.Grant.AdmissionID)
	if err != nil {
		return err
	}
	bank, err := r.store.GetRunTokenBalance(ctx, p.RunID)
	if err != nil {
		return err
	}
	contextBytes := marshal(struct {
		Run, Admission, LeadJustification           string
		KnownUsed, Hard, ProvisionalNormalAvailable int64
	}{p.RunID, p.Grant.AdmissionID, p.Justification, limits.Used, limits.HardLimit, bank.NormalAvailable})
	message := string(contextBytes) + "\n" + HardQuestion(p.Grant.Amount) + "\n"
	n, err := io.WriteString(output, message)
	if err != nil {
		return err
	}
	if n != len(message) {
		return io.ErrShortWrite
	}
	p.PresentedAt = r.at()
	_, err = r.store.RecordHardLimitRequest(ctx, p, statestore.AccountingOptions{At: p.PresentedAt})
	return err
}

// A nil response waits for the persisted deadline; restarting never renews it.
// Timeout authority applies only to the exact presented proposal, not the Run bank.
func (r *Runtime) DecideHardProposal(ctx context.Context, run, id string, h *statestore.HumanBudgetApproval) (statestore.HardLimitDecision, error) {
	var out statestore.HardLimitDecision
	at, err := r.store.WorkflowHardDecisionTime(ctx, run, id)
	if err == nil {
		return r.store.DecideHardLimitRequest(ctx, run, id, h, statestore.AccountingOptions{At: at})
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	_, deadline, err := r.store.WorkflowHardRequest(ctx, run, id)
	if err != nil {
		return out, err
	}
	if h == nil {
		until, err := time.Parse(time.RFC3339Nano, deadline)
		if err != nil {
			return out, err
		}
		timer := time.NewTimer(until.Sub(r.now()))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-timer.C:
		}
	}
	return r.store.DecideHardLimitRequest(ctx, run, id, h, statestore.AccountingOptions{At: r.at()})
}
func (r *Runtime) IncreaseBudget(ctx context.Context, run string, amount int64, h statestore.HumanBudgetApproval) (bool, error) {
	return r.store.IncreaseRunBudget(ctx, run, amount, h, statestore.AccountingOptions{At: r.at()})
}

func HardQuestion(amount int64) string {
	return fmt.Sprintf("Approve %d additional WORK tokens for this exact presented proposal? Respond within five minutes.", amount)
}
