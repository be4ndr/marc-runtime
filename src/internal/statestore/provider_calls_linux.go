package statestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"

	"ai-control-plane/internal/identity"

	"ai-control-plane/internal/numeric"
)

// ProviderCall identifies one physical upstream call. Register it before dispatch;
// retries and fallbacks require distinct identities. All registered calls contribute
// to invocation accounting, regardless of execution outcome. Calls are serial
// within an invocation; only an explicit controller finish permits the next call.
type ProviderCall struct {
	ProviderCallID  string
	RunID           string
	InvocationID    string
	Provider        string
	Model           string
	ReasoningEffort string
}

var errProviderCallAlreadyFinished = errors.New("ProviderCall already finished")

// FinishProviderCall records the controller's observation that this physical
// request ended, using elapsed nanoseconds measured by its owner with a monotonic
// clock. opts.At is audit metadata, never a finish timestamp or a duration source.
// The first accepted duration is immutable; repeated completion is a no-op.
// Completion neither changes usage knowledge nor proves retries/fallbacks ended.
func (s *StateStore) FinishProviderCall(ctx context.Context, run, id string, durationNS int64, opts AccountingOptions) (resultErr error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"provider_call_id", id}); err != nil {
		return err
	}
	defer func() { resultErr = numericFailure("FinishProviderCall", run, "", id, resultErr) }()
	// MaxInt64 is time.Time.Sub's positive saturation sentinel. Reject it rather than
	// accepting an unrepresentable elapsed interval as an exact measurement.
	if durationNS < 0 || durationNS == math.MaxInt64 {
		return numeric.Invalid("duration_ns", "invalid_input", "invalid ProviderCall monotonic duration")
	}
	if err := accountingTime(opts.At); err != nil {
		return err
	}
	err := s.numericMutation(ctx, "FinishProviderCall", run, opts.Audit, func(c *sql.Conn) error {
		var duration *int64
		if err := c.QueryRowContext(ctx, `SELECT p.duration_ns FROM provider_calls p JOIN invocations i USING(invocation_id)
 WHERE p.provider_call_id=? AND i.run_id=?`, id, run).Scan(&duration); err != nil {
			return fmt.Errorf("ProviderCall does not belong to run: %w", err)
		}
		if duration != nil {
			return errProviderCallAlreadyFinished
		}
		_, err := c.ExecContext(ctx, `UPDATE provider_calls SET duration_ns=? WHERE provider_call_id=?`, durationNS, id)
		return err
	})
	if errors.Is(err, errProviderCallAlreadyFinished) {
		return nil
	}
	return err
}

// CreateProviderCall registers physical-call identity before dispatch. opts.At
// supplies observational started_at; ordinal and the unresolved position determine
// sequencing independently of that wall-clock metadata.
func (s *StateStore) CreateProviderCall(ctx context.Context, call ProviderCall, opts AccountingOptions) (resultErr error) {
	defer func() {
		resultErr = numericFailure("CreateProviderCall", call.RunID, call.InvocationID, call.ProviderCallID, resultErr)
	}()
	for _, v := range []string{call.ProviderCallID, call.RunID, call.InvocationID, call.Provider, call.Model} {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("invalid ProviderCall identity/provenance")
		}
	}
	if err := accountingTime(opts.At); err != nil {
		return err
	}
	return s.numericMutation(ctx, "CreateProviderCall", call.RunID, opts.Audit, func(c *sql.Conn) error {

		configured, err := policyExists(ctx, c, call.RunID)
		if err != nil {
			return err
		}
		if configured {
			return fmt.Errorf("budgeted Run requires ClaimAdmissionDispatch")
		}

		return insertProviderCall(ctx, c, call, opts.At)
	})
}

func insertProviderCall(ctx context.Context, c *sql.Conn, call ProviderCall, at string) error {
	if err := requireIDs(
		struct{ name, value string }{"provider_call_id", call.ProviderCallID},
		struct{ name, value string }{"run_id", call.RunID},
		struct{ name, value string }{"invocation_id", call.InvocationID},
	); err != nil {
		return err
	}
	for name, value := range map[string]string{"provider": call.Provider, "model": call.Model, "reasoning_effort": call.ReasoningEffort} {
		if err := identity.CheckText(name, value); err != nil {
			return err
		}
	}
	if strings.TrimSpace(call.Provider) == "" || strings.TrimSpace(call.Model) == "" {
		return fmt.Errorf("ProviderCall requires provider and model")
	}
	var belongs int
	if err := c.QueryRowContext(ctx, `SELECT count(*) FROM invocations WHERE invocation_id=? AND run_id=?`, call.InvocationID, call.RunID).Scan(&belongs); err != nil {
		return err
	}
	if belongs != 1 {
		return fmt.Errorf("ProviderCall invocation does not belong to run")
	}
	var ordinal int64
	if err := c.QueryRowContext(ctx, `SELECT COALESCE(MAX(call_ordinal),0) FROM provider_calls WHERE invocation_id=?`, call.InvocationID).Scan(&ordinal); err != nil {
		return err
	}
	ordinal, err := checkedNext(ordinal, "provider_call_ordinal")
	if err != nil {
		return err
	}
	if err := nativeOrdinal(ordinal, "provider_call_ordinal"); err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, `INSERT INTO provider_calls(provider_call_id,invocation_id,call_ordinal,provider,model,reasoning_effort,started_at)
   VALUES (?,?,?,?,?,?,?)`, call.ProviderCallID, call.InvocationID, ordinal, call.Provider, call.Model, call.ReasoningEffort, at)
	return err
}

const currentUsageEvidenceColumns = `input_receipt_id,cache_receipt_id,fresh_receipt_id,output_receipt_id,reasoning_receipt_id,work_receipt_id,spend_receipt_id`

const currentUsageColumns = `receipt_state,input_amount,input_state,cache_amount,cache_state,fresh_amount,fresh_state,output_amount,output_state,reasoning_amount,reasoning_state,work_amount,work_state,spend_amount,spend_state`

func scanCurrentUsage(scan func(...any) error, u *usageSummary, prefix ...any) error {
	args := append(prefix, &u.ReceiptState)
	for _, v := range usageValues(u) {
		args = append(args, &v.Value, &v.State)
	}
	return scan(args...)
}

// A counter omission does not erase established evidence. Derived amounts retain
// their own coherent observation; operands from separate receipts are never mixed.
// Only evidence supplied by the accepted final itself can become final knowledge.
func advanceProviderCallUsage(ctx context.Context, c *sql.Conn, r UsageReceipt) error {
	incoming, err := usageFromReceipt(r)
	if err != nil {
		return err
	}
	var old usageSummary
	var revision int64
	var evidence [7]*string
	row := c.QueryRowContext(ctx, `SELECT revision,`+currentUsageEvidenceColumns+`,`+currentUsageColumns+` FROM provider_call_usage WHERE provider_call_id=?`, r.ProviderCallID)
	prefix := []any{&revision}
	for n := range evidence {
		prefix = append(prefix, &evidence[n])
	}
	if err := scanCurrentUsage(row.Scan, &old, prefix...); err != nil {
		return err
	}
	if old.ReceiptState == "final" {
		return fmt.Errorf("ProviderCall receipt sequence is closed by FINAL")
	}
	revision, err = checkedNext(revision, "provider_call_usage_revision")
	if err != nil {
		return err
	}
	args := []any{r.ReceiptID, r.Completeness, r.ReportedAt, revision}
	for n, v := range usageValues(&incoming) {
		prior := usageValues(&old)[n]
		if v.Value != nil {
			evidence[n] = &r.ReceiptID
		} else {
			v.Value = prior.Value
			if v.Value != nil || r.Completeness == "partial" || prior.State == "partial" {
				v.State = "partial"
			} else {
				// No amount/evidence for this dimension. A known sibling
				// does not turn an unknown receipt's missing operand into
				// partial evidence for a derived amount (SQL enforces this).
				v.State = "unknown"
			}
		}
		args = append(args, v.Value, v.State, evidence[n])
	}
	args = append(args, r.ProviderCallID)
	result, err := c.ExecContext(ctx, `UPDATE provider_call_usage SET receipt_id=?,receipt_state=?,reported_at=?,revision=?,
 input_amount=?,input_state=?,input_receipt_id=?,cache_amount=?,cache_state=?,cache_receipt_id=?,fresh_amount=?,fresh_state=?,fresh_receipt_id=?,
 output_amount=?,output_state=?,output_receipt_id=?,reasoning_amount=?,reasoning_state=?,reasoning_receipt_id=?,
 work_amount=?,work_state=?,work_receipt_id=?,spend_amount=?,spend_state=?,spend_receipt_id=? WHERE provider_call_id=?`, args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("current ProviderCall usage was not advanced")
	}
	return nil
}

func invocationCurrentDimension(ctx context.Context, q querier, invocation, resource string) (usageValue, error) {
	columns := "work_state,work_amount"
	if resource == "SPEND" {
		columns = "spend_state,spend_amount"
	} else if resource != "WORK" {
		return usageValue{}, fmt.Errorf("invalid usage resource %s", resource)
	}
	rows, err := q.QueryContext(ctx, `SELECT `+columns+` FROM provider_calls p LEFT JOIN provider_call_usage c USING(provider_call_id) WHERE p.invocation_id=? ORDER BY p.call_ordinal`, invocation)
	if err != nil {
		return usageValue{}, err
	}
	defer rows.Close()
	var children []usageValue
	for rows.Next() {
		var a usageValue
		if err := rows.Scan(&a.State, &a.Value); err != nil {
			return usageValue{}, err
		}
		children = append(children, a)
	}
	if err := rows.Err(); err != nil {
		return usageValue{}, err
	}
	return aggregateDimension(children)
}
