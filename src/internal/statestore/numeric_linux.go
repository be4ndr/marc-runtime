package statestore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"ai-control-plane/internal/numeric"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var usageDomains = []string{"input_tokens", "cached_input_tokens", "fresh_input_tokens", "output_tokens", "reasoning_tokens", "WORK", "SPEND"}

func canonicalTime(at string) (string, error) {
	t, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return "", err
	}
	t = t.UTC()
	if t.Year() < 0 || t.Year() > 9999 {
		return "", numeric.Invalid("timestamp", "invalid_input", "canonical RFC3339 year is unrepresentable")
	}
	return t.Format(time.RFC3339Nano), nil
}

func nativeOrdinal(value int64, domain string) error {
	if value < 1 || strconv.IntSize == 32 && value > math.MaxInt32 {
		return numeric.Invalid(domain, "unrepresentable", "ordinal does not fit the active control-plane representation")
	}
	return nil
}

// Nonnegative run totals bound every invocation/role/cycle subtotal. Read current
// usage directly, never receipts or projection data. Checks happen under the same
// writer lock both before and after a mutation, so corrupt state cannot be repaired
// by an otherwise valid mutation. Missing counters remain missing.
func validateRunUsageNumbers(ctx context.Context, q querier, run string) error {
	rows, err := q.QueryContext(ctx, `SELECT i.execution_id,i.attempt_id,i.invocation_id,p.provider_call_id,
 i.cycle_no,a.attempt_ordinal,i.invocation_ordinal,p.call_ordinal,`+currentUsageColumns+`
 FROM provider_calls p JOIN invocations i USING(invocation_id) JOIN attempts a ON a.attempt_id=i.attempt_id
 LEFT JOIN provider_call_usage c USING(provider_call_id) WHERE i.run_id=? ORDER BY p.invocation_id,p.call_ordinal`, run)
	if err != nil {
		return err
	}
	defer rows.Close()
	var sums [7]int64
	for rows.Next() {
		c := numeric.Context{RunID: run}
		var ordinals [4]int64
		var usage usageSummary
		if err := scanCurrentUsage(rows.Scan, &usage, &c.ExecutionID, &c.AttemptID, &c.InvocationID, &c.ProviderCallID,
			&ordinals[0], &ordinals[1], &ordinals[2], &ordinals[3]); err != nil {
			return numeric.WithContext(numeric.Invalid("current_usage", "corrupt_state", "current usage cannot be read exactly"), c)
		}
		for n, domain := range []string{"cycle", "attempt_ordinal", "invocation_ordinal", "provider_call_ordinal"} {
			if err := nativeOrdinal(ordinals[n], domain); err != nil {
				return numeric.WithContext(err, c)
			}
		}
		for n, value := range usageValues(&usage) {
			if _, err := aggregateDimension([]usageValue{*value}); err != nil {
				return numeric.WithContext(err, withDomain(c, usageDomains[n]))
			}
			if value.Value != nil {
				sums[n], err = checkedAdd(sums[n], *value.Value)
				if err != nil {
					return numeric.WithContext(err, withDomain(c, usageDomains[n]))
				}
			}
		}
	}
	return rows.Err()
}

func withDomain(c numeric.Context, domain string) numeric.Context { c.Domain = domain; return c }

func validateRunBalances(ctx context.Context, q querier, run string) error {
	configured, err := policyExists(ctx, q, run)
	if err != nil {
		return err
	}
	if configured {
		_, err := runTokenBalance(ctx, q, run)
		return err
	}
	return nil
}

func validateRunNumbers(ctx context.Context, q querier, run string) error {
	// These fields are scanned into native int by current-state/projection APIs.
	// Check before they can cross that representation boundary, even if an
	// invocation does not yet have a ProviderCall.
	rows, err := q.QueryContext(ctx, `SELECT 'cycle',current_cycle FROM runs WHERE run_id=?
        UNION ALL SELECT 'cycle',cycle_no FROM cycles WHERE run_id=?
        UNION ALL SELECT 'attempt_ordinal',attempt_ordinal FROM attempts WHERE run_id=?
        UNION ALL SELECT 'invocation_ordinal',invocation_ordinal FROM invocations WHERE run_id=?`, run, run, run, run)
	if err != nil {
		return err
	}
	for rows.Next() {
		var domain string
		var value int64
		if err := rows.Scan(&domain, &value); err != nil {
			rows.Close()
			return err
		}
		if err := nativeOrdinal(value, domain); err != nil {
			rows.Close()
			return numeric.WithContext(err, numeric.Context{RunID: run})
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if err := validateRunUsageNumbers(ctx, q, run); err != nil {
		return err
	}
	return validateRunBalances(ctx, q, run)
}

// Validate within the migration transaction: invalid historical arithmetic leaves
// both data and pending schema/history changes unchanged, including on restart.
func validateNumericDatabase(ctx context.Context, q querier) error {
	rows, err := q.QueryContext(ctx, `SELECT run_id FROM runs ORDER BY run_id`)
	if err != nil {
		return err
	}
	var runs []string
	for rows.Next() {
		var run string
		if err := rows.Scan(&run); err != nil {
			rows.Close()
			return err
		}
		runs = append(runs, run)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, run := range runs {
		if err := validateRunNumbers(ctx, q, run); err != nil {
			return numeric.WithContext(err, numeric.Context{Operation: "validate_database", RunID: run})
		}
	}
	rows, err = q.QueryContext(ctx, `SELECT i.run_id,i.execution_id,i.attempt_id,u.invocation_id,u.provider_call_id,u.completeness,u.input_tokens,u.cached_input_tokens,u.output_tokens,u.reasoning_tokens,u.receipt_id,u.reported_at,u.work_tokens,u.spend_tokens FROM usage_receipts u JOIN invocations i USING(invocation_id)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r UsageReceipt
		var c numeric.Context
		var work, spend *int64
		if err := rows.Scan(&c.RunID, &c.ExecutionID, &c.AttemptID, &r.InvocationID, &r.ProviderCallID, &r.Completeness, &r.InputTokens, &r.CachedInputTokens, &r.OutputTokens, &r.ReasoningTokens, &r.ReceiptID, &r.ReportedAt, &work, &spend); err != nil {
			return err
		}
		c.Operation = "validate_database"
		c.InvocationID = r.InvocationID
		c.ProviderCallID = r.ProviderCallID
		r.RunID = c.RunID
		_, w, sp, err := normalizeReceipt(r)
		if err != nil {
			return numeric.WithContext(err, c)
		}
		if !reflect.DeepEqual(w, work) || !reflect.DeepEqual(sp, spend) {
			return numeric.WithContext(numeric.Invalid("receipt_usage", "corrupt_state", "stored receipt derivation disagrees with exact operands"), c)
		}
	}
	return rows.Err()
}

func sqlNumericError(err error) error {
	var n *numeric.Error
	if errors.As(err, &n) {
		return err
	}
	var dbErr *sqlite.Error
	if !errors.As(err, &dbErr) {
		return err
	}
	// SQLite has no dedicated integer-overflow result code. The pinned driver
	// exposes Code and a formatted diagnostic, not the raw sqlite3_errmsg. Match
	// only known diagnostics on the actual driver error, never an outer message.
	var messages []string
	var prefix string
	switch dbErr.Code() {
	case sqlite3.SQLITE_ERROR:
		prefix = "SQL logic error: "
		messages = []string{
			"integer overflow",
		}
	case sqlite3.SQLITE_CONSTRAINT_TRIGGER:
		prefix = "constraint failed: "
		messages = []string{
			"numeric invariant violation: ownership epoch exhausted",
			"numeric invariant violation: actor revision exhausted",
			"numeric invariant violation: semantic revision exhausted",
			"numeric invariant violation: negative usage counter",
			"numeric invariant violation: WORK unrepresentable",
			"numeric invariant violation: current usage revision exhausted",
		}
	default:
		return err
	}
	for _, message := range messages {
		if dbErr.Error() == fmt.Sprintf("%s%s (%d)", prefix, message, dbErr.Code()) {
			// Multiple %w operands retain the entire input tree and driver identity.
			return fmt.Errorf("%w: %w", numeric.Invalid("sqlite_integer", "corrupt_state", "SQL numeric guard rejected state"), err)
		}
	}
	return err
}

func numericFailure(operation, run, invocation, call string, err error) error {
	return numeric.WithContext(sqlNumericError(err), numeric.Context{Operation: operation, RunID: run, InvocationID: invocation, ProviderCallID: call})
}
