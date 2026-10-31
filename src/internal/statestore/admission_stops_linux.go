package statestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type RunHardStop struct {
	StopID, RunID, Cause, SourceID, Classification, CreatedAt string
	ResourceType                                              *string
	ObservedAmount, ApplicableLimit                           *int64
}

func stopIdentity(run, cause, source string) string {
	return admissionDigest([]string{run, cause, source})
}
func persistHardStop(ctx context.Context, c *sql.Conn, run, resource, cause, source string, amount, limit *int64, at string) error {
	id := stopIdentity(run, cause, source)
	var r *string
	if resource != "" {
		r = &resource
	}
	result, err := c.ExecContext(ctx, `INSERT INTO run_hard_stops VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(run_id,cause,source_id) DO NOTHING`, id, run, r, cause, source, amount, limit, at)
	// Exact values are checked before persistence.
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var identical bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_hard_stops WHERE stop_id=? AND run_id=? AND resource_type IS ? AND cause=? AND source_id=? AND observed_amount IS ? AND applicable_limit IS ? AND created_at=?)`, id, run, r, cause, source, amount, limit, at).Scan(&identical); err != nil {
			return err
		}
		if !identical {
			return fmt.Errorf("conflicting immutable hard-stop facts")
		}
		return nil
	}
	if _, err = c.ExecContext(ctx, `INSERT INTO hard_stop_decisions VALUES(?,0,'PENDING',?)`, id, at); err != nil {
		return err
	}
	return snapshotStopUsage(ctx, c, id, 0, run)
}
func resolveHardStop(ctx context.Context, c *sql.Conn, id, classification, at string) error {
	var run string
	if err := c.QueryRowContext(ctx, `SELECT run_id FROM v_run_token_hard_stops WHERE stop_id=?`, id).Scan(&run); err != nil {
		return err
	}
	result, err := c.ExecContext(ctx, `INSERT INTO hard_stop_decisions VALUES(?,1,?,?) ON CONFLICT(stop_id,ordinal) DO NOTHING`, id, classification, at)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var old, recordedAt string
		if err := c.QueryRowContext(ctx, `SELECT classification,decided_at FROM hard_stop_decisions WHERE stop_id=? AND ordinal=1`, id).Scan(&old, &recordedAt); err != nil {
			return err
		}
		if old != classification || recordedAt != at {
			return fmt.Errorf("conflicting immutable hard-stop classification")
		}
		return nil
	}
	return snapshotStopUsage(ctx, c, id, 1, run)
}

// Reconciliation observes current usage; it is not a replay of the first
// immutable observation. Late usage commits without replacing original facts.
func observeHardStop(ctx context.Context, c *sql.Conn, run, resource, cause, source string, amount, limit *int64, at string) error {
	var exists bool
	if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_hard_stops WHERE run_id=? AND cause=? AND source_id=?)`, run, cause, source).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	return persistHardStop(ctx, c, run, resource, cause, source, amount, limit, at)
}

func resolveObservedHardStop(ctx context.Context, c *sql.Conn, id, classification, at string) error {
	var old string
	err := c.QueryRowContext(ctx, `SELECT classification FROM hard_stop_decisions WHERE stop_id=? AND ordinal=1`, id).Scan(&old)
	if err == nil {
		return nil // Final decisions remain immutable even when later evidence changes.
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return resolveHardStop(ctx, c, id, classification, at)
}
func snapshotStopUsage(ctx context.Context, c *sql.Conn, id string, ordinal int, run string) error {
	for _, r := range []string{"WORK", "SPEND", "INPUT_TOKENS", "CACHED_INPUT_TOKENS", "FRESH_INPUT_TOKENS", "OUTPUT_TOKENS", "REASONING_TOKENS"} {
		col := map[string]string{"WORK": "work", "SPEND": "spend", "INPUT_TOKENS": "input", "CACHED_INPUT_TOKENS": "cache", "FRESH_INPUT_TOKENS": "fresh", "OUTPUT_TOKENS": "output", "REASONING_TOKENS": "reasoning"}[r]
		_, err := c.ExecContext(ctx, `INSERT INTO hard_stop_usage_evidence
   SELECT ?,?,p.provider_call_id,?,u.revision,u.`+col+`_receipt_id,u.`+col+`_amount,u.`+col+`_state,d.claim_id,b.amount
   FROM provider_calls p JOIN invocations i USING(invocation_id) JOIN provider_call_usage u USING(provider_call_id)
   LEFT JOIN admission_dispatch_claims d USING(provider_call_id)
   LEFT JOIN admission_dispatch_bounds b ON b.claim_id=d.claim_id AND b.dimension=? WHERE i.run_id=?`, id, ordinal, r, r, run)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StateStore) ListRunHardStops(ctx context.Context, run string) ([]RunHardStop, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT s.stop_id,s.run_id,s.cause,s.source_id,s.resource_type,s.observed_amount,s.applicable_limit,s.created_at,
 COALESCE(f.classification,p.classification) FROM v_run_token_hard_stops s JOIN hard_stop_decisions p ON p.stop_id=s.stop_id AND p.ordinal=0
 LEFT JOIN hard_stop_decisions f ON f.stop_id=s.stop_id AND f.ordinal=1 WHERE s.run_id=? AND (s.resource_type IS NULL OR s.resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) ORDER BY s.stop_id`, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunHardStop{}
	for rows.Next() {
		var s RunHardStop
		if err = rows.Scan(&s.StopID, &s.RunID, &s.Cause, &s.SourceID, &s.ResourceType, &s.ObservedAmount, &s.ApplicableLimit, &s.CreatedAt, &s.Classification); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Runs with no policy retain historical evidence/mechanics, but cannot receive
// admission or a dispatch claim. Every budgeted mutation validates Run arithmetic
// and commits newly observed policy violations together with the valid evidence.
func refreshRunAdmission(ctx context.Context, c *sql.Conn, run string) error {
	configured, err := policyExists(ctx, c, run)
	if err != nil || !configured {
		return err
	}
	at, err := canonicalTime(time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	bank, err := runTokenBalance(ctx, c, run)
	if err != nil {
		return err
	}
	if bank.NormalUsed > bank.NormalBudget {
		if err = observeHardStop(ctx, c, run, "WORK", "BUDGET_EXCEEDED", "bank:normal", &bank.NormalUsed, &bank.NormalBudget, at); err != nil {
			return err
		}
	}
	if bank.ContingencyAvailable < 0 {
		if err = observeHardStop(ctx, c, run, "WORK", "BUDGET_EXCEEDED", "bank:contingency", &bank.ContingencyUsed, &bank.ContingencyBudget, at); err != nil {
			return err
		}
	}
	// Contingency can be exhausted while unused normal capacity remains. Only
	// total confirmed WORK beyond the combined ceiling proves an absolute stop.
	overrun := int64(0)
	if bank.NormalAvailable < 0 {
		overrun = -bank.NormalAvailable
	}
	terminal, err := checkedSubtract(bank.ContingencyUsed, overrun)
	if err != nil {
		return err
	}
	total, err := checkedAdd(bank.NormalUsed, terminal)
	if err != nil {
		return err
	}
	if total > bank.AbsoluteLimit {
		// A pre-017 bank:absolute row may contain an invalid original snapshot.
		// Use fresh evidence identity instead of overwriting it or deduplicating
		// this real ceiling violation against that historical row.
		if err = observeHardStop(ctx, c, run, "WORK", "BUDGET_EXCEEDED", "bank:absolute:confirmed", &total, &bank.AbsoluteLimit, at); err != nil {
			return err
		}
	}
	if err := refreshAgentTokenStops(ctx, c, run, at); err != nil {
		return err
	}

	// Compare only exact dimension matches. Output bounds never stand in for WORK.
	columns := map[string]string{"WORK": "work", "INPUT_TOKENS": "input", "CACHED_INPUT_TOKENS": "cache", "OUTPUT_TOKENS": "output", "REASONING_TOKENS": "reasoning"}
	for _, dimension := range []string{"WORK", "INPUT_TOKENS", "CACHED_INPUT_TOKENS", "OUTPUT_TOKENS", "REASONING_TOKENS"} {
		col := columns[dimension]
		components := "NULL,NULL"
		if dimension == "WORK" {
			components = "u.input_amount,u.output_amount"
		}
		rows, err := c.QueryContext(ctx, `SELECT d.claim_id,u.`+col+`_amount,b.amount,`+components+` FROM admission_dispatch_claims d
   JOIN invocation_admissions a USING(admission_id) JOIN admission_dispatch_bounds b USING(claim_id)
   JOIN provider_call_usage u USING(provider_call_id) WHERE a.run_id=? AND b.dimension=? ORDER BY d.claim_id`, run, dimension)
		if err != nil {
			return err
		}
		type violation struct {
			claim         string
			amount, bound int64
		}
		var violations []violation
		for rows.Next() {
			var v violation
			var amount, left, right *int64
			if err = rows.Scan(&v.claim, &amount, &v.bound, &left, &right); err != nil {
				rows.Close()
				return err
			}
			amount = knownResourceMinimum(amount, left, right)
			if amount != nil && *amount > v.bound {
				v.amount = *amount
				violations = append(violations, v)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, v := range violations {
			source := v.claim + ":" + dimension
			if err = observeHardStop(ctx, c, run, dimension, "DISPATCH_BOUND_EXCEEDED", source, &v.amount, &v.bound, at); err != nil {
				return err
			}
			classification, err := observedBoundClassification(ctx, c, run)
			if err != nil {
				return err
			}
			if err = resolveObservedHardStop(ctx, c, stopIdentity(run, "DISPATCH_BOUND_EXCEEDED", source), classification, at); err != nil {
				return err
			}
		}
	}
	// Resolve only budget stops that have sufficient dimension-specific evidence.
	rows, err := c.QueryContext(ctx, `SELECT stop_id,resource_type FROM v_run_token_hard_stops s WHERE s.run_id=? AND (s.resource_type IS NULL OR s.resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) AND s.cause='BUDGET_EXCEEDED' AND NOT EXISTS(SELECT 1 FROM hard_stop_decisions d WHERE d.stop_id=s.stop_id AND d.ordinal=1) ORDER BY stop_id`, run)
	if err != nil {
		return err
	}
	type pending struct{ id, resource string }
	var pendingStops []pending
	for rows.Next() {
		var p pending
		if err = rows.Scan(&p.id, &p.resource); err != nil {
			rows.Close()
			return err
		}
		pendingStops = append(pendingStops, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range pendingStops {
		classification, err := classifyBudgetOverrun(ctx, c, run, p.resource)
		if err != nil {
			return err
		}
		if classification != "PENDING" {
			if err = resolveObservedHardStop(ctx, c, p.id, classification, at); err != nil {
				return err
			}
		}
	}
	return resolveSupportedPending(ctx, c, run, at)
}

func classifyBudgetOverrun(ctx context.Context, q querier, run, resource string) (string, error) {
	bug, err := controllerBugProved(ctx, q, run)
	if err != nil {
		return "", err
	}
	if bug {
		return "CONTROLLER_ADMISSION_BUG", nil
	}
	if resource != "WORK" {
		return "PENDING", nil
	}
	col := strings.ToLower(resource)
	rows, err := q.QueryContext(ctx, `SELECT u.`+col+`_amount,u.`+col+`_state,b.amount,d.claim_id FROM provider_calls p
  JOIN invocations i USING(invocation_id) JOIN provider_call_usage u USING(provider_call_id)
  LEFT JOIN admission_dispatch_claims d USING(provider_call_id)
  LEFT JOIN admission_dispatch_bounds b ON b.claim_id=d.claim_id AND b.dimension=? WHERE i.run_id=? ORDER BY p.invocation_id,p.call_ordinal`, resource, run)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	complete, exceeded := true, false
	var sum int64
	anyEvidence := false
	for rows.Next() {
		var actual, bound *int64
		var state string
		var claim *string
		if err = rows.Scan(&actual, &state, &bound, &claim); err != nil {
			return "", err
		}
		anyEvidence = true
		if actual != nil && (claim == nil || bound == nil) {
			complete = false
		}
		if actual != nil {
			sum, err = checkedAdd(sum, *actual)
			if err != nil {
				return "", err
			}
			if bound != nil && *actual > *bound && claim != nil {
				exceeded = true
			}
		}
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if exceeded {
		return "OBSERVED_DISPATCH_BOUND_EXCEEDED", nil
	}
	var hard int64
	if err = q.QueryRowContext(ctx, `SELECT hard_limit FROM run_resource_budgets WHERE run_id=? AND resource_type=?`, run, resource).Scan(&hard); err != nil {
		return "", err
	}
	var observedPressure, observedBoundExceeded int
	if err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM v_run_token_hard_stops WHERE (resource_type IS NULL OR resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) AND run_id=? AND resource_type=? AND cause IN ('BUDGET_EXCEEDED','UNPROVEN_CONTINUATION') AND source_id NOT LIKE 'agent:%' AND source_id NOT LIKE 'agent-hard:%' AND observed_amount>applicable_limit)`, run, resource).Scan(&observedPressure); err != nil {
		return "", err
	}
	if err = q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM hard_stop_usage_evidence e JOIN v_run_token_hard_stops s USING(stop_id) WHERE s.run_id=? AND (s.resource_type IS NULL OR s.resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) AND e.resource_type=? AND e.ordinal=0 AND e.amount>e.dispatch_bound)
 OR EXISTS(SELECT 1 FROM v_run_token_hard_stops WHERE (resource_type IS NULL OR resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) AND run_id=? AND resource_type=? AND cause='DISPATCH_BOUND_EXCEEDED' AND observed_amount>applicable_limit)`, run, resource, run, resource).Scan(&observedBoundExceeded); err != nil {
		return "", err
	}
	if observedBoundExceeded != 0 {
		return "OBSERVED_DISPATCH_BOUND_EXCEEDED", nil
	}
	if anyEvidence && complete && (sum > hard || observedPressure != 0) {
		return "BOUNDED_TERMINAL_OVERRUN", nil
	}
	return "PENDING", nil
}

func validateAdmissionDatabase(ctx context.Context, q querier) error {
	rows, err := q.QueryContext(ctx, `SELECT run_id FROM run_admission_policy ORDER BY run_id`)
	if err != nil {
		return err
	}
	var runs []string
	for rows.Next() {
		var run string
		if err = rows.Scan(&run); err != nil {
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
		var dimensions int
		if err = q.QueryRowContext(ctx, `SELECT count(*) FROM run_resource_budgets WHERE run_id=? AND resource_type='WORK'`, run).Scan(&dimensions); err != nil {
			return err
		}
		if dimensions != len(budgetResources) {
			return fmt.Errorf("Run requires one finite durable WORK budget")
		}
		for _, r := range budgetResources {
			if _, err = admissionCharge(ctx, q, run, r); err != nil {
				return fmt.Errorf("Run admission recovery validation: %w", err)
			}
		}
	}
	return nil
}

// Budget-policy upgrades repair enforcement from current confirmed usage in the
// same durable transaction as the schema upgrade, leaving evidence untouched.
func reconcileTokenBankStops(ctx context.Context, c *sql.Conn) error {
	rows, err := c.QueryContext(ctx, `SELECT run_id FROM run_admission_policy ORDER BY run_id`)
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
		if err := refreshRunAdmission(ctx, c, run); err != nil {
			return err
		}
	}
	return nil
}

// Resolving PENDING adds evidence; it never replaces or deletes a prior decision.
func resolveSupportedPending(ctx context.Context, c *sql.Conn, run, at string) error {
	bug, err := controllerBugProved(ctx, c, run)
	if err != nil {
		return err
	}
	if bug {
		rows, err := c.QueryContext(ctx, `SELECT s.stop_id FROM v_run_token_hard_stops s WHERE s.run_id=? AND (s.resource_type IS NULL OR s.resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) AND NOT EXISTS(SELECT 1 FROM hard_stop_decisions d WHERE d.stop_id=s.stop_id AND d.ordinal=1) ORDER BY s.stop_id`, run)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
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
			if err = resolveObservedHardStop(ctx, c, id, "CONTROLLER_ADMISSION_BUG", at); err != nil {
				return err
			}
		}
		return nil
	}
	rows, err := c.QueryContext(ctx, `SELECT s.stop_id,s.resource_type FROM v_run_token_hard_stops s WHERE s.run_id=? AND (s.resource_type IS NULL OR s.resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) AND s.cause='UNPROVEN_CONTINUATION' AND s.resource_type IS NOT NULL AND NOT EXISTS(SELECT 1 FROM hard_stop_decisions d WHERE d.stop_id=s.stop_id AND d.ordinal=1) ORDER BY s.stop_id`, run)
	if err != nil {
		return err
	}
	type pending struct{ id, resource string }
	var stops []pending
	for rows.Next() {
		var p pending
		if err = rows.Scan(&p.id, &p.resource); err != nil {
			rows.Close()
			return err
		}
		stops = append(stops, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range stops {
		classification, err := classifyBudgetOverrun(ctx, c, run, p.resource)
		if err != nil {
			return err
		}
		if classification != "PENDING" {
			if err = resolveObservedHardStop(ctx, c, p.id, classification, at); err != nil {
				return err
			}
		}
	}
	return nil
}

func controllerBugProved(ctx context.Context, q querier, run string) (bool, error) {
	var exists int
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM v_run_token_hard_stops s JOIN hard_stop_decisions d USING(stop_id) WHERE s.run_id=? AND (s.resource_type IS NULL OR s.resource_type IN ('WORK','INPUT_TOKENS','CACHED_INPUT_TOKENS','FRESH_INPUT_TOKENS','OUTPUT_TOKENS','REASONING_TOKENS')) AND s.cause='CONTROLLER_RELEASE_VIOLATION' AND d.ordinal=1 AND d.classification='CONTROLLER_ADMISSION_BUG')`, run).Scan(&exists)
	return exists != 0, err
}
func observedBoundClassification(ctx context.Context, q querier, run string) (string, error) {
	bug, err := controllerBugProved(ctx, q, run)
	if err != nil {
		return "", err
	}
	if bug {
		return "CONTROLLER_ADMISSION_BUG", nil
	}
	return "OBSERVED_DISPATCH_BOUND_EXCEEDED", nil
}
