package statestore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"marc-runtime/internal/identity"
)

// UsageReceipt is the typed persistence input. AcceptUsageReceiptJSON validates
// the P1.7 adapter wire and maps it to this input.
// Counters are nullable cumulative provider observations. Provider/model provenance
// resolves through ProviderCallID. StateStore derives WORK/SPEND, never the caller.
type UsageReceipt struct {
	ReceiptID         string
	RunID             string
	InvocationID      string
	ProviderCallID    string
	ProviderReceiptID *string
	Completeness      string
	InputTokens       *int64
	CachedInputTokens *int64
	OutputTokens      *int64
	ReasoningTokens   *int64
	ReportedAt        string
	ProviderRaw       json.RawMessage
}

var errDuplicateUsage = errors.New("identical immutable usage receipt")

func normalizeReceipt(r UsageReceipt) (UsageReceipt, *int64, *int64, error) {
	if err := requireIDs(
		struct{ name, value string }{"receipt_id", r.ReceiptID},
		struct{ name, value string }{"run_id", r.RunID},
		struct{ name, value string }{"invocation_id", r.InvocationID},
		struct{ name, value string }{"provider_call_id", r.ProviderCallID},
	); err != nil {
		return r, nil, nil, err
	}
	if r.ProviderReceiptID != nil {
		if err := identity.CheckText("provider_receipt_id", *r.ProviderReceiptID); err != nil {
			return r, nil, nil, err
		}
	}
	if err := identity.CheckText("reported_at", r.ReportedAt); err != nil {
		return r, nil, nil, err
	}
	var work, spend *int64
	if strings.TrimSpace(r.ReceiptID) == "" || strings.TrimSpace(r.RunID) == "" || strings.TrimSpace(r.InvocationID) == "" || strings.TrimSpace(r.ProviderCallID) == "" ||
		r.Completeness != "unknown" && r.Completeness != "partial" && r.Completeness != "final" {
		return r, nil, nil, fmt.Errorf("invalid usage receipt identity/completeness")
	}
	canonical, err := canonicalTime(r.ReportedAt)
	if err != nil {
		return r, nil, nil, fmt.Errorf("usage reported time: %w", err)
	}
	r.ReportedAt = canonical
	observed, err := usageFromReceipt(r)
	if err != nil {
		return r, nil, nil, err
	}
	if len(r.ProviderRaw) > 0 {
		if err := identity.CheckJSONEnvelope(r.ProviderRaw); err != nil {
			return r, nil, nil, fmt.Errorf("provider metadata strings: %w", err)
		}
		decoder := json.NewDecoder(bytes.NewReader(r.ProviderRaw))
		decoder.UseNumber()
		var raw any
		if err := decoder.Decode(&raw); err != nil {
			return r, nil, nil, fmt.Errorf("invalid provider metadata: %w", err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return r, nil, nil, fmt.Errorf("trailing provider metadata")
		}
		r.ProviderRaw, err = json.Marshal(raw)
		if err != nil {
			return r, nil, nil, err
		}
	} else {
		r.ProviderRaw = nil
	}
	if r.Completeness == "final" {
		work, spend = observed.Work.Value, observed.Spend.Value
	}
	return r, work, spend, nil
}

// AcceptUsageReceipt is a trusted Orchestrator persistence operation, not an
// adapter mutation endpoint. It independently validates durable call authority,
// receipt lifecycle and affected current totals; it never sums receipt snapshots,
// settles or releases holds. Identical retries append no audit.
// A late final can be accepted after semantic ownership has returned to Lead.
func (s *StateStore) AcceptUsageReceipt(ctx context.Context, input UsageReceipt, opts AccountingOptions) error {
	return s.acceptUsageReceipt(ctx, input, nil, opts)
}

func (s *StateStore) acceptUsageReceipt(ctx context.Context, input UsageReceipt, provenance *usageProvenance, opts AccountingOptions) (resultErr error) {
	defer func() {
		resultErr = numericFailure("AcceptUsageReceipt", input.RunID, input.InvocationID, input.ProviderCallID, resultErr)
	}()
	r, work, spend, err := normalizeReceipt(input)
	if err != nil {
		return err
	}
	if err := accountingTime(opts.At); err != nil {
		return err
	}
	err = s.numericMutation(ctx, "AcceptUsageReceipt", r.RunID, opts.Audit, func(c *sql.Conn) error {
		if err := validateUsageAuthority(ctx, c, r); err != nil {
			return err
		}
		if provenance != nil {
			var source, model string
			if err := c.QueryRowContext(ctx, `SELECT p.provider,p.model FROM provider_calls p JOIN invocations i USING(invocation_id)
 WHERE p.provider_call_id=? AND i.invocation_id=? AND i.run_id=?`, r.ProviderCallID, r.InvocationID, r.RunID).Scan(&source, &model); err != nil {
				return fmt.Errorf("usage wire ProviderCall provenance: %w", err)
			}
			if source != provenance.source || model != provenance.model {
				return fmt.Errorf("usage source/model does not match registered ProviderCall")
			}
		}
		var existing UsageReceipt
		existing.ReceiptID = r.ReceiptID
		var raw sql.NullString
		var accepted sql.NullString
		var existingWork, existingSpend *int64
		err := c.QueryRowContext(ctx, `SELECT i.run_id,u.invocation_id,u.provider_call_id,u.provider_receipt_id,u.completeness,u.input_tokens,u.cached_input_tokens,u.output_tokens,u.reasoning_tokens,u.reported_at,u.provider_raw_json,u.accepted_at,u.work_tokens,u.spend_tokens FROM usage_receipts u JOIN invocations i ON i.invocation_id=u.invocation_id WHERE u.receipt_id=?`, r.ReceiptID).Scan(&existing.RunID, &existing.InvocationID, &existing.ProviderCallID, &existing.ProviderReceiptID, &existing.Completeness, &existing.InputTokens, &existing.CachedInputTokens, &existing.OutputTokens, &existing.ReasoningTokens, &existing.ReportedAt, &raw, &accepted, &existingWork, &existingSpend)
		if err == nil {
			if raw.Valid {
				existing.ProviderRaw = json.RawMessage(raw.String)
			}
			normalized, _, _, err := normalizeReceipt(existing)
			if err != nil || !accepted.Valid || !reflect.DeepEqual(normalized, r) ||
				!reflect.DeepEqual(existingWork, work) || !reflect.DeepEqual(existingSpend, spend) {
				return fmt.Errorf("usage receipt identity conflicts with immutable normalized content")
			}
			return errDuplicateUsage
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := validateUsageProgression(ctx, c, r); err != nil {
			return err
		}
		var providerRaw any
		if len(r.ProviderRaw) > 0 {
			providerRaw = string(r.ProviderRaw)
		}
		_, err = c.ExecContext(ctx, `INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,provider_receipt_id,completeness,input_tokens,cached_input_tokens,output_tokens,reasoning_tokens,work_tokens,spend_tokens,reported_at,accepted_at,provider_raw_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, r.ReceiptID, r.InvocationID, r.ProviderCallID, r.ProviderReceiptID, r.Completeness, r.InputTokens, r.CachedInputTokens, r.OutputTokens, r.ReasoningTokens, work, spend, r.ReportedAt, opts.At, providerRaw)
		if err != nil {
			return err
		}
		return advanceProviderCallUsage(ctx, c, r)
	})
	if errors.Is(err, errDuplicateUsage) {
		return nil
	}
	return err
}

// Compare primitive observations, not derived SPEND or sums of snapshots.
// Exact retries have already returned before this check. FINAL closes the receipt
// sequence even with NULL counters; settlement rechecks the existing evidence.
func validateUsageProgression(ctx context.Context, q querier, r UsageReceipt) error {
	{
		var closed bool
		if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM usage_receipts WHERE provider_call_id=? AND completeness='final' AND accepted_at IS NOT NULL)
 OR EXISTS(SELECT 1 FROM provider_call_usage WHERE provider_call_id=? AND receipt_state='final')`, r.ProviderCallID, r.ProviderCallID).Scan(&closed); err != nil {
			return err
		}
		if closed {
			return fmt.Errorf("ProviderCall receipt sequence is closed by FINAL")
		}
	}
	var maxima [4]sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT MAX(input_tokens),MAX(cached_input_tokens),MAX(output_tokens),MAX(reasoning_tokens)
 FROM usage_receipts WHERE provider_call_id=? AND accepted_at IS NOT NULL`, r.ProviderCallID).Scan(&maxima[0], &maxima[1], &maxima[2], &maxima[3]); err != nil {
		return err
	}
	incoming := [4]*int64{r.InputTokens, r.CachedInputTokens, r.OutputTokens, r.ReasoningTokens}
	for i, n := range incoming {
		if n != nil && maxima[i].Valid && *n < maxima[i].Int64 {
			return fmt.Errorf("contradictory cumulative primitive counter %d", i)
		}
	}
	// A newly known parent must include already established subset evidence.
	// An unknown parent has no asserted upper bound and is never estimated.
	for _, pair := range [][2]int{{0, 1}, {2, 3}} {
		if n := incoming[pair[0]]; n != nil && maxima[pair[1]].Valid && *n < maxima[pair[1]].Int64 {
			return fmt.Errorf("primitive counter contradicts established subset")
		}
	}
	return nil
}

// For budgeted Runs, the original controller dispatch claim binds the physical
// call to its admitted invocation and semantic ownership/content provenance.
// Current ownership, completion and hard stops do not revoke late observations
// from that already-authorized call. Historical unconfigured calls retain their
// explicit controller registration contract and acquire no dispatch authority.
func validateUsageAuthority(ctx context.Context, c *sql.Conn, r UsageReceipt) error {
	var belongs bool
	if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM provider_calls p JOIN invocations i USING(invocation_id)
 WHERE p.provider_call_id=? AND i.invocation_id=? AND i.run_id=?)`, r.ProviderCallID, r.InvocationID, r.RunID).Scan(&belongs); err != nil {
		return err
	}
	if !belongs {
		return fmt.Errorf("usage ProviderCall does not belong to invocation/run")
	}
	configured, err := policyExists(ctx, c, r.RunID)
	if err != nil || !configured {
		return err
	}
	var authorized bool
	err = c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admission_dispatch_claims d
 JOIN invocation_admissions a USING(admission_id) JOIN provider_calls p USING(provider_call_id)
 JOIN invocations i ON i.invocation_id=p.invocation_id
 WHERE p.provider_call_id=? AND a.run_id=? AND a.invocation_id=?
 AND a.invocation_id=i.invocation_id AND a.execution_id=i.execution_id AND a.attempt_id=i.attempt_id
 AND a.cycle_no=i.cycle_no AND a.role=i.role AND a.purpose=i.purpose
 AND a.ownership_epoch=i.ownership_epoch AND a.content_identity IS i.content_identity
 AND ((a.kind='NORMAL' AND a.actor_invocation_id=a.invocation_id AND a.actor_role=a.role AND a.actor_content_identity IS a.content_identity)
 OR (a.kind='CONTAINMENT' AND a.role='ORCHESTRATOR' AND a.purpose='RECOVER' AND a.actor_invocation_id IS NULL
 AND EXISTS(SELECT 1 FROM admission_contingency_authorizations t WHERE t.admission_id=a.admission_id))))`, r.ProviderCallID, r.RunID, r.InvocationID).Scan(&authorized)
	if err != nil {
		return err
	}
	if !authorized {
		return fmt.Errorf("usage requires original Orchestrator dispatch authorization and ownership provenance")
	}
	return nil
}
