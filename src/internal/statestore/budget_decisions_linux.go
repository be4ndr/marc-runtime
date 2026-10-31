package statestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"modernc.org/sqlite"
)

// Actual human input captured by the trusted Orchestrator. Readiness dispositions
// and Worker output are not budget authorization.
type HumanBudgetApproval struct {
	ApprovalID, InteractionID, Prompt, Response, Decision string
}

func validateBudgetApproval(h HumanBudgetApproval, approveOnly bool) error {
	if err := requireIDs(struct{ name, value string }{"approval_id", h.ApprovalID}, struct{ name, value string }{"interaction_id", h.InteractionID}); err != nil {
		return err
	}
	if err := admissionUTF8(h.Decision); err != nil {
		return err
	}
	if err := descriptionUTF8(h.Prompt, h.Response); err != nil {
		return err
	}
	if strings.TrimSpace(h.Prompt) == "" || strings.TrimSpace(h.Response) == "" || h.Decision != "APPROVE" && (approveOnly || h.Decision != "DENY") {
		return fmt.Errorf("explicit human budget response required")
	}
	return nil
}
func approvalUnused(ctx context.Context, q querier, h HumanBudgetApproval) error {
	var used bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_budget_confirmations WHERE approval_id=?1 OR json_extract(approval_json,'$.InteractionID')=?2)
 OR EXISTS(SELECT 1 FROM run_budget_increases WHERE approval_id=?1 OR json_extract(approval_json,'$.InteractionID')=?2)
 OR EXISTS(SELECT 1 FROM hard_limit_decisions WHERE json_extract(response_json,'$.ApprovalID')=?1 OR json_extract(response_json,'$.InteractionID')=?2)`, h.ApprovalID, h.InteractionID).Scan(&used); err != nil {
		return err
	}
	if used {
		return fmt.Errorf("human interaction approval is already bound to another decision")
	}
	return nil
}

// IncreaseRunBudget preserves the original policy and profiles. The checked sum
// of approved increments supplies effective normal capacity; no grant is changed.
func (s *StateStore) IncreaseRunBudget(ctx context.Context, run string, amount int64, h HumanBudgetApproval, opts AccountingOptions) (bool, error) {
	if err := requireIDs(struct{ name, value string }{"run_id", run}); err != nil {
		return false, err
	}
	if amount <= 0 {
		return false, fmt.Errorf("positive normal budget increment required")
	}
	if err := validateBudgetApproval(h, true); err != nil {
		return false, err
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return false, err
	}
	payload := struct {
		Run      string
		Amount   int64
		Approval HumanBudgetApproval
		At       string
	}{run, amount, h, at}
	digest := admissionDigest(payload)
	created := false
	err = s.numericMutation(ctx, "IncreaseRunBudget", run, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT contract_digest FROM run_budget_increases WHERE approval_id=?`, h.ApprovalID).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting human budget increment replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := approvalUnused(ctx, c, h); err != nil {
			return err
		}
		var status string
		if err := c.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=?`, run).Scan(&status); err != nil {
			return err
		}
		if status != "RUNNING" {
			return fmt.Errorf("budget increment requires a running Run")
		}
		b, err := runTokenBalance(ctx, c, run)
		if err != nil {
			return err
		}
		normal, err := checkedAdd(b.NormalBudget, amount)
		if err != nil {
			return err
		}
		if _, err = checkedAdd(normal, normal/10); err != nil {
			return err
		}
		raw, _ := json.Marshal(h)
		if _, err = c.ExecContext(ctx, `INSERT INTO run_budget_increases VALUES(?,?,?,?,?,?)`, h.ApprovalID, run, amount, string(raw), at, digest); err != nil {
			return err
		}
		created = true
		return nil
	})
	return committedCreation(created, err)
}
func committedCreation(created bool, err error) (bool, error) {
	if errors.Is(err, errAdmissionReplay) {
		return false, nil
	}
	var projection *MetricsProjectionError
	if err != nil && !errors.As(err, &projection) {
		return false, err
	}
	return created, err
}

type HardLimitRequest struct {
	RequestID, RunID, Justification, PresentedAt string
	Grant                                        ResourceGrant
}
type HardLimitDecision struct{ Created, Applied bool }

// RecordHardLimitRequest records a successfully presented specific proposal.
// PresentedAt starts the immutable five-minute response deadline. Runtime message
// delivery is outside this API; no presentation means no timeout authority.
func (s *StateStore) RecordHardLimitRequest(ctx context.Context, p HardLimitRequest, opts AccountingOptions) (bool, error) {
	if err := requireIDs(struct{ name, value string }{"request_id", p.RequestID}, struct{ name, value string }{"run_id", p.RunID}); err != nil {
		return false, err
	}
	if err := descriptionUTF8(p.Justification); err != nil {
		return false, err
	}
	if strings.TrimSpace(p.Justification) == "" {
		return false, fmt.Errorf("Lead justification required")
	}
	if err := requireIDs(struct{ name, value string }{"admission_id", p.Grant.AdmissionID}, struct{ name, value string }{"allocation_id", p.Grant.AllocationID}); err != nil {
		return false, err
	}
	if p.Grant.Amount <= 0 || p.Grant.Account.RunID != p.RunID || strings.TrimSpace(p.Grant.Source) == "" {
		return false, fmt.Errorf("specific positive same-Run grant required")
	}
	if err := admissionUTF8(p.Grant.Source); err != nil {
		return false, err
	}
	if p.Grant.RequestID != nil {
		if err := requireIDs(struct{ name, value string }{"request_id", *p.Grant.RequestID}); err != nil {
			return false, err
		}
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return false, err
	}
	p.PresentedAt, err = canonicalTime(p.PresentedAt)
	if err != nil {
		return false, err
	}
	if p.PresentedAt != at {
		return false, fmt.Errorf("presentation must match authoritative proposal acceptance time")
	}
	if p.Grant.ApprovalID != "" {
		return false, fmt.Errorf("proposal cannot carry an earlier approval")
	}
	t, _ := time.Parse(time.RFC3339Nano, at)
	deadline, err := canonicalTime(t.Add(5 * time.Minute).Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	raw, _ := json.Marshal(p)
	digest := admissionDigest(p)
	created := false
	err = s.numericMutation(ctx, "RecordHardLimitRequest", p.RunID, opts.Audit, func(c *sql.Conn) error {
		var old string
		err := c.QueryRowContext(ctx, `SELECT contract_digest FROM hard_limit_requests WHERE request_id=?`, p.RequestID).Scan(&old)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting Hard proposal replay")
			}
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := validateTokenTopUp(ctx, c, p.RunID, p.Grant, at, true); err != nil {
			return err
		}
		a, err := readAdmission(ctx, c, p.RunID, p.Grant.AdmissionID)
		if err != nil {
			return err
		}
		limits, err := agentTokenLimits(ctx, c, a)
		if err != nil {
			return err
		}
		if limits.HardLimit <= 0 || limits.Used != limits.HardLimit {
			return fmt.Errorf("Hard proposal requires exactly reached active Hard limit")
		}
		var pending bool
		if err := c.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM hard_limit_requests r WHERE r.admission_id=? AND NOT EXISTS(SELECT 1 FROM hard_limit_decisions d WHERE d.request_id=r.request_id))`, a.AdmissionID).Scan(&pending); err != nil {
			return err
		}
		if pending {
			return fmt.Errorf("an active Hard proposal is already pending")
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO hard_limit_requests VALUES(?,?,?,?,?,?,?,?,?,?)`, p.RequestID, p.RunID, a.AdmissionID, p.Grant.AllocationID, p.Grant.Amount, limits.HardLimit, at, deadline, string(raw), digest); err != nil {
			return err
		}
		created = true
		return nil
	})
	return committedCreation(created, err)
}

// A nil response processes delegated timeout policy. Explicit responses never
// increase the Run bank. First committed decision wins; replay cannot grant twice.
func (s *StateStore) DecideHardLimitRequest(ctx context.Context, run, id string, h *HumanBudgetApproval, opts AccountingOptions) (HardLimitDecision, error) {
	var out HardLimitDecision
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"request_id", id}); err != nil {
		return out, err
	}
	if h != nil {
		if err := validateBudgetApproval(*h, false); err != nil {
			return out, err
		}
	}
	at, err := canonicalTime(opts.At)
	if err != nil {
		return out, err
	}
	payload := struct {
		Run, ID, At string
		Response    *HumanBudgetApproval
	}{run, id, at, h}
	digest := admissionDigest(payload)
	err = s.numericMutation(ctx, "DecideHardLimitRequest", run, opts.Audit, func(c *sql.Conn) error {
		var old string
		var applied bool
		err := c.QueryRowContext(ctx, `SELECT d.contract_digest,d.applied FROM hard_limit_decisions d JOIN hard_limit_requests r USING(request_id) WHERE d.request_id=? AND r.run_id=?`, id, run).Scan(&old, &applied)
		if err == nil {
			if old != digest {
				return fmt.Errorf("conflicting final Hard decision")
			}
			out.Applied = applied
			return errAdmissionReplay
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var raw, deadline, proposalDigest string
		var originalHard int64
		if err := c.QueryRowContext(ctx, `SELECT proposal_json,deadline,initial_hard_limit,contract_digest FROM hard_limit_requests WHERE request_id=? AND run_id=?`, id, run).Scan(&raw, &deadline, &originalHard, &proposalDigest); err != nil {
			return err
		}
		var p HardLimitRequest
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return err
		}
		presented, err := time.Parse(time.RFC3339Nano, p.PresentedAt)
		if err != nil || p.RunID != run || p.RequestID != id || admissionDigest(p) != proposalDigest || presented.Add(5*time.Minute).Format(time.RFC3339Nano) != deadline {
			return fmt.Errorf("Hard proposal identity or immutable deadline changed")
		}
		if beforeDeadline(at, p.PresentedAt) {
			return fmt.Errorf("Hard decision precedes proposal presentation")
		}
		decision := "TIMEOUT"
		var response any
		if h != nil {
			if !beforeDeadline(at, deadline) {
				return fmt.Errorf("human Hard response arrived after the immutable deadline")
			}
			if err := approvalUnused(ctx, c, *h); err != nil {
				return err
			}
			decision = h.Decision
			b, _ := json.Marshal(h)
			response = string(b)
		} else if beforeDeadline(at, deadline) {
			return fmt.Errorf("Hard approval deadline has not elapsed")
		}
		a, err := readAdmission(ctx, c, run, p.Grant.AdmissionID)
		if err != nil {
			return err
		}
		limits, err := agentTokenLimits(ctx, c, a)
		if err != nil {
			return err
		}
		reason := ""
		if decision != "DENY" {
			err := validateTokenTopUp(ctx, c, run, p.Grant, at, true)
			var dbError *sqlite.Error
			if err != nil && (errors.As(err, &dbError) || ctx.Err() != nil) {
				return err
			}
			if err != nil {
				reason = err.Error()
			} else if limits.HardLimit != originalHard {
				reason = "proposal's original Hard limit changed"
			} else {
				out.Applied = true
			}
		}
		if _, err = c.ExecContext(ctx, `INSERT INTO hard_limit_decisions VALUES(?,?,?,?,?,?,?)`, id, decision, response, at, digest, out.Applied, reason); err != nil {
			return err
		}
		if out.Applied {
			p.Grant.ApprovalID = id
			if err := grantResources(ctx, c, run, []ResourceGrant{p.Grant}, at); err != nil {
				return err
			}
		}
		if decision == "DENY" && limits.HardLimit == originalHard {
			if p.Grant.RequestID != nil {
				// A separately decided resource intent cannot invalidate the
				// actual human rejection of this conditional Hard proposal.
				if _, err := c.ExecContext(ctx, `UPDATE resource_requests SET status='DENIED',revision=revision+1 WHERE request_id=? AND run_id=? AND status='PENDING'`, requestStorageKey(run, *p.Grant.RequestID), run); err != nil {
					return err
				}
			}
		}
		out.Created = true
		return nil
	})
	if errors.Is(err, errAdmissionReplay) {
		return out, nil
	}
	var projection *MetricsProjectionError
	if err != nil && !errors.As(err, &projection) {
		return HardLimitDecision{}, err
	}
	return out, err
}

func closeResourceRequest(ctx context.Context, c *sql.Conn, run, id, status string) error {
	r, err := c.ExecContext(ctx, `UPDATE resource_requests SET status=?,revision=revision+1 WHERE request_id=? AND run_id=? AND status='PENDING'`, status, requestStorageKey(run, id), run)
	if err != nil {
		return err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("resource request already has a final decision")
	}
	return nil
}
func (s *StateStore) DenyResourceRequest(ctx context.Context, run, id string, opts AccountingOptions) error {
	if err := requireIDs(struct{ name, value string }{"run_id", run}, struct{ name, value string }{"request_id", id}); err != nil {
		return err
	}
	if err := accountingTime(opts.At); err != nil {
		return err
	}
	err := s.numericMutation(ctx, "DenyResourceRequest", run, opts.Audit, func(c *sql.Conn) error {
		var status string
		if err := c.QueryRowContext(ctx, `SELECT status FROM resource_requests WHERE request_id=? AND run_id=?`, requestStorageKey(run, id), run).Scan(&status); err != nil {
			return err
		}
		if status == "DENIED" {
			return errAdmissionReplay
		}
		return closeResourceRequest(ctx, c, run, id, "DENIED")
	})
	if errors.Is(err, errAdmissionReplay) {
		return nil
	}
	return err
}
