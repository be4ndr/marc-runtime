package statestore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

// All amounts are int64 token accounting units.
// ResourceAccount is an exact, explicit accounting address. IDs are never parsed.
// The current address is RUN/WORK; descendant scope fields must remain nil.
type ResourceAccount struct {
	PoolID, RunID, Scope, ResourceType string
	ExecutionID, AttemptID             *string
}
type ResourceEligibility struct{ Role, Purpose string }
type ResourcePool struct {
	Account     ResourceAccount
	Eligibility []ResourceEligibility
}

// AccountingOptions is controller-supplied time and opaque atomic audit data.
// These operations confer no semantic ownership or lifecycle authority.
type AccountingOptions struct {
	At    string
	Audit []AuditRow
}
type ResourceGrant struct {
	Account              ResourceAccount
	AllocationID, Source string
	AdmissionID          string  // required for normal-token top-ups to the active admitted agent
	RequestID            *string // accepted request's wire ID; RunID qualifies it for the installation key
	Amount               int64
	ApprovalID           string // immutable Hard decision; required for an extension at Hard
}
type AdmissionResource struct {
	Account ResourceAccount
	Amount  int64
}
type ResourceBalance struct{ Granted, Consumed, Available int64 }

func consumable(resource string) bool {
	return tokenResource(resource)
}

func tokenResource(resource string) bool { return resource == "WORK" }
func validateAccount(a ResourceAccount) error {
	fields := []struct{ name, value string }{{"pool_id", a.PoolID}, {"run_id", a.RunID}}
	if a.ExecutionID != nil {
		fields = append(fields, struct{ name, value string }{"execution_id", *a.ExecutionID})
	}
	if a.AttemptID != nil {
		fields = append(fields, struct{ name, value string }{"attempt_id", *a.AttemptID})
	}
	if err := requireIDs(fields...); err != nil {
		return err
	}
	if strings.TrimSpace(a.PoolID) == "" || strings.TrimSpace(a.RunID) == "" || !consumable(a.ResourceType) {
		return fmt.Errorf("invalid pool/run/consumable resource")
	}
	if a.Scope != "RUN" || a.ResourceType != "WORK" || a.ExecutionID != nil || a.AttemptID != nil {
		return fmt.Errorf("one Run WORK accounting address required")
	}
	return nil
}
func accountingTime(at string) error {
	if _, err := canonicalTime(at); err != nil {
		return fmt.Errorf("accounting timestamp: %w", err)
	}
	return nil
}
func resourceAmount(resource string, amount int64, positive bool) error {
	if !consumable(resource) || amount < 0 || positive && amount == 0 {
		return fmt.Errorf("invalid resource amount")
	}
	return nil
}
func validatePool(ctx context.Context, q querier, a ResourceAccount) error {
	if err := validateAccount(a); err != nil {
		return err
	}
	var exists int
	err := q.QueryRowContext(ctx, `SELECT count(*) FROM resource_pools WHERE pool_id=? AND run_id=? AND scope=? AND resource_type=? AND execution_id IS ? AND attempt_id IS ?`, a.PoolID, a.RunID, a.Scope, a.ResourceType, a.ExecutionID, a.AttemptID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists != 1 {
		return fmt.Errorf("pool does not match exact accounting address")
	}
	return nil
}

// CreateResourcePool persists the shared Run WORK address with the required
// primary-purpose and controller eligibility. It creates no capacity or grant.
func (s *StateStore) CreateResourcePool(ctx context.Context, p ResourcePool, opts AccountingOptions) error {
	a := p.Account
	if err := validateAccount(a); err != nil {
		return err
	}
	if err := accountingTime(opts.At); err != nil {
		return numericFailure("CreateResourcePool", a.RunID, "", "", err)
	}
	return s.numericMutation(ctx, "CreateResourcePool", a.RunID, opts.Audit, func(c *sql.Conn) error {
		if len(p.Eligibility) != 8 {
			return fmt.Errorf("WORK address must support all primary purposes and controller closure")
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO resource_pools(pool_id,run_id,scope,execution_id,attempt_id,resource_type,created_at) VALUES (?,?,?,?,?,?,?)`, a.PoolID, a.RunID, a.Scope, a.ExecutionID, a.AttemptID, a.ResourceType, opts.At); err != nil {
			return err
		}
		for _, e := range p.Eligibility {
			if !semanticPurpose(e.Role, e.Purpose) && !(e.Role == "ORCHESTRATOR" && e.Purpose == "RECOVER") {
				return fmt.Errorf("invalid pool role/purpose eligibility")
			}
			if _, err := c.ExecContext(ctx, `INSERT INTO resource_pool_eligibility(pool_id,role,purpose) VALUES (?,?,?)`, a.PoolID, e.Role, e.Purpose); err != nil {
				return err
			}
		}
		return nil
	})
}

// GrantResources atomically appends positive allocations; no request changes a
// balance by itself. A partial grant is simply a smaller explicit allocation.
func (s *StateStore) GrantResources(ctx context.Context, runID string, grants []ResourceGrant, opts AccountingOptions) error {
	if len(grants) == 0 {
		return fmt.Errorf("grant requires allocations; denial creates none")
	}
	if err := accountingTime(opts.At); err != nil {
		return numericFailure("GrantResources", runID, "", "", err)
	}
	return s.numericMutation(ctx, "GrantResources", runID, opts.Audit, func(c *sql.Conn) error { return grantResources(ctx, c, runID, grants, opts.At) })
}
func grantResources(ctx context.Context, c *sql.Conn, runID string, grants []ResourceGrant, at string) error {

	for _, g := range grants {
		a := g.Account
		ids := []struct{ name, value string }{{"allocation_id", g.AllocationID}}
		if g.AdmissionID != "" {
			ids = append(ids, struct{ name, value string }{"admission_id", g.AdmissionID})
		}
		if g.RequestID != nil {
			ids = append(ids, struct{ name, value string }{"request_id", *g.RequestID})
		}
		if err := requireIDs(ids...); err != nil {
			return err
		}
		if err := admissionUTF8(g.Source); err != nil {
			return err
		}
		if a.RunID != runID || strings.TrimSpace(g.AllocationID) == "" || strings.TrimSpace(g.Source) == "" {
			return fmt.Errorf("invalid grant identity/source/run")
		}
		if err := validatePool(ctx, c, a); err != nil {
			return err
		}
		if err := resourceAmount(a.ResourceType, g.Amount, true); err != nil {
			return err
		}
		configured, err := policyExists(ctx, c, runID)
		if err != nil {
			return err
		}

		if !configured {
			return fmt.Errorf("Run budget confirmation required")
		}
		if err := validateTokenTopUp(ctx, c, runID, g, at, false); err != nil {
			return err
		}
		if g.RequestID != nil {
			// Validate accepted request provenance and the requested dimension, not a
			// filename/display key or a caller's reconstructed target relationship.
			qualifiedRequestID := requestStorageKey(runID, *g.RequestID)
			var matches int
			if err := c.QueryRowContext(ctx, `SELECT count(*) FROM resource_requests r JOIN resource_request_sources b ON b.request_id=r.request_id
     JOIN resource_request_amounts m ON m.request_id=r.request_id AND m.resource_type=?
     JOIN resource_pool_eligibility e ON e.pool_id=? AND e.role=r.target_role AND e.purpose=r.target_purpose
     WHERE r.request_id=? AND r.run_id=? AND (? IS NULL OR r.target_execution_id=?) AND (? IS NULL OR r.target_attempt_id=?)`, a.ResourceType, a.PoolID, qualifiedRequestID, runID, a.ExecutionID, a.ExecutionID, a.AttemptID, a.AttemptID).Scan(&matches); err != nil {
				return err
			}
			if matches != 1 {
				return fmt.Errorf("grant does not match accepted resource request/target/pool")
			}
		}
		var qualifiedRequestID *string
		if g.RequestID != nil {
			value := requestStorageKey(runID, *g.RequestID)
			qualifiedRequestID = &value
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO resource_allocations(allocation_id,run_id,execution_id,attempt_id,resource_type,amount,source,created_at,pool_id,request_id) VALUES (?,?,?,?,?,?,?,?,?,?)`, g.AllocationID, runID, a.ExecutionID, a.AttemptID, a.ResourceType, g.Amount, g.Source, at, a.PoolID, qualifiedRequestID); err != nil {
			return err
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO admission_grants VALUES(?,?,?)`, g.AdmissionID, a.ResourceType, g.AllocationID); err != nil {
			return err
		}
		if _, err := resourceBalance(ctx, c, a); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, g := range grants {
		if g.RequestID != nil && !seen[*g.RequestID] {
			if err := closeResourceRequest(ctx, c, runID, *g.RequestID, "GRANTED"); err != nil {
				return err
			}
			seen[*g.RequestID] = true
		}
	}

	return nil

}

func resourceBalance(ctx context.Context, q querier, a ResourceAccount) (ResourceBalance, error) {
	var b ResourceBalance
	if err := validatePool(ctx, q, a); err != nil {
		return b, err
	}
	bank, err := runTokenBalance(ctx, q, a.RunID)
	return ResourceBalance{Granted: bank.NormalBudget, Consumed: bank.NormalUsed, Available: bank.NormalAvailable}, err
}
func (s *StateStore) GetResourceBalance(ctx context.Context, a ResourceAccount) (_ ResourceBalance, resultErr error) {
	defer func() { resultErr = numericFailure("GetResourceBalance", a.RunID, "", "", resultErr) }()
	// Pool validation and bank reads must share one snapshot.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return ResourceBalance{}, err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "BEGIN"); err != nil {
		return ResourceBalance{}, err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(cleanup, "ROLLBACK"); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	b, err := resourceBalance(ctx, conn, a)
	if err != nil {
		return b, err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	committed = err == nil
	return b, err
}
