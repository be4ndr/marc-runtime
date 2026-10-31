package statestore

import (
	"context"
	"database/sql"
	"fmt"
)

// SemanticFence is the expected authoritative owner tuple. Nil InvocationID
// requires an unbound owner; it does not skip the invocation comparison.
// ContentIdentity, when supplied, must match the currently bound invocation.
type SemanticFence struct {
	Role            string
	InvocationID    *string
	OwnershipEpoch  int64
	ActorRevision   int64
	ContentIdentity *string
}

// OwnershipOptions supplies controller change time and opaque atomic audit data.
// Binding/clearing preserves the owner's original activation time.
type OwnershipOptions struct {
	Expected  SemanticFence
	ChangedAt string
	Audit     []AuditRow
}

// InvocationBinding comes from structured controller state, never parsed IDs.
// The invocation must already exist with this ancestry, purpose and content.
type InvocationBinding struct {
	InvocationID    string
	ExecutionID     string
	AttemptID       string
	Cycle           int
	Purpose         string
	ContentIdentity string
}

func semanticRole(role string) bool {
	return role == "LEAD" || workerRole(role)
}

func workerRole(role string) bool {
	return role == "DEVELOPER" || role == "QA" || role == "REVIEWER"
}

func semanticPurpose(role, purpose string) bool {
	switch role {
	case "LEAD":
		return purpose == "LEAD"
	case "DEVELOPER":
		return purpose == "IMPLEMENT" || purpose == "REMEDIATE" || purpose == "FINALIZE_IMPLEMENTATION"
	case "QA":
		return purpose == "QA"
	case "REVIEWER":
		return purpose == "REVIEW" || purpose == "FINALIZE_REVIEW"
	}
	return false
}

func validateOwner(ctx context.Context, c *sql.Conn, run string, expected SemanticFence) (ActiveActor, error) {
	var actor ActiveActor
	fields := []struct{ name, value string }{}
	if expected.InvocationID != nil {
		fields = append(fields, struct{ name, value string }{"invocation_id", *expected.InvocationID})
	}
	if expected.ContentIdentity != nil {
		fields = append(fields, struct{ name, value string }{"content_identity", *expected.ContentIdentity})
	}
	if err := requireIDs(fields...); err != nil {
		return actor, err
	}
	err := c.QueryRowContext(ctx, `SELECT a.run_id,a.active_role,a.active_invocation_id,a.ownership_epoch,a.revision,a.activated_at
 FROM run_active_actor a JOIN runs r ON r.run_id=a.run_id WHERE a.run_id=?`, run).Scan(&actor.RunID, &actor.Role, &actor.InvocationID, &actor.OwnershipEpoch, &actor.Revision, &actor.ActivatedAt)
	if err != nil {
		return actor, err
	}
	if !semanticRole(expected.Role) || actor.Role != expected.Role || actor.OwnershipEpoch != expected.OwnershipEpoch || actor.Revision != expected.ActorRevision || !sameOptionalString(actor.InvocationID, expected.InvocationID) {
		return actor, fmt.Errorf("stale or incompatible semantic owner fence")
	}
	if expected.ContentIdentity != nil {
		if actor.InvocationID == nil {
			return actor, fmt.Errorf("content fence requires a bound invocation")
		}
		var content sql.NullString
		if err := c.QueryRowContext(ctx, "SELECT content_identity FROM invocations WHERE invocation_id=? AND run_id=?", *actor.InvocationID, run).Scan(&content); err != nil {
			return actor, err
		}
		if !content.Valid || content.String != *expected.ContentIdentity {
			return actor, fmt.Errorf("active invocation content fence mismatch")
		}
	}
	return actor, nil
}

func sameOptionalString(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func validateInvocationBinding(ctx context.Context, c *sql.Conn, run, role string, epoch int64, target InvocationBinding) error {
	if err := validateWorkflowSemanticLifecycle(ctx, c, run, target.InvocationID); err != nil {
		return err
	}
	if err := requireIDs(struct{ name, value string }{"invocation_id", target.InvocationID}, struct{ name, value string }{"execution_id", target.ExecutionID}, struct{ name, value string }{"attempt_id", target.AttemptID}, struct{ name, value string }{"content_identity", target.ContentIdentity}); err != nil {
		return err
	}
	if target.ContentIdentity == "" || !semanticPurpose(role, target.Purpose) {
		return fmt.Errorf("invalid semantic invocation content or role/purpose")
	}
	var execution, attempt, storedRole, purpose, executionRole string
	var content sql.NullString
	var cycle, currentCycle int
	var storedEpoch int64
	err := c.QueryRowContext(ctx, `SELECT i.execution_id,i.attempt_id,i.cycle_no,i.role,i.purpose,i.content_identity,i.ownership_epoch,r.current_cycle,e.role
 FROM invocations i JOIN attempts t ON t.attempt_id=i.attempt_id AND t.execution_id=i.execution_id AND t.run_id=i.run_id AND t.cycle_no=i.cycle_no
 JOIN executions e ON e.execution_id=t.execution_id AND e.run_id=t.run_id AND e.cycle_no=t.cycle_no
 JOIN runs r ON r.run_id=i.run_id WHERE i.invocation_id=? AND i.run_id=?`, target.InvocationID, run).Scan(&execution, &attempt, &cycle, &storedRole, &purpose, &content, &storedEpoch, &currentCycle, &executionRole)
	if err != nil {
		return err
	}
	if execution != target.ExecutionID || attempt != target.AttemptID || cycle != target.Cycle || currentCycle != cycle || storedRole != role || executionRole != role || purpose != target.Purpose || !content.Valid || content.String != target.ContentIdentity || storedEpoch != epoch {
		return fmt.Errorf("target invocation ancestry/content/ownership binding mismatch")
	}
	var accepted int
	if err := c.QueryRowContext(ctx, "SELECT count(*) FROM role_results WHERE invocation_id=?", target.InvocationID).Scan(&accepted); err != nil {
		return err
	}
	if accepted != 0 {
		return fmt.Errorf("accepted result invocation cannot reacquire ownership")
	}
	return nil
}

func updateOwner(ctx context.Context, c *sql.Conn, actor ActiveActor, role string, invocation *string, changedAt string) (resultErr error) {
	defer func() {
		id := ""
		if actor.InvocationID != nil {
			id = *actor.InvocationID
		}
		resultErr = numericFailure("updateOwner", actor.RunID, id, "", resultErr)
	}()
	epoch := actor.OwnershipEpoch
	activatedAt := actor.ActivatedAt
	if role != actor.Role {
		var err error
		epoch, err = checkedNext(epoch, "ownership_epoch")
		if err != nil {
			return err
		}
		activatedAt = changedAt
	}
	revision, err := checkedNext(actor.Revision, "actor_revision")
	if err != nil {
		return err
	}
	changed, err := c.ExecContext(ctx, `UPDATE run_active_actor SET active_role=?,active_invocation_id=?,ownership_epoch=?,revision=?,activated_at=?
 WHERE run_id=? AND active_role=? AND active_invocation_id IS ? AND ownership_epoch=? AND revision=?`, role, invocation, epoch, revision, activatedAt, actor.RunID, actor.Role, actor.InvocationID, actor.OwnershipEpoch, actor.Revision)
	if err != nil {
		return err
	}
	rows, err := changed.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("stale semantic ownership update")
	}
	return nil
}

func validateOwnershipTime(at string) error {
	if _, err := canonicalTime(at); err != nil {
		return fmt.Errorf("ownership change timestamp: %w", err)
	}
	return nil
}

// TransferSemanticOwnership changes owner once. Workers require an existing
// target invocation prepared for epoch N+1; Lead may receive NULL. Lifecycle
// policy/admission is the caller's responsibility, not inferred here.
func (s *StateStore) TransferSemanticOwnership(ctx context.Context, run, role string, target *InvocationBinding, options OwnershipOptions) error {
	if target != nil {
		if err := requireIDs(struct{ name, value string }{"invocation_id", target.InvocationID}, struct{ name, value string }{"execution_id", target.ExecutionID}, struct{ name, value string }{"attempt_id", target.AttemptID}, struct{ name, value string }{"content_identity", target.ContentIdentity}); err != nil {
			return err
		}
	}
	if err := validateOwnershipTime(options.ChangedAt); err != nil {
		return numericFailure("TransferSemanticOwnership", run, "", "", err)
	}
	return s.numericMutation(ctx, "TransferSemanticOwnership", run, options.Audit, func(c *sql.Conn) error {
		actor, err := validateOwner(ctx, c, run, options.Expected)
		if err != nil {
			return err
		}
		if !semanticRole(role) || role == actor.Role || target == nil && role != "LEAD" {
			return fmt.Errorf("transfer requires a different semantic owner and bound Worker target")
		}
		var invocation *string
		if target != nil {
			nextEpoch, err := checkedNext(actor.OwnershipEpoch, "ownership_epoch")
			if err != nil {
				return err
			}
			if err := validateInvocationBinding(ctx, c, run, role, nextEpoch, *target); err != nil {
				return err
			}
			invocation = &target.InvocationID
		}
		return updateOwner(ctx, c, actor, role, invocation, options.ChangedAt)
	})
}

// BindActiveInvocation changes only the invocation binding and row revision.
func (s *StateStore) BindActiveInvocation(ctx context.Context, run string, target InvocationBinding, options OwnershipOptions) error {
	if err := requireIDs(struct{ name, value string }{"invocation_id", target.InvocationID}, struct{ name, value string }{"execution_id", target.ExecutionID}, struct{ name, value string }{"attempt_id", target.AttemptID}, struct{ name, value string }{"content_identity", target.ContentIdentity}); err != nil {
		return err
	}
	if err := validateOwnershipTime(options.ChangedAt); err != nil {
		return numericFailure("BindActiveInvocation", run, "", "", err)
	}
	return s.numericMutation(ctx, "BindActiveInvocation", run, options.Audit, func(c *sql.Conn) error {
		actor, err := validateOwner(ctx, c, run, options.Expected)
		if err != nil {
			return err
		}
		if err := validateInvocationBinding(ctx, c, run, actor.Role, actor.OwnershipEpoch, target); err != nil {
			return err
		}
		return updateOwner(ctx, c, actor, actor.Role, &target.InvocationID, options.ChangedAt)
	})
}

// ClearActiveInvocation leaves the semantic owner and epoch unchanged.
func (s *StateStore) ClearActiveInvocation(ctx context.Context, run string, options OwnershipOptions) error {
	if err := validateOwnershipTime(options.ChangedAt); err != nil {
		return numericFailure("ClearActiveInvocation", run, "", "", err)
	}
	return s.numericMutation(ctx, "ClearActiveInvocation", run, options.Audit, func(c *sql.Conn) error {
		actor, err := validateOwner(ctx, c, run, options.Expected)
		if err != nil {
			return err
		}
		return updateOwner(ctx, c, actor, actor.Role, nil, options.ChangedAt)
	})
}

// RecoverSemanticOwnershipToLead is an explicit controller operation. It creates
// no semantic result or retry and does not run automatically on a timer.
func (s *StateStore) RecoverSemanticOwnershipToLead(ctx context.Context, run string, options OwnershipOptions) error {
	if err := validateOwnershipTime(options.ChangedAt); err != nil {
		return numericFailure("RecoverSemanticOwnershipToLead", run, "", "", err)
	}
	return s.numericMutation(ctx, "RecoverSemanticOwnershipToLead", run, options.Audit, func(c *sql.Conn) error {
		actor, err := validateOwner(ctx, c, run, options.Expected)
		if err != nil {
			return err
		}
		if !workerRole(actor.Role) {
			return fmt.Errorf("ownership recovery requires a Worker owner")
		}
		return updateOwner(ctx, c, actor, "LEAD", nil, options.ChangedAt)
	})
}
