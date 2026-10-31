package statestore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"ai-control-plane/internal/identity"
)

// ErrMutationAuthority means the local runtime did not identify ORCHESTRATOR as
// the initiator. It is independent of audit labels and affected semantic owners.
var ErrMutationAuthority = errors.New("authoritative mutation requires initiating actor ORCHESTRATOR")

type mutationActorKey struct{}

// WithMutationActor identifies the initiating actor using the existing role
// strings. The local runtime supplies this identity; it is not authentication.
// StateStore never infers it from an audit row, invocation or ownership fence.
func WithMutationActor(ctx context.Context, actorRole string) context.Context {
	return context.WithValue(ctx, mutationActorKey{}, actorRole)
}

func requireMutationActor(ctx context.Context) error {
	if ctx == nil || ctx.Value(mutationActorKey{}) != "ORCHESTRATOR" {
		return ErrMutationAuthority
	}
	return nil
}

// MutationOptions protects both semantic execution ownership and the selected
// Lead snapshot. ExpectedSemanticRevision is the current pointer's revision,
// including zero while its LeadState is NULL. No ownership transfer occurs.
type MutationOptions struct {
	Fence                    ResultFence
	ExpectedSemanticRevision int64
	Audit                    []AuditRow
}

// authoritativeMutation holds one physical connection from writer-lock
// acquisition through validation/mutation, optional audit and commit. It is
// private; agents and callers cannot submit arbitrary SQL callbacks.
func (s *StateStore) authoritativeMutation(ctx context.Context, runID string, audit []AuditRow, mutate func(*sql.Conn) error) error {
	return s.numericMutation(ctx, "authoritative_mutation", runID, audit, mutate)
}

func (s *StateStore) numericMutation(ctx context.Context, operation, runID string, audit []AuditRow, mutate func(*sql.Conn) error) error {
	if err := s.commitMutation(ctx, runID, audit, mutate); err != nil {
		return numericFailure(operation, runID, "", "", err)
	}
	// The writer connection is returned before reading the projection snapshot.
	// Projection failure is reported separately and cannot roll back the commit.
	if err := s.refreshEnabledMetrics(ctx, runID); err != nil {
		return &MetricsProjectionError{RunID: runID, Err: err}
	}
	return nil
}

func (s *StateStore) commitMutation(ctx context.Context, runID string, audit []AuditRow, mutate func(*sql.Conn) error) error {
	if err := requireMutationActor(ctx); err != nil {
		return err
	}
	if runID != "" {
		if err := identity.CheckID("run_id", runID); err != nil {
			return err
		}
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin immediate authoritative mutation: %w", err)
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

	if err := validateRunNumbers(ctx, conn, runID); err != nil {
		return err
	}

	if err := mutate(conn); err != nil {
		return err
	}

	if err := validateRunNumbers(ctx, conn, runID); err != nil {
		return err
	}

	// Usage beyond policy is valid evidence. Commit its hard stop in the
	// same transaction; never reject it as a budget/numeric error.
	if err := refreshRunAdmission(ctx, conn, runID); err != nil {
		return err
	}

	if err := appendAudit(ctx, conn, runID, audit); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit authoritative mutation: %w", err)
	}
	committed = true
	return nil
}
