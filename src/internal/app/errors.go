package app

import (
	"context"
	"errors"
	"log/slog"

	"marc-runtime/internal/numeric"
)

// LogNumericFailure is called by the owning runtime after the failed operation
// returns. It logs classification and identity, never provider payloads or an
// arbitrary wrapped error string. It does not determine process exit policy.
func LogNumericFailure(ctx context.Context, logger *slog.Logger, err error) bool {
	var failure *numeric.Error
	if !errors.As(err, &failure) {
		return false
	}
	logger.ErrorContext(ctx, "control-plane numeric invariant failure",
		"error_category", failure.Category, "operation", failure.Operation, "numeric_domain", failure.Domain, "reason", failure.Reason,
		"run_id", failure.RunID, "execution_id", failure.ExecutionID, "attempt_id", failure.AttemptID,
		"invocation_id", failure.InvocationID, "provider_call_id", failure.ProviderCallID)
	return true
}
