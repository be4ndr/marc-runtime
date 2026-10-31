// Package numeric defines internal numeric failures shared by input validation
// and persistence. It never logs, exits, or supplies a replacement value.
package numeric

import (
	"errors"
	"fmt"
)

type Context struct {
	Operation, Domain                                           string
	RunID, ExecutionID, AttemptID, InvocationID, ProviderCallID string
}

// Error means the operation cannot safely use the supplied numeric state.
// Reason and Category are internal descriptions, never external payloads.
type Error struct {
	Context
	Category string
	Reason   string
}

func (e *Error) Error() string {
	return fmt.Sprintf("numeric invariant violation: %s (%s)", e.Reason, e.Domain)
}

func Invalid(domain, category, reason string) error {
	return &Error{Context: Context{Domain: domain}, Category: category, Reason: reason}
}

// contextualError adds diagnostic metadata without replacing the original error
// tree. As exposes the enriched canonical Error; Unwrap retains every existing
// wrapper, cause and joined sibling for errors.Is/As.
type contextualError struct {
	err     error
	failure Error
}

func (e *contextualError) Error() string { return e.err.Error() }
func (e *contextualError) Unwrap() error { return e.err }
func (e *contextualError) As(target any) bool {
	if failure, ok := target.(**Error); ok {
		*failure = &e.failure
		return true
	}
	return false
}

// WithContext preserves the full error tree while adding the owning operation's IDs.
func WithContext(err error, c Context) error {
	var original *Error
	if !errors.As(err, &original) {
		return err
	}
	e := *original
	if c.Operation != "" {
		e.Operation = c.Operation
	}
	if c.Domain != "" {
		e.Domain = c.Domain
	}
	if c.RunID != "" {
		e.RunID = c.RunID
	}
	if c.ExecutionID != "" {
		e.ExecutionID = c.ExecutionID
	}
	if c.AttemptID != "" {
		e.AttemptID = c.AttemptID
	}
	if c.InvocationID != "" {
		e.InvocationID = c.InvocationID
	}
	if c.ProviderCallID != "" {
		e.ProviderCallID = c.ProviderCallID
	}
	return &contextualError{err: err, failure: e}
}
