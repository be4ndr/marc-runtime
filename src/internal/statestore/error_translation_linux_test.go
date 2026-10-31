package statestore

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"testing"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestSQLNumericTranslationLeavesUnrelatedErrorsUnchanged(t *testing.T) {
	s := testStore(t)
	var ignored int64
	sqlErr := s.db.QueryRow(`SELECT * FROM missing_table`).Scan(&ignored)
	if sqlErr == nil {
		t.Fatal("expected SQLite error")
	}
	for _, err := range []error{
		nil,
		errors.New("integer overflow"),
		errors.New("numeric invariant violation: WORK unrepresentable"),
		&os.PathError{Op: "open", Path: "/tmp/integer overflow", Err: os.ErrPermission},
		&MetricsProjectionError{RunID: "numeric invariant violation:", Err: os.ErrPermission},
		fmt.Errorf("integer overflow; numeric invariant violation: %w", sqlErr),
	} {
		if got := numericFailure("test", "run", "invocation", "call", err); got != err {
			t.Fatalf("unrelated error changed: %v", got)
		}
	}
	if _, err := s.db.Exec(`CREATE TEMP TABLE translation_probe(value INTEGER);
CREATE TEMP TRIGGER translation_probe_insert BEFORE INSERT ON translation_probe BEGIN
SELECT RAISE(ABORT,'unrelated integer overflow'); END;`); err != nil {
		t.Fatal(err)
	}
	_, err := s.db.Exec(`INSERT INTO translation_probe VALUES(0)`)
	var driverError *sqlite.Error
	if !errors.As(err, &driverError) || driverError.Code() != sqlite3.SQLITE_CONSTRAINT_TRIGGER || sqlNumericError(err) != err {
		t.Fatal("unrelated trigger error misclassified", err)
	}
}

func TestSQLNumericTranslationPreservesWrappedDriverErrors(t *testing.T) {
	s := testStore(t)
	for _, tc := range []struct {
		name, query string
		args        []any
		code        int
	}{
		{"sum overflow", `SELECT SUM(value) FROM (SELECT ? AS value UNION ALL SELECT 1)`, []any{int64(math.MaxInt64)}, sqlite3.SQLITE_ERROR},
		{"abs overflow", `SELECT abs(?)`, []any{int64(math.MinInt64)}, sqlite3.SQLITE_ERROR},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ignored any
			err := s.db.QueryRow(tc.query, tc.args...).Scan(&ignored)
			var driverError *sqlite.Error
			if !errors.As(err, &driverError) || driverError.Code() != tc.code {
				t.Fatalf("unexpected SQLite failure: %v", err)
			}
			projection := &MetricsProjectionError{RunID: "run", Err: fmt.Errorf("read snapshot: %w", err)}
			sibling := errors.New("sibling")
			tree := errors.Join(projection, sibling)
			got := numericFailure("test", "run", "invocation", "call", tree)
			failure := requireNumeric(t, got, "sqlite_integer")
			if failure.Operation != "test" || failure.ProviderCallID != "call" {
				t.Fatal("missing context", failure)
			}
			for _, cause := range []error{tree, projection, sibling, err, driverError} {
				if !errors.Is(got, cause) {
					t.Fatalf("lost cause %T", cause)
				}
			}
			var retainedDriver *sqlite.Error
			var retainedProjection *MetricsProjectionError
			if !errors.As(got, &retainedDriver) || retainedDriver != driverError || !errors.As(got, &retainedProjection) || retainedProjection != projection {
				t.Fatal("lost wrapper classification")
			}
		})
	}
	if _, err := s.db.Exec(`CREATE TEMP TABLE translation_guard(value INTEGER);
CREATE TEMP TRIGGER translation_guard_insert BEFORE INSERT ON translation_guard BEGIN
SELECT RAISE(ABORT,'numeric invariant violation: WORK unrepresentable'); END;`); err != nil {
		t.Fatal(err)
	}
	_, err := s.db.Exec(`INSERT INTO translation_guard VALUES(0)`)
	var driverError *sqlite.Error
	if !errors.As(err, &driverError) || driverError.Code() != sqlite3.SQLITE_CONSTRAINT_TRIGGER {
		t.Fatal("unexpected trigger code", err)
	}
	got := numericFailure("test", "run", "invocation", "call", fmt.Errorf("guard: %w", err))
	requireNumeric(t, got, "sqlite_integer")
	if !errors.Is(got, driverError) {
		t.Fatal("lost trigger cause")
	}
}

func TestSQLiteOverflowRollsBackMutation(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	var cause error
	err := s.numericMutation(accountingContext, "overflow_probe", "r1", accountingAudit(), func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(accountingContext, `UPDATE provider_calls SET duration_ns=1 WHERE provider_call_id='i1-call'`); err != nil {
			return err
		}
		var ignored int64
		cause = conn.QueryRowContext(accountingContext, `SELECT SUM(value) FROM (SELECT ? AS value UNION ALL SELECT 1)`, int64(math.MaxInt64)).Scan(&ignored)
		return fmt.Errorf("read checked aggregate: %w", cause)
	})
	failure := requireNumeric(t, err, "sqlite_integer")
	if failure.Operation != "overflow_probe" || !errors.Is(err, cause) {
		t.Fatal("missing context or SQLite cause", err)
	}
	var duration *int64
	if err := s.db.QueryRow(`SELECT duration_ns FROM provider_calls WHERE provider_call_id='i1-call'`).Scan(&duration); err != nil {
		t.Fatal(err)
	}
	if duration != nil || resultCount(t, s, "events") != 0 {
		t.Fatal("SQLite overflow did not roll back mutation/audit")
	}
}
