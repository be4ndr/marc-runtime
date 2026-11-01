package statestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"marc-runtime/internal/numeric"
	"strings"

	"marc-runtime/internal/identity"
)

// AuditRow is controller-supplied journal data. EventType and Details remain
// opaque; P2.4 owns EventRecord semantics. The domain operation supplies RunID.
// Optional ancestry identifies the event's subject, not necessarily its actor.
type AuditRow struct {
	EventType    string
	Cycle        *int
	ExecutionID  *string
	AttemptID    *string
	InvocationID *string
	ActorRole    string
	OccurredAt   string
	Details      json.RawMessage
}

// JournalEntry is a persisted audit row, ordered by the installation sequence.
// It is observational and cannot select current state or authorize execution.
type JournalEntry struct {
	Sequence int64
	RunID    string
	AuditRow
}

const MaxJournalPageSize = 1000

// appendAudit runs only inside an authoritative mutation's writer transaction.
// SQL validates every supplied ancestry field, including partial ancestry. NULL
// stays NULL; neither current selection nor composite IDs fill omitted fields.
func appendAudit(ctx context.Context, conn *sql.Conn, runID string, audit []AuditRow) error {
	if err := identity.CheckID("run_id", runID); err != nil {
		return err
	}
	if len(audit) > 0 {
		var sequence int64
		if err := conn.QueryRowContext(ctx, `SELECT MAX(n) FROM (SELECT COALESCE(MAX(event_seq),0) n FROM events UNION ALL SELECT COALESCE(MAX(seq),0) n FROM sqlite_sequence WHERE name='events')`).Scan(&sequence); err != nil {
			return err
		}
		if sequence < 0 {
			return numeric.Invalid("event_sequence", "corrupt_state", "negative journal sequence")
		}
		if _, err := checkedAdd(sequence, int64(len(audit))); err != nil {
			return numeric.WithContext(err, numeric.Context{Domain: "event_sequence"})
		}
	}
	for n, row := range audit {
		if err := identity.CheckText("event_type", row.EventType); err != nil {
			return fmt.Errorf("audit row %d: %w", n+1, err)
		}
		if err := identity.CheckText("actor_role", row.ActorRole); err != nil {
			return fmt.Errorf("audit row %d: %w", n+1, err)
		}
		if strings.TrimSpace(row.EventType) == "" {
			return fmt.Errorf("audit row %d: empty event type", n+1)
		}
		if _, err := canonicalTime(row.OccurredAt); err != nil {
			return fmt.Errorf("audit row %d timestamp: %w", n+1, err)
		}
		for _, pair := range []struct {
			name string
			id   *string
		}{{"execution_id", row.ExecutionID}, {"attempt_id", row.AttemptID}, {"invocation_id", row.InvocationID}} {
			if pair.id != nil {
				if err := identity.CheckID(pair.name, *pair.id); err != nil {
					return fmt.Errorf("audit row %d: %w", n+1, err)
				}
			}
		}
		var details any
		if len(row.Details) > 0 {
			if err := identity.CheckJSONEnvelope(row.Details); err != nil {
				return fmt.Errorf("audit row %d: %w", n+1, err)
			}
			if !json.Valid(row.Details) {
				return fmt.Errorf("audit row %d: invalid details JSON", n+1)
			}
			details = string(row.Details)
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO events(run_id,event_type,cycle_no,execution_id,attempt_id,invocation_id,actor_role,occurred_at,details_json) VALUES (?,?,?,?,?,?,?,?,?)`, runID, row.EventType, row.Cycle, row.ExecutionID, row.AttemptID, row.InvocationID, row.ActorRole, row.OccurredAt, details); err != nil {
			return fmt.Errorf("append audit row %d: %w", n+1, err)
		}
	}
	return nil
}

// ListEvents reads one bounded page for a run from a single SQLite snapshot.
// afterSequence is exclusive; gaps and sequences belonging to other runs are
// normal. A missing run or exhausted page returns an empty slice. Timestamps do
// not determine order. Callers continue with the last returned Sequence.
func (s *StateStore) ListEvents(ctx context.Context, runID string, afterSequence int64, limit int) ([]JournalEntry, error) {
	if identity.CheckID("run_id", runID) != nil || afterSequence < 0 || limit < 1 || limit > MaxJournalPageSize {
		return nil, fmt.Errorf("invalid journal run, cursor or page size")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT event_seq,run_id,event_type,cycle_no,execution_id,attempt_id,invocation_id,actor_role,occurred_at,details_json FROM events WHERE run_id=? AND event_seq>? ORDER BY event_seq LIMIT ?`, runID, afterSequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]JournalEntry, 0)
	for rows.Next() {
		var entry JournalEntry
		var details sql.NullString
		if err := rows.Scan(&entry.Sequence, &entry.RunID, &entry.EventType, &entry.Cycle, &entry.ExecutionID, &entry.AttemptID, &entry.InvocationID, &entry.ActorRole, &entry.OccurredAt, &details); err != nil {
			return nil, err
		}
		if details.Valid {
			entry.Details = json.RawMessage(details.String)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}
