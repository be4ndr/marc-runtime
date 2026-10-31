package statestore

import (
	"context"

	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func journalSubject() AuditRow {
	cycle := 1
	return AuditRow{EventType: "opaque-fixture", Cycle: &cycle, ExecutionID: stringPointer("e1"), AttemptID: stringPointer("a1"), InvocationID: stringPointer("i1"), ActorRole: "ORCHESTRATOR", OccurredAt: semanticTimestamp, Details: json.RawMessage(`{"fixture":true}`)}
}

func seedJournalSubjects(t *testing.T, s *StateStore) {
	t.Helper()
	seedInvocation(t, s)
	execSQL(t, s.db, `
INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES ('r1',2,'fixture','now');
INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES ('e2','r1',2,'QA','QA','fixture','now');
INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES ('a2','e2','r1',2,1,'fixture','now'),('a3','e1','r1',1,2,'fixture','now');
INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch)
VALUES ('i2','a2','e2','r1',2,1,'QA','QA','fake','fake','high','fixture','now',1),('i3','a3','e1','r1',1,1,'DEVELOPER','IMPLEMENT','fake','fake','high','fixture','now',1);
INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES ('r2','RUNNING',1,'now','now');
INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES ('r2',1,'fixture','now');`)
}

func journalPool(ctx context.Context, s *StateStore, id, run string, audit ...AuditRow) error {
	return s.numericMutation(ctx, "journal fixture", run, audit, func(c *sql.Conn) error {
		_, err := c.ExecContext(ctx, `INSERT INTO evidence_artifacts VALUES(?,'other','sqlite',?,0,?,NULL,?)`, "journal-"+run+"-"+id, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", []byte{}, semanticTimestamp)
		return err
	})
}

func TestJournalPartialAncestry(t *testing.T) {
	s := testStore(t)
	seedJournalSubjects(t, s)
	ctx := orchestratorContext()
	for mask := 0; mask < 16; mask++ {
		row := journalSubject()
		if mask&1 == 0 {
			row.Cycle = nil
		}
		if mask&2 == 0 {
			row.ExecutionID = nil
		}
		if mask&4 == 0 {
			row.AttemptID = nil
		}
		if mask&8 == 0 {
			row.InvocationID = nil
		}
		if err := journalPool(ctx, s, fmt.Sprint(mask), "r1", row); err != nil {
			t.Fatal(mask, err)
		}
		page, err := s.ListEvents(ctx, "r1", int64(mask), 1)
		if err != nil || len(page) != 1 || page[0].Sequence != int64(mask+1) || !reflect.DeepEqual(page[0].AuditRow, row) {
			t.Fatalf("mask %d: page=%+v err=%v", mask, page, err)
		}
	}
}

func TestJournalInvalidRowsRollBackStateAndEntireBatch(t *testing.T) {
	cases := map[string]func(*AuditRow){
		"invocation-cycle":      func(r *AuditRow) { r.ExecutionID = nil; r.AttemptID = nil; r.InvocationID = stringPointer("i2") },
		"attempt-cycle":         func(r *AuditRow) { r.ExecutionID = nil; r.InvocationID = nil; r.AttemptID = stringPointer("a2") },
		"execution-cycle":       func(r *AuditRow) { r.AttemptID = nil; r.InvocationID = nil; r.ExecutionID = stringPointer("e2") },
		"invocation-execution":  func(r *AuditRow) { r.Cycle = nil; r.AttemptID = nil; r.ExecutionID = stringPointer("e2") },
		"attempt-execution":     func(r *AuditRow) { r.Cycle = nil; r.InvocationID = nil; r.ExecutionID = stringPointer("e2") },
		"invocation-attempt":    func(r *AuditRow) { r.Cycle = nil; r.ExecutionID = nil; r.AttemptID = stringPointer("a3") },
		"missing-invocation":    func(r *AuditRow) { r.InvocationID = stringPointer("absent") },
		"missing-attempt":       func(r *AuditRow) { r.AttemptID = stringPointer("absent") },
		"missing-execution":     func(r *AuditRow) { r.ExecutionID = stringPointer("absent") },
		"missing-cycle":         func(r *AuditRow) { n := 99; r.Cycle = &n; r.ExecutionID = nil; r.AttemptID = nil; r.InvocationID = nil },
		"zero-cycle":            func(r *AuditRow) { n := 0; r.Cycle = &n },
		"empty-id":              func(r *AuditRow) { r.InvocationID = stringPointer(" ") },
		"malformed-id":          func(r *AuditRow) { r.InvocationID = stringPointer("bad_id") },
		"invalid-utf8-details":  func(r *AuditRow) { r.Details = json.RawMessage([]byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}) },
		"oversized-detail-text": func(r *AuditRow) { r.Details = json.RawMessage(`{"value":"` + strings.Repeat("x", 257) + `"}`) },
		"blank-type":            func(r *AuditRow) { r.EventType = " \n" },
		"invalid-role":          func(r *AuditRow) { r.ActorRole = "WORKER" },
		"timestamp":             func(r *AuditRow) { r.OccurredAt = "now" },
		"json":                  func(r *AuditRow) { r.Details = json.RawMessage(`{`) },
		"trailing-json":         func(r *AuditRow) { r.Details = json.RawMessage(`{} {}`) },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			s := testStore(t)
			seedJournalSubjects(t, s)
			row := journalSubject()
			corrupt(&row)
			if err := journalPool(orchestratorContext(), s, "rejected", "r1", journalSubject(), row); err == nil {
				t.Fatal("invalid event accepted")
			}
			if resultCount(t, s, "events") != 0 || resultCount(t, s, "evidence_artifacts") != 0 {
				t.Fatal("partial journal/state survived")
			}
			if err := journalPool(orchestratorContext(), s, "valid", "r1", journalSubject()); err != nil {
				t.Fatal("failed transaction poisoned connection", err)
			}
		})
	}
	t.Run("cross-run", func(t *testing.T) {
		s := testStore(t)
		seedJournalSubjects(t, s)
		row := journalSubject()
		row.Cycle = nil
		row.ExecutionID = nil
		row.AttemptID = nil
		if err := journalPool(orchestratorContext(), s, "rejected", "r2", row); err == nil || resultCount(t, s, "events") != 0 || resultCount(t, s, "evidence_artifacts") != 0 {
			t.Fatal("cross-run event survived", err)
		}
	})
}

func TestJournalOrderingPaginationAndCancellation(t *testing.T) {
	s := testStore(t)
	seedJournalSubjects(t, s)
	ctx := orchestratorContext()
	first, second := journalSubject(), journalSubject()
	first.OccurredAt = "2026-10-06T00:00:00Z"
	second.OccurredAt = "2026-10-05T00:00:00Z"
	second.Details = nil
	if err := journalPool(ctx, s, "one", "r1", first, second); err != nil {
		t.Fatal(err)
	}
	other := AuditRow{EventType: "other-run", ActorRole: "LEAD", OccurredAt: semanticTimestamp, Details: json.RawMessage(`null`)}
	if err := journalPool(ctx, s, "two", "r2", other); err != nil {
		t.Fatal(err)
	}
	if err := journalPool(ctx, s, "three", "r1", first); err != nil {
		t.Fatal(err)
	}
	page, err := s.ListEvents(ctx, "r1", 0, 2)
	if err != nil || len(page) != 2 || page[0].Sequence != 1 || page[1].Sequence != 2 || page[1].Details != nil {
		t.Fatal(page, err)
	}
	page, err = s.ListEvents(ctx, "r1", page[1].Sequence, 2)
	if err != nil || len(page) != 1 || page[0].Sequence != 4 {
		t.Fatal(page, err)
	}
	page, err = s.ListEvents(ctx, "r2", 0, 1)
	if err != nil || len(page) != 1 || page[0].Sequence != 3 || string(page[0].Details) != "null" || page[0].InvocationID != nil {
		t.Fatal(page, err)
	}
	for _, run := range []string{"r1", "missing"} {
		page, err = s.ListEvents(ctx, run, 4, 1)
		if err != nil || len(page) != 0 {
			t.Fatal(page, err)
		}
	}
	for _, args := range []struct {
		run   string
		after int64
		limit int
	}{{"", 0, 1}, {"r1", -1, 1}, {"r1", 0, 0}, {"r1", 0, -1}, {"r1", 0, MaxJournalPageSize + 1}} {
		if _, err := s.ListEvents(ctx, args.run, args.after, args.limit); err == nil {
			t.Fatal("invalid page accepted", args)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ListEvents(cancelled, "r1", 0, 1); err == nil {
		t.Fatal("cancelled read succeeded")
	}
	if err := journalPool(cancelled, s, "cancelled", "r1", first); err == nil {
		t.Fatal("cancelled mutation succeeded")
	}
	if resultCount(t, s, "events") != 4 || resultCount(t, s, "evidence_artifacts") != 3 {
		t.Fatal("cancelled mutation persisted")
	}
	// Subjects can be historical; journal append does not require active ownership.
	if resultCount(t, s, "run_active_actor") != 0 {
		t.Fatal("journal selected an actor")
	}
}

func TestJournalConcurrentBatchesStayTogether(t *testing.T) {
	dir := privateTempDir(t)
	ctx := orchestratorContext()
	s, err := openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedInvocation(t, s)
	other, err := openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	const batches = 8
	start := make(chan struct{})
	results := make(chan error, batches)
	var wg sync.WaitGroup
	for n := 0; n < batches; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			store := s
			if n%2 == 1 {
				store = other
			}
			row := journalSubject()
			row.EventType = fmt.Sprint(n)
			results <- journalPool(ctx, store, fmt.Sprint(n), "r1", row, row)
		}(n)
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.ListEvents(ctx, "r1", 0, MaxJournalPageSize)
	if err != nil || len(page) != 2*batches {
		t.Fatal(page, err)
	}
	for n := 0; n < len(page); n += 2 {
		if page[n+1].Sequence != page[n].Sequence+1 || page[n].EventType != page[n+1].EventType {
			t.Fatal("interleaved audit batch", page)
		}
	}
}

func TestJournalRestartPreservesImmutableHistory(t *testing.T) {
	ctx := orchestratorContext()
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	dir := privateTempDir(t)
	s, err := openTestMigrations(ctx, dir, migrations)
	if err != nil {
		t.Fatal(err)
	}
	seedJournalSubjects(t, s)
	row := journalSubject()
	row.ExecutionID = nil
	row.AttemptID = nil
	row.Cycle = nil
	if err := journalPool(ctx, s, "legacy", "r1", row); err != nil {
		t.Fatal(err)
	}
	before, err := s.ListEvents(ctx, "r1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var checksums string
	if err := s.db.QueryRow("SELECT group_concat(checksum,'|') FROM (SELECT checksum FROM schema_migrations ORDER BY version)").Scan(&checksums); err != nil {
		t.Fatal(err)
	}
	s.Close()
	for restart := 0; restart < 2; restart++ {
		s, err = openTest(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		page, err := s.ListEvents(ctx, "r1", 0, 10)
		if err != nil || !reflect.DeepEqual(page, before) || s.Info().SchemaVersion != 1 {
			t.Fatal(page, err, s.Info())
		}
		var unchanged string
		if err := s.db.QueryRow("SELECT group_concat(checksum,'|') FROM (SELECT checksum FROM schema_migrations WHERE version<=7 ORDER BY version)").Scan(&unchanged); err != nil || unchanged != checksums {
			t.Fatal("history changed", err)
		}
		for _, stmt := range []string{"UPDATE events SET event_type='changed'", "DELETE FROM events"} {
			if _, err := s.db.Exec(stmt); err == nil {
				t.Fatal("append-only protection lost")
			}
		}
		if err := validateDatabase(ctx, s.db); err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
}
