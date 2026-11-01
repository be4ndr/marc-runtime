package statestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func orchestratorContext() context.Context {
	return WithMutationActor(context.Background(), "ORCHESTRATOR")
}

func privateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}
func openTest(ctx context.Context, dir string) (*StateStore, error) {
	return Open(ctx, dir, OpenOptions{AllowVolatileForTests: true})
}
func openTestMigrations(ctx context.Context, dir string, migrations []migration) (*StateStore, error) {
	return open(ctx, dir, migrations, OpenOptions{AllowVolatileForTests: true})
}

func testStore(t *testing.T) *StateStore {
	t.Helper()
	s, err := openTest(orchestratorContext(), privateTempDir(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func execSQL(t *testing.T, db *sql.DB, statement string, args ...any) {
	t.Helper()
	if _, err := db.Exec(statement, args...); err != nil {
		t.Fatal(err)
	}
}

func seedInvocation(t *testing.T, s *StateStore) {
	t.Helper()
	execSQL(t, s.db, `
INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES ('r1','RUNNING',1,'2026-10-05T00:00:00Z','2026-10-05T00:00:00Z');
INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES ('r1',1,'RUNNING','2026-10-05T00:00:00Z');
INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at) VALUES ('e1','r1',1,'DEVELOPER','IMPLEMENT','RUNNING','2026-10-05T00:00:00Z');
INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES ('a1','e1','r1',1,1,'RUNNING','2026-10-05T00:00:00Z');
INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch)
VALUES ('i1','a1','e1','r1',1,1,'DEVELOPER','IMPLEMENT','fake','fake-model','high','RUNNING','2026-10-05T00:00:00Z','content1',1);`)
	fixtureProviderCall(t, s, "i1", "r1", "fake-model")
}

func seedResult(t *testing.T, s *StateStore) {
	t.Helper()
	seedInvocation(t, s)
	execSQL(t, s.db, `INSERT INTO role_results(result_id,run_id,cycle_no,result_ordinal,execution_id,attempt_id,invocation_id,role,outcome,summary,content_identity,produced_at)
VALUES ('result1','r1',1,1,'e1','a1','i1','DEVELOPER','fixture','summary','content1','2026-10-05T00:00:00Z')`)
}

func TestFreshInitializationAndRestart(t *testing.T) {
	dir := filepath.Join(privateTempDir(t), "new-data")
	ctx := orchestratorContext()
	s, err := openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, DatabaseName)); err != nil {
		t.Fatal(err)
	}
	var runtimeVersion string
	if err := s.db.QueryRow("SELECT sqlite_version()").Scan(&runtimeVersion); err != nil {
		t.Fatal(err)
	}
	if err := validateSQLiteVersion(runtimeVersion); err != nil {
		t.Fatal(err)
	}
	if s.Info().SQLiteVersion != runtimeVersion || s.Info().SchemaVersion != 1 {
		t.Fatalf("unexpected runtime: %+v", s.Info())
	}
	t.Logf("SQLite runtime %s", s.Info().SQLiteVersion)
	tables := strings.Fields(`schema_migrations workflow_runs workflow_steps runs cycles executions attempts invocations lead_states lead_acceptance_criteria lead_finding_dispositions next_actions next_action_findings resource_requests resource_request_amounts role_results developer_remaining_work developer_blockers qa_checks findings finding_occurrences finding_occurrence_locations evidence_artifacts role_result_evidence finding_occurrence_evidence qa_check_evidence developer_blocker_evidence resource_allocations usage_receipts run_active_actor events run_current_semantic_state lead_state_bindings lead_finding_sources next_action_producers resource_request_sources resource_pools resource_pool_eligibility provider_calls provider_call_usage run_admission_policy run_resource_budgets execution_admission_deadlines invocation_admissions admission_resources admission_grants admission_dispatch_claims admission_dispatch_bounds admission_non_dispatch_proofs admission_controller_release_evidence run_hard_stops hard_stop_decisions hard_stop_usage_evidence admission_controller_release_bounds authority_proofs human_readiness_dispositions authority_proof_dependencies human_disposition_dependencies admission_contingency_authorizations run_agent_token_profiles admission_agent_token_limits admission_token_topup_denials run_budget_confirmations run_budget_increases hard_limit_requests hard_limit_decisions admission_closures admission_release_authorizations`)
	views := strings.Fields(`v_run_status v_active_actor v_invocation_usage v_finding_history v_pending_resource_requests v_cycle_summary v_resource_balance v_run_token_balance v_run_token_hard_stops v_current_admission_fences v_release_token_capacity`)
	indexes := strings.Fields(`executions_run_cycle_status invocations_status invocations_run_cycle role_results_run_cycle finding_occurrences_result usage_receipts_invocation_completeness resource_requests_pending events_run_sequence resource_requests_id_run usage_receipts_one_final resource_allocations_pool usage_receipts_call provider_calls_one_active`)
	for kind, names := range map[string][]string{"table": tables, "view": views, "index": indexes} {
		for _, name := range names {
			var count int
			if err := s.db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE type=? AND name=?", kind, name).Scan(&count); err != nil || count != 1 {
				t.Fatalf("missing %s %s: count=%d err=%v", kind, name, count, err)
			}
			if kind == "view" {
				rows, err := s.db.Query("SELECT * FROM " + name + " LIMIT 0")
				if err != nil {
					t.Fatal(err)
				}
				rows.Close()
			}
		}
	}
	var strictCount int
	if err := s.db.QueryRow("SELECT count(*) FROM pragma_table_list WHERE schema='main' AND name NOT GLOB 'sqlite_*' AND type='table' AND strict=1").Scan(&strictCount); err != nil || strictCount != len(tables) {
		t.Fatalf("STRICT table count %d, err=%v", strictCount, err)
	}
	var appliedAt, checksum string
	if err := s.db.QueryRow("SELECT applied_at, checksum FROM schema_migrations WHERE version=1").Scan(&appliedAt, &checksum); err != nil {
		t.Fatal(err)
	}
	if len(checksum) != 64 {
		t.Fatal("migration checksum missing")
	}
	seedInvocation(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Neither absent nor malformed metrics can influence restart.
	if err := os.WriteFile(filepath.Join(dir, "metrics.json"), []byte("invalid observational JSON"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err = openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var count int
	var restartedAt string
	if err := s.db.QueryRow("SELECT count(*), min(applied_at) FROM schema_migrations").Scan(&count, &restartedAt); err != nil {
		t.Fatal(err)
	}
	if count != 1 || restartedAt != appliedAt {
		t.Fatal("migration reapplied on restart")
	}
	if err := s.db.QueryRow("SELECT count(*) FROM invocations").Scan(&count); err != nil || count != 1 {
		t.Fatal("restart lost existing state", err)
	}
}

func TestPrivateSQLiteFilesOnCreateAndReopen(t *testing.T) {
	dir := privateTempDir(t)
	ctx := orchestratorContext()
	s, err := openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	seedInvocation(t, s) // keeps the WAL path exercised after schema creation
	dbPath := s.Info().Path
	checkPrivate := func(path string, required bool) {
		t.Helper()
		info, err := os.Stat(path)
		if os.IsNotExist(err) && !required {
			return
		}
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("%s mode=%#o, want 0600", path, info.Mode().Perm())
		}
	}
	checkPrivate(dbPath, true)
	for _, suffix := range []string{"-wal", "-shm"} {
		checkPrivate(dbPath+suffix, false)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dbPath, 0644); err != nil {
		t.Fatal(err)
	}
	s, err = openTest(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	checkPrivate(dbPath, true)
	for _, suffix := range []string{"-wal", "-shm"} {
		checkPrivate(dbPath+suffix, false)
	}
}

func TestPragmasAndForeignKeysOnReplacementConnection(t *testing.T) {
	s := testStore(t)
	ctx := orchestratorContext()
	for i := 0; i < 2; i++ {
		if err := validatePragmas(ctx, s.db); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec("INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES ('missing',1,'RUNNING','now')"); err == nil {
			t.Fatal("foreign key not enforced")
		}
		conn, err := s.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Raw(func(any) error { return driver.ErrBadConn }); err != driver.ErrBadConn {
			t.Fatal(err)
		}
		conn.Close()
	}
}

func TestMigrationsAcquireImmediateWriterLockBeforeHistory(t *testing.T) {
	s := testStore(t)
	ctx := orchestratorContext()
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately configure a deferred driver transaction and fail immediately
	// on lock contention. The migration runner itself must acquire the writer lock,
	// even on restart with no pending DDL or migration-history writes.
	db, err := sql.Open("sqlite", s.Info().Path+"?_txlock=deferred&_pragma=busy_timeout(0)&_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	writer, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	// A deferred BEGIN alone can succeed while another connection owns the writer
	// lock. This control distinguishes the intended migration behavior.
	deferred, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := deferred.Rollback(); err != nil {
		t.Fatal(err)
	}
	err = migrate(ctx, db, migrations)
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code() != sqlite3.SQLITE_BUSY ||
		!strings.Contains(err.Error(), "begin immediate migration transaction") {
		t.Fatalf("migration did not fail at immediate writer-lock acquisition: %v", err)
	}
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := migrate(ctx, db, migrations); err != nil {
		t.Fatal("migration did not recover after writer lock release:", err)
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&count); err != nil || count != len(migrations) {
		t.Fatalf("history changed during lock contention: count=%d err=%v", count, err)
	}
}

func TestActiveActorAllowsOnlySemanticRoles(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	for _, role := range []string{"LEAD", "DEVELOPER", "QA", "REVIEWER"} {
		execSQL(t, s.db, "INSERT INTO run_active_actor VALUES ('r1',?,NULL,1,'now',0)", role)
		execSQL(t, s.db, "DELETE FROM run_active_actor WHERE run_id='r1'")
	}
	if _, err := s.db.Exec("INSERT INTO run_active_actor VALUES ('r1','ORCHESTRATOR',NULL,1,'now',0)"); err == nil {
		t.Fatal("Orchestrator accepted as a semantic actor")
	}
	execSQL(t, s.db, "INSERT INTO run_active_actor VALUES ('r1','LEAD',NULL,1,'now',0)")
	if _, err := s.db.Exec("UPDATE run_active_actor SET active_role='ORCHESTRATOR',ownership_epoch=2"); err == nil {
		t.Fatal("semantic ownership transferred to Orchestrator")
	}
	// Control-plane execution and audit roles still permit Orchestrator.
	execSQL(t, s.db, `INSERT INTO executions(execution_id,run_id,cycle_no,role,purpose,status,created_at)
VALUES ('recovery','r1',1,'ORCHESTRATOR','RECOVER','RUNNING','now')`)
	execSQL(t, s.db, `INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at)
VALUES ('recovery-attempt','recovery','r1',1,1,'RUNNING','now')`)
	execSQL(t, s.db, `INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,ownership_epoch)
VALUES ('recovery-invocation','recovery-attempt','recovery','r1',1,1,'ORCHESTRATOR','RECOVER','fake','fake-model','high','RUNNING','now',1)`)
	execSQL(t, s.db, `INSERT INTO events(run_id,event_type,invocation_id,actor_role,occurred_at)
VALUES ('r1','fixture','recovery-invocation','ORCHESTRATOR','now')`)
}

func TestStructuralConstraints(t *testing.T) {
	s := testStore(t)
	seedResult(t, s)
	execSQL(t, s.db, `INSERT INTO findings(run_id,finding_id,finding_ordinal,origin_cycle,origin_result_id,blocking,summary,created_at) VALUES ('r1','fnd-000001',1,1,'result1',1,'issue','now')`)
	execSQL(t, s.db, `INSERT INTO run_active_actor VALUES ('r1','DEVELOPER','i1',1,'now',0)`)
	cases := map[string]string{
		"invalid epoch":                   `UPDATE run_active_actor SET ownership_epoch=0`,
		"invalid revision":                `UPDATE runs SET revision=-1`,
		"strict type":                     `UPDATE run_active_actor SET ownership_epoch='wrong-type'`,
		"actor mutation without revision": `UPDATE run_active_actor SET active_invocation_id=NULL`,
		"actor invocation epoch mismatch": `UPDATE run_active_actor SET ownership_epoch=2`,
		"two actors":                      `INSERT INTO run_active_actor VALUES ('r1','LEAD',NULL,1,'now',0)`,
		"two results for invocation":      `INSERT INTO role_results SELECT 'result2',run_id,cycle_no,2,execution_id,attempt_id,invocation_id,role,outcome,summary,content_identity,produced_at FROM role_results`,
		"duplicate finding ordinal":       `INSERT INTO findings VALUES ('r1','fnd-000002',1,1,'result1',1,'other','now')`,
		"immutable finding origin":        `UPDATE findings SET origin_cycle=2`,
		"inline and file":                 `INSERT INTO evidence_artifacts VALUES ('bad','raw','sqlite','` + strings.Repeat("a", 64) + `',1,x'01','path','now')`,
		"file without path":               `INSERT INTO evidence_artifacts VALUES ('bad','raw','file','` + strings.Repeat("a", 64) + `',1,NULL,NULL,'now')`,
		"inline without bytes":            `INSERT INTO evidence_artifacts VALUES ('bad','raw','sqlite','` + strings.Repeat("a", 64) + `',0,NULL,NULL,'now')`,
		"incorrect byte length":           `INSERT INTO evidence_artifacts VALUES ('bad','raw','sqlite','` + strings.Repeat("a", 64) + `',2,x'01',NULL,'now')`,
		"bad digest":                      `INSERT INTO evidence_artifacts VALUES ('bad','raw','file','wrong',1,NULL,'path','now')`,
		"cached subset":                   `INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,completeness,input_tokens,cached_input_tokens,reported_at) VALUES ('bad','i1','i1-call','partial',1,2,'now')`,
		"reasoning subset":                `INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,completeness,output_tokens,reasoning_tokens,reported_at) VALUES ('bad','i1','i1-call','partial',1,2,'now')`,
		"wrong work formula":              `INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,completeness,input_tokens,output_tokens,work_tokens,reported_at) VALUES ('bad','i1','i1-call','final',10,5,99,'now')`,
		"wrong spend formula":             `INSERT INTO usage_receipts(receipt_id,invocation_id,provider_call_id,completeness,input_tokens,cached_input_tokens,output_tokens,spend_tokens,reported_at) VALUES ('bad','i1','i1-call','final',10,2,5,99,'now')`,
		"malformed event JSON":            `INSERT INTO events(run_id,event_type,actor_role,occurred_at,details_json) VALUES ('r1','fixture','ORCHESTRATOR','now','bad json')`,
		"restrict historical deletion":    `DELETE FROM invocations WHERE invocation_id='i1'`,
	}
	for name, statement := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := s.db.Exec(statement); err == nil {
				t.Fatal("constraint accepted invalid row")
			}
		})
	}
	execSQL(t, s.db, `UPDATE run_active_actor SET active_invocation_id=NULL, active_role='LEAD',ownership_epoch=2, revision=revision+1`)
	if _, err := s.db.Exec(`UPDATE run_active_actor SET ownership_epoch=1`); err == nil {
		t.Fatal("epoch decreased")
	}
	// Both valid storage forms and an empty (populated) BLOB are supported.
	for _, evidence := range []struct {
		id, kind string
		blob     any
		path     any
		size     int
	}{
		{"inline", "sqlite", []byte{1}, nil, 1}, {"empty", "sqlite", []byte{}, nil, 0}, {"external", "file", nil, "artifacts/raw.txt", 5},
	} {
		execSQL(t, s.db, `INSERT INTO evidence_artifacts VALUES (?,'raw',?,?,?,?,?,'now')`, evidence.id, evidence.kind, strings.Repeat("a", 64), evidence.size, evidence.blob, evidence.path)
	}
	execSQL(t, s.db, `INSERT INTO role_result_evidence VALUES ('result1','inline','src/example.go',1,2)`)
	if _, err := s.db.Exec(`INSERT INTO role_result_evidence VALUES ('result1','external','src/example.go',5,2)`); err == nil {
		t.Fatal("reversed evidence lines accepted")
	}
	execSQL(t, s.db, `INSERT INTO events(run_id,event_type,invocation_id,actor_role,occurred_at) VALUES ('r1','fixture','i1','ORCHESTRATOR','now')`)
	for _, statement := range []string{"UPDATE events SET event_type='edited'", "DELETE FROM events"} {
		if _, err := s.db.Exec(statement); err == nil {
			t.Fatal("audit history mutated")
		}
	}
}

func TestNullUsageAndMultipleReceipts(t *testing.T) {
	s := testStore(t)
	seedInvocation(t, s)
	var receiptID, work, spend sql.NullString
	if err := s.db.QueryRow("SELECT receipt_id,work_tokens,spend_tokens FROM v_invocation_usage WHERE invocation_id='i1'").Scan(&receiptID, &work, &spend); err != nil {
		t.Fatal(err)
	}
	if receiptID.Valid || work.Valid || spend.Valid {
		t.Fatal("invented absent usage")
	}
	acceptUsage(t, s, receipt("u1", "unknown"))
	r := receipt("u2", "final")
	r.InputTokens = counter(10)
	r.CachedInputTokens = counter(2)
	r.OutputTokens = counter(5)
	acceptUsage(t, s, r)
	if err := s.db.QueryRow("SELECT work_tokens,spend_tokens FROM usage_receipts WHERE receipt_id='u1'").Scan(&work, &spend); err != nil {
		t.Fatal(err)
	}
	if work.Valid || spend.Valid {
		t.Fatal("unknown usage coerced to zero")
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM v_invocation_usage WHERE invocation_id='i1'").Scan(&count); err != nil || count != 2 {
		t.Fatalf("receipt provenance collapsed: count %d err %v", count, err)
	}
}

func TestCrossRunProvenanceRejected(t *testing.T) {
	s := testStore(t)
	seedResult(t, s)
	execSQL(t, s.db, `INSERT INTO runs(run_id,status,current_cycle,created_at,updated_at) VALUES ('r2','RUNNING',1,'now','now'); INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES ('r2',1,'RUNNING','now')`)
	for _, statement := range []string{
		`INSERT INTO run_active_actor VALUES ('r2','DEVELOPER','i1',1,'now',0)`,
		`INSERT INTO attempts(attempt_id,execution_id,run_id,cycle_no,attempt_ordinal,status,created_at) VALUES ('bad','e1','r2',1,2,'RUNNING','now')`,
		`INSERT INTO resource_allocations VALUES ('bad','r2','e1','a1','WORK',10,'fixture','now')`,
		`INSERT INTO events(run_id,event_type,invocation_id,actor_role,occurred_at) VALUES ('r2','fixture','i1','ORCHESTRATOR','now')`,
		`INSERT INTO findings VALUES ('r2','fnd-000001',1,1,'result1',1,'issue','now')`,
	} {
		if _, err := s.db.Exec(statement); err == nil {
			t.Fatalf("cross-run provenance accepted: %s", statement)
		}
	}
}

func TestStableFindingsAndEvidenceLinks(t *testing.T) {
	s := testStore(t)
	seedResult(t, s)
	execSQL(t, s.db, `INSERT INTO findings VALUES ('r1','fnd-000001',1,1,'result1',1,'stable issue','now');
INSERT INTO finding_occurrences VALUES (1,'r1','fnd-000001','result1','now');
INSERT INTO invocations(invocation_id,attempt_id,execution_id,run_id,cycle_no,invocation_ordinal,role,purpose,provider,model,reasoning_effort,status,started_at,content_identity,ownership_epoch)
VALUES ('i2','a1','e1','r1',1,2,'DEVELOPER','FINALIZE_IMPLEMENTATION','fake','fake-model','high','COMPLETED','now','content1',1);
INSERT INTO role_results(result_id,run_id,cycle_no,result_ordinal,execution_id,attempt_id,invocation_id,role,outcome,summary,content_identity,produced_at)
VALUES ('result2','r1',1,2,'e1','a1','i2','DEVELOPER','fixture','summary','content1','now');
INSERT INTO finding_occurrences VALUES (2,'r1','fnd-000001','result2','later');`)
	var origin string
	var count int
	if err := s.db.QueryRow("SELECT origin_result_id FROM findings").Scan(&origin); err != nil || origin != "result1" {
		t.Fatal("origin changed", err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM v_finding_history").Scan(&count); err != nil || count != 2 {
		t.Fatal("history missing", err)
	}
	execSQL(t, s.db, `INSERT INTO evidence_artifacts VALUES ('proof','raw','sqlite',?,1,x'01',NULL,'now')`, strings.Repeat("a", 64))
	execSQL(t, s.db, `INSERT INTO qa_checks VALUES ('result1','check1',1,'fixture','checked');
INSERT INTO developer_blockers VALUES ('result1','blocked');
INSERT INTO role_result_evidence(result_id,evidence_id) VALUES ('result1','proof');
INSERT INTO finding_occurrence_evidence(occurrence_id,evidence_id) VALUES (2,'proof');
INSERT INTO qa_check_evidence(result_id,check_id,evidence_id) VALUES ('result1','check1','proof');
INSERT INTO developer_blocker_evidence(result_id,evidence_id) VALUES ('result1','proof')`)
	// Fixture checks structural relationships only; P1.6 will validate role payloads.
	if _, err := s.db.Exec(`INSERT INTO qa_check_evidence(result_id,check_id,evidence_id) VALUES ('result1','missing','proof')`); err == nil {
		t.Fatal("missing QA check link accepted")
	}
	if _, err := s.db.Exec(`DELETE FROM evidence_artifacts WHERE evidence_id='proof'`); err == nil {
		t.Fatal("referenced proof deleted")
	}
}

func TestMigrationFailureRollsBackAndNeverReturnsReady(t *testing.T) {
	ctx := orchestratorContext()
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	bad := migration{version: len(migrations) + 1, description: "failure_fixture", sql: "CREATE TABLE rollback_fixture (id INTEGER) STRICT; INVALID SQL;", checksum: strings.Repeat("a", 64)}
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			dir := privateTempDir(t)
			if existing {
				s, err := openTest(ctx, dir)
				if err != nil {
					t.Fatal(err)
				}
				seedInvocation(t, s)
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			s, err := openTestMigrations(ctx, dir, append(migrations, bad))
			if err == nil || s != nil || !strings.Contains(err.Error(), fmt.Sprintf("migration %03d", bad.version)) {
				t.Fatalf("failed migration reported ready: store=%v error=%v", s, err)
			}
			db, err := sql.Open("sqlite", databaseDSN(filepath.Join(dir, DatabaseName)))
			if err != nil {
				t.Fatal(err)
			}
			var count int
			if err := db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name='rollback_fixture'").Scan(&count); err != nil || count != 0 {
				t.Fatal("failed migration persisted partial schema", err)
			}
			if !existing {
				if err := db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name='schema_migrations'").Scan(&count); err != nil || count != 0 {
					t.Fatal("fresh migration history survived rollback", err)
				}
			} else {
				if err := db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&count); err != nil || count != len(migrations) {
					t.Fatal("failed migration history recorded", err)
				}
				if err := db.QueryRow("SELECT count(*) FROM invocations").Scan(&count); err != nil || count != 1 {
					t.Fatal("prior data lost", err)
				}
			}
			db.Close()
			s, err = openTest(ctx, dir)
			if err != nil {
				t.Fatal("clean retry failed", err)
			}
			s.Close()
		})
	}
}

func TestPendingMigrationAndVersionInspection(t *testing.T) {
	dir := privateTempDir(t)
	s, err := openTest(orchestratorContext(), dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	body := "CREATE TABLE later_fixture (id INTEGER PRIMARY KEY) STRICT;"
	migrations = append(migrations, migration{len(migrations) + 1, "later_fixture", body, fmt.Sprintf("%x", sha256.Sum256([]byte(body)))})
	for i := 0; i < 2; i++ {
		s, err := openTestMigrations(orchestratorContext(), dir, migrations)
		if err != nil {
			t.Fatal(err)
		}
		var versions string
		if err := s.db.QueryRow("SELECT group_concat(version, ',') FROM (SELECT version FROM schema_migrations ORDER BY version)").Scan(&versions); err != nil || versions != "1,2" {
			t.Fatalf("versions %q, err %v", versions, err)
		}
		if s.Info().SchemaVersion != len(migrations) {
			t.Fatal("incorrect version")
		}
		s.Close()
	}
	if s, err := openTest(orchestratorContext(), dir); err == nil || s != nil {
		t.Fatal("older binary accepted unknown migration")
	}
}

func TestInvalidSchemaFailsClosed(t *testing.T) {
	for name, damage := range map[string]string{
		"missing index":          "DROP INDEX executions_run_cycle_status",
		"changed view":           "DROP VIEW v_run_status; CREATE VIEW v_run_status AS SELECT run_id FROM runs",
		"checksum":               "UPDATE schema_migrations SET checksum='" + strings.Repeat("0", 64) + "'",
		"missing history":        "DELETE FROM schema_migrations",
		"extra object":           "CREATE TABLE alien (id INTEGER) STRICT",
		"foreign key corruption": "PRAGMA foreign_keys=OFF; INSERT INTO cycles(run_id,cycle_no,status,created_at) VALUES ('missing',1,'RUNNING','now')",
	} {
		t.Run(name, func(t *testing.T) {
			dir := privateTempDir(t)
			s, err := openTest(orchestratorContext(), dir)
			if err != nil {
				t.Fatal(err)
			}
			execSQL(t, s.db, damage)
			s.Close()
			if s, err := openTest(orchestratorContext(), dir); err == nil || s != nil {
				t.Fatal("invalid authoritative schema reported ready")
			}
		})
	}
}

func TestLocalStorageAndVersionGuards(t *testing.T) {
	for _, kind := range []int64{0x6969, 0xff534d42, 0x01021997, 0x65735546} { // NFS, CIFS, 9p, FUSE
		if err := validateFilesystemType(kind); err == nil {
			t.Fatalf("unsupported filesystem %#x accepted", kind)
		}
	}
	for _, path := range []string{"/mnt/c", "/mnt/c/data", "/mnt/d/data"} {
		if !isWindowsMount(path) {
			t.Fatal("Windows mount accepted", path)
		}
	}
	if isWindowsMount("/mnt/control-plane") {
		t.Fatal("local mount misclassified")
	}
	if _, err := openTest(orchestratorContext(), ""); err == nil {
		t.Fatal("empty data directory accepted")
	}
	dir := privateTempDir(t)
	if err := os.Symlink("/mnt/c", filepath.Join(dir, "alias")); err != nil {
		t.Fatal(err)
	}
	if _, err := openTest(orchestratorContext(), filepath.Join(dir, "alias", "data")); err == nil {
		t.Fatal("Windows symlink accepted")
	}
	for _, version := range []string{"3.51.2", "3.50.9", "invalid"} {
		if err := validateSQLiteVersion(version); err == nil {
			t.Fatal("unsafe version accepted", version)
		}
	}
	for _, version := range []string{"3.51.3", "3.53.0", "3.54.1"} {
		if err := validateSQLiteVersion(version); err != nil {
			t.Fatal(err)
		}
	}
}
