package statestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

type sqliteProcessRequest struct {
	Op    string
	Fence SemanticFence
}

type sqliteProcessReply struct {
	Error  string
	Owner  SemanticFence
	Events int
}

func TestSQLiteStateProcess(t *testing.T) {
	dir := os.Getenv("ACP_SQLITE_STATE_PROCESS")
	if dir == "" {
		return
	}
	if err := serveSQLiteTestProcess(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func serveSQLiteTestProcess(dir string) (err error) {
	ctx, stop := signal.NotifyContext(orchestratorContext(), syscall.SIGTERM)
	defer stop()
	s, err := openTest(ctx, dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	in, out := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	if err := out.Encode(sqliteProcessReply{}); err != nil {
		return err
	}
	for {
		var q sqliteProcessRequest
		if err := in.Decode(&q); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		var reply sqliteProcessReply
		var opErr error
		switch q.Op {
		case "create":
			opErr = s.CreateWorkflow(ctx, WorkflowSpec{RunID: "shared", Goal: "Multiprocess durability", ContentIdentity: "content", StartedAt: "2026-10-19T00:00:00Z", HardDeadline: "2026-10-20T00:00:00Z", NormalTokens: 1000, Confirmation: HumanBudgetApproval{ApprovalID: "approval", InteractionID: "interaction", Prompt: "Approve 1000 normal WORK tokens?", Response: "I approve 1000 normal WORK tokens.", Decision: "APPROVE"}})
		case "clear":
			opErr = s.ClearActiveInvocation(ctx, "shared", OwnershipOptions{Expected: q.Fence, ChangedAt: ownershipTimestamp, Audit: []AuditRow{{EventType: "multiprocess_transition", ActorRole: "ORCHESTRATOR", OccurredAt: ownershipTimestamp}}})
		case "read":
			var w WorkflowSnapshot
			w, opErr = s.GetWorkflow(ctx, "shared")
			reply.Owner = w.Owner
			if opErr == nil {
				var events []JournalEntry
				events, opErr = s.ListEvents(ctx, "shared", 0, MaxJournalPageSize)
				reply.Events = len(events)
			}
		case "check":
			opErr = checkSQLiteTestDatabase(ctx, s)
		case "cancel":
			opErr = s.commitMutation(ctx, "shared", nil, func(c *sql.Conn) error {
				if _, err := c.ExecContext(ctx, "UPDATE run_active_actor SET revision=revision+1 WHERE run_id='shared'"); err != nil {
					return err
				}
				if err := out.Encode(sqliteProcessReply{}); err != nil {
					return err
				}
				<-ctx.Done()
				return ctx.Err()
			})
			if !errors.Is(opErr, context.Canceled) {
				return fmt.Errorf("expected cancellation during mutation, got %v", opErr)
			}
			return nil // Existing transaction cleanup must roll back before Close.
		case "hold":
			// Force dirty pages into WAL before SIGKILL, through the existing
			// transaction boundary. No schema or durable contract is changed.
			if _, err := s.db.ExecContext(ctx, "PRAGMA cache_size=1"); err != nil {
				return err
			}
			return s.commitMutation(ctx, "shared", nil, func(c *sql.Conn) error {
				if _, err := c.ExecContext(ctx, "UPDATE run_active_actor SET revision=revision+1 WHERE run_id='shared'"); err != nil {
					return err
				}
				if _, err := c.ExecContext(ctx, `INSERT INTO evidence_artifacts VALUES('uncommitted','test','sqlite',?,2097152,zeroblob(2097152),NULL,'now')`, strings.Repeat("0", 64)); err != nil {
					return err
				}
				if err := out.Encode(sqliteProcessReply{}); err != nil {
					return err
				}
				var next sqliteProcessRequest
				_ = in.Decode(&next) // Parent kills us while the transaction is open.
				return fmt.Errorf("test transaction must not commit")
			})
		default:
			return fmt.Errorf("unknown test operation %q", q.Op)
		}
		if opErr != nil {
			reply.Error = opErr.Error()
		}
		if err := out.Encode(reply); err != nil {
			return err
		}
	}
}

func checkSQLiteTestDatabase(ctx context.Context, s *StateStore) error {
	if err := validatePragmas(ctx, s.db); err != nil {
		return err
	}
	for _, pragma := range []string{"integrity_check", "foreign_key_check"} {
		rows, err := s.db.QueryContext(ctx, "PRAGMA "+pragma)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			count++
			if pragma == "foreign_key_check" {
				rows.Close()
				return fmt.Errorf("foreign key violation")
			}
			var result string
			if err := rows.Scan(&result); err != nil || result != "ok" {
				rows.Close()
				return fmt.Errorf("integrity_check: %s: %v", result, err)
			}
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil {
			return err
		}
		if pragma == "integrity_check" && count != 1 {
			return fmt.Errorf("integrity_check returned %d rows", count)
		}
	}
	var count int
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM evidence_artifacts WHERE evidence_id='uncommitted'").Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("uncommitted evidence survived crash")
	}
	return nil
}

type sqliteTestProcess struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *json.Decoder
}

func startSQLiteTestProcess(t *testing.T, dir string) *sqliteTestProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	p := &sqliteTestProcess{cmd: exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSQLiteStateProcess$")}
	p.cmd.Env = append(os.Environ(), "ACP_SQLITE_STATE_PROCESS="+dir)
	p.cmd.Stderr = os.Stderr
	var err error
	p.in, err = p.cmd.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	out, err := p.cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	p.out = json.NewDecoder(out)
	if err := p.cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		p.in.Close()
		if p.cmd.ProcessState == nil {
			_ = p.cmd.Wait()
		}
	})
	var ready sqliteProcessReply
	if err := p.out.Decode(&ready); err != nil {
		t.Fatal("process startup:", err)
	}
	return p
}

func (p *sqliteTestProcess) send(t *testing.T, q sqliteProcessRequest) {
	t.Helper()
	if err := json.NewEncoder(p.in).Encode(q); err != nil {
		t.Fatal(err)
	}
}

func (p *sqliteTestProcess) receive(t *testing.T) sqliteProcessReply {
	t.Helper()
	var reply sqliteProcessReply
	if err := p.out.Decode(&reply); err != nil {
		t.Fatal(err)
	}
	return reply
}

func (p *sqliteTestProcess) call(t *testing.T, q sqliteProcessRequest) sqliteProcessReply {
	t.Helper()
	p.send(t, q)
	reply := p.receive(t)
	if reply.Error != "" {
		t.Fatal(reply.Error)
	}
	return reply
}

func (p *sqliteTestProcess) close(t *testing.T) {
	t.Helper()
	if err := p.in.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Wait(); err != nil {
		t.Fatal("process close:", err)
	}
}

func TestSQLiteMultiprocessDurabilityAndRecovery(t *testing.T) {
	dir := privateTempDir(t)
	a := startSQLiteTestProcess(t, dir)
	a.call(t, sqliteProcessRequest{Op: "create"})
	b := startSQLiteTestProcess(t, dir)
	// An acknowledged creation replayed through a peer must add no authority
	// or journal rows. The revision/event assertions below cover both stores.
	b.call(t, sqliteProcessRequest{Op: "create"})
	assertState := func(p *sqliteTestProcess, revision int64) SemanticFence {
		t.Helper()
		r := p.call(t, sqliteProcessRequest{Op: "read"})
		if r.Owner.ActorRevision != revision || r.Owner.OwnershipEpoch != 1 || r.Owner.Role != "LEAD" || r.Events != int(revision)+1 {
			t.Fatalf("divergent committed state: %+v, want revision %d and %d events", r, revision, revision+1)
		}
		return r.Owner
	}
	for revision := int64(0); revision < 8; revision++ {
		owner := assertState(a, revision)
		assertState(b, revision)
		q := sqliteProcessRequest{Op: "clear", Fence: owner}
		// Both OS processes race the same expected revision. Exactly one must
		// commit; the loser must reject the fence, with no extra audit event.
		a.send(t, q)
		b.send(t, q)
		x, y := a.receive(t), b.receive(t)
		if (x.Error == "") == (y.Error == "") || x.Error != "" && !strings.Contains(x.Error, "stale") || y.Error != "" && !strings.Contains(y.Error, "stale") {
			t.Fatalf("concurrent fencing: a=%+v b=%+v", x, y)
		}
		assertState(a, revision+1)
		assertState(b, revision+1)
	}
	a.close(t)
	owner := assertState(b, 8)
	b.call(t, sqliteProcessRequest{Op: "clear", Fence: owner})
	assertState(b, 9)
	b.close(t)
	a = startSQLiteTestProcess(t, dir)
	owner = assertState(a, 9) // Every connection was closed before restart.
	a.call(t, sqliteProcessRequest{Op: "check"})
	a.call(t, sqliteProcessRequest{Op: "clear", Fence: owner})
	a.call(t, sqliteProcessRequest{Op: "hold"})
	if err := a.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := a.cmd.Wait(); err == nil {
		t.Fatal("SIGKILL unexpectedly succeeded")
	}
	a = startSQLiteTestProcess(t, dir)
	owner = assertState(a, 10) // Last acknowledged commit survives; dirty write does not.
	a.call(t, sqliteProcessRequest{Op: "check"})
	b = startSQLiteTestProcess(t, dir)
	assertState(b, 10)
	b.call(t, sqliteProcessRequest{Op: "clear", Fence: owner})
	assertState(a, 11)
	a.call(t, sqliteProcessRequest{Op: "check"})
	b.call(t, sqliteProcessRequest{Op: "check"})
	// Graceful cancellation of an in-flight writer rolls back its dirty state
	// and closes the pool before the surviving process performs another write.
	a.call(t, sqliteProcessRequest{Op: "cancel"})
	if err := a.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := a.cmd.Wait(); err != nil {
		t.Fatal("cancel active mutation:", err)
	}
	owner = assertState(b, 11)
	b.call(t, sqliteProcessRequest{Op: "clear", Fence: owner})
	assertState(b, 12)
	b.call(t, sqliteProcessRequest{Op: "check"})
	b.close(t)
}
