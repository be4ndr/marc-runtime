package statestore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
	"modernc.org/sqlite"
)

// F_GETLK observes locks from an independent process without changing them.
// Never open/close SQLite files for this probe in the SQLite-owning process.
func TestSQLiteLockProbeProcess(t *testing.T) {
	path := os.Getenv("ACP_SQLITE_LOCK_PROBE")
	if path == "" {
		return
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: unix.SEEK_SET}
	err = unix.FcntlFlock(uintptr(fd), unix.F_GETLK, &lock)
	closeErr := unix.Close(fd)
	if err != nil || closeErr != nil {
		fmt.Fprintln(os.Stderr, err, closeErr)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(lock); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func requireSQLiteLocks(t *testing.T, path string) {
	t.Helper()
	for _, suffix := range []string{"", "-shm"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSQLiteLockProbeProcess$")
		cmd.Env = append(os.Environ(), "ACP_SQLITE_LOCK_PROBE="+path+suffix)
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("lock probe: %v: %s", err, raw)
		}
		var lock unix.Flock_t
		if err := json.Unmarshal(raw, &lock); err != nil {
			t.Fatal(err)
		}
		if lock.Type == unix.F_UNLCK || lock.Pid != int32(os.Getpid()) {
			t.Fatalf("SQLite lost POSIX locks on %s: %+v", path+suffix, lock)
		}
	}
}

func TestSQLiteSecurityPreservesPOSIXLocks(t *testing.T) {
	dir := privateTempDir(t)
	s, err := openTest(orchestratorContext(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	path := filepath.Join(dir, DatabaseName)
	requireSQLiteLocks(t, path)
	// Startup of another Store in this process must not drop the first Store's
	// locks, including when its pool closes while the first remains active.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Chmod(path+suffix, 0644); err != nil {
			t.Fatal(err)
		}
	}
	other, err := openTest(orchestratorContext(), dir)
	if err != nil {
		t.Fatal(err)
	}
	requireSQLiteLocks(t, path)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, err := os.Lstat(path + suffix); err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("live SQLite file permissions were not repaired", info, err)
		}
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	requireSQLiteLocks(t, path)
}

func TestSQLiteFileSecurityRejectsUnsafeLeaves(t *testing.T) {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		for _, kind := range []string{"symlink", "directory", "fifo"} {
			t.Run(suffix+"/"+kind, func(t *testing.T) {
				dir := privateTempDir(t)
				name := filepath.Join(dir, DatabaseName+suffix)
				target := filepath.Join(privateTempDir(t), "target")
				if err := os.WriteFile(target, nil, 0644); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(target, 0644); err != nil {
					t.Fatal(err)
				}
				var err error
				switch kind {
				case "symlink":
					err = os.Symlink(target, name)
				case "directory":
					err = os.Mkdir(name, 0700)
				case "fifo":
					err = unix.Mkfifo(name, 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
				if s, err := openTest(orchestratorContext(), dir); err == nil {
					s.Close()
					t.Fatal("unsafe SQLite leaf accepted")
				}
				if info, err := os.Stat(target); err != nil || info.Mode().Perm() != 0644 {
					t.Fatal("security check changed symlink target", info, err)
				}
			})
		}
	}
}

var errSQLiteTestClose = errors.New("injected SQLite close failure")

type sqliteCloseErrorConnector struct{}

func (sqliteCloseErrorConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c sqliteCloseErrorConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(":memory:")
	if err != nil {
		return nil, err
	}
	return sqliteCloseErrorConn{conn}, nil
}

type sqliteCloseErrorConn struct{ driver.Conn }

func (c sqliteCloseErrorConn) Close() error {
	return errors.Join(c.Conn.Close(), errSQLiteTestClose)
}

func TestSQLiteCloseFailureStillClosesDataRoot(t *testing.T) {
	db := sql.OpenDB(sqliteCloseErrorConnector{})
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	root, err := openLocalDirectory(privateTempDir(t), false, true)
	if err != nil {
		t.Fatal(err)
	}
	s := &StateStore{db: db, dataRoot: root}
	if err := s.Close(); !errors.Is(err, errSQLiteTestClose) {
		t.Fatalf("SQLite close failure lost: %v", err)
	}
	if db.Stats().OpenConnections != 0 {
		t.Fatal("SQLite pool still has open connections")
	}
	if _, err := root.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("data root not closed after SQLite close failure", err)
	}
}
