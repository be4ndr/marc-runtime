// Package statestore owns the only physical write interface to authoritative state.
// SQL handles stay private; future control-plane operations belong in this package.
package statestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const DatabaseName = "control-plane.db"

// Info reports bootstrap metadata, never a connection or writable handle.
type Info struct {
	Path          string
	SQLiteVersion string
	SchemaVersion int
}

type StateStore struct {
	db          *sql.DB
	info        Info
	workingDir  string
	dumpDir     string
	dataRoot    *os.File
	metricsMu   sync.Mutex
	metricsDirs map[string]*metricsBinding
}

// OpenOptions permits disposable volatile storage only for explicit test callers.
// Command startup has no option or flag that enables this exception.
type OpenOptions struct{ AllowVolatileForTests bool }

// Open requires the initiating Orchestrator actor because startup may create or
// migrate authoritative state. It returns only after critical validation passes.
func Open(ctx context.Context, dataDir string, options ...OpenOptions) (*StateStore, error) {
	migrations, err := loadMigrations()
	if err != nil {
		return nil, err
	}
	return open(ctx, dataDir, migrations, options...)
}

func open(ctx context.Context, dataDir string, migrations []migration, options ...OpenOptions) (_ *StateStore, err error) {
	if err := requireMutationActor(ctx); err != nil {
		return nil, err
	}
	if len(options) > 1 {
		return nil, fmt.Errorf("only one OpenOptions is permitted")
	}
	allowVolatile := len(options) == 1 && options[0].AllowVolatileForTests
	if dataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	for _, component := range strings.Split(dataDir, string(filepath.Separator)) {
		if component == ".." {
			return nil, fmt.Errorf("parent traversal is not a data directory")
		}
	}
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	root, err := openLocalDirectory(dir, true, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, root.Close())
		}
	}()
	var fs syscall.Statfs_t
	if err = syscall.Fstatfs(int(root.Fd()), &fs); err != nil {
		return nil, err
	}
	if err = validateFilesystemType(int64(uint32(fs.Type)), allowVolatile); err != nil {
		return nil, err
	}
	path := filepath.Join(descriptorPath(root), DatabaseName)
	if err := secureSQLiteFiles(root, true); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", databaseDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	// A bounded writer connection also keeps transactional operations serialized.
	// DSN PRAGMAs apply whenever database/sql replaces this physical connection.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() {
		if err != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	var version string
	if err = db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return nil, err
	}
	if err = validateSQLiteVersion(version); err != nil {
		return nil, err
	}
	if err = validatePragmas(ctx, db); err != nil {
		return nil, err
	}
	if err = migrate(ctx, db, migrations); err != nil {
		return nil, fmt.Errorf("initialize authoritative database: %w", err)
	}
	if err = secureSQLiteFiles(root, false); err != nil {
		return nil, err
	}
	if err = validateDatabase(ctx, db); err != nil {
		return nil, fmt.Errorf("validate authoritative database: %w", err)
	}
	return &StateStore{db: db, dataRoot: root, info: Info{Path: filepath.Join(dir, DatabaseName), SQLiteVersion: version, SchemaVersion: migrations[len(migrations)-1].version}, workingDir: dir, dumpDir: filepath.Join(dir, "dump")}, nil
}

// secureSQLiteFiles uses only metadata operations beneath the held private root.
// Opening and closing even an existing file before this Store connects can drop
// POSIX locks held by another SQLite pool in this process. SQLite alone owns all
// data-bearing descriptors and the creation/removal of its sidecars.
func secureSQLiteFiles(root *os.File, create bool) error {
	dirfd := int(root.Fd())
	if create {
		// Unlike open(O_CREAT), mknodat creates a private empty regular file
		// without any descriptor whose close could disturb SQLite locks.
		if err := unix.Mknodat(dirfd, DatabaseName, unix.S_IFREG|0600, 0); err != nil && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("create private database: %w", err)
		}
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		name := DatabaseName + suffix
		var info unix.Stat_t
		err := unix.Fstatat(dirfd, name, &info, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(err, unix.ENOENT) && suffix != "" {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Uid != uint32(os.Geteuid()) {
			return fmt.Errorf("SQLite file must be controller-owned and regular: %s", name)
		}
		if info.Mode&07777 != 0600 {
			// Never follow a substituted leaf. Unsupported no-follow chmod fails
			// closed; do not fall back to opening a SQLite-managed file.
			err = unix.Fchmodat(dirfd, name, 0600, unix.AT_SYMLINK_NOFOLLOW)
			if errors.Is(err, unix.ENOENT) && suffix != "" {
				continue // Another SQLite process may have closed its last pool.
			}
			if err != nil {
				return fmt.Errorf("secure SQLite file %s: %w", name, err)
			}
		}
	}
	return nil
}

func (s *StateStore) Info() Info { return s.info }

// ProtectedDataDirectory reports metadata for the held installation directory,
// including its current location after a rename. It exposes no database handle.
func (s *StateStore) ProtectedDataDirectory() (string, os.FileInfo, error) {
	if s == nil || s.dataRoot == nil {
		return "", nil, fmt.Errorf("protected data directory is unavailable")
	}
	info, err := s.dataRoot.Stat()
	if err != nil {
		return "", nil, fmt.Errorf("protected data directory is unavailable")
	}
	path, err := os.Readlink(descriptorPath(s.dataRoot))
	if err != nil || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", nil, fmt.Errorf("protected data directory location is unavailable")
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, current) {
		return "", nil, fmt.Errorf("protected data directory location changed")
	}
	return path, info, nil
}

func (s *StateStore) Close() error {
	err := s.db.Close()
	if s.dataRoot != nil {
		closeErr := s.dataRoot.Close()
		if !errors.Is(closeErr, os.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}
	return err
}

func databaseDSN(path string) string {
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{}
	// busy_timeout must precede journal_mode, which may need to acquire a lock.
	for _, pragma := range []string{"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(FULL)", "foreign_keys(ON)", "trusted_schema(OFF)"} {
		q.Add("_pragma", pragma)
	}
	q.Set("_txlock", "immediate")
	u.RawQuery = q.Encode()
	return u.String()
}

func validatePragmas(ctx context.Context, db *sql.DB) error {
	var journal string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
		return err
	}
	if journal != "wal" {
		return fmt.Errorf("journal_mode = %q; require WAL", journal)
	}
	for _, p := range []struct {
		name  string
		value int
	}{{"synchronous", 2}, {"foreign_keys", 1}, {"trusted_schema", 0}, {"busy_timeout", 5000}} {
		var actual int
		if err := db.QueryRowContext(ctx, "PRAGMA "+p.name).Scan(&actual); err != nil {
			return err
		}
		if actual != p.value {
			return fmt.Errorf("PRAGMA %s = %d; require %d", p.name, actual, p.value)
		}
	}
	return nil
}

func validateSQLiteVersion(version string) error {
	parts := strings.Split(version, ".")
	var v [3]int
	if len(parts) != len(v) {
		return fmt.Errorf("unrecognized SQLite version %q", version)
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return fmt.Errorf("unrecognized SQLite version %q", version)
		}
		v[i] = n
	}
	if v[0] < 3 || (v[0] == 3 && (v[1] < 51 || (v[1] == 51 && v[2] < 3))) {
		return fmt.Errorf("SQLite %s lacks required >= 3.51.3 WAL fix", version)
	}
	return nil
}

func isWindowsMount(path string) bool {
	// WSL drive mounts, including the explicitly prohibited /mnt/c.
	parts := strings.Split(strings.TrimPrefix(filepath.Clean(path), "/"), "/")
	return len(parts) >= 2 && parts[0] == "mnt" && len(parts[1]) == 1 && parts[1][0] >= 'a' && parts[1][0] <= 'z'
}

func validateFilesystemType(kind int64, allowVolatile ...bool) error {
	// Known local Linux filesystems. Reject unrecognized/FUSE/network/9p mounts
	// rather than assuming their locking and durability satisfy SQLite WAL.
	switch kind {
	case 0xef53, 0x58465342, 0x9123683e, 0x794c7630, 0xf2f52010, 0x2fc12fc1:
		return nil // ext2/3/4, XFS, Btrfs, overlayfs, F2FS, ZFS
	case 0x01021994, 0x858458f6:
		if len(allowVolatile) == 1 && allowVolatile[0] {
			return nil
		}
		return fmt.Errorf("volatile authoritative storage requires explicit test opt-in")
	default:
		return fmt.Errorf("authoritative database requires a known local Linux filesystem (type %#x)", kind)
	}
}
