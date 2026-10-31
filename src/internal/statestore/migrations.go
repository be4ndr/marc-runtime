package statestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version     int
	description string
	sql         string
	checksum    string
}

func loadMigrations() ([]migration, error) {
	entries, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var migrations []migration
	for _, e := range entries {
		name := e.Name()
		prefix, description, ok := strings.Cut(strings.TrimSuffix(name, ".sql"), "_")
		version, err := strconv.Atoi(prefix)
		if e.IsDir() || !strings.HasSuffix(name, ".sql") || !ok || err != nil || version < 1 || description == "" {
			return nil, fmt.Errorf("invalid migration filename %q", name)
		}
		body, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, migration{version, description, string(body), fmt.Sprintf("%x", sha256.Sum256(body))})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	if len(migrations) == 0 {
		return nil, fmt.Errorf("no embedded migrations")
	}
	for i, m := range migrations {
		if m.version != i+1 {
			return nil, fmt.Errorf("migration versions must be a contiguous sequence starting at 1")
		}
	}
	return migrations, nil
}

// All pending migrations and their history records commit together. Reserve one
// database/sql connection and explicitly acquire SQLite's writer lock before
// inspecting history, independently of the driver's default transaction mode.
func migrate(ctx context.Context, db *sql.DB, migrations []migration) error {
	tx, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer tx.Close()
	committed := false
	defer func() {
		if committed {
			return
		}
		// Cancellation must not prevent rollback or return an open transaction to
		// the pool. Discard the connection if rollback cannot complete.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := tx.ExecContext(cleanupCtx, "ROLLBACK"); err != nil {
			_ = tx.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	if _, err := tx.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin immediate migration transaction: %w", err)
	}
	applied, err := inspectHistory(ctx, tx, migrations)
	if err != nil {
		return err
	}
	// Validate existing schema before adding anything to it.
	if err := validateSchema(ctx, tx, migrations[:applied]); err != nil {
		return err
	}
	for _, m := range migrations[applied:] {
		if _, err := tx.ExecContext(ctx, m.sql); err != nil {
			return numericFailure("migrate", "", "", "", fmt.Errorf("migration %03d (%s): %w", m.version, m.description, err))
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at, description, checksum) VALUES (?, ?, ?, ?)",
			m.version, time.Now().UTC().Format(time.RFC3339Nano), m.description, m.checksum); err != nil {
			return fmt.Errorf("record migration %03d: %w", m.version, err)
		}
	}
	if err := validateSchema(ctx, tx, migrations); err != nil {
		return err
	}
	if err := validateDatabase(ctx, tx); err != nil {
		return err
	}
	if err := validateNumericDatabase(ctx, tx); err != nil {
		return numericFailure("validate_database", "", "", "", err)
	}
	if err := validateAdmissionDatabase(ctx, tx); err != nil {
		return numericFailure("validate_admission_database", "", "", "", err)
	}
	if err := reconcileTokenBankStops(ctx, tx); err != nil {
		return fmt.Errorf("reconcile token-bank stops: %w", err)
	}
	if err := validateSealedAuthorityDatabase(ctx, tx); err != nil {
		return fmt.Errorf("validate sealed authority: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	committed = true
	return nil
}

type querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func inspectHistory(ctx context.Context, q querier, migrations []migration) (int, error) {
	var exists int
	if err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='schema_migrations'").Scan(&exists); err != nil {
		return 0, err
	}
	if exists == 0 {
		return 0, nil
	}
	rows, err := q.QueryContext(ctx, "SELECT version, description, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var version int
		var description, checksum string
		if err := rows.Scan(&version, &description, &checksum); err != nil {
			return 0, err
		}
		if count >= len(migrations) || version != migrations[count].version {
			return 0, fmt.Errorf("unknown or noncontiguous applied migration version %d", version)
		}
		m := migrations[count]
		if checksum != m.checksum || description != m.description {
			return 0, fmt.Errorf("migration %03d history differs from embedded source", version)
		}
		count++
	}
	return count, rows.Err()
}

func schemaManifest(ctx context.Context, q querier) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	manifest := map[string]string{}
	for rows.Next() {
		var kind, name, table, statement string
		if err := rows.Scan(&kind, &name, &table, &statement); err != nil {
			return nil, err
		}
		manifest[kind+":"+name] = table + ":" + statement
	}
	return manifest, rows.Err()
}

// Build the expected schema from the embedded source in an isolated in-memory
// database. Compare actual definitions, not just object names or a version stamp.
func validateSchema(ctx context.Context, actual querier, migrations []migration) (err error) {
	reference, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return err
	}
	reference.SetMaxOpenConns(1)
	defer func() { err = errors.Join(err, reference.Close()) }()
	for _, m := range migrations {
		if _, err := reference.ExecContext(ctx, m.sql); err != nil {
			return fmt.Errorf("invalid embedded migration %03d: %w", m.version, err)
		}
	}
	want, err := schemaManifest(ctx, reference)
	if err != nil {
		return err
	}
	got, err := schemaManifest(ctx, actual)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(want)+len(got))
	for key := range want {
		keys = append(keys, key)
	}
	for key := range got {
		if _, exists := want[key]; !exists {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if got[key] != want[key] {
			return fmt.Errorf("schema definition %s differs from applied embedded migrations", key)
		}
	}
	return nil
}

func validateDatabase(ctx context.Context, q querier) error {
	var integrity string
	if err := q.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&integrity); err != nil {
		return err
	}
	if integrity != "ok" {
		return fmt.Errorf("SQLite quick_check: %s", integrity)
	}
	rows, err := q.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("foreign key integrity violation")
	}
	return rows.Err()
}
